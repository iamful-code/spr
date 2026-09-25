//! Binance book-ticker streams as a reference venue (USDT-M futures or spot). Only the
//! best bid/ask is consumed and delivered as `MarketEvent::Reference`. Symbols that are
//! not listed there simply never send anything.

use crate::types::{MarketEvent, SymbolId, SymbolMeta};
use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

pub const FUTURES_URL: &str = "wss://fstream.binance.com";
pub const SPOT_URL: &str = "wss://stream.binance.com:9443";

#[derive(Deserialize)]
struct Combined<'a> {
    #[serde(borrow, default)]
    data: Option<BookTicker<'a>>,
}

#[derive(Deserialize)]
struct BookTicker<'a> {
    #[serde(borrow)]
    s: &'a str,
    #[serde(borrow)]
    b: &'a str,
    #[serde(borrow)]
    a: &'a str,
    /// transaction time (futures only)
    #[serde(rename = "T", default)]
    t: Option<i64>,
    /// event time (futures only)
    #[serde(rename = "E", default)]
    e: Option<i64>,
}

struct Spec {
    idx: usize,
    label: String,
    base_url: String,
    symbols: Vec<(String, SymbolId)>,
    rank: u8,
}

/// One task per `per_conn` symbols. Binance allows up to 1024 streams per connection.
pub fn spawn(base_url: &str, label: &str, symbols: &[SymbolMeta], rank: u8, per_conn: usize, tx: mpsc::Sender<MarketEvent>) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();
    for (idx, chunk) in symbols.chunks(per_conn.clamp(1, 1000)).enumerate() {
        let spec = Spec { idx, label: label.to_string(), base_url: base_url.to_string(), symbols: chunk.iter().map(|m| (m.name.clone(), m.id)).collect(), rank };
        let tx = tx.clone();
        handles.push(tokio::spawn(connection_loop(spec, tx)));
    }
    handles
}

async fn connection_loop(spec: Spec, tx: mpsc::Sender<MarketEvent>) {
    let mut backoff = 1u64;
    loop {
        let started = std::time::Instant::now();
        if let Err(e) = run_connection(&spec, &tx).await {
            let _ = tx.send(MarketEvent::Status { conn: 1000 + spec.idx, msg: format!("{}: disconnected: {e:#}; reconnect in {backoff}s", spec.label) }).await;
        }
        if tx.is_closed() {
            return;
        }
        if started.elapsed() > Duration::from_secs(60) {
            backoff = 1;
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(60);
    }
}

async fn run_connection(spec: &Spec, tx: &mpsc::Sender<MarketEvent>) -> Result<()> {
    let streams: Vec<String> = spec.symbols.iter().map(|(n, _)| format!("{}@bookTicker", n.to_lowercase())).collect();
    let url = format!("{}/stream?streams={}", spec.base_url.trim_end_matches('/'), streams.join("/"));
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.with_context(|| format!("connecting {}", spec.base_url))?;
    let (mut sink, mut stream) = ws.split();
    let by_name: HashMap<&str, SymbolId> = spec.symbols.iter().map(|(n, id)| (n.as_str(), *id)).collect();
    tx.send(MarketEvent::Status { conn: 1000 + spec.idx, msg: format!("{}: connected, {} symbols", spec.label, spec.symbols.len()) }).await.ok();

    let mut last_msg = std::time::Instant::now();
    let mut watchdog = tokio::time::interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            _ = watchdog.tick() => {
                // Binance pings every 3 minutes; silence beyond that means the socket is dead
                if last_msg.elapsed() > Duration::from_secs(300) {
                    bail!("no frames for 5 minutes");
                }
            }
            msg = stream.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => bail!("read error: {e}"),
                    None => bail!("stream closed"),
                };
                last_msg = std::time::Instant::now();
                match msg {
                    Message::Text(txt) => {
                        if let Some(ev) = parse_book_ticker(&txt, &by_name, spec.rank) {
                            tx.send(ev).await.ok();
                        }
                    }
                    Message::Ping(p) => { sink.send(Message::Pong(p)).await.ok(); }
                    Message::Close(f) => bail!("server closed: {f:?}"),
                    _ => {}
                }
            }
        }
    }
}

fn parse_book_ticker(txt: &str, by_name: &HashMap<&str, SymbolId>, rank: u8) -> Option<MarketEvent> {
    let c: Combined = serde_json::from_str(txt).ok()?;
    let d = c.data?;
    let sym = *by_name.get(d.s)?;
    let bid: f64 = d.b.parse().ok()?;
    let ask: f64 = d.a.parse().ok()?;
    if !(bid > 0.0 && ask >= bid) {
        return None;
    }
    let ts = d.t.or(d.e).unwrap_or_else(crate::types::now_ms);
    Some(MarketEvent::Reference { sym, ts, bid, ask, rank })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_futures_and_spot_payloads() {
        let mut by_name = HashMap::new();
        by_name.insert("PEAQUSDT", 4u32);
        let fut = r#"{"stream":"peaqusdt@bookTicker","data":{"e":"bookTicker","u":400900217,"E":1568014460893,"T":1568014460891,"s":"PEAQUSDT","b":"0.0123","B":"31.2","a":"0.0124","A":"40.6"}}"#;
        match parse_book_ticker(fut, &by_name, 0).unwrap() {
            MarketEvent::Reference { sym, ts, bid, ask, rank } => assert_eq!((sym, ts, bid, ask, rank), (4, 1568014460891, 0.0123, 0.0124, 0)),
            _ => panic!(),
        }
        let spot = r#"{"stream":"peaqusdt@bookTicker","data":{"u":1,"s":"PEAQUSDT","b":"0.0120","B":"1","a":"0.0121","A":"1"}}"#;
        assert!(matches!(parse_book_ticker(spot, &by_name, 1), Some(MarketEvent::Reference { rank: 1, .. })));
        let unknown = r#"{"stream":"xusdt@bookTicker","data":{"u":1,"s":"XUSDT","b":"1","B":"1","a":"2","A":"1"}}"#;
        assert!(parse_book_ticker(unknown, &by_name, 0).is_none());
    }
}
