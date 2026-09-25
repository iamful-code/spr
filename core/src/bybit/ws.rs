//! Bybit V5 public WebSocket feed. Symbols are spread over several connections
//! (`topics_per_connection`), subscriptions are sent in batches (`args_per_subscribe`),
//! a ping goes out every 20 s and a dead connection reconnects with backoff. JSON is
//! parsed here, on the tokio workers, and only compact `MarketEvent`s reach the engine.

use crate::config::ExchangeCfg;
use crate::types::{MarketEvent, Side, SymbolId, SymbolMeta, Trade};
use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow, default)]
    topic: Option<&'a str>,
    #[serde(rename = "type", borrow, default)]
    typ: Option<&'a str>,
    #[serde(default)]
    ts: Option<i64>,
    #[serde(borrow, default)]
    data: Option<&'a RawValue>,
    #[serde(borrow, default)]
    op: Option<&'a str>,
    #[serde(default)]
    success: Option<bool>,
    #[serde(borrow, default)]
    ret_msg: Option<&'a str>,
}

#[derive(Deserialize)]
struct BookData<'a> {
    #[serde(borrow)]
    s: &'a str,
    #[serde(borrow, default)]
    b: Vec<[&'a str; 2]>,
    #[serde(borrow, default)]
    a: Vec<[&'a str; 2]>,
    #[serde(default)]
    u: Option<u64>,
}

#[derive(Deserialize)]
struct TradeData<'a> {
    #[serde(rename = "T")]
    ts: i64,
    #[serde(borrow)]
    s: &'a str,
    #[serde(rename = "S", borrow)]
    side: &'a str,
    #[serde(borrow)]
    v: &'a str,
    #[serde(borrow)]
    p: &'a str,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum FeedMode {
    /// Our trading venue: full book updates and trades.
    Primary,
    /// A leading venue: only the top of book, delivered as `MarketEvent::Reference`.
    Reference { rank: u8 },
}

struct ConnSpec {
    idx: usize,
    label: String,
    url: String,
    depth: u32,
    args_per_subscribe: usize,
    symbols: Vec<(String, SymbolId)>,
    mode: FeedMode,
}

/// Spawn one task per connection. Returns the join handles (they run until aborted).
pub fn spawn_connections(cfg: &ExchangeCfg, symbols: &[SymbolMeta], tx: mpsc::Sender<MarketEvent>) -> Vec<tokio::task::JoinHandle<()>> {
    let per_conn = (cfg.topics_per_connection / 2).max(1);
    let mut handles = Vec::new();
    for (idx, chunk) in symbols.chunks(per_conn).enumerate() {
        let spec = ConnSpec {
            idx,
            label: "ws".into(),
            url: cfg.ws_url.clone(),
            depth: cfg.orderbook_depth.max(1),
            args_per_subscribe: cfg.args_per_subscribe.max(1),
            symbols: chunk.iter().map(|m| (m.name.clone(), m.id)).collect(),
            mode: FeedMode::Primary,
        };
        let tx = tx.clone();
        handles.push(tokio::spawn(connection_loop(spec, tx)));
    }
    handles
}

/// Bybit spot (or any other Bybit public stream) as a reference venue. Subscriptions go
/// one topic per request so a symbol that does not exist there does not sink the batch.
pub fn spawn_reference_connections(url: &str, label: &str, symbols: &[SymbolMeta], rank: u8, per_conn: usize, tx: mpsc::Sender<MarketEvent>) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();
    for (idx, chunk) in symbols.chunks(per_conn.max(1)).enumerate() {
        let spec = ConnSpec {
            idx,
            label: label.to_string(),
            url: url.to_string(),
            depth: 1,
            args_per_subscribe: 1,
            symbols: chunk.iter().map(|m| (m.name.clone(), m.id)).collect(),
            mode: FeedMode::Reference { rank },
        };
        let tx = tx.clone();
        handles.push(tokio::spawn(connection_loop(spec, tx)));
    }
    handles
}

/// Depth-1 book kept per reference symbol: Bybit sends the top level as snapshot/delta
/// like any other depth, so deletions must be applied before reading the best price.
#[derive(Default)]
struct MiniBook {
    bids: HashMap<String, f64>,
    asks: HashMap<String, f64>,
}

impl MiniBook {
    fn apply(&mut self, snapshot: bool, bids: &[[&str; 2]], asks: &[[&str; 2]]) {
        if snapshot {
            self.bids.clear();
            self.asks.clear();
        }
        for (side, levels) in [(&mut self.bids, bids), (&mut self.asks, asks)] {
            for [p, q] in levels {
                let qty: f64 = q.parse().unwrap_or(0.0);
                if qty <= 0.0 {
                    side.remove(*p);
                } else {
                    side.insert((*p).to_string(), qty);
                }
            }
        }
    }
    fn best(&self) -> Option<(f64, f64)> {
        let bid = self.bids.keys().filter_map(|p| p.parse::<f64>().ok()).fold(f64::NAN, f64::max);
        let ask = self.asks.keys().filter_map(|p| p.parse::<f64>().ok()).fold(f64::NAN, f64::min);
        if bid.is_finite() && ask.is_finite() && bid > 0.0 && ask >= bid {
            Some((bid, ask))
        } else {
            None
        }
    }
}

async fn connection_loop(spec: ConnSpec, tx: mpsc::Sender<MarketEvent>) {
    let spec = Arc::new(spec);
    let mut backoff = 1u64;
    loop {
        let started = std::time::Instant::now();
        match run_connection(&spec, &tx).await {
            Ok(()) => {}
            Err(e) => {
                let _ = tx.send(MarketEvent::Status { conn: spec.idx, msg: format!("{}: disconnected: {e:#}; reconnect in {backoff}s", spec.label) }).await;
            }
        }
        if tx.is_closed() {
            return;
        }
        // a connection that lived for a while resets the backoff
        if started.elapsed() > Duration::from_secs(60) {
            backoff = 1;
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}

async fn run_connection(spec: &ConnSpec, tx: &mpsc::Sender<MarketEvent>) -> Result<()> {
    let (ws, _) = tokio_tungstenite::connect_async(&spec.url).await.with_context(|| format!("connecting {}", spec.url))?;
    let (mut sink, mut stream) = ws.split();
    let by_name: HashMap<&str, SymbolId> = spec.symbols.iter().map(|(n, id)| (n.as_str(), *id)).collect();

    let mut topics: Vec<String> = Vec::with_capacity(spec.symbols.len() * 2);
    for (name, _) in &spec.symbols {
        topics.push(format!("orderbook.{}.{}", spec.depth, name));
        if spec.mode == FeedMode::Primary {
            topics.push(format!("publicTrade.{}", name));
        }
    }
    for (i, chunk) in topics.chunks(spec.args_per_subscribe).enumerate() {
        let msg = serde_json::json!({ "op": "subscribe", "req_id": format!("c{}-{}", spec.idx, i), "args": chunk });
        sink.send(Message::Text(msg.to_string())).await.context("sending subscribe")?;
    }
    tx.send(MarketEvent::Status { conn: spec.idx, msg: format!("{}: connected, {} symbols, {} topics", spec.label, spec.symbols.len(), topics.len()) }).await.ok();
    let mut books: HashMap<SymbolId, MiniBook> = HashMap::new();

    let mut ping = tokio::time::interval(Duration::from_secs(20));
    ping.tick().await; // first tick fires immediately; skip it
    let mut last_msg = std::time::Instant::now();
    let mut watchdog = tokio::time::interval(Duration::from_secs(5));
    let mut n_sub_ok = 0usize;
    loop {
        tokio::select! {
            _ = ping.tick() => {
                sink.send(Message::Text(r#"{"op":"ping"}"#.to_string())).await.context("sending ping")?;
            }
            _ = watchdog.tick() => {
                if last_msg.elapsed() > Duration::from_secs(40) {
                    bail!("no messages for 40s");
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
                        if let Err(e) = handle_text(&txt, &by_name, tx, &mut n_sub_ok, spec.idx, spec.mode, &mut books).await {
                            tracing::debug!("ws[{}] bad message: {e:#}: {}", spec.idx, txt.chars().take(200).collect::<String>());
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

async fn handle_text(txt: &str, by_name: &HashMap<&str, SymbolId>, tx: &mpsc::Sender<MarketEvent>, n_sub_ok: &mut usize, conn: usize, mode: FeedMode, books: &mut HashMap<SymbolId, MiniBook>) -> Result<()> {
    let env: Envelope = serde_json::from_str(txt).context("envelope")?;
    if let Some(op) = env.op {
        match op {
            "subscribe" => {
                if env.success == Some(false) {
                    // reference feeds subscribe one symbol at a time: a missing spot pair is expected
                    if mode == FeedMode::Primary {
                        tx.send(MarketEvent::Status { conn, msg: format!("subscribe failed: {}", env.ret_msg.unwrap_or("")) }).await.ok();
                    } else {
                        tracing::debug!("reference subscribe failed: {}", env.ret_msg.unwrap_or(""));
                    }
                } else {
                    *n_sub_ok += 1;
                }
            }
            "ping" | "pong" => {}
            other => tracing::debug!("ws op {other}: {}", env.ret_msg.unwrap_or("")),
        }
        return Ok(());
    }
    let (Some(topic), Some(data)) = (env.topic, env.data) else { return Ok(()) };
    if topic.starts_with("orderbook.") {
        let d: BookData = serde_json::from_str(data.get()).context("orderbook data")?;
        let Some(&sym) = by_name.get(d.s) else { return Ok(()) };
        let snapshot = env.typ == Some("snapshot") || d.u == Some(1);
        if let FeedMode::Reference { rank } = mode {
            let book = books.entry(sym).or_default();
            book.apply(snapshot, &d.b, &d.a);
            if let Some((bid, ask)) = book.best() {
                tx.send(MarketEvent::Reference { sym, ts: env.ts.unwrap_or(0), bid, ask, rank }).await.ok();
            }
            return Ok(());
        }
        let parse = |v: &[[&str; 2]]| -> Vec<(f64, f64)> { v.iter().filter_map(|[p, q]| Some((p.parse::<f64>().ok()?, q.parse::<f64>().ok()?))).collect() };
        let ev = MarketEvent::Book { sym, ts: env.ts.unwrap_or(0), snapshot, bids: parse(&d.b), asks: parse(&d.a) };
        tx.send(ev).await.ok();
    } else if topic.starts_with("publicTrade.") {
        let list: Vec<TradeData> = serde_json::from_str(data.get()).context("trade data")?;
        if list.is_empty() {
            return Ok(());
        }
        let Some(&sym) = by_name.get(list[0].s) else { return Ok(()) };
        let trades: Vec<Trade> = list
            .iter()
            .filter_map(|t| {
                Some(Trade { ts: t.ts, price: t.p.parse().ok()?, qty: t.v.parse().ok()?, taker_side: if t.side == "Buy" { Side::Buy } else { Side::Sell } })
            })
            .collect();
        tx.send(MarketEvent::Trades { sym, trades }).await.ok();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parses_book_and_trade_messages() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut by_name = HashMap::new();
        by_name.insert("BTCUSDT", 3u32);
        let mut n = 0;
        let mut books = HashMap::new();
        let book = r#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000000,"data":{"s":"BTCUSDT","b":[["84228.9","1.25"]],"a":[["84229.0","0.5"]],"u":1,"seq":123},"cts":1699999999999}"#;
        handle_text(book, &by_name, &tx, &mut n, 0, FeedMode::Primary, &mut books).await.unwrap();
        match rx.recv().await.unwrap() {
            MarketEvent::Book { sym, snapshot, bids, asks, .. } => {
                assert_eq!(sym, 3);
                assert!(snapshot);
                assert_eq!(bids, vec![(84228.9, 1.25)]);
                assert_eq!(asks, vec![(84229.0, 0.5)]);
            }
            _ => panic!("expected book"),
        }
        let trade = r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1700000000001,"data":[{"T":1700000000000,"s":"BTCUSDT","S":"Sell","v":"0.002","p":"84228.9","L":"MinusTick","i":"abc","BT":false}]}"#;
        handle_text(trade, &by_name, &tx, &mut n, 0, FeedMode::Primary, &mut books).await.unwrap();
        match rx.recv().await.unwrap() {
            MarketEvent::Trades { sym, trades } => {
                assert_eq!(sym, 3);
                assert_eq!(trades.len(), 1);
                assert_eq!(trades[0].taker_side, Side::Sell);
                assert_eq!(trades[0].qty, 0.002);
            }
            _ => panic!("expected trades"),
        }
        let sub = r#"{"success":true,"ret_msg":"subscribe","conn_id":"x","op":"subscribe"}"#;
        handle_text(sub, &by_name, &tx, &mut n, 0, FeedMode::Primary, &mut books).await.unwrap();
        assert_eq!(n, 1);
        let pong = r#"{"success":true,"ret_msg":"pong","conn_id":"x","op":"ping"}"#;
        handle_text(pong, &by_name, &tx, &mut n, 0, FeedMode::Primary, &mut books).await.unwrap();
        // unknown symbol is ignored
        let other = r#"{"topic":"orderbook.1.ETHUSDT","type":"delta","ts":1,"data":{"s":"ETHUSDT","b":[],"a":[["1","1"]],"u":5,"seq":1}}"#;
        handle_text(other, &by_name, &tx, &mut n, 0, FeedMode::Primary, &mut books).await.unwrap();
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn reference_mode_emits_top_of_book() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut by_name = HashMap::new();
        by_name.insert("BTCUSDT", 1u32);
        let mut n = 0;
        let mut books = HashMap::new();
        let mode = FeedMode::Reference { rank: 2 };
        let snap = r#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":10,"data":{"s":"BTCUSDT","b":[["100.0","1"]],"a":[["100.2","1"]],"u":7,"seq":1}}"#;
        handle_text(snap, &by_name, &tx, &mut n, 0, mode, &mut books).await.unwrap();
        match rx.recv().await.unwrap() {
            MarketEvent::Reference { sym, bid, ask, rank, .. } => {
                assert_eq!((sym, bid, ask, rank), (1, 100.0, 100.2, 2));
            }
            _ => panic!("expected reference"),
        }
        // delta: the old ask is deleted and a better one appears
        let delta = r#"{"topic":"orderbook.1.BTCUSDT","type":"delta","ts":11,"data":{"s":"BTCUSDT","b":[],"a":[["100.2","0"],["100.1","3"]],"u":8,"seq":2}}"#;
        handle_text(delta, &by_name, &tx, &mut n, 0, mode, &mut books).await.unwrap();
        match rx.recv().await.unwrap() {
            MarketEvent::Reference { ask, .. } => assert_eq!(ask, 100.1),
            _ => panic!("expected reference"),
        }
    }
}
