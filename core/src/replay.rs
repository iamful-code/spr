//! Run the engine over recorded market data. Used by the optimizer (`spr replay
//! --params-json ...`) and for manual what-if checks. Streams the files, so memory is
//! flat regardless of the period length.

use crate::config::{Config, Overrides, SymbolLists};
use crate::engine::{Engine, Summary};
use crate::recorder::{list_segments, read_segment, MdEvent};
use crate::store::StoreHandle;
use crate::types::{Bbo, MarketEvent, Side, SymbolId, SymbolMeta, Trade};
use anyhow::{bail, Result};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct ReplayRequest {
    pub md_dir: PathBuf,
    pub from_ms: i64,
    pub to_ms: i64,
    /// Empty = every recorded symbol.
    pub symbols: Vec<String>,
    /// Parameter overrides applied to every replayed symbol, on top of the config files.
    pub params: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ReplayStats {
    pub segments: usize,
    pub symbols: usize,
    pub n_bbo: u64,
    pub n_trades: u64,
    pub first_ts: i64,
    pub last_ts: i64,
    pub elapsed_ms: u128,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReplayReport {
    pub summary: Summary,
    pub stats: ReplayStats,
}

pub fn run_replay(mut cfg: Config, mut overrides: Overrides, lists: SymbolLists, req: &ReplayRequest, store: StoreHandle, run_id: i64) -> Result<ReplayReport> {
    let started = std::time::Instant::now();
    let segments = list_segments(&req.md_dir, req.from_ms, req.to_ms)?;
    if segments.is_empty() {
        bail!("no recorded data in {} for the requested period", req.md_dir.display());
    }
    // union of instruments across segments, first definition wins
    let want: Option<HashSet<&str>> = if req.symbols.is_empty() { None } else { Some(req.symbols.iter().map(|s| s.as_str()).collect()) };
    let mut metas: Vec<SymbolMeta> = Vec::new();
    let mut seen: HashMap<String, usize> = HashMap::new();
    for seg in &segments {
        for m in &seg.symbols {
            if want.as_ref().is_none_or(|w| w.contains(m.name.as_str())) && !seen.contains_key(&m.name) {
                seen.insert(m.name.clone(), metas.len());
                metas.push(m.clone());
            }
        }
    }
    if metas.is_empty() {
        bail!("none of the requested symbols appear in the recorded data");
    }
    if let Some(p) = &req.params {
        for m in &metas {
            let entry = overrides.per_symbol.entry(m.name.clone()).or_default();
            for (k, v) in p {
                entry.insert(k.clone(), v.clone());
            }
        }
    }
    // replay never re-records market data
    cfg.recording.enabled = false;
    let mut engine = Engine::new(cfg, overrides, lists, metas, store, None, run_id, "replay", req.from_ms);

    let mut stats = ReplayStats { segments: segments.len(), symbols: engine.symbols.len(), ..Default::default() };
    let mut last_ts = req.from_ms;
    for seg in &segments {
        // local id (in this segment) -> engine id
        let map: Vec<Option<SymbolId>> = seg.symbols.iter().map(|m| engine.symbol_id(&m.name)).collect();
        let local_filter: HashSet<SymbolId> = map.iter().enumerate().filter_map(|(i, m)| m.map(|_| i as SymbolId)).collect();
        let filter = if local_filter.len() == seg.symbols.len() { None } else { Some(&local_filter) };
        let (nb, nt) = read_segment(seg, req.from_ms, req.to_ms, filter, |ev: MdEvent| {
            let Some(Some(sym)) = map.get(ev.sym() as usize).copied() else { return };
            let ts = ev.ts();
            if stats.first_ts == 0 {
                stats.first_ts = ts;
            }
            last_ts = ts;
            let mev = match ev {
                MdEvent::Bbo(b) => MarketEvent::Bbo { sym, bbo: Bbo { ts, bid: b.bid, ask: b.ask, bid_qty: b.bid_qty, ask_qty: b.ask_qty } },
                MdEvent::Trade(t) => MarketEvent::Trades { sym, trades: vec![Trade { ts, price: t.price, qty: t.qty, taker_side: Side::from_u8(t.side) }] },
            };
            engine.on_market(mev, ts);
        })?;
        stats.n_bbo += nb;
        stats.n_trades += nt;
    }
    engine.finish(last_ts);
    stats.last_ts = last_ts;
    stats.elapsed_ms = started.elapsed().as_millis();
    Ok(ReplayReport { summary: engine.summary(), stats })
}

/// Parse `--from/--to` values: unix milliseconds, unix seconds, `YYYY-MM-DD`,
/// `YYYY-MM-DDTHH:MM[:SS]` (UTC) or full RFC 3339.
pub fn parse_time(s: &str) -> Result<i64> {
    let s = s.trim();
    if let Ok(n) = s.parse::<i64>() {
        return Ok(if n < 100_000_000_000 { n * 1000 } else { n });
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis());
    }
    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"] {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(dt.and_utc().timestamp_millis());
        }
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp_millis());
    }
    bail!("cannot parse time '{s}': use ms, seconds, YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS or RFC3339")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_times() {
        assert_eq!(parse_time("1700000000").unwrap(), 1_700_000_000_000);
        assert_eq!(parse_time("1700000000000").unwrap(), 1_700_000_000_000);
        assert_eq!(parse_time("2023-11-14T22:13:20").unwrap(), 1_700_000_000_000);
        assert_eq!(parse_time("2023-11-14").unwrap(), 1_699_920_000_000);
        assert!(parse_time("yesterday").is_err());
    }
}
