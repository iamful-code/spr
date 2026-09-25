//! Online symbol filter: decides which instruments are worth quoting right now and
//! ranks them. The Python `pairs` scorer computes the same score offline over history
//! and can pin/blacklist symbols through config/symbols.json.

use crate::config::{EligibilityCfg, SymbolLists};
use crate::stats::SymbolStats;
use crate::types::SymbolMeta;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Verdict {
    pub eligible: bool,
    pub reason: &'static str,
    pub score: f64,
}

/// Expected capture rate proxy: wider spread and more trades are good, volatility is bad.
pub fn score(spread_med_bps: f64, trades_per_min: f64, vol_bps: f64) -> f64 {
    if spread_med_bps <= 0.0 || trades_per_min <= 0.0 {
        return 0.0;
    }
    spread_med_bps * trades_per_min.sqrt() / (1.0 + vol_bps)
}

/// What the core has measured about its own fills on this symbol so far.
#[derive(Clone, Copy, Debug, Default)]
pub struct Experience {
    /// EWMA of the markout after our fills (bps, negative = adverse selection).
    pub markout_bps: f64,
    pub markout_n: u32,
}

pub fn evaluate(cfg: &EligibilityCfg, meta: &SymbolMeta, stats: &SymbolStats, lists: &SymbolLists, min_spread_bps: f64, now_ms: i64, exp: Experience) -> Verdict {
    let no = |reason: &'static str| Verdict { eligible: false, reason, score: 0.0 };
    if cfg.deny.iter().any(|s| s == &meta.name) || lists.deny.iter().any(|s| s == &meta.name) {
        return no("denied");
    }
    let allow_cfg = !cfg.allow.is_empty();
    let allow_lists = !lists.allow.is_empty();
    if allow_cfg && !cfg.allow.iter().any(|s| s == &meta.name) {
        return no("not_in_allow");
    }
    if allow_lists && !lists.allow.iter().any(|s| s == &meta.name) {
        return no("not_in_allow");
    }
    if stats.last_bbo_ts == 0 || now_ms - stats.last_bbo_ts > 5000 {
        return no("stale_book");
    }
    if cfg.min_turnover_24h_usd > 0.0 && meta.turnover_24h > 0.0 && meta.turnover_24h < cfg.min_turnover_24h_usd {
        return no("low_turnover");
    }
    if stats.samples_in_window() < (cfg.window_secs as usize / 4).max(10) {
        return no("warming_up");
    }
    let need_spread = cfg.min_spread_med_bps.max(min_spread_bps);
    if stats.spread_med_bps < need_spread {
        return no("spread_low");
    }
    if stats.trades_per_min < cfg.min_trades_per_min {
        return no("few_trades");
    }
    if stats.vol_bps > 0.0 && cfg.min_spread_vol_ratio > 0.0 && stats.spread_med_bps / stats.vol_bps < cfg.min_spread_vol_ratio {
        return no("vol_high");
    }
    let experienced = cfg.min_markout_samples > 0 && exp.markout_n >= cfg.min_markout_samples;
    if experienced && cfg.max_adverse_markout_bps > 0.0 && exp.markout_bps < -cfg.max_adverse_markout_bps {
        return no("toxic_flow");
    }
    let online = score(stats.spread_med_bps, stats.trades_per_min, stats.vol_bps);
    let mut s = match lists.scores.get(&meta.name) {
        Some(off) if *off > 0.0 => 0.5 * (online + *off),
        _ => online,
    };
    if experienced {
        // realized experience beats the quoted spread: a symbol that pays us after fills
        // ranks higher, one that runs over us ranks lower
        let half = (stats.spread_med_bps * 0.5).max(1.0);
        s *= 1.0 + (exp.markout_bps / half).clamp(-0.5, 0.5);
    }
    Verdict { eligible: true, reason: "ok", score: s }
}
