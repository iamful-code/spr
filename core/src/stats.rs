//! Rolling per-symbol market statistics: spread median, short-term volatility,
//! trade rate and taker flow imbalance. All O(1) per event with per-second buckets.

use crate::types::{Bbo, Side, Trade};

#[derive(Clone, Copy, Debug, Default)]
struct SecBucket {
    sec: i64,
    trades: u32,
    buy_notional: f64,
    sell_notional: f64,
}

#[derive(Debug)]
pub struct SymbolStats {
    window: usize,
    // one sample per second of spread (bps) and mid
    spread_samples: Vec<f64>,
    mid_samples: Vec<f64>,
    sample_secs: Vec<i64>,
    head: usize,
    filled: usize,
    last_sample_sec: i64,
    buckets: Vec<SecBucket>,
    pub last_bbo: Option<Bbo>,
    pub last_bbo_ts: i64,
    pub last_trade_ts: i64,
    // cached derived values (refreshed by `refresh`)
    pub spread_med_bps: f64,
    pub spread_mean_bps: f64,
    pub vol_bps: f64,
    pub trades_per_min: f64,
    pub n_samples: usize,
}

impl SymbolStats {
    pub fn new(window_secs: u32) -> Self {
        let w = window_secs.max(10) as usize;
        Self {
            window: w,
            spread_samples: vec![0.0; w],
            mid_samples: vec![0.0; w],
            sample_secs: vec![-1; w],
            head: 0,
            filled: 0,
            last_sample_sec: -1,
            buckets: vec![SecBucket::default(); w.max(300)],
            last_bbo: None,
            last_bbo_ts: 0,
            last_trade_ts: 0,
            spread_med_bps: 0.0,
            spread_mean_bps: 0.0,
            vol_bps: 0.0,
            trades_per_min: 0.0,
            n_samples: 0,
        }
    }

    pub fn on_bbo(&mut self, bbo: &Bbo, now_ms: i64) {
        self.last_bbo = Some(*bbo);
        self.last_bbo_ts = now_ms;
        let sec = now_ms / 1000;
        if sec != self.last_sample_sec {
            // record one sample per second (first quote seen in that second)
            self.spread_samples[self.head] = bbo.spread_bps();
            self.mid_samples[self.head] = bbo.mid();
            self.sample_secs[self.head] = sec;
            self.head = (self.head + 1) % self.window;
            self.filled = (self.filled + 1).min(self.window);
            self.last_sample_sec = sec;
        }
    }

    pub fn on_trade(&mut self, t: &Trade, now_ms: i64) {
        self.last_trade_ts = now_ms;
        let sec = t.ts / 1000;
        let n = self.buckets.len() as i64;
        let idx = (sec.rem_euclid(n)) as usize;
        let b = &mut self.buckets[idx];
        if b.sec != sec {
            *b = SecBucket { sec, ..Default::default() };
        }
        b.trades += 1;
        let notional = t.price * t.qty;
        match t.taker_side {
            Side::Buy => b.buy_notional += notional,
            Side::Sell => b.sell_notional += notional,
        }
    }

    /// Taker flow imbalance over the last `secs` seconds in [-1, 1]; positive = net buying.
    pub fn flow_imbalance(&self, now_ms: i64, secs: u32) -> f64 {
        let now_sec = now_ms / 1000;
        let n = self.buckets.len() as i64;
        let mut buy = 0.0;
        let mut sell = 0.0;
        for s in (now_sec - secs as i64 + 1)..=now_sec {
            let b = &self.buckets[(s.rem_euclid(n)) as usize];
            if b.sec == s {
                buy += b.buy_notional;
                sell += b.sell_notional;
            }
        }
        let tot = buy + sell;
        if tot > 0.0 {
            (buy - sell) / tot
        } else {
            0.0
        }
    }

    fn trades_in_last(&self, now_ms: i64, secs: i64) -> u32 {
        let now_sec = now_ms / 1000;
        let n = self.buckets.len() as i64;
        let mut c = 0u32;
        for s in (now_sec - secs + 1)..=now_sec {
            let b = &self.buckets[(s.rem_euclid(n)) as usize];
            if b.sec == s {
                c += b.trades;
            }
        }
        c
    }

    /// Recompute the cached aggregates. Call about once per second per symbol.
    pub fn refresh(&mut self, now_ms: i64) {
        self.n_samples = self.filled;
        let now_sec = now_ms / 1000;
        let min_sec = now_sec - self.window as i64;
        let mut spreads: Vec<f64> = Vec::with_capacity(self.filled);
        let mut mids: Vec<(i64, f64)> = Vec::with_capacity(self.filled);
        for i in 0..self.filled {
            if self.sample_secs[i] >= min_sec {
                spreads.push(self.spread_samples[i]);
                mids.push((self.sample_secs[i], self.mid_samples[i]));
            }
        }
        if spreads.is_empty() {
            self.spread_med_bps = 0.0;
            self.spread_mean_bps = 0.0;
            self.vol_bps = 0.0;
        } else {
            spreads.sort_by(|a, b| a.partial_cmp(b).unwrap());
            self.spread_med_bps = spreads[spreads.len() / 2];
            self.spread_mean_bps = spreads.iter().sum::<f64>() / spreads.len() as f64;
            mids.sort_by_key(|m| m.0);
            // stddev of 1-second log returns, scaled to a 1-minute horizon, in bps
            let mut rets: Vec<f64> = Vec::with_capacity(mids.len());
            for w in mids.windows(2) {
                let (s0, m0) = w[0];
                let (s1, m1) = w[1];
                if m0 > 0.0 && m1 > 0.0 && s1 > s0 {
                    let dt = (s1 - s0) as f64;
                    rets.push((m1 / m0).ln() / dt.sqrt());
                }
            }
            if rets.len() >= 5 {
                let mean = rets.iter().sum::<f64>() / rets.len() as f64;
                let var = rets.iter().map(|r| (r - mean) * (r - mean)).sum::<f64>() / (rets.len() - 1) as f64;
                self.vol_bps = var.sqrt() * 60f64.sqrt() * 1e4;
            } else {
                self.vol_bps = 0.0;
            }
        }
        let secs = (self.window as i64).min(60);
        self.trades_per_min = self.trades_in_last(now_ms, secs) as f64 * 60.0 / secs as f64;
    }

    pub fn samples_in_window(&self) -> usize {
        self.filled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spread_median_and_trade_rate() {
        let mut s = SymbolStats::new(60);
        let t0 = 1_700_000_000_000i64;
        for i in 0..60 {
            let bbo = Bbo { ts: t0 + i * 1000, bid: 100.0, ask: 100.0 + 0.01 * (1 + (i % 3)) as f64, bid_qty: 1.0, ask_qty: 1.0 };
            s.on_bbo(&bbo, t0 + i * 1000);
            if i % 2 == 0 {
                s.on_trade(&Trade { ts: t0 + i * 1000, price: 100.0, qty: 1.0, taker_side: Side::Buy }, t0 + i * 1000);
            }
        }
        s.refresh(t0 + 59 * 1000);
        // spreads cycle 1,2,3 ticks of 0.01 on ~100 => 1,2,3 bps; median = 2 bps
        assert!((s.spread_med_bps - 2.0).abs() < 0.05, "med={}", s.spread_med_bps);
        assert!((s.trades_per_min - 30.0).abs() < 1.0, "tpm={}", s.trades_per_min);
        assert!(s.vol_bps < 1e-6);
        assert!((s.flow_imbalance(t0 + 59 * 1000, 10) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn volatility_is_positive_for_moving_mid() {
        let mut s = SymbolStats::new(120);
        let t0 = 1_700_000_000_000i64;
        let mut px = 100.0;
        for i in 0..120 {
            px *= if i % 2 == 0 { 1.001 } else { 0.999 };
            let bbo = Bbo { ts: t0 + i * 1000, bid: px, ask: px + 0.01, bid_qty: 1.0, ask_qty: 1.0 };
            s.on_bbo(&bbo, t0 + i * 1000);
        }
        s.refresh(t0 + 119 * 1000);
        assert!(s.vol_bps > 50.0, "vol={}", s.vol_bps);
    }
}
