//! Synthetic market for tests and offline runs. Each symbol is a random walk with
//! trend regimes, a mean-reverting spread, Poisson trade arrivals and a trade-side
//! bias in the direction of the regime (toxic flow) plus per-trade price impact, so
//! adverse selection is present and the analytics have something real to detect.

use crate::types::{Bbo, MarketEvent, Side, SymbolId, SymbolMeta, Trade};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, LogNormal, Normal, Poisson};
use std::collections::VecDeque;

#[derive(Clone, Debug)]
pub struct SimParams {
    pub n_symbols: usize,
    pub seed: u64,
    pub step_ms: i64,
    /// 0 = trade sides independent of future moves, 1 = fully informed flow.
    pub toxicity: f64,
    /// The synthetic perp book follows the "true" (reference) price with this lag, ms.
    /// 0 = no reference feed.
    pub ref_lag_ms: i64,
}

impl Default for SimParams {
    fn default() -> Self {
        Self { n_symbols: 20, seed: 42, step_ms: 100, toxicity: 0.4, ref_lag_ms: 300 }
    }
}

struct SimSym {
    meta: SymbolMeta,
    mid: f64,
    spread_mean_ticks: f64,
    spread_ticks: f64,
    sigma_step: f64,
    lambda_step: f64,
    impact: f64,
    drift: f64,
    drift_left: i64,
    size_scale: f64,
    last_bbo: Option<Bbo>,
    last_emit_ts: i64,
    /// recent true mids; the book is built from the oldest one (the lag)
    mid_hist: VecDeque<f64>,
    last_ref_mid: f64,
    last_ref_ts: i64,
}

pub struct SimFeed {
    rng: StdRng,
    syms: Vec<SimSym>,
    pub t: i64,
    step_ms: i64,
    toxicity: f64,
    lag_steps: usize,
    normal: Normal<f64>,
    size_dist: LogNormal<f64>,
}

impl SimFeed {
    pub fn new(p: &SimParams, start_ts: i64) -> Self {
        let mut rng = StdRng::seed_from_u64(p.seed);
        let mut syms = Vec::with_capacity(p.n_symbols);
        for i in 0..p.n_symbols {
            // price log-uniform in [0.01, 50000]
            let price = 10f64.powf(rng.gen_range(-2.0..4.7));
            // tick between 0.5 and 8 bps of price, snapped to a power of ten
            let tick_bps_target: f64 = rng.gen_range(0.5..8.0);
            let tick_raw = price * tick_bps_target / 1e4;
            let k = (-tick_raw.log10()).ceil().max(0.0) as u32;
            let tick = 10f64.powi(-(k as i32));
            // lot step so that one step is worth ~0.5..5 USD
            let step_usd: f64 = rng.gen_range(0.5..5.0);
            let j = (-(step_usd / price).log10()).floor().max(0.0) as i32;
            let qty_step = 10f64.powi(-j);
            let meta = SymbolMeta {
                id: i as SymbolId,
                name: format!("SIM{i:03}USDT"),
                base_coin: format!("SIM{i:03}"),
                quote_coin: "USDT".into(),
                tick_size: tick,
                qty_step,
                min_qty: qty_step,
                max_qty: 1e12,
                min_notional: 5.0,
                price_scale: k,
                turnover_24h: 10f64.powf(rng.gen_range(6.0..9.0)),
            };
            let tick_bps = tick / price * 1e4;
            // typical spread 2..40 bps, log-uniform
            let spread_bps = 10f64.powf(rng.gen_range(0.3..1.6));
            let spread_mean_ticks = (spread_bps / tick_bps).max(1.0);
            // volatility 3..40 bps per minute -> per step
            let vol_bpm = 10f64.powf(rng.gen_range(0.5..1.6));
            let steps_per_min = 60_000.0 / p.step_ms as f64;
            let sigma_step = vol_bpm / 1e4 / steps_per_min.sqrt();
            // 1..300 trades per minute
            let tpm = 10f64.powf(rng.gen_range(0.0..2.5));
            let lambda_step = tpm / steps_per_min;
            let size_scale = 40.0 / price; // ~40 USD median trade
            syms.push(SimSym {
                meta,
                mid: price,
                spread_mean_ticks,
                spread_ticks: spread_mean_ticks,
                sigma_step,
                lambda_step,
                impact: sigma_step * 0.25,
                drift: 0.0,
                drift_left: 0,
                size_scale,
                last_bbo: None,
                last_emit_ts: 0,
                mid_hist: VecDeque::new(),
                last_ref_mid: 0.0,
                last_ref_ts: 0,
            });
        }
        Self {
            rng,
            syms,
            t: start_ts,
            step_ms: p.step_ms,
            toxicity: p.toxicity.clamp(0.0, 1.0),
            lag_steps: if p.ref_lag_ms > 0 { ((p.ref_lag_ms + p.step_ms - 1) / p.step_ms).max(1) as usize } else { 0 },
            normal: Normal::new(0.0, 1.0).unwrap(),
            size_dist: LogNormal::new(0.0, 1.0).unwrap(),
        }
    }

    pub fn metas(&self) -> Vec<SymbolMeta> {
        self.syms.iter().map(|s| s.meta.clone()).collect()
    }

    /// The mid the book is built from: the true mid delayed by the lag.
    fn book_mid(&self, s: &SimSym) -> f64 {
        if self.lag_steps > 0 {
            s.mid_hist.front().copied().unwrap_or(s.mid)
        } else {
            s.mid
        }
    }

    fn bbo_of(&self, s: &SimSym, ts: i64) -> Bbo {
        let tick = s.meta.tick_size;
        let ticks = s.spread_ticks.round().max(1.0);
        let half = ticks * tick / 2.0;
        let mid = self.book_mid(s);
        let bid = ((mid - half) / tick).floor() * tick;
        let bid = s.meta.round_price(bid);
        let ask = s.meta.round_price(bid + ticks * tick);
        Bbo { ts, bid, ask, bid_qty: 0.0, ask_qty: 0.0 }
    }

    /// Advance one step and append the resulting events. Trades come before the
    /// book update that reflects them.
    pub fn step(&mut self, out: &mut Vec<MarketEvent>) {
        self.t += self.step_ms;
        let t = self.t;
        let n = self.syms.len();
        for i in 0..n {
            // regime switching
            if self.syms[i].drift_left <= 0 {
                let steps = self.rng.gen_range(50..600);
                let r: f64 = self.rng.gen();
                let sign = if r < 0.25 { -1.0 } else if r < 0.5 { 1.0 } else { 0.0 };
                let s = &mut self.syms[i];
                s.drift_left = steps;
                s.drift = sign * s.sigma_step * 0.5;
            }
            self.syms[i].drift_left -= 1;

            // trades
            let lambda = self.syms[i].lambda_step;
            let n_trades = if lambda > 0.0 { Poisson::new(lambda).map(|d| d.sample(&mut self.rng) as usize).unwrap_or(0) } else { 0 };
            if n_trades > 0 {
                let bbo = self.bbo_of(&self.syms[i], t);
                let mut trades = Vec::with_capacity(n_trades);
                let drift_sign = self.syms[i].drift.signum();
                // informed flow: regime direction, plus arbitrageurs who see the true price
                // while the book still shows the lagged one
                let gap = (self.syms[i].mid - bbo.mid()) / ((bbo.ask - bbo.bid) * 0.5).max(1e-12);
                let p_buy = (0.5 + 0.5 * self.toxicity * drift_sign + 0.3 * self.toxicity * gap.clamp(-1.0, 1.0)).clamp(0.05, 0.95);
                let mut impact_sum = 0.0;
                for k in 0..n_trades {
                    let buy = self.rng.gen::<f64>() < p_buy;
                    let qty_raw = self.syms[i].size_scale * self.size_dist.sample(&mut self.rng);
                    let qty = self.syms[i].meta.round_qty_down(qty_raw).max(self.syms[i].meta.qty_step);
                    trades.push(Trade {
                        ts: t + k as i64 * (self.step_ms / (n_trades as i64 + 1)),
                        price: if buy { bbo.ask } else { bbo.bid },
                        qty,
                        taker_side: if buy { Side::Buy } else { Side::Sell },
                    });
                    impact_sum += if buy { self.syms[i].impact } else { -self.syms[i].impact };
                }
                out.push(MarketEvent::Trades { sym: i as SymbolId, trades });
                self.syms[i].mid *= (impact_sum).exp();
            }

            // price and spread dynamics
            let z = self.normal.sample(&mut self.rng);
            let z2 = self.normal.sample(&mut self.rng);
            let lag_steps = self.lag_steps;
            let s = &mut self.syms[i];
            s.mid *= (s.drift + s.sigma_step * z).exp();
            if lag_steps > 0 {
                s.mid_hist.push_back(s.mid);
                while s.mid_hist.len() > lag_steps {
                    s.mid_hist.pop_front();
                }
            }
            let widen = if s.drift != 0.0 { 1.3 } else { 1.0 };
            let target = s.spread_mean_ticks * widen;
            s.spread_ticks += 0.05 * (target - s.spread_ticks) + 0.15 * target * z2;
            s.spread_ticks = s.spread_ticks.max(1.0);

            // book
            let mut bbo = self.bbo_of(&self.syms[i], t);
            let s = &mut self.syms[i];
            let base = s.size_scale * 8.0;
            let sz = |rng: &mut StdRng, d: &LogNormal<f64>, meta: &SymbolMeta| meta.round_qty_down(base * d.sample(rng)).max(meta.qty_step);
            let changed = s.last_bbo.is_none_or(|p| p.bid != bbo.bid || p.ask != bbo.ask);
            if changed || t - s.last_emit_ts >= 3000 {
                bbo.bid_qty = sz(&mut self.rng, &self.size_dist, &s.meta);
                bbo.ask_qty = sz(&mut self.rng, &self.size_dist, &s.meta);
                s.last_bbo = Some(bbo);
                s.last_emit_ts = t;
                out.push(MarketEvent::Bbo { sym: i as SymbolId, bbo });
            }
            // reference venue: the true price, one tick wide, whenever it moved a tick
            if lag_steps > 0 {
                let tick = s.meta.tick_size;
                let ref_mid = s.meta.round_price(s.mid);
                if (ref_mid - s.last_ref_mid).abs() >= tick * 0.5 || t - s.last_ref_ts >= 1000 {
                    s.last_ref_mid = ref_mid;
                    s.last_ref_ts = t;
                    // one tick each side of the true price: the reference mid is exact
                    out.push(MarketEvent::Reference { sym: i as SymbolId, ts: t, bid: s.meta.round_price(ref_mid - tick), ask: s.meta.round_price(ref_mid + tick), rank: 0 });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_consistent_books_and_trades() {
        let mut f = SimFeed::new(&SimParams { n_symbols: 5, seed: 1, step_ms: 100, toxicity: 0.5, ref_lag_ms: 300 }, 1_700_000_000_000);
        let mut out = Vec::new();
        let mut n_bbo = 0;
        let mut n_trd = 0;
        for _ in 0..600 {
            f.step(&mut out);
            for ev in out.drain(..) {
                match ev {
                    MarketEvent::Bbo { bbo, .. } => {
                        assert!(bbo.is_valid());
                        assert!(bbo.bid_qty > 0.0 && bbo.ask_qty > 0.0);
                        n_bbo += 1;
                    }
                    MarketEvent::Trades { trades, .. } => {
                        for t in trades {
                            assert!(t.qty > 0.0 && t.price > 0.0);
                            n_trd += 1;
                        }
                    }
                    MarketEvent::Reference { bid, ask, .. } => assert!(ask > bid && bid > 0.0),
                    _ => panic!("unexpected event"),
                }
            }
        }
        assert!(n_bbo > 100 && n_trd > 10, "bbo={n_bbo} trd={n_trd}");
        let metas = f.metas();
        assert_eq!(metas.len(), 5);
        assert!(metas.iter().all(|m| m.tick_size > 0.0 && m.qty_step > 0.0));
    }
}
