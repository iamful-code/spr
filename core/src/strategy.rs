//! Quote generation. A pure function of the market state, position and parameters,
//! so it is trivially reusable by the live engine and the replay optimizer.

use crate::config::StrategyParams;
use crate::stats::SymbolStats;
use crate::types::{Bbo, Purpose, Side, SymbolMeta};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Quote {
    pub side: Side,
    pub price: f64,
    pub qty: f64,
    pub purpose: Purpose,
    /// true = cross the spread with a taker order (only for stale exits)
    pub taker: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Quotes {
    pub bid: Option<Quote>,
    pub ask: Option<Quote>,
    /// Human-readable reason when one or both sides are withheld.
    pub reason: &'static str,
}

pub struct QuoteInput<'a> {
    pub meta: &'a SymbolMeta,
    pub bbo: &'a Bbo,
    pub stats: &'a SymbolStats,
    pub params: &'a StrategyParams,
    pub maker_fee: f64,
    pub position_qty: f64,
    pub position_avg: f64,
    pub position_age_secs: i64,
    pub now_ms: i64,
    /// Risk layer verdicts.
    pub allow_new_exposure: bool,
    /// Symbol dropped out of the active set: only reduce.
    pub reduce_only: bool,
}

/// Compute the desired quotes. Returns empty quotes with a reason when the symbol should not be quoted.
pub fn compute_quotes(inp: &QuoteInput) -> Quotes {
    let p = inp.params;
    let bbo = inp.bbo;
    let meta = inp.meta;
    let mut out = Quotes::default();

    if !bbo.is_valid() {
        out.reason = "invalid_bbo";
        return out;
    }
    let mid = bbo.mid();
    let spread_bps = bbo.spread_bps();
    let tick_bps = meta.tick_bps(mid);
    let pos_notional = inp.position_qty * mid;
    let have_pos = inp.position_qty.abs() * mid >= meta.min_notional.max(1.0);

    // ---- stale inventory: exit first, no matter what the spread looks like ----
    if have_pos && inp.position_age_secs >= p.max_hold_secs {
        let side = if inp.position_qty > 0.0 { Side::Sell } else { Side::Buy };
        let qty = meta.round_qty_down(inp.position_qty.abs());
        if qty > 0.0 {
            let (price, taker) = if p.stale_exit_mode == "taker" {
                (if side == Side::Sell { bbo.bid } else { bbo.ask }, true)
            } else {
                // improve by one tick inside the spread when there is room, else join
                let improved = if side == Side::Sell { bbo.ask - meta.tick_size } else { bbo.bid + meta.tick_size };
                let px = if side == Side::Sell {
                    if improved > bbo.bid { improved } else { bbo.ask }
                } else if improved < bbo.ask {
                    improved
                } else {
                    bbo.bid
                };
                (meta.round_price(px), false)
            };
            let q = Quote { side, price, qty, purpose: Purpose::StaleExit, taker };
            if side == Side::Sell {
                out.ask = Some(q);
            } else {
                out.bid = Some(q);
            }
        }
        out.reason = "stale_exit";
        return out;
    }

    // ---- guards that stop new quoting entirely ----
    if inp.stats.vol_bps > p.max_vol_bps && p.max_vol_bps > 0.0 {
        out.reason = "vol_guard";
        // still allow exit side for an open position, passively at the best price
        add_exit_only(&mut out, inp, mid);
        return out;
    }

    // Minimum total spread we need: fees on both legs plus the edge target.
    let required_bps = p.min_spread_bps.max(2.0 * inp.maker_fee * 1e4 + p.min_edge_bps);

    // Decide prices: join the best level or improve by one tick when the spread allows.
    let improve = p.quote_mode == "improve" && spread_bps - 2.0 * tick_bps >= required_bps;
    let (mut bid_px, mut ask_px) = if improve {
        (bbo.bid + meta.tick_size, bbo.ask - meta.tick_size)
    } else {
        (bbo.bid, bbo.ask)
    };
    let quoted_spread_bps = (ask_px - bid_px) / mid * 1e4;
    if quoted_spread_bps < required_bps {
        out.reason = "spread_too_narrow";
        add_exit_only(&mut out, inp, mid);
        return out;
    }

    // Inventory skew: shift both quotes against the position, in whole ticks.
    let inv_ratio = if p.max_position_notional_usd > 0.0 {
        (pos_notional / p.max_position_notional_usd).clamp(-1.0, 1.0)
    } else {
        0.0
    };
    let skew_bps = p.inventory_skew_bps * inv_ratio;
    if skew_bps.abs() > 0.0 && tick_bps > 0.0 {
        let skew_ticks = (skew_bps / tick_bps).round() * meta.tick_size;
        // long => push both quotes down (sell sooner, buy later); short => push up
        bid_px -= skew_ticks;
        ask_px -= skew_ticks;
        // never cross the market: keep quotes passive
        if bid_px >= bbo.ask {
            bid_px = bbo.bid;
        }
        if ask_px <= bbo.bid {
            ask_px = bbo.ask;
        }
    }
    bid_px = meta.round_price(bid_px);
    ask_px = meta.round_price(ask_px);

    // Toxic flow filter: heavy net buying means price likely rises; don't sell into it.
    let imb = inp.stats.flow_imbalance(inp.now_ms, p.toxicity_window_secs);
    let block_ask = p.toxicity_imbalance > 0.0 && imb > p.toxicity_imbalance;
    let block_bid = p.toxicity_imbalance > 0.0 && imb < -p.toxicity_imbalance;

    // Sizes.
    let base_qty = meta.round_qty_down(p.order_notional_usd / mid);
    if base_qty <= 0.0 || base_qty * mid < meta.min_notional {
        out.reason = "size_below_min";
        return out;
    }
    let room_long = p.max_position_notional_usd - pos_notional; // how much more we may buy
    let room_short = p.max_position_notional_usd + pos_notional; // how much more we may sell

    // BID (buy): entry when short/flat room exists; when long at cap, no bid.
    let bid_purpose = if inp.position_qty < 0.0 { Purpose::Exit } else { Purpose::Entry };
    let bid_allowed = !block_bid
        && room_long >= p.order_notional_usd * 0.5
        && (bid_purpose == Purpose::Exit || (inp.allow_new_exposure && !inp.reduce_only));
    if bid_allowed {
        let mut qty = base_qty;
        if bid_purpose == Purpose::Exit {
            // do not flip through zero when reducing: cap at the position size
            qty = qty.min(meta.round_qty_down(inp.position_qty.abs())).max(0.0);
        } else {
            qty = qty.min(meta.round_qty_down(room_long / mid));
        }
        if qty > 0.0 && qty * mid >= meta.min_notional {
            out.bid = Some(Quote { side: Side::Buy, price: bid_px, qty, purpose: bid_purpose, taker: false });
        }
    }

    // ASK (sell)
    let ask_purpose = if inp.position_qty > 0.0 { Purpose::Exit } else { Purpose::Entry };
    let ask_allowed = !block_ask
        && room_short >= p.order_notional_usd * 0.5
        && (ask_purpose == Purpose::Exit || (inp.allow_new_exposure && !inp.reduce_only));
    if ask_allowed {
        let mut qty = base_qty;
        if ask_purpose == Purpose::Exit {
            qty = qty.min(meta.round_qty_down(inp.position_qty.abs())).max(0.0);
        } else {
            qty = qty.min(meta.round_qty_down(room_short / mid));
        }
        if qty > 0.0 && qty * mid >= meta.min_notional {
            out.ask = Some(Quote { side: Side::Sell, price: ask_px, qty, purpose: ask_purpose, taker: false });
        }
    }

    out.reason = if out.bid.is_none() && out.ask.is_none() {
        if block_bid || block_ask {
            "toxic_flow"
        } else if inp.reduce_only {
            "reduce_only"
        } else if !inp.allow_new_exposure {
            "risk_limit"
        } else {
            "position_cap"
        }
    } else if block_bid || block_ask {
        "toxic_one_side"
    } else {
        "ok"
    };
    out
}

/// When new exposure is not allowed, still let an open position leave passively at the best price.
fn add_exit_only(out: &mut Quotes, inp: &QuoteInput, mid: f64) {
    let meta = inp.meta;
    let bbo = inp.bbo;
    if inp.position_qty.abs() * mid < meta.min_notional.max(1.0) {
        return;
    }
    let qty = meta.round_qty_down(inp.position_qty.abs());
    if qty <= 0.0 {
        return;
    }
    if inp.position_qty > 0.0 {
        out.ask = Some(Quote { side: Side::Sell, price: bbo.ask, qty, purpose: Purpose::Exit, taker: false });
    } else {
        out.bid = Some(Quote { side: Side::Buy, price: bbo.bid, qty, purpose: Purpose::Exit, taker: false });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> SymbolMeta {
        SymbolMeta {
            id: 0,
            name: "TESTUSDT".into(),
            base_coin: "TEST".into(),
            quote_coin: "USDT".into(),
            tick_size: 0.001,
            qty_step: 1.0,
            min_qty: 1.0,
            max_qty: 1e9,
            min_notional: 5.0,
            price_scale: 3,
            turnover_24h: 1e7,
        }
    }

    fn input<'a>(meta: &'a SymbolMeta, bbo: &'a Bbo, stats: &'a SymbolStats, params: &'a StrategyParams) -> QuoteInput<'a> {
        QuoteInput {
            meta,
            bbo,
            stats,
            params,
            maker_fee: 0.0002,
            position_qty: 0.0,
            position_avg: 0.0,
            position_age_secs: 0,
            now_ms: 1_700_000_000_000,
            allow_new_exposure: true,
            reduce_only: false,
        }
    }

    #[test]
    fn joins_both_sides_when_spread_is_wide() {
        let m = meta();
        // spread 0.02 on ~1.0 => 200 bps
        let bbo = Bbo { ts: 0, bid: 1.000, ask: 1.020, bid_qty: 100.0, ask_qty: 100.0 };
        let st = SymbolStats::new(60);
        let p = StrategyParams::default();
        let q = compute_quotes(&input(&m, &bbo, &st, &p));
        assert_eq!(q.reason, "ok");
        let b = q.bid.unwrap();
        let a = q.ask.unwrap();
        assert_eq!(b.price, 1.000);
        assert_eq!(a.price, 1.020);
        assert_eq!(b.qty, 99.0); // 100 usd / 1.01 mid = 99.0 rounded down to lot 1
        assert_eq!(b.purpose, Purpose::Entry);
    }

    #[test]
    fn withholds_when_spread_narrow() {
        let m = meta();
        let bbo = Bbo { ts: 0, bid: 1.000, ask: 1.0005, bid_qty: 100.0, ask_qty: 100.0 };
        let st = SymbolStats::new(60);
        let p = StrategyParams::default();
        let q = compute_quotes(&input(&m, &bbo, &st, &p));
        assert_eq!(q.reason, "spread_too_narrow");
        assert!(q.bid.is_none() && q.ask.is_none());
    }

    #[test]
    fn improve_mode_steps_inside() {
        let m = meta();
        let bbo = Bbo { ts: 0, bid: 1.000, ask: 1.020, bid_qty: 100.0, ask_qty: 100.0 };
        let st = SymbolStats::new(60);
        let mut p = StrategyParams::default();
        p.quote_mode = "improve".into();
        let q = compute_quotes(&input(&m, &bbo, &st, &p));
        assert_eq!(q.bid.unwrap().price, 1.001);
        assert_eq!(q.ask.unwrap().price, 1.019);
    }

    #[test]
    fn long_at_cap_only_sells_and_skews_down() {
        let m = meta();
        let bbo = Bbo { ts: 0, bid: 1.000, ask: 1.020, bid_qty: 100.0, ask_qty: 100.0 };
        let st = SymbolStats::new(60);
        let p = StrategyParams::default();
        let mut i = input(&m, &bbo, &st, &p);
        i.position_qty = 300.0; // ~303 usd, at the cap
        i.position_avg = 1.0;
        let q = compute_quotes(&i);
        assert!(q.bid.is_none());
        let a = q.ask.unwrap();
        assert_eq!(a.purpose, Purpose::Exit);
        assert!(a.price < 1.020 && a.price > 1.000, "ask {}", a.price);
        assert_eq!(a.qty, 99.0);
    }

    #[test]
    fn stale_position_exits_aggressively() {
        let m = meta();
        let bbo = Bbo { ts: 0, bid: 1.000, ask: 1.020, bid_qty: 100.0, ask_qty: 100.0 };
        let st = SymbolStats::new(60);
        let mut p = StrategyParams::default();
        p.stale_exit_mode = "taker".into();
        let mut i = input(&m, &bbo, &st, &p);
        i.position_qty = -50.0;
        i.position_age_secs = 1000;
        let q = compute_quotes(&i);
        assert_eq!(q.reason, "stale_exit");
        let b = q.bid.unwrap();
        assert!(b.taker);
        assert_eq!(b.price, 1.020);
        assert_eq!(b.qty, 50.0);
        assert!(q.ask.is_none());
    }

    #[test]
    fn reduce_only_blocks_entries_but_not_exits() {
        let m = meta();
        let bbo = Bbo { ts: 0, bid: 1.000, ask: 1.020, bid_qty: 100.0, ask_qty: 100.0 };
        let st = SymbolStats::new(60);
        let p = StrategyParams::default();
        let mut i = input(&m, &bbo, &st, &p);
        i.reduce_only = true;
        i.position_qty = 40.0;
        let q = compute_quotes(&i);
        assert!(q.bid.is_none());
        assert_eq!(q.ask.unwrap().qty, 40.0);
    }
}
