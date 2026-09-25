//! Quote generation. A pure function of the market state, position and parameters,
//! so it is trivially reusable by the live engine and the replay optimizer.
//!
//! The quotes are placed around a *fair value*, not around the mid: the top-of-book size
//! imbalance, the recent taker flow and the deviation of a leading venue's price all shift
//! the fair price, and a side whose quote would have to sit too deep behind the best level
//! to keep the required edge is simply not quoted. This is what turns "sit at the best
//! price and get run over" into "quote where the informed flow is not coming from".

use crate::config::StrategyParams;
use crate::stats::SymbolStats;
use crate::types::{Bbo, Purpose, Side, SymbolMeta};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Quote {
    pub side: Side,
    pub price: f64,
    pub qty: f64,
    pub purpose: Purpose,
    /// true = cross the spread with a taker order (stale exits and stop-losses)
    pub taker: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Quotes {
    pub bid: Option<Quote>,
    pub ask: Option<Quote>,
    /// Human-readable reason when one or both sides are withheld.
    pub reason: &'static str,
    /// Fair value shift that was applied, in bps of mid (positive = price expected to rise).
    pub fair_shift_bps: f64,
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
    /// (reference mid + basis - our mid) / our mid in bps; 0 when no fresh reference.
    pub ref_dev_bps: f64,
}

fn floor_tick(meta: &SymbolMeta, px: f64) -> f64 {
    meta.ticks_to_price((px / meta.tick_size + 1e-9).floor() as i64)
}

fn ceil_tick(meta: &SymbolMeta, px: f64) -> f64 {
    meta.ticks_to_price((px / meta.tick_size - 1e-9).ceil() as i64)
}

/// Build the exit quote for a position that must leave now (stale or stop-loss).
fn forced_exit(inp: &QuoteInput, mode: &str, purpose: Purpose) -> Option<Quote> {
    let meta = inp.meta;
    let bbo = inp.bbo;
    let side = if inp.position_qty > 0.0 { Side::Sell } else { Side::Buy };
    let qty = meta.round_qty_down(inp.position_qty.abs());
    if qty <= 0.0 {
        return None;
    }
    let (price, taker) = if mode == "taker" {
        (if side == Side::Sell { bbo.bid } else { bbo.ask }, true)
    } else {
        // improve by one tick inside the spread when there is room, else join
        let improved = if side == Side::Sell { bbo.ask - meta.tick_size } else { bbo.bid + meta.tick_size };
        let px = if side == Side::Sell {
            if improved > bbo.bid {
                improved
            } else {
                bbo.ask
            }
        } else if improved < bbo.ask {
            improved
        } else {
            bbo.bid
        };
        (meta.round_price(px), false)
    };
    Some(Quote { side, price, qty, purpose, taker })
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

    // ---- stop-loss: the mid moved against the position beyond the limit ----
    if have_pos && p.stop_loss_bps > 0.0 && inp.position_avg > 0.0 {
        let adverse_bps = -(inp.position_qty.signum()) * (mid - inp.position_avg) / inp.position_avg * 1e4;
        if adverse_bps >= p.stop_loss_bps {
            if let Some(q) = forced_exit(inp, &p.stop_loss_mode, Purpose::StopLoss) {
                if q.side == Side::Sell {
                    out.ask = Some(q);
                } else {
                    out.bid = Some(q);
                }
            }
            out.reason = "stop_loss";
            return out;
        }
    }

    // ---- stale inventory: exit first, no matter what the spread looks like ----
    if have_pos && inp.position_age_secs >= p.max_hold_secs {
        if let Some(q) = forced_exit(inp, &p.stale_exit_mode, Purpose::StaleExit) {
            if q.side == Side::Sell {
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

    // Decide base prices: join the best level, improve by one tick when the spread allows,
    // or ("inside") stand strictly inside the spread around fair value and never queue.
    let inside = p.quote_mode == "inside";
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

    // ---- fair value: where the price is likely to be in a moment ----
    let half = (bbo.ask - bbo.bid) * 0.5;
    let book_imb = bbo.imbalance();
    let flow_imb = inp.stats.flow_imbalance(inp.now_ms, p.toxicity_window_secs);
    let shift = (p.imbalance_weight * book_imb + p.flow_weight * flow_imb) * half + p.ref_weight * inp.ref_dev_bps / 1e4 * mid;
    let fair = mid + shift;
    out.fair_shift_bps = shift / mid * 1e4;
    // each quote keeps at least half of the required spread away from fair value
    let req_half = required_bps * 0.5 / 1e4 * mid;
    let mut lean_bid = false;
    let mut lean_ask = false;
    let mut no_room_bid = false;
    let mut no_room_ask = false;
    if inside {
        // Best price that keeps the required edge from fair value and a share of the touch
        // spread, strictly inside the spread so we are first in line. If that price would
        // be at or behind the touch, we do not queue behind others: the side is skipped.
        let half_q = req_half.max(p.inside_spread_frac * half);
        let b = floor_tick(meta, fair - half_q).min(meta.round_price(bbo.ask - meta.tick_size));
        let a = ceil_tick(meta, fair + half_q).max(meta.round_price(bbo.bid + meta.tick_size));
        if b > bbo.bid + meta.tick_size * 0.5 {
            bid_px = b;
        } else {
            no_room_bid = true;
        }
        if a < bbo.ask - meta.tick_size * 0.5 {
            ask_px = a;
        } else {
            no_room_ask = true;
        }
    } else {
        if bid_px > fair - req_half {
            bid_px = floor_tick(meta, fair - req_half);
            let ticks_behind = ((bbo.bid - bid_px) / meta.tick_size).round() as i64;
            lean_bid = ticks_behind > p.max_lean_ticks;
        }
        if ask_px < fair + req_half {
            ask_px = ceil_tick(meta, fair + req_half);
            let ticks_behind = ((ask_px - bbo.ask) / meta.tick_size).round() as i64;
            lean_ask = ticks_behind > p.max_lean_ticks;
        }
    }

    // ---- inventory skew: shift both quotes against the position, in whole ticks ----
    let inv_ratio = if p.max_position_notional_usd > 0.0 { (pos_notional / p.max_position_notional_usd).clamp(-1.0, 1.0) } else { 0.0 };
    let skew_bps = p.inventory_skew_bps * inv_ratio;
    if skew_bps.abs() > 0.0 && tick_bps > 0.0 {
        let mut ticks = (skew_bps / tick_bps).round();
        // With a coarse tick the skew can round to nothing; once the position is at
        // least half the cap we still want to lean by one tick so inventory unwinds.
        if ticks == 0.0 && inv_ratio.abs() >= 0.5 {
            ticks = inv_ratio.signum();
        }
        let skew_ticks = ticks * meta.tick_size;
        // long => push both quotes down (sell sooner, buy later); short => push up
        bid_px -= skew_ticks;
        ask_px -= skew_ticks;
    }
    // never cross the market: keep quotes passive (inside mode stays inside the spread)
    if bid_px >= bbo.ask {
        bid_px = if inside { bbo.ask - meta.tick_size } else { bbo.bid };
    }
    if ask_px <= bbo.bid {
        ask_px = if inside { bbo.bid + meta.tick_size } else { bbo.ask };
    }
    bid_px = meta.round_price(bid_px);
    ask_px = meta.round_price(ask_px);
    if inside {
        // the skew may have pushed a quote onto or behind the touch: then it is a queue quote, drop it
        if bid_px <= bbo.bid + meta.tick_size * 0.5 {
            no_room_bid = true;
        }
        if ask_px >= bbo.ask - meta.tick_size * 0.5 {
            no_room_ask = true;
        }
    }

    // ---- hard blocks: toxic flow and a reference that already moved ----
    let tox_ask = p.toxicity_imbalance > 0.0 && flow_imb > p.toxicity_imbalance;
    let tox_bid = p.toxicity_imbalance > 0.0 && flow_imb < -p.toxicity_imbalance;
    let ref_ask = p.ref_block_bps > 0.0 && inp.ref_dev_bps > p.ref_block_bps;
    let ref_bid = p.ref_block_bps > 0.0 && inp.ref_dev_bps < -p.ref_block_bps;
    let block_bid = tox_bid || ref_bid || lean_bid || no_room_bid;
    let block_ask = tox_ask || ref_ask || lean_ask || no_room_ask;

    // ---- sizes ----
    let base_qty = meta.round_qty_down(p.order_notional_usd / mid);
    if base_qty <= 0.0 || base_qty * mid < meta.min_notional {
        out.reason = "size_below_min";
        return out;
    }
    let room_long = p.max_position_notional_usd - pos_notional; // how much more we may buy
    let room_short = p.max_position_notional_usd + pos_notional; // how much more we may sell

    // BID (buy): entry when short/flat room exists; when long at cap, no bid.
    let bid_purpose = if inp.position_qty < 0.0 { Purpose::Exit } else { Purpose::Entry };
    let bid_allowed = !block_bid && room_long >= p.order_notional_usd * 0.5 && (bid_purpose == Purpose::Exit || (inp.allow_new_exposure && !inp.reduce_only));
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
    let ask_allowed = !block_ask && room_short >= p.order_notional_usd * 0.5 && (ask_purpose == Purpose::Exit || (inp.allow_new_exposure && !inp.reduce_only));
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
        if ref_bid || ref_ask {
            "ref_block"
        } else if tox_bid || tox_ask {
            "toxic_flow"
        } else if lean_bid || lean_ask {
            "lean_out"
        } else if no_room_bid || no_room_ask {
            "no_room_inside"
        } else if inp.reduce_only {
            "reduce_only"
        } else if !inp.allow_new_exposure {
            "risk_limit"
        } else {
            "position_cap"
        }
    } else if ref_bid || ref_ask {
        "ref_one_side"
    } else if tox_bid || tox_ask {
        "toxic_one_side"
    } else if lean_bid || lean_ask {
        "lean_one_side"
    } else if no_room_bid || no_room_ask {
        "inside_one_side"
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

    /// Parameters with the fair-value model switched off, to test the classic behaviour.
    fn plain() -> StrategyParams {
        StrategyParams { quote_mode: "join".into(), min_spread_bps: 10.0, imbalance_weight: 0.0, flow_weight: 0.0, ref_weight: 0.0, ref_block_bps: 0.0, stop_loss_bps: 0.0, ..StrategyParams::default() }
    }

    /// Default model (fair value on) but the classic join mode, for the older tests.
    fn joined() -> StrategyParams {
        StrategyParams { quote_mode: "join".into(), min_spread_bps: 10.0, ..StrategyParams::default() }
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
            ref_dev_bps: 0.0,
        }
    }

    #[test]
    fn joins_both_sides_when_spread_is_wide() {
        let m = meta();
        // spread 0.02 on ~1.0 => 200 bps
        let bbo = Bbo { ts: 0, bid: 1.000, ask: 1.020, bid_qty: 100.0, ask_qty: 100.0 };
        let st = SymbolStats::new(60);
        let p = plain();
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
    fn balanced_book_with_default_model_still_joins() {
        let m = meta();
        let bbo = Bbo { ts: 0, bid: 1.000, ask: 1.020, bid_qty: 100.0, ask_qty: 100.0 };
        let st = SymbolStats::new(60);
        let p = joined();
        let q = compute_quotes(&input(&m, &bbo, &st, &p));
        assert_eq!(q.reason, "ok");
        assert_eq!(q.bid.unwrap().price, 1.000);
        assert_eq!(q.ask.unwrap().price, 1.020);
        assert!(q.fair_shift_bps.abs() < 1e-9);
    }

    #[test]
    fn withholds_when_spread_narrow() {
        let m = meta();
        let bbo = Bbo { ts: 0, bid: 1.000, ask: 1.0005, bid_qty: 100.0, ask_qty: 100.0 };
        let st = SymbolStats::new(60);
        let p = plain();
        let q = compute_quotes(&input(&m, &bbo, &st, &p));
        assert_eq!(q.reason, "spread_too_narrow");
        assert!(q.bid.is_none() && q.ask.is_none());
    }

    #[test]
    fn improve_mode_steps_inside() {
        let m = meta();
        let bbo = Bbo { ts: 0, bid: 1.000, ask: 1.020, bid_qty: 100.0, ask_qty: 100.0 };
        let st = SymbolStats::new(60);
        let mut p = plain();
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
        let p = plain();
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
        let mut p = plain();
        p.stale_exit_mode = "taker".into();
        let mut i = input(&m, &bbo, &st, &p);
        i.position_qty = -50.0;
        i.position_avg = 1.01;
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
        let p = plain();
        let mut i = input(&m, &bbo, &st, &p);
        i.reduce_only = true;
        i.position_qty = 40.0;
        i.position_avg = 1.0;
        let q = compute_quotes(&i);
        assert!(q.bid.is_none());
        assert_eq!(q.ask.unwrap().qty, 40.0);
    }

    #[test]
    fn stop_loss_exits_at_market_when_price_runs_away() {
        let m = meta();
        // long from 1.010, market now 0.990/0.992: ~-20 bps against us
        let bbo = Bbo { ts: 0, bid: 0.990, ask: 0.992, bid_qty: 100.0, ask_qty: 100.0 };
        let st = SymbolStats::new(60);
        let mut p = joined();
        p.stop_loss_bps = 12.0;
        let mut i = input(&m, &bbo, &st, &p);
        i.position_qty = 90.0;
        i.position_avg = 1.010;
        let q = compute_quotes(&i);
        assert_eq!(q.reason, "stop_loss");
        let a = q.ask.unwrap();
        assert!(a.taker);
        assert_eq!(a.purpose, Purpose::StopLoss);
        assert_eq!(a.price, 0.990);
        assert!(q.bid.is_none());
        // a small move is tolerated
        let bbo2 = Bbo { ts: 0, bid: 1.005, ask: 1.025, bid_qty: 100.0, ask_qty: 100.0 };
        let mut i2 = input(&m, &bbo2, &st, &p);
        i2.position_qty = 90.0;
        i2.position_avg = 1.010;
        assert_ne!(compute_quotes(&i2).reason, "stop_loss");
    }

    #[test]
    fn book_imbalance_leans_the_dangerous_side() {
        let m = meta();
        // heavy asks, thin bids: price likely to fall -> fair below mid -> bid leans down
        let bbo = Bbo { ts: 0, bid: 1.000, ask: 1.020, bid_qty: 5.0, ask_qty: 200.0 };
        let st = SymbolStats::new(60);
        let mut p = joined();
        p.imbalance_weight = 1.0;
        p.max_lean_ticks = 100;
        let q = compute_quotes(&input(&m, &bbo, &st, &p));
        assert!(q.fair_shift_bps < 0.0);
        let b = q.bid.unwrap();
        assert!(b.price < 1.000, "bid should lean down, got {}", b.price);
        assert_eq!(q.ask.unwrap().price, 1.020, "ask stays at the touch");
        // when no leaning is allowed the leaned bid is dropped instead
        p.max_lean_ticks = 0;
        let q = compute_quotes(&input(&m, &bbo, &st, &p));
        assert!(q.bid.is_none());
        assert_eq!(q.reason, "lean_one_side");
    }

    #[test]
    fn inside_mode_quotes_inside_the_spread_and_never_queues() {
        let m = meta();
        // 200 bps spread: with inside_spread_frac 0.8 we quote 160 bps wide, strictly inside
        let bbo = Bbo { ts: 0, bid: 1.000, ask: 1.020, bid_qty: 100.0, ask_qty: 100.0 };
        let st = SymbolStats::new(60);
        let p = StrategyParams::default(); // inside, frac 0.8, min_spread 10
        let q = compute_quotes(&input(&m, &bbo, &st, &p));
        assert_eq!(q.reason, "ok");
        let b = q.bid.unwrap().price;
        let a = q.ask.unwrap().price;
        assert!(b > 1.000 && a < 1.020, "bid {b} ask {a}");
        assert!((b - 1.002).abs() < 1e-9 && (a - 1.018).abs() < 1e-9, "bid {b} ask {a}");
        // a narrow spread of 2 ticks leaves no room inside: nothing is quoted, nothing queues
        let narrow = Bbo { ts: 0, bid: 1.000, ask: 1.002, bid_qty: 100.0, ask_qty: 100.0 };
        let q = compute_quotes(&input(&m, &narrow, &st, &p));
        assert!(q.bid.is_none() && q.ask.is_none(), "{}", q.reason);
        // spread of 12 bps (12 ticks): 80% of the half spread -> quotes ~5 bps around mid, inside
        let mid_spread = Bbo { ts: 0, bid: 1.000, ask: 1.012, bid_qty: 100.0, ask_qty: 100.0 };
        let q = compute_quotes(&input(&m, &mid_spread, &st, &p));
        assert_eq!(q.reason, "ok");
        assert!(q.bid.unwrap().price > 1.000 && q.ask.unwrap().price < 1.012);
        // a long position: the ask side is an exit and stays inside, the bid leans down
        let mut i = input(&m, &bbo, &st, &p);
        i.position_qty = 150.0;
        i.position_avg = 1.010;
        let q = compute_quotes(&i);
        assert_eq!(q.ask.unwrap().purpose, Purpose::Exit);
        assert!(q.ask.unwrap().price < 1.020);
    }

    #[test]
    fn reference_deviation_blocks_and_shifts() {
        let m = meta();
        let bbo = Bbo { ts: 0, bid: 1.000, ask: 1.020, bid_qty: 100.0, ask_qty: 100.0 };
        let st = SymbolStats::new(60);
        let p = joined(); // ref_weight 1, ref_block_bps 4
        // the leading venue is already 10 bps higher: do not sell, bid may stay
        let mut i = input(&m, &bbo, &st, &p);
        i.ref_dev_bps = 10.0;
        let q = compute_quotes(&i);
        assert!(q.ask.is_none());
        assert!(q.bid.is_some());
        assert_eq!(q.reason, "ref_one_side");
        // a small positive deviation just shifts fair value: ask leans up by a tick or two
        let mut i = input(&m, &bbo, &st, &p);
        i.ref_dev_bps = 3.0;
        let q = compute_quotes(&i);
        assert!(q.ask.is_some() && q.bid.is_some(), "{}", q.reason);
        assert!(q.fair_shift_bps > 2.9 && q.fair_shift_bps < 3.1);
    }
}
