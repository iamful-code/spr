//! Paper exchange: simulates Bybit maker execution for our resting limit orders using
//! the public trade stream and top-of-book moves.
//!
//! Model:
//! * an order becomes active `latency_ms` after placement; a cancel takes effect
//!   `latency_ms` after the request (the order can still fill in between);
//! * on activation the queue ahead of us equals the displayed size at our price
//!   (0 if we are alone inside the spread); if the displayed size later drops below
//!   our estimate, we move up (cancellations ahead of us);
//! * a public trade at our price on our side first consumes the queue ahead, the rest
//!   fills us; a trade through our price fills the remainder;
//! * a top-of-book move through our price (best ask <= our bid) fills the remainder;
//! * post-only semantics: an order that would cross on activation is rejected.

use crate::types::{Bbo, Fill, OrderDone, Purpose, Side, SymbolId, Trade};
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Pending,
    Active,
    CancelPending,
}

#[derive(Clone, Debug)]
pub struct PaperOrder {
    pub id: u64,
    pub sym: SymbolId,
    pub side: Side,
    pub price: f64,
    pub qty: f64,
    pub filled: f64,
    pub taker: bool,
    pub purpose: Purpose,
    pub ts_created: i64,
    pub ts_active: i64,
    pub cancel_at: Option<i64>,
    state: State,
    pub queue_ahead: f64,
    pub queue_ahead_initial: f64,
    pub mid_at_place: f64,
    pub spread_bps_at_place: f64,
    pub inventory_before: f64,
    pub param_version: u32,
}

impl PaperOrder {
    pub fn remaining(&self) -> f64 {
        (self.qty - self.filled).max(0.0)
    }
    pub fn is_live(&self) -> bool {
        matches!(self.state, State::Pending | State::Active | State::CancelPending)
    }
    pub fn is_cancel_pending(&self) -> bool {
        self.state == State::CancelPending
    }
}

pub struct PlaceReq {
    pub sym: SymbolId,
    pub side: Side,
    pub price: f64,
    pub qty: f64,
    pub taker: bool,
    pub purpose: Purpose,
    pub inventory_before: f64,
    pub param_version: u32,
}

pub struct PaperExchange {
    pub latency_ms: i64,
    pub maker_fee: f64,
    pub taker_fee: f64,
    pub qty_eps: f64,
    orders: HashMap<u64, PaperOrder>,
    by_sym: HashMap<SymbolId, Vec<u64>>,
    last_bbo: HashMap<SymbolId, Bbo>,
    next_id: u64,
    pub fills: Vec<Fill>,
    pub done: Vec<OrderDone>,
}

impl PaperExchange {
    pub fn new(latency_ms: i64, maker_fee: f64, taker_fee: f64) -> Self {
        Self {
            latency_ms,
            maker_fee,
            taker_fee,
            qty_eps: 1e-9,
            orders: HashMap::new(),
            by_sym: HashMap::new(),
            last_bbo: HashMap::new(),
            next_id: 1,
            fills: Vec::new(),
            done: Vec::new(),
        }
    }

    pub fn order(&self, id: u64) -> Option<&PaperOrder> {
        self.orders.get(&id)
    }

    pub fn live_orders(&self, sym: SymbolId) -> impl Iterator<Item = &PaperOrder> {
        self.by_sym.get(&sym).into_iter().flatten().filter_map(move |id| self.orders.get(id)).filter(|o| o.is_live())
    }

    pub fn n_live(&self) -> usize {
        self.orders.values().filter(|o| o.is_live()).count()
    }

    pub fn place(&mut self, now: i64, req: PlaceReq) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        let bbo = self.last_bbo.get(&req.sym).copied().unwrap_or_default();
        let mid = if bbo.is_valid() { bbo.mid() } else { req.price };
        let o = PaperOrder {
            id,
            sym: req.sym,
            side: req.side,
            price: req.price,
            qty: req.qty,
            filled: 0.0,
            taker: req.taker,
            purpose: req.purpose,
            ts_created: now,
            ts_active: now + self.latency_ms,
            cancel_at: None,
            state: State::Pending,
            queue_ahead: 0.0,
            queue_ahead_initial: 0.0,
            mid_at_place: mid,
            spread_bps_at_place: if bbo.is_valid() { bbo.spread_bps() } else { 0.0 },
            inventory_before: req.inventory_before,
            param_version: req.param_version,
        };
        self.orders.insert(id, o);
        self.by_sym.entry(req.sym).or_default().push(id);
        id
    }

    pub fn cancel(&mut self, now: i64, id: u64) {
        if let Some(o) = self.orders.get_mut(&id) {
            if o.is_live() && o.cancel_at.is_none() {
                o.cancel_at = Some(now + self.latency_ms);
                if o.state == State::Active {
                    o.state = State::CancelPending;
                }
            }
        }
    }

    pub fn cancel_all(&mut self, now: i64, sym: SymbolId) {
        let ids: Vec<u64> = self.live_orders(sym).map(|o| o.id).collect();
        for id in ids {
            self.cancel(now, id);
        }
    }

    fn finish(&mut self, id: u64, now: i64, status: &'static str) {
        if let Some(o) = self.orders.remove(&id) {
            if let Some(v) = self.by_sym.get_mut(&o.sym) {
                v.retain(|x| *x != id);
            }
            self.done.push(OrderDone {
                order_id: o.id,
                sym: o.sym,
                side: o.side,
                price: o.price,
                qty: o.qty,
                filled: o.filled,
                ts_created: o.ts_created,
                ts_done: now,
                status,
                purpose: o.purpose,
                queue_ahead_initial: o.queue_ahead_initial,
                spread_bps_at_place: o.spread_bps_at_place,
                param_version: o.param_version,
            });
        }
    }

    fn emit_fill(&mut self, id: u64, now: i64, qty: f64, price: f64, is_maker: bool) -> bool {
        let fee_rate = if is_maker { self.maker_fee } else { self.taker_fee };
        let (fill, complete) = {
            let o = self.orders.get_mut(&id).unwrap();
            o.filled += qty;
            let bbo = self.last_bbo.get(&o.sym).copied().unwrap_or_default();
            let f = Fill {
                order_id: o.id,
                sym: o.sym,
                side: o.side,
                price,
                qty,
                fee: qty * price * fee_rate,
                ts: now,
                is_maker,
                purpose: o.purpose,
                bid: bbo.bid,
                ask: bbo.ask,
                placed_ts: o.ts_created,
                mid_at_place: o.mid_at_place,
                spread_bps_at_place: o.spread_bps_at_place,
                queue_ahead_initial: o.queue_ahead_initial,
                inventory_before: o.inventory_before,
                param_version: o.param_version,
            };
            (f, o.remaining() <= self.qty_eps)
        };
        self.fills.push(fill);
        if complete {
            self.finish(id, now, "filled");
        }
        complete
    }

    /// Activate pending orders and finalize cancels whose latency elapsed.
    pub fn on_time(&mut self, now: i64) {
        let ids: Vec<u64> = self.orders.iter().filter(|(_, o)| o.state == State::Pending && o.ts_active <= now || o.cancel_at.is_some_and(|c| c <= now)).map(|(id, _)| *id).collect();
        for id in ids {
            let (pending_activation, cancel_due) = {
                let o = &self.orders[&id];
                (o.state == State::Pending && o.ts_active <= now, o.cancel_at.is_some_and(|c| c <= now))
            };
            if cancel_due {
                let st = if self.orders[&id].filled > 0.0 { "partial_cancelled" } else { "cancelled" };
                self.finish(id, now, st);
                continue;
            }
            if pending_activation {
                self.activate(id, now);
            }
        }
    }

    fn activate(&mut self, id: u64, now: i64) {
        let (sym, side, price, taker) = {
            let o = &self.orders[&id];
            (o.sym, o.side, o.price, o.taker)
        };
        let bbo = self.last_bbo.get(&sym).copied().unwrap_or_default();
        if !bbo.is_valid() {
            // no market: keep pending until we see a book
            return;
        }
        if taker {
            // market-like: fill at the touch on the opposite side, taker fee
            let px = if side == Side::Buy { bbo.ask } else { bbo.bid };
            let rem = self.orders[&id].remaining();
            self.orders.get_mut(&id).unwrap().state = State::Active;
            self.emit_fill(id, now, rem, px, false);
            return;
        }
        // post-only check
        let crosses = match side {
            Side::Buy => price >= bbo.ask,
            Side::Sell => price <= bbo.bid,
        };
        if crosses {
            self.finish(id, now, "rejected_post_only");
            return;
        }
        let ahead = match side {
            Side::Buy => {
                if price == bbo.bid {
                    bbo.bid_qty
                } else if price > bbo.bid {
                    0.0
                } else {
                    // deeper than best: unknown depth for L1 feeds; assume best-size as a proxy
                    bbo.bid_qty
                }
            }
            Side::Sell => {
                if price == bbo.ask {
                    bbo.ask_qty
                } else if price < bbo.ask {
                    0.0
                } else {
                    bbo.ask_qty
                }
            }
        };
        let o = self.orders.get_mut(&id).unwrap();
        o.state = State::Active;
        o.ts_active = now;
        o.queue_ahead = ahead;
        o.queue_ahead_initial = ahead;
    }

    /// Top-of-book update: refresh queue estimates and fill orders the market moved through.
    pub fn on_bbo(&mut self, now: i64, sym: SymbolId, bbo: &Bbo) {
        self.last_bbo.insert(sym, *bbo);
        if !bbo.is_valid() {
            return;
        }
        let ids: Vec<u64> = self.by_sym.get(&sym).cloned().unwrap_or_default();
        for id in ids {
            let (state, side, price) = match self.orders.get(&id) {
                Some(o) => (o.state, o.side, o.price),
                None => continue,
            };
            if state == State::Pending {
                continue;
            }
            let swept = match side {
                Side::Buy => bbo.ask <= price,
                Side::Sell => bbo.bid >= price,
            };
            if swept {
                let rem = self.orders[&id].remaining();
                self.emit_fill(id, now, rem, price, true);
                continue;
            }
            // queue shrink from cancellations ahead of us
            let o = self.orders.get_mut(&id).unwrap();
            match side {
                Side::Buy if price == bbo.bid => o.queue_ahead = o.queue_ahead.min(bbo.bid_qty),
                Side::Sell if price == bbo.ask => o.queue_ahead = o.queue_ahead.min(bbo.ask_qty),
                Side::Buy if price > bbo.bid => o.queue_ahead = 0.0,
                Side::Sell if price < bbo.ask => o.queue_ahead = 0.0,
                _ => {}
            }
        }
    }

    /// Public trade: consume queue ahead of us, then fill.
    pub fn on_trade(&mut self, now: i64, sym: SymbolId, t: &Trade) {
        let ids: Vec<u64> = self.by_sym.get(&sym).cloned().unwrap_or_default();
        for id in ids {
            let (state, side, price) = match self.orders.get(&id) {
                Some(o) => (o.state, o.side, o.price),
                None => continue,
            };
            if state == State::Pending {
                continue;
            }
            // a taker SELL executes against resting BIDS (ours is a bid => side Buy)
            let hits_us = match side {
                Side::Buy => t.taker_side == Side::Sell && t.price <= price,
                Side::Sell => t.taker_side == Side::Buy && t.price >= price,
            };
            if !hits_us {
                continue;
            }
            let through = match side {
                Side::Buy => t.price < price,
                Side::Sell => t.price > price,
            };
            let rem = self.orders[&id].remaining();
            if through {
                self.emit_fill(id, now, rem, price, true);
                continue;
            }
            let o = self.orders.get_mut(&id).unwrap();
            let mut v = t.qty;
            if o.queue_ahead > 0.0 {
                let eat = v.min(o.queue_ahead);
                o.queue_ahead -= eat;
                v -= eat;
            }
            if v > self.qty_eps {
                let q = v.min(rem);
                self.emit_fill(id, now, q, price, true);
            }
        }
    }

    pub fn drain(&mut self) -> (Vec<Fill>, Vec<OrderDone>) {
        (std::mem::take(&mut self.fills), std::mem::take(&mut self.done))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bbo(bid: f64, ask: f64, bq: f64, aq: f64) -> Bbo {
        Bbo { ts: 0, bid, ask, bid_qty: bq, ask_qty: aq }
    }

    fn req(side: Side, price: f64, qty: f64) -> PlaceReq {
        PlaceReq { sym: 0, side, price, qty, taker: false, purpose: Purpose::Entry, inventory_before: 0.0, param_version: 1 }
    }

    #[test]
    fn queue_then_fill_on_trades() {
        let mut ex = PaperExchange::new(50, 0.0002, 0.00055);
        ex.on_bbo(0, 0, &bbo(100.0, 100.1, 10.0, 10.0));
        let id = ex.place(0, req(Side::Buy, 100.0, 5.0));
        ex.on_time(49); // still pending
        ex.on_trade(49, 0, &Trade { ts: 49, price: 100.0, qty: 100.0, taker_side: Side::Sell });
        assert!(ex.fills.is_empty());
        ex.on_time(50); // active, queue ahead = 10
        assert_eq!(ex.order(id).unwrap().queue_ahead, 10.0);
        ex.on_trade(60, 0, &Trade { ts: 60, price: 100.0, qty: 8.0, taker_side: Side::Sell });
        assert!(ex.fills.is_empty());
        assert_eq!(ex.order(id).unwrap().queue_ahead, 2.0);
        // taker buy at our price does not hit a bid
        ex.on_trade(61, 0, &Trade { ts: 61, price: 100.0, qty: 8.0, taker_side: Side::Buy });
        assert!(ex.fills.is_empty());
        ex.on_trade(70, 0, &Trade { ts: 70, price: 100.0, qty: 5.0, taker_side: Side::Sell });
        assert_eq!(ex.fills.len(), 1);
        assert_eq!(ex.fills[0].qty, 3.0);
        assert!(ex.order(id).is_some());
        // trade through our price fills the rest
        ex.on_trade(80, 0, &Trade { ts: 80, price: 99.9, qty: 0.1, taker_side: Side::Sell });
        assert_eq!(ex.fills.len(), 2);
        assert_eq!(ex.fills[1].qty, 2.0);
        assert!(ex.order(id).is_none());
        assert_eq!(ex.done.len(), 1);
        assert_eq!(ex.done[0].status, "filled");
        let fee: f64 = ex.fills.iter().map(|f| f.fee).sum();
        assert!((fee - 5.0 * 100.0 * 0.0002).abs() < 1e-9);
    }

    #[test]
    fn cancel_has_latency_and_can_fill_meanwhile() {
        let mut ex = PaperExchange::new(50, 0.0002, 0.00055);
        ex.on_bbo(0, 0, &bbo(100.0, 100.1, 0.0, 10.0));
        let id = ex.place(0, req(Side::Buy, 100.0, 5.0));
        ex.on_time(50);
        ex.cancel(60, id);
        assert!(ex.order(id).unwrap().is_cancel_pending());
        ex.on_trade(70, 0, &Trade { ts: 70, price: 100.0, qty: 1.0, taker_side: Side::Sell });
        assert_eq!(ex.fills.len(), 1);
        ex.on_time(110);
        assert!(ex.order(id).is_none());
        assert_eq!(ex.done[0].status, "partial_cancelled");
        assert_eq!(ex.done[0].filled, 1.0);
    }

    #[test]
    fn post_only_reject_and_sweep_fill() {
        let mut ex = PaperExchange::new(0, 0.0002, 0.00055);
        ex.on_bbo(0, 0, &bbo(100.0, 100.1, 1.0, 1.0));
        let id = ex.place(0, req(Side::Sell, 100.0, 1.0)); // at the bid => crosses
        ex.on_time(0);
        assert!(ex.order(id).is_none());
        assert_eq!(ex.done[0].status, "rejected_post_only");
        let id2 = ex.place(1, req(Side::Sell, 100.1, 1.0));
        ex.on_time(1);
        assert_eq!(ex.order(id2).unwrap().queue_ahead, 1.0);
        // market lifts through our ask
        ex.on_bbo(2, 0, &bbo(100.2, 100.3, 1.0, 1.0));
        assert_eq!(ex.fills.len(), 1);
        assert_eq!(ex.fills[0].price, 100.1);
        assert_eq!(ex.fills[0].side, Side::Sell);
    }

    #[test]
    fn taker_order_fills_at_touch_with_taker_fee() {
        let mut ex = PaperExchange::new(10, 0.0002, 0.00055);
        ex.on_bbo(0, 0, &bbo(100.0, 100.1, 1.0, 1.0));
        let mut r = req(Side::Buy, 100.1, 2.0);
        r.taker = true;
        ex.place(0, r);
        ex.on_time(10);
        assert_eq!(ex.fills.len(), 1);
        assert_eq!(ex.fills[0].price, 100.1);
        assert!(!ex.fills[0].is_maker);
        assert!((ex.fills[0].fee - 2.0 * 100.1 * 0.00055).abs() < 1e-9);
    }
}
