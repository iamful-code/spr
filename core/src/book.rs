//! Local order book kept from Bybit orderbook snapshots/deltas. Prices are keyed by
//! integer ticks so float noise never produces duplicate levels.

use crate::types::{Bbo, SymbolMeta};
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct Book {
    bids: BTreeMap<i64, f64>,
    asks: BTreeMap<i64, f64>,
    tick_size: f64,
    price_scale: u32,
    pub last_ts: i64,
    pub has_snapshot: bool,
}

impl Book {
    pub fn new(meta: &SymbolMeta) -> Self {
        Self {
            tick_size: meta.tick_size,
            price_scale: meta.price_scale,
            ..Default::default()
        }
    }

    fn key(&self, price: f64) -> i64 {
        (price / self.tick_size).round() as i64
    }

    fn price(&self, key: i64) -> f64 {
        let p = key as f64 * self.tick_size;
        let scale = 10f64.powi(self.price_scale as i32);
        (p * scale).round() / scale
    }

    pub fn apply(&mut self, ts: i64, snapshot: bool, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        if snapshot {
            self.bids.clear();
            self.asks.clear();
            self.has_snapshot = true;
        }
        for &(p, q) in bids {
            let k = self.key(p);
            if q <= 0.0 {
                self.bids.remove(&k);
            } else {
                self.bids.insert(k, q);
            }
        }
        for &(p, q) in asks {
            let k = self.key(p);
            if q <= 0.0 {
                self.asks.remove(&k);
            } else {
                self.asks.insert(k, q);
            }
        }
        // Defensive: a delta that leaves the book crossed means we missed something;
        // drop the stale side levels until it is consistent again.
        while let (Some((&bk, _)), Some((&ak, _))) = (self.bids.iter().next_back(), self.asks.iter().next()) {
            if bk >= ak {
                // remove the older-looking one: keep the side that was updated in this message
                if !bids.is_empty() && asks.is_empty() {
                    self.asks.remove(&ak);
                } else {
                    self.bids.remove(&bk);
                }
            } else {
                break;
            }
        }
        self.last_ts = ts;
    }

    pub fn bbo(&self) -> Option<Bbo> {
        let (&bk, &bq) = self.bids.iter().next_back()?;
        let (&ak, &aq) = self.asks.iter().next()?;
        let b = Bbo {
            ts: self.last_ts,
            bid: self.price(bk),
            ask: self.price(ak),
            bid_qty: bq,
            ask_qty: aq,
        };
        if b.is_valid() {
            Some(b)
        } else {
            None
        }
    }

    /// Displayed size resting at an exact price on a side (0 if none).
    pub fn size_at(&self, is_bid: bool, price: f64) -> f64 {
        let k = self.key(price);
        if is_bid {
            self.bids.get(&k).copied().unwrap_or(0.0)
        } else {
            self.asks.get(&k).copied().unwrap_or(0.0)
        }
    }

    pub fn depth(&self) -> (usize, usize) {
        (self.bids.len(), self.asks.len())
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
            tick_size: 0.01,
            qty_step: 0.1,
            min_qty: 0.1,
            max_qty: 1e6,
            min_notional: 5.0,
            price_scale: 2,
            turnover_24h: 0.0,
        }
    }

    #[test]
    fn snapshot_delta_and_bbo() {
        let mut b = Book::new(&meta());
        b.apply(1, true, &[(100.0, 5.0), (99.99, 2.0)], &[(100.02, 3.0), (100.03, 1.0)]);
        let q = b.bbo().unwrap();
        assert_eq!(q.bid, 100.0);
        assert_eq!(q.ask, 100.02);
        assert_eq!(q.bid_qty, 5.0);
        // delete best bid, new best is 99.99
        b.apply(2, false, &[(100.0, 0.0)], &[]);
        assert_eq!(b.bbo().unwrap().bid, 99.99);
        // improve ask inside
        b.apply(3, false, &[], &[(100.01, 0.5)]);
        let q = b.bbo().unwrap();
        assert_eq!(q.ask, 100.01);
        assert_eq!(q.ask_qty, 0.5);
        assert_eq!(b.size_at(false, 100.02), 3.0);
        assert_eq!(b.size_at(true, 50.0), 0.0);
    }

    #[test]
    fn crossed_book_is_repaired() {
        let mut b = Book::new(&meta());
        b.apply(1, true, &[(100.0, 1.0)], &[(100.02, 1.0)]);
        // a bid arrives above the stale ask: the ask must go
        b.apply(2, false, &[(100.05, 1.0)], &[]);
        let q = b.bbo();
        assert!(q.is_none() || q.unwrap().ask > q.unwrap().bid);
    }
}
