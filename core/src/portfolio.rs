//! Positions and PnL with average-cost accounting per symbol.

use crate::types::{Fill, Side, SymbolId};
use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub struct Position {
    /// Signed quantity (positive = long).
    pub qty: f64,
    pub avg_price: f64,
    pub realized_pnl: f64,
    pub fees: f64,
    pub n_fills: u32,
    /// When the current non-flat position was opened (ms).
    pub opened_ts: i64,
    pub last_fill_ts: i64,
    pub last_mid: f64,
}

impl Position {
    pub fn is_flat(&self) -> bool {
        self.qty.abs() < 1e-12
    }
    pub fn unrealized(&self) -> f64 {
        if self.is_flat() || self.last_mid <= 0.0 {
            0.0
        } else {
            (self.last_mid - self.avg_price) * self.qty
        }
    }
    pub fn notional(&self) -> f64 {
        self.qty.abs() * if self.last_mid > 0.0 { self.last_mid } else { self.avg_price }
    }
    pub fn age_secs(&self, now: i64) -> i64 {
        if self.is_flat() {
            0
        } else {
            (now - self.opened_ts) / 1000
        }
    }
}

pub struct Portfolio {
    pub initial_equity: f64,
    pub positions: HashMap<SymbolId, Position>,
    pub realized_total: f64,
    pub fees_total: f64,
    pub n_fills: u64,
    pub day_start_equity: f64,
    pub day_key: i64,
    pub peak_equity: f64,
    pub max_drawdown: f64,
}

impl Portfolio {
    pub fn new(initial_equity: f64) -> Self {
        Self {
            initial_equity,
            positions: HashMap::new(),
            realized_total: 0.0,
            fees_total: 0.0,
            n_fills: 0,
            day_start_equity: initial_equity,
            day_key: -1,
            peak_equity: initial_equity,
            max_drawdown: 0.0,
        }
    }

    pub fn position(&self, sym: SymbolId) -> Option<&Position> {
        self.positions.get(&sym)
    }

    pub fn qty(&self, sym: SymbolId) -> f64 {
        self.positions.get(&sym).map_or(0.0, |p| p.qty)
    }

    pub fn on_mid(&mut self, sym: SymbolId, mid: f64) {
        if let Some(p) = self.positions.get_mut(&sym) {
            p.last_mid = mid;
        }
    }

    /// Apply a fill. Returns the realized PnL of this fill (before fees).
    pub fn on_fill(&mut self, f: &Fill) -> f64 {
        let p = self.positions.entry(f.sym).or_default();
        let signed = f.qty * f.side.sign();
        let mut realized = 0.0;
        if p.is_flat() || (p.qty > 0.0) == (signed > 0.0) {
            // opening or adding
            let new_qty = p.qty + signed;
            if p.is_flat() {
                p.opened_ts = f.ts;
                p.avg_price = f.price;
            } else {
                p.avg_price = (p.avg_price * p.qty.abs() + f.price * f.qty) / new_qty.abs();
            }
            p.qty = new_qty;
        } else {
            // reducing (possibly flipping)
            let close_qty = f.qty.min(p.qty.abs());
            let dir = if p.qty > 0.0 { 1.0 } else { -1.0 };
            realized = (f.price - p.avg_price) * close_qty * dir;
            let remaining = f.qty - close_qty;
            p.qty += signed;
            if p.qty.abs() < 1e-12 {
                p.qty = 0.0;
                p.avg_price = 0.0;
                p.opened_ts = 0;
            } else if remaining > 1e-12 {
                // flipped: the leftover opens a new position at the fill price
                p.avg_price = f.price;
                p.opened_ts = f.ts;
            }
        }
        p.realized_pnl += realized;
        p.fees += f.fee;
        p.n_fills += 1;
        p.last_fill_ts = f.ts;
        if p.last_mid <= 0.0 {
            p.last_mid = f.price;
        }
        self.realized_total += realized;
        self.fees_total += f.fee;
        self.n_fills += 1;
        realized
    }

    pub fn unrealized_total(&self) -> f64 {
        self.positions.values().map(|p| p.unrealized()).sum()
    }

    pub fn equity(&self) -> f64 {
        self.initial_equity + self.realized_total - self.fees_total + self.unrealized_total()
    }

    pub fn gross_exposure(&self) -> f64 {
        self.positions.values().map(|p| p.notional()).sum()
    }

    pub fn open_positions(&self) -> usize {
        self.positions.values().filter(|p| !p.is_flat()).count()
    }

    /// Update day boundary and drawdown bookkeeping. Returns daily PnL.
    pub fn mark(&mut self, now_ms: i64) -> f64 {
        let day = now_ms / 86_400_000;
        let eq = self.equity();
        if day != self.day_key {
            self.day_key = day;
            self.day_start_equity = eq;
        }
        if eq > self.peak_equity {
            self.peak_equity = eq;
        }
        let dd = self.peak_equity - eq;
        if dd > self.max_drawdown {
            self.max_drawdown = dd;
        }
        eq - self.day_start_equity
    }

    pub fn daily_pnl(&self) -> f64 {
        self.equity() - self.day_start_equity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Purpose;

    fn fill(side: Side, price: f64, qty: f64, ts: i64) -> Fill {
        Fill {
            order_id: 0,
            sym: 0,
            side,
            price,
            qty,
            fee: qty * price * 0.0002,
            ts,
            is_maker: true,
            purpose: Purpose::Entry,
            bid: 0.0,
            ask: 0.0,
            placed_ts: ts,
            mid_at_place: price,
            spread_bps_at_place: 0.0,
            queue_ahead_initial: 0.0,
            inventory_before: 0.0,
            param_version: 1,
        }
    }

    #[test]
    fn round_trip_and_flip() {
        let mut pf = Portfolio::new(1000.0);
        pf.on_fill(&fill(Side::Buy, 100.0, 2.0, 1000));
        pf.on_fill(&fill(Side::Buy, 102.0, 2.0, 2000));
        let p = pf.position(0).unwrap();
        assert_eq!(p.qty, 4.0);
        assert_eq!(p.avg_price, 101.0);
        assert_eq!(p.opened_ts, 1000);
        let r = pf.on_fill(&fill(Side::Sell, 103.0, 3.0, 3000));
        assert!((r - 6.0).abs() < 1e-9);
        // sell 3 more: closes remaining 1 at +2, opens short 2 at 103
        let r = pf.on_fill(&fill(Side::Sell, 103.0, 3.0, 4000));
        assert!((r - 2.0).abs() < 1e-9);
        let p = pf.position(0).unwrap();
        assert_eq!(p.qty, -2.0);
        assert_eq!(p.avg_price, 103.0);
        assert_eq!(p.opened_ts, 4000);
        pf.on_mid(0, 100.0);
        assert!((pf.unrealized_total() - 6.0).abs() < 1e-9);
        assert!(pf.fees_total > 0.0);
        assert!((pf.equity() - (1000.0 + 8.0 - pf.fees_total + 6.0)).abs() < 1e-9);
    }
}
