//! Shared value types used by every module (live engine, paper exchange, replay).

use serde::{Deserialize, Serialize};

/// Dense index of a symbol inside one run. Stable for the lifetime of the process.
pub type SymbolId = u32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn sign(self) -> f64 {
        match self {
            Side::Buy => 1.0,
            Side::Sell => -1.0,
        }
    }
    pub fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "Buy",
            Side::Sell => "Sell",
        }
    }
    pub fn as_u8(self) -> u8 {
        match self {
            Side::Buy => 0,
            Side::Sell => 1,
        }
    }
    pub fn from_u8(v: u8) -> Side {
        if v == 0 {
            Side::Buy
        } else {
            Side::Sell
        }
    }
}

/// Static instrument description (from REST instruments-info) plus slowly changing
/// fields refreshed from the tickers endpoint.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SymbolMeta {
    pub id: SymbolId,
    pub name: String,
    pub base_coin: String,
    pub quote_coin: String,
    pub tick_size: f64,
    pub qty_step: f64,
    pub min_qty: f64,
    pub max_qty: f64,
    pub min_notional: f64,
    pub price_scale: u32,
    /// 24h turnover in quote currency, refreshed periodically (0 if unknown).
    pub turnover_24h: f64,
}

impl SymbolMeta {
    pub fn price_to_ticks(&self, price: f64) -> i64 {
        (price / self.tick_size).round() as i64
    }
    pub fn ticks_to_price(&self, ticks: i64) -> f64 {
        let p = ticks as f64 * self.tick_size;
        // round to the instrument's decimal places to kill float noise
        let scale = 10f64.powi(self.price_scale as i32);
        (p * scale).round() / scale
    }
    pub fn round_price(&self, price: f64) -> f64 {
        self.ticks_to_price(self.price_to_ticks(price))
    }
    /// Round quantity DOWN to the lot step; returns 0 if below min_qty.
    pub fn round_qty_down(&self, qty: f64) -> f64 {
        if self.qty_step <= 0.0 {
            return qty;
        }
        let steps = (qty / self.qty_step + 1e-9).floor();
        let q = steps * self.qty_step;
        let q = (q * 1e10).round() / 1e10;
        if q < self.min_qty - 1e-12 {
            0.0
        } else {
            q.min(self.max_qty)
        }
    }
    pub fn tick_bps(&self, price: f64) -> f64 {
        if price <= 0.0 {
            0.0
        } else {
            self.tick_size / price * 1e4
        }
    }
}

/// Best bid / offer snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Bbo {
    pub ts: i64,
    pub bid: f64,
    pub ask: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
}

impl Bbo {
    pub fn is_valid(&self) -> bool {
        self.bid > 0.0 && self.ask > 0.0 && self.ask > self.bid
    }
    pub fn mid(&self) -> f64 {
        (self.bid + self.ask) * 0.5
    }
    pub fn spread_bps(&self) -> f64 {
        let m = self.mid();
        if m > 0.0 {
            (self.ask - self.bid) / m * 1e4
        } else {
            0.0
        }
    }
    /// Size imbalance at the top of book in [-1, 1] (positive = more bid size).
    pub fn imbalance(&self) -> f64 {
        let tot = self.bid_qty + self.ask_qty;
        if tot > 0.0 {
            (self.bid_qty - self.ask_qty) / tot
        } else {
            0.0
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Trade {
    pub ts: i64,
    pub price: f64,
    pub qty: f64,
    /// Side of the taker (aggressor).
    pub taker_side: Side,
}

/// Events flowing from a feed (WebSocket, synthetic generator or replay) into the engine.
#[derive(Clone, Debug)]
pub enum MarketEvent {
    /// Level-2 update (snapshot or delta) from the WebSocket orderbook topic.
    Book {
        sym: SymbolId,
        ts: i64,
        snapshot: bool,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
    },
    /// Direct top-of-book update (replay and synthetic feed).
    Bbo { sym: SymbolId, bbo: Bbo },
    Trades { sym: SymbolId, trades: Vec<Trade> },
    /// 24h turnover refresh from REST tickers: (symbol id, turnover).
    Turnover(Vec<(SymbolId, f64)>),
    /// Connection status message for logging / dashboard.
    Status { conn: usize, msg: String },
}

/// Why an order was placed: helps analytics separate entries from exits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Purpose {
    Entry,
    Exit,
    StaleExit,
}

impl Purpose {
    pub fn as_str(self) -> &'static str {
        match self {
            Purpose::Entry => "entry",
            Purpose::Exit => "exit",
            Purpose::StaleExit => "stale_exit",
        }
    }
}

/// A (partial) execution of a paper order.
#[derive(Clone, Debug)]
pub struct Fill {
    pub order_id: u64,
    pub sym: SymbolId,
    pub side: Side,
    pub price: f64,
    pub qty: f64,
    pub fee: f64,
    pub ts: i64,
    pub is_maker: bool,
    pub purpose: Purpose,
    /// Market context at the moment of the fill.
    pub bid: f64,
    pub ask: f64,
    /// Order context at placement time.
    pub placed_ts: i64,
    pub mid_at_place: f64,
    pub spread_bps_at_place: f64,
    pub queue_ahead_initial: f64,
    pub inventory_before: f64,
    pub param_version: u32,
}

/// Final state of an order (written once, when it leaves the book).
#[derive(Clone, Debug)]
pub struct OrderDone {
    pub order_id: u64,
    pub sym: SymbolId,
    pub side: Side,
    pub price: f64,
    pub qty: f64,
    pub filled: f64,
    pub ts_created: i64,
    pub ts_done: i64,
    pub status: &'static str,
    pub purpose: Purpose,
    pub queue_ahead_initial: f64,
    pub spread_bps_at_place: f64,
    pub param_version: u32,
}

pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
