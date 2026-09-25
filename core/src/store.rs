//! Persistence thread: the engine sends `StoreMsg`s through an unbounded channel and
//! never waits for disk. One thread owns the SQLite connection (WAL mode, so the
//! dashboard and analytics can read concurrently) and the market-data recorder.
//! Messages are written in batches inside one transaction.

use crate::recorder::{BboRec, Recorder, TrdRec};
use crate::types::{now_ms, Fill, OrderDone, SymbolMeta};
use anyhow::{Context, Result};
use crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender};
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Debug)]
pub enum StoreMsg {
    RunStart { started_ts: i64, mode: String, config_json: String, symbols_json: String },
    RunEnd { ended_ts: i64 },
    Fill { fill: Fill, symbol: String, realized: f64 },
    OrderDone { done: OrderDone, symbol: String },
    PositionState { symbol: String, qty: f64, avg_price: f64, realized_pnl: f64, fees: f64, opened_ts: i64, last_mid: f64, updated_ts: i64 },
    SymbolStats(SymbolStatsRow),
    Pnl(PnlRow),
    Event { ts: i64, level: &'static str, msg: String },
    ParamVersion { version: u32, ts: i64, params_json: String },
    MdBbo(BboRec),
    MdTrade(TrdRec),
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct SymbolStatsRow {
    pub ts: i64,
    pub symbol: String,
    pub spread_med_bps: f64,
    pub spread_mean_bps: f64,
    pub vol_bps: f64,
    pub trades_per_min: f64,
    pub turnover_24h: f64,
    pub eligible: bool,
    pub reason: &'static str,
    pub score: f64,
    pub active: bool,
    pub bid: f64,
    pub ask: f64,
    pub position_qty: f64,
    pub quote_reason: &'static str,
}

#[derive(Debug, Clone)]
pub struct PnlRow {
    pub ts: i64,
    pub equity: f64,
    pub realized: f64,
    pub fees: f64,
    pub unrealized: f64,
    pub gross_exposure: f64,
    pub open_positions: usize,
    pub n_fills: u64,
    pub daily_pnl: f64,
    pub max_drawdown: f64,
    pub n_live_orders: usize,
    pub n_active_symbols: usize,
    pub halted: bool,
}

/// Cheap, cloneable handle. A handle created by `null_store` drops everything.
#[derive(Clone)]
pub struct StoreHandle {
    tx: Option<Sender<StoreMsg>>,
}

impl StoreHandle {
    pub fn send(&self, msg: StoreMsg) {
        if let Some(tx) = &self.tx {
            if tx.send(msg).is_err() {
                static LAST_WARN: AtomicI64 = AtomicI64::new(0);
                let now = now_ms();
                if now - LAST_WARN.load(Ordering::Relaxed) > 60_000 {
                    LAST_WARN.store(now, Ordering::Relaxed);
                    tracing::error!("store thread is gone: nothing is being written to the database or market data files");
                }
            }
        }
    }
    pub fn is_null(&self) -> bool {
        self.tx.is_none()
    }
}

pub struct Store {
    pub handle: StoreHandle,
    join: Option<JoinHandle<()>>,
}

impl Store {
    /// Flush everything and stop the thread.
    pub fn close(mut self) {
        self.handle.send(StoreMsg::Shutdown);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

pub fn null_store() -> StoreHandle {
    StoreHandle { tx: None }
}

pub const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY,
    started_ts INTEGER NOT NULL,
    ended_ts INTEGER,
    mode TEXT NOT NULL,
    config_json TEXT,
    symbols_json TEXT
);
CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    price REAL NOT NULL,
    qty REAL NOT NULL,
    fee REAL NOT NULL,
    ts INTEGER NOT NULL,
    is_maker INTEGER NOT NULL,
    purpose TEXT NOT NULL,
    bid REAL, ask REAL,
    placed_ts INTEGER,
    mid_at_place REAL,
    spread_bps_at_place REAL,
    queue_ahead_initial REAL,
    inventory_before REAL,
    param_version INTEGER,
    realized_pnl REAL
);
CREATE INDEX IF NOT EXISTS fills_run_sym_ts ON fills(run_id, symbol, ts);
CREATE INDEX IF NOT EXISTS fills_ts ON fills(ts);
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    price REAL NOT NULL,
    qty REAL NOT NULL,
    filled REAL NOT NULL,
    ts_created INTEGER NOT NULL,
    ts_done INTEGER NOT NULL,
    status TEXT NOT NULL,
    purpose TEXT NOT NULL,
    queue_ahead_initial REAL,
    spread_bps_at_place REAL,
    param_version INTEGER
);
CREATE INDEX IF NOT EXISTS orders_run_sym ON orders(run_id, symbol, ts_done);
CREATE TABLE IF NOT EXISTS symbol_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    spread_med_bps REAL, spread_mean_bps REAL, vol_bps REAL, trades_per_min REAL,
    turnover_24h REAL,
    eligible INTEGER, reason TEXT, score REAL, active INTEGER,
    bid REAL, ask REAL, position_qty REAL, quote_reason TEXT
);
CREATE INDEX IF NOT EXISTS symbol_stats_ts ON symbol_stats(run_id, ts);
CREATE INDEX IF NOT EXISTS symbol_stats_sym ON symbol_stats(symbol, ts);
CREATE TABLE IF NOT EXISTS pnl_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    equity REAL, realized REAL, fees REAL, unrealized REAL,
    gross_exposure REAL, open_positions INTEGER, n_fills INTEGER,
    daily_pnl REAL, max_drawdown REAL, n_live_orders INTEGER, n_active_symbols INTEGER, halted INTEGER
);
CREATE INDEX IF NOT EXISTS pnl_ts ON pnl_snapshots(run_id, ts);
CREATE TABLE IF NOT EXISTS position_state (
    run_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    qty REAL, avg_price REAL, realized_pnl REAL, fees REAL, opened_ts INTEGER, last_mid REAL, updated_ts INTEGER,
    PRIMARY KEY (run_id, symbol)
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    level TEXT NOT NULL,
    msg TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS param_versions (
    run_id INTEGER NOT NULL,
    version INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    params_json TEXT NOT NULL,
    PRIMARY KEY (run_id, version)
);
-- written by the Python side
CREATE TABLE IF NOT EXISTS recommendations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_ts INTEGER NOT NULL,
    run_id INTEGER,
    symbol TEXT,
    rule TEXT NOT NULL,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    param TEXT,
    current_value TEXT,
    suggested_value TEXT,
    evidence_json TEXT,
    status TEXT NOT NULL DEFAULT 'open'
);
CREATE TABLE IF NOT EXISTS optimizer_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_ts INTEGER NOT NULL,
    symbol TEXT,
    n_trials INTEGER,
    train_from INTEGER, train_to INTEGER, valid_from INTEGER, valid_to INTEGER,
    best_params_json TEXT,
    train_score REAL, valid_score REAL, baseline_valid_score REAL,
    applied INTEGER NOT NULL DEFAULT 0,
    note TEXT
);
CREATE TABLE IF NOT EXISTS pair_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_ts INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    score REAL,
    spread_med_bps REAL, trades_per_min REAL, vol_bps REAL,
    realized_pnl REAL, n_roundtrips INTEGER,
    verdict TEXT
);
"#;

pub fn open_db(path: &Path) -> Result<Connection> {
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir)?;
        }
    }
    let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "busy_timeout", 30_000)?;
    conn.execute_batch(SCHEMA)?;
    Ok(conn)
}

/// Start the writer thread. `record_md = false` disables the binary recorder.
pub fn open_store(db_path: &Path, md_dir: &Path, run_id: i64, symbols: Vec<SymbolMeta>, record_md: bool) -> Result<Store> {
    let conn = open_db(db_path)?;
    let recorder = if record_md { Some(Recorder::new(md_dir, run_id, symbols)) } else { None };
    let (tx, rx) = unbounded::<StoreMsg>();
    let join = std::thread::Builder::new()
        .name("spr-store".into())
        .spawn(move || writer_loop(conn, recorder, run_id, rx))
        .context("spawning store thread")?;
    Ok(Store { handle: StoreHandle { tx: Some(tx) }, join: Some(join) })
}

fn writer_loop(mut conn: Connection, mut recorder: Option<Recorder>, run_id: i64, rx: Receiver<StoreMsg>) {
    let mut batch: Vec<StoreMsg> = Vec::with_capacity(2048);
    let mut shutdown = false;
    let mut consecutive_failures = 0u32;
    let mut md_error_logged_ms = 0i64;
    while !shutdown {
        batch.clear();
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(m) => batch.push(m),
            Err(RecvTimeoutError::Timeout) => {
                if let Some(r) = recorder.as_mut() {
                    if let Err(e) = r.flush() {
                        tracing::error!("market data flush failed: {e:#}");
                    }
                }
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
        // Gather what is already queued, bounded so a burst cannot starve durability.
        while batch.len() < 2000 {
            match rx.try_recv() {
                Ok(m) => batch.push(m),
                Err(_) => break,
            }
        }
        if batch.iter().any(|m| matches!(m, StoreMsg::Shutdown)) {
            shutdown = true;
        }
        // 1. Market data files: independent of SQLite, an error here never stops the rows.
        if let Some(rec) = recorder.as_mut() {
            for m in &batch {
                let r = match m {
                    StoreMsg::MdBbo(b) => rec.write_bbo(b),
                    StoreMsg::MdTrade(t) => rec.write_trade(t),
                    _ => Ok(()),
                };
                if let Err(e) = r {
                    let now = now_ms();
                    if now - md_error_logged_ms > 60_000 {
                        md_error_logged_ms = now;
                        tracing::error!("market data write failed (disk full?): {e:#}");
                    }
                    break;
                }
            }
            if let Err(e) = rec.flush() {
                tracing::error!("market data flush failed: {e:#}");
            }
        }
        // 2. Database rows in one transaction, retried on transient errors such as
        //    SQLITE_BUSY while the analytics side is writing. The thread never exits
        //    because of a write error: at worst one batch is dropped and logged.
        let n_rows = batch.iter().filter(|m| !matches!(m, StoreMsg::MdBbo(_) | StoreMsg::MdTrade(_) | StoreMsg::Shutdown)).count();
        if n_rows > 0 {
            let mut attempt = 0u32;
            loop {
                match write_batch(&mut conn, run_id, &batch) {
                    Ok(()) => {
                        if consecutive_failures > 0 {
                            tracing::info!("database writes recovered after {consecutive_failures} failed attempts");
                        }
                        consecutive_failures = 0;
                        break;
                    }
                    Err(e) => {
                        attempt += 1;
                        consecutive_failures += 1;
                        if attempt >= 5 {
                            tracing::error!("dropping {n_rows} database rows after {attempt} failed attempts: {e:#}");
                            break;
                        }
                        tracing::warn!("database write failed (attempt {attempt}): {e:#}; retrying in 1s");
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
            }
        }
    }
    if let Some(r) = recorder.as_mut() {
        if let Err(e) = r.flush() {
            tracing::error!("market data flush failed: {e:#}");
        }
    }
}

fn write_batch(conn: &mut Connection, run_id: i64, batch: &[StoreMsg]) -> Result<()> {
    let tx = conn.transaction()?;
    for m in batch {
        match m {
            StoreMsg::MdBbo(_) | StoreMsg::MdTrade(_) | StoreMsg::Shutdown => {}
            other => write_row(&tx, run_id, other)?,
        }
    }
    tx.commit()?;
    Ok(())
}

fn write_row(tx: &rusqlite::Transaction, run_id: i64, m: &StoreMsg) -> Result<()> {
    match m {
        StoreMsg::RunStart { started_ts, mode, config_json, symbols_json } => {
            tx.prepare_cached("INSERT OR REPLACE INTO runs(run_id, started_ts, mode, config_json, symbols_json) VALUES (?1, ?2, ?3, ?4, ?5)")?
                .execute(params![run_id, started_ts, mode, config_json, symbols_json])?;
        }
        StoreMsg::RunEnd { ended_ts } => {
            tx.prepare_cached("UPDATE runs SET ended_ts = ?2 WHERE run_id = ?1")?.execute(params![run_id, ended_ts])?;
        }
        StoreMsg::Fill { fill: f, symbol, realized } => {
            tx.prepare_cached(
                "INSERT INTO fills(run_id, order_id, symbol, side, price, qty, fee, ts, is_maker, purpose, bid, ask, placed_ts, mid_at_place, spread_bps_at_place, queue_ahead_initial, inventory_before, param_version, realized_pnl) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
            )?
            .execute(params![
                run_id,
                f.order_id as i64,
                symbol,
                f.side.as_str(),
                f.price,
                f.qty,
                f.fee,
                f.ts,
                f.is_maker as i32,
                f.purpose.as_str(),
                f.bid,
                f.ask,
                f.placed_ts,
                f.mid_at_place,
                f.spread_bps_at_place,
                f.queue_ahead_initial,
                f.inventory_before,
                f.param_version as i64,
                realized
            ])?;
        }
        StoreMsg::OrderDone { done: o, symbol } => {
            tx.prepare_cached(
                "INSERT INTO orders(run_id, order_id, symbol, side, price, qty, filled, ts_created, ts_done, status, purpose, queue_ahead_initial, spread_bps_at_place, param_version) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            )?
            .execute(params![
                run_id,
                o.order_id as i64,
                symbol,
                o.side.as_str(),
                o.price,
                o.qty,
                o.filled,
                o.ts_created,
                o.ts_done,
                o.status,
                o.purpose.as_str(),
                o.queue_ahead_initial,
                o.spread_bps_at_place,
                o.param_version as i64
            ])?;
        }
        StoreMsg::PositionState { symbol, qty, avg_price, realized_pnl, fees, opened_ts, last_mid, updated_ts } => {
            tx.prepare_cached(
                "INSERT OR REPLACE INTO position_state(run_id, symbol, qty, avg_price, realized_pnl, fees, opened_ts, last_mid, updated_ts) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )?
            .execute(params![run_id, symbol, qty, avg_price, realized_pnl, fees, opened_ts, last_mid, updated_ts])?;
        }
        StoreMsg::SymbolStats(s) => {
            tx.prepare_cached(
                "INSERT INTO symbol_stats(run_id, ts, symbol, spread_med_bps, spread_mean_bps, vol_bps, trades_per_min, turnover_24h, eligible, reason, score, active, bid, ask, position_qty, quote_reason) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            )?
            .execute(params![
                run_id,
                s.ts,
                s.symbol,
                s.spread_med_bps,
                s.spread_mean_bps,
                s.vol_bps,
                s.trades_per_min,
                s.turnover_24h,
                s.eligible as i32,
                s.reason,
                s.score,
                s.active as i32,
                s.bid,
                s.ask,
                s.position_qty,
                s.quote_reason
            ])?;
        }
        StoreMsg::Pnl(p) => {
            tx.prepare_cached(
                "INSERT INTO pnl_snapshots(run_id, ts, equity, realized, fees, unrealized, gross_exposure, open_positions, n_fills, daily_pnl, max_drawdown, n_live_orders, n_active_symbols, halted) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            )?
            .execute(params![
                run_id,
                p.ts,
                p.equity,
                p.realized,
                p.fees,
                p.unrealized,
                p.gross_exposure,
                p.open_positions as i64,
                p.n_fills as i64,
                p.daily_pnl,
                p.max_drawdown,
                p.n_live_orders as i64,
                p.n_active_symbols as i64,
                p.halted as i32
            ])?;
        }
        StoreMsg::Event { ts, level, msg } => {
            tx.prepare_cached("INSERT INTO events(run_id, ts, level, msg) VALUES (?1, ?2, ?3, ?4)")?.execute(params![run_id, ts, level, msg])?;
        }
        StoreMsg::ParamVersion { version, ts, params_json } => {
            tx.prepare_cached("INSERT OR REPLACE INTO param_versions(run_id, version, ts, params_json) VALUES (?1, ?2, ?3, ?4)")?
                .execute(params![run_id, *version as i64, ts, params_json])?;
        }
        StoreMsg::Shutdown | StoreMsg::MdBbo(_) | StoreMsg::MdTrade(_) => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Purpose, Side};

    #[test]
    fn writes_fill_and_event() {
        let dir = std::env::temp_dir().join(format!("spr_store_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("t.db");
        let store = open_store(&db, &dir.join("md"), 1, vec![], false).unwrap();
        store.handle.send(StoreMsg::RunStart { started_ts: 1, mode: "test".into(), config_json: "{}".into(), symbols_json: "[]".into() });
        let f = Fill {
            order_id: 9,
            sym: 0,
            side: Side::Buy,
            price: 1.0,
            qty: 2.0,
            fee: 0.0004,
            ts: 5,
            is_maker: true,
            purpose: Purpose::Entry,
            bid: 1.0,
            ask: 1.01,
            placed_ts: 1,
            mid_at_place: 1.005,
            spread_bps_at_place: 10.0,
            queue_ahead_initial: 3.0,
            inventory_before: 0.0,
            param_version: 1,
        };
        store.handle.send(StoreMsg::Fill { fill: f, symbol: "AUSDT".into(), realized: 0.0 });
        store.handle.send(StoreMsg::Event { ts: 6, level: "info", msg: "hello".into() });
        store.close();
        let conn = Connection::open(&db).unwrap();
        let n: i64 = conn.query_row("SELECT count(*) FROM fills WHERE symbol='AUSDT' AND run_id=1", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 1);
        let m: String = conn.query_row("SELECT msg FROM events", [], |r| r.get(0)).unwrap();
        assert_eq!(m, "hello");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
