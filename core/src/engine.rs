//! The trading core. Single-threaded: every piece of mutable state lives here and is
//! touched only from `on_market` / `on_time`. The same code runs live (system clock,
//! WebSocket feed), in simulation (synthetic feed) and in replay (recorded feed, clock
//! = record timestamps), so what the optimizer measures is exactly what trades.

use crate::book::Book;
use crate::config::{Config, ConfigWatcher, Overrides, StrategyParams, SymbolLists};
use crate::eligibility::{self, Experience, Verdict};
use crate::paper::{PaperExchange, PlaceReq};
use crate::portfolio::Portfolio;
use crate::recorder::{BboRec, TrdRec};
use crate::risk::{self, RiskState};
use crate::stats::SymbolStats;
use crate::store::{PnlRow, StoreHandle, StoreMsg, SymbolStatsRow};
use crate::strategy::{compute_quotes, Quote, QuoteInput};
use crate::types::{Bbo, MarketEvent, Side, SymbolId, SymbolMeta};
use serde::Serialize;
use std::collections::HashMap;

pub struct SymbolState {
    pub meta: SymbolMeta,
    pub book: Book,
    pub stats: SymbolStats,
    pub bbo: Option<Bbo>,
    pub bid_order: Option<u64>,
    pub ask_order: Option<u64>,
    pub last_requote_ms: i64,
    pub verdict: Verdict,
    pub active: bool,
    pub params: StrategyParams,
    pub quote_reason: &'static str,
    last_rec_bbo_ts: i64,
    last_rec_bbo: Option<Bbo>,
    last_rec_ref_ts: i64,
    /// realized pnl and fees of the symbol when its current position was opened
    rt_open_marks: Option<(f64, f64)>,
    /// leading-venue top of book and its provider rank / arrival time
    pub ref_bbo: Option<Bbo>,
    ref_ts: i64,
    ref_rank: u8,
    /// EWMA of (our mid - reference mid): perps trade at a basis to spot / other venues
    pub basis: f64,
    basis_n: u32,
    pub last_ref_dev_bps: f64,
    /// online markouts after our own fills
    pub experience: Experience,
    pending_markouts: Vec<(i64, f64, f64)>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Counters {
    pub events: u64,
    pub books: u64,
    pub trades: u64,
    pub requotes: u64,
    pub placed: u64,
    pub cancelled: u64,
    pub orders_filled: u64,
    pub orders_done: u64,
    pub orders_rejected: u64,
    pub roundtrips: u64,
    pub roundtrip_wins: u64,
    pub ticks: u64,
    pub exposure_sum: f64,
}

/// End-of-run metrics, also the objective inputs for the optimizer.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Summary {
    pub net_pnl: f64,
    pub gross_pnl: f64,
    pub fees: f64,
    pub unrealized: f64,
    pub equity: f64,
    pub max_drawdown: f64,
    pub n_fills: u64,
    pub n_roundtrips: u64,
    pub win_rate: f64,
    pub fill_ratio: f64,
    pub avg_abs_inventory: f64,
    pub n_events: u64,
    pub n_placed: u64,
    pub n_cancelled: u64,
    pub per_symbol: HashMap<String, SymbolSummary>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct SymbolSummary {
    pub realized: f64,
    pub fees: f64,
    pub n_fills: u32,
    pub position_qty: f64,
    pub unrealized: f64,
}

pub struct Engine {
    pub cfg: Config,
    pub overrides: Overrides,
    pub lists: SymbolLists,
    pub symbols: Vec<SymbolState>,
    pub by_name: HashMap<String, SymbolId>,
    pub paper: PaperExchange,
    pub portfolio: Portfolio,
    pub risk: RiskState,
    pub halted: bool,
    pub store: StoreHandle,
    watcher: Option<ConfigWatcher>,
    pub param_version: u32,
    pub run_id: i64,
    record_md: bool,
    last_tick_sec: i64,
    last_refresh_ms: i64,
    last_stats_snap_ms: i64,
    last_pnl_snap_ms: i64,
    last_cfg_check_ms: i64,
    pub n_active: usize,
    pub counters: Counters,
    pub started_ts: i64,
}

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(cfg: Config, overrides: Overrides, lists: SymbolLists, metas: Vec<SymbolMeta>, store: StoreHandle, watcher: Option<ConfigWatcher>, run_id: i64, mode: &str, start_ts: i64) -> Self {
        let mut symbols = Vec::with_capacity(metas.len());
        let mut by_name = HashMap::with_capacity(metas.len());
        for (i, mut m) in metas.into_iter().enumerate() {
            m.id = i as SymbolId;
            by_name.insert(m.name.clone(), i as SymbolId);
            let params = overrides.resolve(&cfg.strategy, &m.name).unwrap_or_else(|_| cfg.strategy.clone());
            symbols.push(SymbolState {
                book: Book::new(&m),
                stats: SymbolStats::new(cfg.eligibility.window_secs),
                bbo: None,
                bid_order: None,
                ask_order: None,
                last_requote_ms: 0,
                verdict: Verdict { eligible: false, reason: "warming_up", score: 0.0 },
                active: false,
                params,
                quote_reason: "init",
                last_rec_bbo_ts: 0,
                last_rec_bbo: None,
                last_rec_ref_ts: 0,
                rt_open_marks: None,
                ref_bbo: None,
                ref_ts: 0,
                ref_rank: u8::MAX,
                basis: 0.0,
                basis_n: 0,
                last_ref_dev_bps: 0.0,
                experience: Experience::default(),
                pending_markouts: Vec::new(),
                meta: m,
            });
        }
        let paper = PaperExchange::new(cfg.paper.latency_ms, cfg.fees.maker_rate, cfg.fees.taker_rate);
        let portfolio = Portfolio::new(cfg.paper.initial_equity_usd);
        let record_md = cfg.recording.enabled && !store.is_null();
        let e = Engine {
            symbols,
            by_name,
            paper,
            portfolio,
            risk: RiskState { allow_entries: true, ..Default::default() },
            halted: false,
            store,
            watcher,
            param_version: 1,
            run_id,
            record_md,
            last_tick_sec: start_ts / 1000,
            last_refresh_ms: 0,
            last_stats_snap_ms: 0,
            last_pnl_snap_ms: 0,
            last_cfg_check_ms: start_ts,
            n_active: 0,
            counters: Counters::default(),
            started_ts: start_ts,
            cfg,
            overrides,
            lists,
        };
        let metas: Vec<&SymbolMeta> = e.symbols.iter().map(|s| &s.meta).collect();
        e.store.send(StoreMsg::RunStart {
            started_ts: start_ts,
            mode: mode.to_string(),
            config_json: serde_json::to_string(&e.cfg).unwrap_or_default(),
            symbols_json: serde_json::to_string(&metas).unwrap_or_default(),
        });
        e.store_param_version(start_ts);
        e
    }

    pub fn symbol_id(&self, name: &str) -> Option<SymbolId> {
        self.by_name.get(name).copied()
    }

    fn event(&self, ts: i64, level: &'static str, msg: String) {
        match level {
            "error" => tracing::error!("{msg}"),
            "warn" => tracing::warn!("{msg}"),
            _ => tracing::info!("{msg}"),
        }
        self.store.send(StoreMsg::Event { ts, level, msg });
    }

    fn store_param_version(&self, ts: i64) {
        let json = serde_json::json!({
            "strategy": self.cfg.strategy,
            "overrides": self.overrides,
            "lists": self.lists,
            "paper": self.cfg.paper,
            "fees": self.cfg.fees,
            "eligibility": self.cfg.eligibility,
            "risk": self.cfg.risk,
        });
        self.store.send(StoreMsg::ParamVersion { version: self.param_version, ts, params_json: json.to_string() });
    }

    // ------------------------------------------------------------------------------
    // Inputs
    // ------------------------------------------------------------------------------

    /// Feed one market event. `now` is the engine clock (system time live, record time in replay).
    pub fn on_market(&mut self, ev: MarketEvent, now: i64) {
        self.counters.events += 1;
        self.paper.on_time(now);
        match ev {
            MarketEvent::Book { sym, ts, snapshot, bids, asks } => {
                let idx = sym as usize;
                if idx >= self.symbols.len() {
                    return;
                }
                self.counters.books += 1;
                let st = &mut self.symbols[idx];
                st.book.apply(ts, snapshot, &bids, &asks);
                if let Some(bbo) = st.book.bbo() {
                    self.handle_bbo(sym, bbo, now);
                }
            }
            MarketEvent::Bbo { sym, bbo } => {
                if (sym as usize) < self.symbols.len() {
                    self.counters.books += 1;
                    self.handle_bbo(sym, bbo, now);
                }
            }
            MarketEvent::Trades { sym, trades } => {
                let idx = sym as usize;
                if idx >= self.symbols.len() {
                    return;
                }
                let record = self.record_md && self.should_record(idx);
                for t in &trades {
                    self.counters.trades += 1;
                    self.symbols[idx].stats.on_trade(t, now);
                    self.paper.on_trade(now, sym, t);
                    if record {
                        self.store.send(StoreMsg::MdTrade(TrdRec { ts: now, sym, side: t.taker_side.as_u8(), price: t.price, qty: t.qty }));
                    }
                }
                self.drain_fills(now);
            }
            MarketEvent::Turnover(v) => {
                for (sym, t) in v {
                    if let Some(st) = self.symbols.get_mut(sym as usize) {
                        st.meta.turnover_24h = t;
                    }
                }
            }
            MarketEvent::Reference { sym, ts, bid, ask, rank } => {
                let idx = sym as usize;
                if idx >= self.symbols.len() || !(bid > 0.0 && ask >= bid) {
                    return;
                }
                let stale = self.cfg.reference.stale_ms;
                let st = &mut self.symbols[idx];
                // a better provider always wins; a worse one only fills in when the better one is silent
                if rank <= st.ref_rank || now - st.ref_ts > stale {
                    st.ref_rank = rank;
                    st.ref_ts = now;
                    let rb = Bbo { ts, bid, ask, bid_qty: 0.0, ask_qty: 0.0 };
                    st.ref_bbo = Some(rb);
                    if let Some(ours) = st.bbo {
                        let sample = ours.mid() - rb.mid();
                        if st.basis_n == 0 {
                            st.basis = sample;
                        } else {
                            let a = self.cfg.reference.basis_alpha;
                            st.basis += a * (sample - st.basis);
                        }
                        st.basis_n += 1;
                    }
                    if self.record_md && self.should_record(idx) {
                        let st = &mut self.symbols[idx];
                        if now - st.last_rec_ref_ts >= self.cfg.recording.bbo_min_interval_ms {
                            st.last_rec_ref_ts = now;
                            self.store.send(StoreMsg::MdRef(BboRec { ts: now, sym, bid, ask, bid_qty: 0.0, ask_qty: 0.0 }));
                        }
                    }
                    // a leading-venue move is exactly when our quotes must be re-checked
                    self.requote(sym, now);
                }
            }
            MarketEvent::Status { conn, msg } => {
                self.event(now, "info", format!("ws[{conn}]: {msg}"));
            }
        }
        self.maybe_tick(now);
    }

    /// (reference mid + basis - our mid) / our mid in bps, 0 when the reference is stale or unknown.
    fn ref_dev_bps(&self, idx: usize, now: i64) -> f64 {
        let st = &self.symbols[idx];
        match (st.ref_bbo, st.bbo) {
            (Some(r), Some(o)) if now - st.ref_ts <= self.cfg.reference.stale_ms && st.basis_n >= 5 => {
                let fair = r.mid() + st.basis;
                let m = o.mid();
                if m > 0.0 {
                    (fair - m) / m * 1e4
                } else {
                    0.0
                }
            }
            _ => 0.0,
        }
    }

    /// Resolve the markouts of fills whose horizon has passed and fold them into the EWMA.
    fn resolve_markouts(&mut self, now: i64) {
        for st in self.symbols.iter_mut() {
            if st.pending_markouts.is_empty() {
                continue;
            }
            let Some(bbo) = st.bbo else { continue };
            let mid = bbo.mid();
            let mut i = 0;
            while i < st.pending_markouts.len() {
                let (due, sign, mid0) = st.pending_markouts[i];
                if due <= now {
                    if mid0 > 0.0 && mid > 0.0 {
                        let mo = sign * (mid - mid0) / mid0 * 1e4;
                        let e = &mut st.experience;
                        if e.markout_n == 0 {
                            e.markout_bps = mo;
                        } else {
                            // running mean for the first samples, then an EWMA over ~30 fills
                            let a = (1.0 / (e.markout_n as f64 + 1.0)).max(1.0 / 30.0);
                            e.markout_bps += a * (mo - e.markout_bps);
                        }
                        e.markout_n += 1;
                    }
                    st.pending_markouts.swap_remove(i);
                } else {
                    i += 1;
                }
            }
        }
    }

    /// Advance the clock without a market event (timers in the live loop).
    pub fn on_time(&mut self, now: i64) {
        self.paper.on_time(now);
        self.drain_fills(now);
        self.maybe_tick(now);
    }

    fn should_record(&self, idx: usize) -> bool {
        match self.cfg.recording.symbols.as_str() {
            "eligible" => {
                let st = &self.symbols[idx];
                st.active || st.verdict.eligible
            }
            _ => true,
        }
    }

    fn handle_bbo(&mut self, sym: SymbolId, bbo: Bbo, now: i64) {
        let idx = sym as usize;
        {
            let st = &mut self.symbols[idx];
            st.bbo = Some(bbo);
            st.stats.on_bbo(&bbo, now);
        }
        self.paper.on_bbo(now, sym, &bbo);
        self.portfolio.on_mid(sym, bbo.mid());
        if self.record_md && self.should_record(idx) {
            let st = &mut self.symbols[idx];
            let changed = st.last_rec_bbo.is_none_or(|p| p.bid != bbo.bid || p.ask != bbo.ask || p.bid_qty != bbo.bid_qty || p.ask_qty != bbo.ask_qty);
            if changed && now - st.last_rec_bbo_ts >= self.cfg.recording.bbo_min_interval_ms {
                st.last_rec_bbo_ts = now;
                st.last_rec_bbo = Some(bbo);
                self.store.send(StoreMsg::MdBbo(BboRec { ts: now, sym, bid: bbo.bid, ask: bbo.ask, bid_qty: bbo.bid_qty, ask_qty: bbo.ask_qty }));
            }
        }
        self.drain_fills(now);
        self.requote(sym, now);
    }

    // ------------------------------------------------------------------------------
    // Fills and orders
    // ------------------------------------------------------------------------------

    fn drain_fills(&mut self, now: i64) {
        if self.paper.fills.is_empty() && self.paper.done.is_empty() {
            return;
        }
        let (fills, done) = self.paper.drain();
        for f in fills {
            let idx = f.sym as usize;
            let was_flat = self.portfolio.qty(f.sym).abs() < 1e-12;
            let realized = self.portfolio.on_fill(&f);
            let pos = self.portfolio.position(f.sym).cloned().unwrap_or_default();
            let st = &mut self.symbols[idx];
            if was_flat && !pos.is_flat() {
                st.rt_open_marks = Some((pos.realized_pnl - realized, pos.fees - f.fee));
            } else if !was_flat && pos.is_flat() {
                if let Some((r0, f0)) = st.rt_open_marks.take() {
                    let rt_pnl = (pos.realized_pnl - r0) - (pos.fees - f0);
                    self.counters.roundtrips += 1;
                    if rt_pnl > 0.0 {
                        self.counters.roundtrip_wins += 1;
                    }
                }
            } else if !was_flat && !pos.is_flat() && st.rt_open_marks.is_none() {
                st.rt_open_marks = Some((pos.realized_pnl - realized, pos.fees - f.fee));
            }
            let mid0 = if f.bid > 0.0 && f.ask > f.bid { (f.bid + f.ask) * 0.5 } else { f.price };
            st.pending_markouts.push((f.ts + self.cfg.eligibility.markout_horizon_secs as i64 * 1000, f.side.sign(), mid0));
            tracing::debug!("fill {} {:?} {}@{} fee={:.5} realized={:.5} pos={}", st.meta.name, f.side, f.qty, f.price, f.fee, realized, pos.qty);
            self.store.send(StoreMsg::Fill { symbol: st.meta.name.clone(), realized, fill: f });
            self.store.send(StoreMsg::PositionState {
                symbol: st.meta.name.clone(),
                qty: pos.qty,
                avg_price: pos.avg_price,
                realized_pnl: pos.realized_pnl,
                fees: pos.fees,
                opened_ts: pos.opened_ts,
                last_mid: pos.last_mid,
                updated_ts: now,
            });
        }
        for d in done {
            let idx = d.sym as usize;
            let st = &mut self.symbols[idx];
            if st.bid_order == Some(d.order_id) {
                st.bid_order = None;
            }
            if st.ask_order == Some(d.order_id) {
                st.ask_order = None;
            }
            self.counters.orders_done += 1;
            match d.status {
                "filled" => self.counters.orders_filled += 1,
                "rejected_post_only" => self.counters.orders_rejected += 1,
                _ => {}
            }
            self.store.send(StoreMsg::OrderDone { symbol: st.meta.name.clone(), done: d });
        }
    }

    fn requote(&mut self, sym: SymbolId, now: i64) {
        let idx = sym as usize;
        let pos = self.portfolio.position(sym).cloned().unwrap_or_default();
        let flat = pos.is_flat();
        let (bbo, active, min_requote) = {
            let st = &self.symbols[idx];
            (st.bbo, st.active, st.params.min_requote_ms)
        };
        let Some(bbo) = bbo else { return };
        if flat && (!active || self.halted) {
            // nothing to do here: make sure nothing rests in the book
            if self.symbols[idx].bid_order.is_some() || self.symbols[idx].ask_order.is_some() {
                self.paper.cancel_all(now, sym);
                self.counters.cancelled += 1;
            }
            self.symbols[idx].quote_reason = if self.halted { "halted" } else { "inactive" };
            return;
        }
        if now - self.symbols[idx].last_requote_ms < min_requote {
            return;
        }
        let ref_dev = self.ref_dev_bps(idx, now);
        self.symbols[idx].last_ref_dev_bps = ref_dev;
        let quotes = {
            let st = &self.symbols[idx];
            let inp = QuoteInput {
                meta: &st.meta,
                bbo: &bbo,
                stats: &st.stats,
                params: &st.params,
                maker_fee: self.cfg.fees.maker_rate,
                position_qty: pos.qty,
                position_avg: pos.avg_price,
                position_age_secs: pos.age_secs(now),
                now_ms: now,
                allow_new_exposure: self.risk.allow_entries && !self.halted,
                reduce_only: !st.active,
                ref_dev_bps: ref_dev,
            };
            compute_quotes(&inp)
        };
        self.counters.requotes += 1;
        self.symbols[idx].quote_reason = quotes.reason;
        self.symbols[idx].last_requote_ms = now;
        self.sync_side(sym, Side::Buy, quotes.bid, now);
        self.sync_side(sym, Side::Sell, quotes.ask, now);
    }

    /// Bring the resting order on one side in line with the desired quote:
    /// keep it if it already matches, otherwise cancel it (and place the new one once
    /// the cancel has gone through, on a later requote), or place when nothing rests.
    fn sync_side(&mut self, sym: SymbolId, side: Side, desired: Option<Quote>, now: i64) {
        let idx = sym as usize;
        let existing = match side {
            Side::Buy => self.symbols[idx].bid_order,
            Side::Sell => self.symbols[idx].ask_order,
        };
        if let Some(id) = existing {
            match self.paper.order(id) {
                None => {
                    // finished but not yet drained: treat as gone
                    self.set_order_id(idx, side, None);
                }
                Some(o) => {
                    if o.is_cancel_pending() {
                        return;
                    }
                    let tick = self.symbols[idx].meta.tick_size;
                    let same = desired.is_some_and(|d| (d.price - o.price).abs() < tick * 0.5 && d.taker == o.taker && d.purpose == o.purpose);
                    if same {
                        return;
                    }
                    self.paper.cancel(now, id);
                    self.counters.cancelled += 1;
                    return;
                }
            }
        }
        if let Some(d) = desired {
            let inv = self.portfolio.qty(sym);
            let st = &self.symbols[idx];
            let req = PlaceReq {
                sym,
                side,
                price: d.price,
                qty: d.qty,
                taker: d.taker,
                purpose: d.purpose,
                inventory_before: inv,
                param_version: self.param_version,
                ref_dev_bps: st.last_ref_dev_bps,
                imbalance: st.bbo.map_or(0.0, |b| b.imbalance()),
            };
            let id = self.paper.place(now, req);
            self.set_order_id(idx, side, Some(id));
            self.counters.placed += 1;
        }
    }

    fn set_order_id(&mut self, idx: usize, side: Side, id: Option<u64>) {
        match side {
            Side::Buy => self.symbols[idx].bid_order = id,
            Side::Sell => self.symbols[idx].ask_order = id,
        }
    }

    // ------------------------------------------------------------------------------
    // Once-per-second housekeeping
    // ------------------------------------------------------------------------------

    fn maybe_tick(&mut self, now: i64) {
        let sec = now / 1000;
        if sec != self.last_tick_sec {
            self.last_tick_sec = sec;
            self.tick(now);
        }
    }

    fn tick(&mut self, now: i64) {
        self.counters.ticks += 1;
        self.resolve_markouts(now);
        // 1. statistics and eligibility
        for st in self.symbols.iter_mut() {
            st.stats.refresh(now);
            st.verdict = eligibility::evaluate(&self.cfg.eligibility, &st.meta, &st.stats, &self.lists, st.params.min_spread_bps, now, st.experience);
        }
        // 2. active set
        if now - self.last_refresh_ms >= self.cfg.eligibility.refresh_secs as i64 * 1000 {
            self.last_refresh_ms = now;
            self.select_active(now);
        }
        // 3. portfolio marks and risk
        self.portfolio.mark(now);
        self.risk = risk::evaluate(&self.cfg.risk, &self.portfolio, self.halted);
        if self.risk.halted && !self.halted {
            self.halted = true;
            self.event(now, "error", format!("KILL SWITCH: daily pnl {:.2} <= -{:.2}; cancelling entries, exiting positions", self.risk.daily_pnl, self.cfg.risk.max_daily_loss_usd));
        }
        self.counters.exposure_sum += self.portfolio.gross_exposure();
        // 4. stale orders and a requote pass over everything that matters
        for idx in 0..self.symbols.len() {
            let max_age = self.symbols[idx].params.max_order_age_ms;
            for id in [self.symbols[idx].bid_order, self.symbols[idx].ask_order].into_iter().flatten() {
                if let Some(o) = self.paper.order(id) {
                    if !o.is_cancel_pending() && max_age > 0 && now - o.ts_created > max_age {
                        self.paper.cancel(now, id);
                        self.counters.cancelled += 1;
                    }
                }
            }
            let has_pos = !self.portfolio.qty(idx as SymbolId).abs().eq(&0.0);
            if self.symbols[idx].active || has_pos || self.symbols[idx].bid_order.is_some() || self.symbols[idx].ask_order.is_some() {
                self.requote(idx as SymbolId, now);
            }
        }
        // 5. snapshots
        if now - self.last_pnl_snap_ms >= self.cfg.storage.pnl_snapshot_interval_secs as i64 * 1000 {
            self.last_pnl_snap_ms = now;
            self.snapshot_pnl(now);
        }
        if now - self.last_stats_snap_ms >= self.cfg.storage.symbol_stats_interval_secs as i64 * 1000 {
            self.last_stats_snap_ms = now;
            self.snapshot_symbols(now);
        }
        // 6. hot config reload
        if now - self.last_cfg_check_ms >= 5000 {
            self.last_cfg_check_ms = now;
            self.check_config(now);
        }
    }

    fn select_active(&mut self, now: i64) {
        let mut cands: Vec<(f64, usize)> = self.symbols.iter().enumerate().filter(|(_, s)| s.verdict.eligible).map(|(i, s)| (s.verdict.score, i)).collect();
        cands.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        let top: std::collections::HashSet<usize> = cands.iter().take(self.cfg.eligibility.max_active_symbols).map(|c| c.1).collect();
        let mut activated = Vec::new();
        let mut deactivated = Vec::new();
        for (i, st) in self.symbols.iter_mut().enumerate() {
            let now_active = top.contains(&i);
            if now_active && !st.active {
                activated.push(st.meta.name.clone());
            } else if !now_active && st.active {
                deactivated.push(st.meta.name.clone());
            }
            st.active = now_active;
        }
        self.n_active = top.len();
        if !activated.is_empty() || !deactivated.is_empty() {
            self.event(now, "info", format!("active set: {} symbols (+{} {:?} / -{} {:?})", self.n_active, activated.len(), truncate(&activated, 8), deactivated.len(), truncate(&deactivated, 8)));
        }
    }

    fn snapshot_pnl(&self, now: i64) {
        let pf = &self.portfolio;
        self.store.send(StoreMsg::Pnl(PnlRow {
            ts: now,
            equity: pf.equity(),
            realized: pf.realized_total,
            fees: pf.fees_total,
            unrealized: pf.unrealized_total(),
            gross_exposure: pf.gross_exposure(),
            open_positions: pf.open_positions(),
            n_fills: pf.n_fills,
            daily_pnl: pf.daily_pnl(),
            max_drawdown: pf.max_drawdown,
            n_live_orders: self.paper.n_live(),
            n_active_symbols: self.n_active,
            halted: self.halted,
        }));
    }

    fn snapshot_symbols(&self, now: i64) {
        for (i, st) in self.symbols.iter().enumerate() {
            let Some(bbo) = st.bbo else { continue };
            // computed here rather than taken from the last requote, so inactive symbols show it too
            let ref_dev = self.ref_dev_bps(i, now);
            self.store.send(StoreMsg::SymbolStats(SymbolStatsRow {
                ts: now,
                symbol: st.meta.name.clone(),
                spread_med_bps: st.stats.spread_med_bps,
                spread_mean_bps: st.stats.spread_mean_bps,
                vol_bps: st.stats.vol_bps,
                trades_per_min: st.stats.trades_per_min,
                turnover_24h: st.meta.turnover_24h,
                eligible: st.verdict.eligible,
                reason: st.verdict.reason,
                score: st.verdict.score,
                active: st.active,
                bid: bbo.bid,
                ask: bbo.ask,
                position_qty: self.portfolio.qty(i as SymbolId),
                quote_reason: st.quote_reason,
                markout_bps: st.experience.markout_bps,
                markout_n: st.experience.markout_n,
                ref_dev_bps: ref_dev,
            }));
        }
    }

    fn check_config(&mut self, now: i64) {
        let Some(w) = self.watcher.as_mut() else { return };
        if !w.changed() {
            return;
        }
        match w.load_all() {
            Ok((cfg, ov, lists)) => {
                self.apply_config(cfg, ov, lists, now);
            }
            Err(e) => self.event(now, "error", format!("config reload rejected, keeping previous: {e:#}")),
        }
    }

    /// Replace tunables at runtime. Structural settings (exchange, storage) need a restart.
    pub fn apply_config(&mut self, cfg: Config, ov: Overrides, lists: SymbolLists, now: i64) {
        self.param_version += 1;
        let mut errors = 0;
        for st in self.symbols.iter_mut() {
            match ov.resolve(&cfg.strategy, &st.meta.name) {
                Ok(p) => st.params = p,
                Err(e) => {
                    errors += 1;
                    if errors <= 3 {
                        tracing::warn!("override for {} rejected: {e:#}", st.meta.name);
                    }
                }
            }
        }
        self.paper.latency_ms = cfg.paper.latency_ms;
        self.paper.maker_fee = cfg.fees.maker_rate;
        self.paper.taker_fee = cfg.fees.taker_rate;
        self.cfg = cfg;
        self.overrides = ov;
        self.lists = lists;
        self.store_param_version(now);
        self.event(now, "info", format!("config reloaded: param version {} ({} symbol overrides rejected)", self.param_version, errors));
    }

    // ------------------------------------------------------------------------------
    // Shutdown / metrics
    // ------------------------------------------------------------------------------

    /// Cancel everything, write final snapshots. Positions stay as they are (paper).
    pub fn finish(&mut self, now: i64) {
        for idx in 0..self.symbols.len() {
            if self.symbols[idx].bid_order.is_some() || self.symbols[idx].ask_order.is_some() {
                self.paper.cancel_all(now, idx as SymbolId);
            }
        }
        self.paper.on_time(now + self.paper.latency_ms + 1);
        self.drain_fills(now);
        self.portfolio.mark(now);
        self.snapshot_pnl(now);
        self.snapshot_symbols(now);
        self.store.send(StoreMsg::RunEnd { ended_ts: now });
    }

    pub fn summary(&self) -> Summary {
        let pf = &self.portfolio;
        let mut per_symbol = HashMap::new();
        for (i, st) in self.symbols.iter().enumerate() {
            if let Some(p) = pf.position(i as SymbolId) {
                if p.n_fills > 0 {
                    per_symbol.insert(st.meta.name.clone(), SymbolSummary { realized: p.realized_pnl, fees: p.fees, n_fills: p.n_fills, position_qty: p.qty, unrealized: p.unrealized() });
                }
            }
        }
        let c = &self.counters;
        let decided = c.orders_done.saturating_sub(c.orders_rejected);
        Summary {
            net_pnl: pf.realized_total - pf.fees_total + pf.unrealized_total(),
            gross_pnl: pf.realized_total,
            fees: pf.fees_total,
            unrealized: pf.unrealized_total(),
            equity: pf.equity(),
            max_drawdown: pf.max_drawdown,
            n_fills: pf.n_fills,
            n_roundtrips: c.roundtrips,
            win_rate: if c.roundtrips > 0 { c.roundtrip_wins as f64 / c.roundtrips as f64 } else { 0.0 },
            fill_ratio: if decided > 0 { c.orders_filled as f64 / decided as f64 } else { 0.0 },
            avg_abs_inventory: if c.ticks > 0 { c.exposure_sum / c.ticks as f64 } else { 0.0 },
            n_events: c.events,
            n_placed: c.placed,
            n_cancelled: c.cancelled,
            per_symbol,
        }
    }

    pub fn status_line(&self) -> String {
        let pf = &self.portfolio;
        format!(
            "eq={:.2} real={:.2} fees={:.2} unreal={:.2} pos={} live={} active={}/{} fills={} events={} {}",
            pf.equity(),
            pf.realized_total,
            pf.fees_total,
            pf.unrealized_total(),
            pf.open_positions(),
            self.paper.n_live(),
            self.n_active,
            self.symbols.len(),
            pf.n_fills,
            self.counters.events,
            if self.halted { "HALTED" } else { self.risk.reason }
        )
    }
}

fn truncate(v: &[String], n: usize) -> Vec<&str> {
    v.iter().take(n).map(|s| s.as_str()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::null_store;
    use crate::types::Trade;

    fn meta(name: &str) -> SymbolMeta {
        SymbolMeta { id: 0, name: name.into(), base_coin: "T".into(), quote_coin: "USDT".into(), tick_size: 0.001, qty_step: 1.0, min_qty: 1.0, max_qty: 1e9, min_notional: 5.0, price_scale: 3, turnover_24h: 1e7 }
    }

    fn engine() -> Engine {
        let mut cfg: Config = toml::from_str("").unwrap();
        cfg.eligibility.min_trades_per_min = 1.0;
        cfg.eligibility.window_secs = 20;
        cfg.eligibility.refresh_secs = 1;
        cfg.recording.enabled = false;
        cfg.paper.latency_ms = 10;
        Engine::new(cfg, Overrides::default(), SymbolLists::default(), vec![meta("AUSDT")], null_store(), None, 1, "test", 1_700_000_000_000)
    }

    #[test]
    fn warms_up_activates_and_captures_spread() {
        let mut e = engine();
        let t0 = 1_700_000_000_000i64;
        let bbo = Bbo { ts: t0, bid: 1.000, ask: 1.020, bid_qty: 50.0, ask_qty: 50.0 };
        // 30 seconds of quotes and trades: enough samples to become eligible
        let mut now = t0;
        for i in 0..30 {
            now = t0 + i * 1000;
            e.on_market(MarketEvent::Bbo { sym: 0, bbo: Bbo { ts: now, ..bbo } }, now);
            e.on_market(MarketEvent::Trades { sym: 0, trades: vec![Trade { ts: now, price: 1.010, qty: 1.0, taker_side: if i % 2 == 0 { Side::Buy } else { Side::Sell } }] }, now + 1);
        }
        assert!(e.symbols[0].verdict.eligible, "reason={}", e.symbols[0].verdict.reason);
        assert!(e.symbols[0].active);
        // after the next quotes both sides should rest in the paper book (two steps: an
        // order that just aged out is re-placed only once its cancel has gone through)
        for _ in 0..2 {
            now += 1000;
            e.on_market(MarketEvent::Bbo { sym: 0, bbo: Bbo { ts: now, ..bbo } }, now);
            e.on_time(now + 20);
        }
        assert!(e.symbols[0].bid_order.is_some() && e.symbols[0].ask_order.is_some(), "{}", e.symbols[0].quote_reason);
        // a seller sweeps through our bid, then a buyer lifts through our ask: one round trip
        e.on_market(MarketEvent::Trades { sym: 0, trades: vec![Trade { ts: now + 30, price: 0.999, qty: 500.0, taker_side: Side::Sell }] }, now + 30);
        assert!(e.portfolio.qty(0) > 0.0, "no long after sweep");
        e.on_market(MarketEvent::Trades { sym: 0, trades: vec![Trade { ts: now + 40, price: 1.021, qty: 500.0, taker_side: Side::Buy }] }, now + 40);
        assert_eq!(e.portfolio.qty(0), 0.0);
        let s = e.summary();
        assert_eq!(s.n_roundtrips, 1);
        assert!(s.gross_pnl > 0.0, "gross={}", s.gross_pnl);
        assert!(s.net_pnl > 0.0, "net={}", s.net_pnl);
        assert_eq!(s.win_rate, 1.0);
    }

    #[test]
    fn kill_switch_stops_entries() {
        let mut e = engine();
        e.cfg.risk.max_daily_loss_usd = 1.0;
        let t0 = 1_700_000_000_000i64;
        let bbo = Bbo { ts: t0, bid: 1.000, ask: 1.020, bid_qty: 50.0, ask_qty: 50.0 };
        let mut now = t0;
        for i in 0..30 {
            now = t0 + i * 1000;
            e.on_market(MarketEvent::Bbo { sym: 0, bbo: Bbo { ts: now, ..bbo } }, now);
            e.on_market(MarketEvent::Trades { sym: 0, trades: vec![Trade { ts: now, price: 1.010, qty: 1.0, taker_side: if i % 2 == 0 { Side::Buy } else { Side::Sell } }] }, now + 1);
        }
        for _ in 0..2 {
            now += 1000;
            e.on_market(MarketEvent::Bbo { sym: 0, bbo: Bbo { ts: now, ..bbo } }, now);
            e.on_time(now + 20);
        }
        assert!(e.symbols[0].bid_order.is_some(), "{}", e.symbols[0].quote_reason);
        // fill our bid then crash the market: the loss trips the daily limit
        e.on_market(MarketEvent::Trades { sym: 0, trades: vec![Trade { ts: now + 30, price: 0.999, qty: 500.0, taker_side: Side::Sell }] }, now + 30);
        let crash = Bbo { ts: now + 2000, bid: 0.900, ask: 0.920, bid_qty: 50.0, ask_qty: 50.0 };
        e.on_market(MarketEvent::Bbo { sym: 0, bbo: crash }, now + 2000);
        e.on_time(now + 3000);
        assert!(e.halted);
        assert!(!e.risk.allow_entries);
        // only the exit side may rest
        e.on_time(now + 4000);
        assert!(e.symbols[0].bid_order.is_none() || e.paper.order(e.symbols[0].bid_order.unwrap()).map_or(true, |o| o.is_cancel_pending()));
    }
}
