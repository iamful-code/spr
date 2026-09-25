//! Configuration: base TOML file, per-symbol JSON overrides written by the optimizer,
//! and the allow/deny symbol lists written by the pair scorer. All three are hot-reloaded.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExchangeCfg {
    #[serde(default = "d_category")]
    pub category: String,
    #[serde(default = "d_rest_url")]
    pub rest_url: String,
    #[serde(default = "d_ws_url")]
    pub ws_url: String,
    #[serde(default = "d_depth")]
    pub orderbook_depth: u32,
    #[serde(default = "d_topics_per_conn")]
    pub topics_per_connection: usize,
    #[serde(default = "d_args_per_sub")]
    pub args_per_subscribe: usize,
    #[serde(default = "d_quote_coin")]
    pub quote_coin: String,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub max_symbols: usize,
    #[serde(default = "d_tickers_refresh")]
    pub tickers_refresh_secs: u64,
}

fn d_category() -> String {
    "linear".into()
}
fn d_rest_url() -> String {
    "https://api.bybit.com".into()
}
fn d_ws_url() -> String {
    "wss://stream.bybit.com/v5/public/linear".into()
}
fn d_depth() -> u32 {
    1
}
fn d_topics_per_conn() -> usize {
    200
}
fn d_args_per_sub() -> usize {
    50
}
fn d_quote_coin() -> String {
    "USDT".into()
}
fn d_tickers_refresh() -> u64 {
    300
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FeesCfg {
    #[serde(default = "d_maker")]
    pub maker_rate: f64,
    #[serde(default = "d_taker")]
    pub taker_rate: f64,
}
fn d_maker() -> f64 {
    0.0002
}
fn d_taker() -> f64 {
    0.00055
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PaperCfg {
    #[serde(default = "d_latency")]
    pub latency_ms: i64,
    #[serde(default = "d_equity")]
    pub initial_equity_usd: f64,
}
fn d_latency() -> i64 {
    60
}
fn d_equity() -> f64 {
    10000.0
}

/// The tunable part of the strategy. This is what the optimizer searches over and
/// what per-symbol overrides can replace field by field.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct StrategyParams {
    pub min_spread_bps: f64,
    pub min_edge_bps: f64,
    pub quote_mode: String,
    pub order_notional_usd: f64,
    pub max_position_notional_usd: f64,
    pub inventory_skew_bps: f64,
    pub min_requote_ms: i64,
    pub max_order_age_ms: i64,
    pub max_hold_secs: i64,
    pub stale_exit_mode: String,
    pub max_vol_bps: f64,
    pub toxicity_imbalance: f64,
    pub toxicity_window_secs: u32,
    /// Fair-value model: how much of the half spread the top-of-book size imbalance shifts
    /// the fair price (0 = ignore the book).
    pub imbalance_weight: f64,
    /// Same for the taker-flow imbalance over `toxicity_window_secs`.
    pub flow_weight: f64,
    /// Share of the reference-venue deviation (reference mid + basis - our mid) added to fair value.
    pub ref_weight: f64,
    /// Block a side outright when the reference says the price is already this many bps away (0 = off).
    pub ref_block_bps: f64,
    /// Give up a side when fair value pushes its quote deeper than this many ticks behind the best level.
    pub max_lean_ticks: i64,
    /// Exit at once when the mid moved this many bps against the position (0 = off).
    pub stop_loss_bps: f64,
    /// "taker" (cross the spread) or "improve" (one tick inside) for stop-loss exits.
    pub stop_loss_mode: String,
}

impl Default for StrategyParams {
    fn default() -> Self {
        Self {
            min_spread_bps: 10.0,
            min_edge_bps: 3.0,
            quote_mode: "join".into(),
            order_notional_usd: 100.0,
            max_position_notional_usd: 300.0,
            inventory_skew_bps: 4.0,
            min_requote_ms: 300,
            max_order_age_ms: 15000,
            max_hold_secs: 120,
            stale_exit_mode: "improve".into(),
            max_vol_bps: 25.0,
            toxicity_imbalance: 0.6,
            toxicity_window_secs: 10,
            imbalance_weight: 0.5,
            flow_weight: 0.3,
            ref_weight: 1.0,
            ref_block_bps: 4.0,
            max_lean_ticks: 3,
            stop_loss_bps: 12.0,
            stop_loss_mode: "taker".into(),
        }
    }
}

impl StrategyParams {
    /// Apply a JSON object of overrides on top of `self` (unknown keys are ignored,
    /// wrong types are reported as an error so a bad optimizer output never silently applies).
    pub fn with_overrides(&self, ov: &serde_json::Map<String, serde_json::Value>) -> Result<StrategyParams> {
        let mut v = serde_json::to_value(self)?;
        let obj = v.as_object_mut().unwrap();
        for (k, val) in ov {
            if obj.contains_key(k) {
                obj.insert(k.clone(), val.clone());
            }
        }
        let p: StrategyParams = serde_json::from_value(v).context("bad override types")?;
        p.validate()?;
        Ok(p)
    }

    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(self.min_spread_bps > 0.0, "min_spread_bps must be > 0");
        anyhow::ensure!(self.order_notional_usd > 0.0, "order_notional_usd must be > 0");
        anyhow::ensure!(self.max_position_notional_usd >= self.order_notional_usd, "max_position_notional_usd must be >= order_notional_usd");
        anyhow::ensure!(self.quote_mode == "join" || self.quote_mode == "improve", "quote_mode must be join|improve");
        anyhow::ensure!(self.stale_exit_mode == "improve" || self.stale_exit_mode == "taker", "stale_exit_mode must be improve|taker");
        anyhow::ensure!(self.toxicity_window_secs >= 1 && self.toxicity_window_secs <= 300, "toxicity_window_secs in 1..300");
        anyhow::ensure!((0.0..=1.0).contains(&self.imbalance_weight), "imbalance_weight in 0..1");
        anyhow::ensure!((0.0..=1.0).contains(&self.flow_weight), "flow_weight in 0..1");
        anyhow::ensure!((0.0..=2.0).contains(&self.ref_weight), "ref_weight in 0..2");
        anyhow::ensure!(self.ref_block_bps >= 0.0, "ref_block_bps must be >= 0");
        anyhow::ensure!(self.max_lean_ticks >= 0, "max_lean_ticks must be >= 0");
        anyhow::ensure!(self.stop_loss_bps >= 0.0, "stop_loss_bps must be >= 0");
        anyhow::ensure!(self.stop_loss_mode == "taker" || self.stop_loss_mode == "improve", "stop_loss_mode must be taker|improve");
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EligibilityCfg {
    #[serde(default = "d_min_turnover")]
    pub min_turnover_24h_usd: f64,
    #[serde(default = "d_min_spread_med")]
    pub min_spread_med_bps: f64,
    #[serde(default = "d_min_tpm")]
    pub min_trades_per_min: f64,
    #[serde(default = "d_min_svr")]
    pub min_spread_vol_ratio: f64,
    #[serde(default = "d_window")]
    pub window_secs: u32,
    #[serde(default = "d_max_active")]
    pub max_active_symbols: usize,
    #[serde(default = "d_refresh")]
    pub refresh_secs: u64,
    #[serde(default)]
    pub allow: Vec<String>,
    #[serde(default)]
    pub deny: Vec<String>,
    /// Horizon for the online markout the core measures after its own entries, seconds.
    #[serde(default = "d_markout_h")]
    pub markout_horizon_secs: u32,
    /// Exclude a symbol whose average entry markout is worse than this (bps, 0 = off).
    #[serde(default = "d_max_adverse")]
    pub max_adverse_markout_bps: f64,
    #[serde(default = "d_min_markout_n")]
    pub min_markout_samples: u32,
}
fn d_markout_h() -> u32 {
    5
}
fn d_max_adverse() -> f64 {
    3.0
}
fn d_min_markout_n() -> u32 {
    20
}
fn d_min_turnover() -> f64 {
    2_000_000.0
}
fn d_min_spread_med() -> f64 {
    10.0
}
fn d_min_tpm() -> f64 {
    3.0
}
fn d_min_svr() -> f64 {
    0.8
}
fn d_window() -> u32 {
    120
}
fn d_max_active() -> usize {
    40
}
fn d_refresh() -> u64 {
    15
}

/// Leading-venue price feeds. A cheap perp on Bybit follows the same coin on a liquid
/// venue with a lag; the deviation of (reference mid + basis) from our mid is the
/// strongest short-term signal we have.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReferenceCfg {
    #[serde(default = "d_true")]
    pub enabled: bool,
    /// In priority order: "binance_futures", "binance_spot", "bybit_spot".
    #[serde(default = "d_ref_providers")]
    pub providers: Vec<String>,
    /// A reference quote older than this is ignored, ms.
    #[serde(default = "d_ref_stale")]
    pub stale_ms: i64,
    #[serde(default = "d_ref_per_conn")]
    pub symbols_per_connection: usize,
    /// EWMA factor for the basis (our mid - reference mid), per reference update.
    #[serde(default = "d_basis_alpha")]
    pub basis_alpha: f64,
}
fn d_ref_providers() -> Vec<String> {
    vec!["binance_futures".into(), "bybit_spot".into()]
}
fn d_ref_stale() -> i64 {
    3000
}
fn d_ref_per_conn() -> usize {
    100
}
fn d_basis_alpha() -> f64 {
    0.02
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RiskCfg {
    #[serde(default = "d_gross")]
    pub max_gross_exposure_usd: f64,
    #[serde(default = "d_daily_loss")]
    pub max_daily_loss_usd: f64,
    #[serde(default = "d_max_open")]
    pub max_open_positions: usize,
}
fn d_gross() -> f64 {
    3000.0
}
fn d_daily_loss() -> f64 {
    200.0
}
fn d_max_open() -> usize {
    30
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordingCfg {
    #[serde(default = "d_true")]
    pub enabled: bool,
    #[serde(default = "d_bbo_min_interval")]
    pub bbo_min_interval_ms: i64,
    #[serde(default = "d_rec_symbols")]
    pub symbols: String,
}
fn d_true() -> bool {
    true
}
fn d_bbo_min_interval() -> i64 {
    250
}
fn d_rec_symbols() -> String {
    "all".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageCfg {
    #[serde(default = "d_db")]
    pub db_path: String,
    #[serde(default = "d_md")]
    pub md_dir: String,
    #[serde(default = "d_stats_int")]
    pub symbol_stats_interval_secs: u64,
    #[serde(default = "d_pnl_int")]
    pub pnl_snapshot_interval_secs: u64,
}
fn d_db() -> String {
    "data/spr.db".into()
}
fn d_md() -> String {
    "data/md".into()
}
fn d_stats_int() -> u64 {
    30
}
fn d_pnl_int() -> u64 {
    5
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_exchange")]
    pub exchange: ExchangeCfg,
    #[serde(default = "default_fees")]
    pub fees: FeesCfg,
    #[serde(default = "default_paper")]
    pub paper: PaperCfg,
    #[serde(default)]
    pub strategy: StrategyParams,
    #[serde(default = "default_elig")]
    pub eligibility: EligibilityCfg,
    #[serde(default = "default_reference")]
    pub reference: ReferenceCfg,
    #[serde(default = "default_risk")]
    pub risk: RiskCfg,
    #[serde(default = "default_rec")]
    pub recording: RecordingCfg,
    #[serde(default = "default_storage")]
    pub storage: StorageCfg,
}

fn default_exchange() -> ExchangeCfg {
    toml::from_str("").unwrap()
}
fn default_fees() -> FeesCfg {
    toml::from_str("").unwrap()
}
fn default_paper() -> PaperCfg {
    toml::from_str("").unwrap()
}
fn default_elig() -> EligibilityCfg {
    toml::from_str("").unwrap()
}
fn default_reference() -> ReferenceCfg {
    toml::from_str("").unwrap()
}
fn default_risk() -> RiskCfg {
    toml::from_str("").unwrap()
}
fn default_rec() -> RecordingCfg {
    toml::from_str("").unwrap()
}
fn default_storage() -> StorageCfg {
    toml::from_str("").unwrap()
}

impl Config {
    pub fn load(path: &Path) -> Result<Config> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let cfg: Config = toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        cfg.strategy.validate()?;
        Ok(cfg)
    }
}

/// Per-symbol parameter overrides: {"BTCUSDT": {"min_spread_bps": 12.0}, "*": {...}}.
/// The special key "*" applies to every symbol before the symbol-specific block.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Overrides {
    #[serde(flatten)]
    pub per_symbol: HashMap<String, serde_json::Map<String, serde_json::Value>>,
}

impl Overrides {
    pub fn load(path: &Path) -> Result<Overrides> {
        if !path.exists() {
            return Ok(Overrides::default());
        }
        let text = std::fs::read_to_string(path)?;
        if text.trim().is_empty() {
            return Ok(Overrides::default());
        }
        serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
    }

    pub fn resolve(&self, base: &StrategyParams, symbol: &str) -> Result<StrategyParams> {
        let mut p = base.clone();
        if let Some(g) = self.per_symbol.get("*") {
            p = p.with_overrides(g)?;
        }
        if let Some(s) = self.per_symbol.get(symbol) {
            p = p.with_overrides(s)?;
        }
        Ok(p)
    }
}

/// Symbol lists produced by `spr_analytics pairs` (or edited by hand).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SymbolLists {
    #[serde(default)]
    pub allow: Vec<String>,
    #[serde(default)]
    pub deny: Vec<String>,
    #[serde(default)]
    pub scores: HashMap<String, f64>,
}

impl SymbolLists {
    pub fn load(path: &Path) -> Result<SymbolLists> {
        if !path.exists() {
            return Ok(SymbolLists::default());
        }
        let text = std::fs::read_to_string(path)?;
        if text.trim().is_empty() {
            return Ok(SymbolLists::default());
        }
        serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
    }
}

/// Tracks the three config files and reports when any of them changed on disk.
pub struct ConfigWatcher {
    pub config_path: PathBuf,
    pub overrides_path: PathBuf,
    pub symbols_path: PathBuf,
    mtimes: [Option<SystemTime>; 3],
}

impl ConfigWatcher {
    pub fn new(config_path: &Path) -> Self {
        let dir = config_path.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| PathBuf::from("."));
        let mut w = Self {
            config_path: config_path.to_path_buf(),
            overrides_path: dir.join("overrides.json"),
            symbols_path: dir.join("symbols.json"),
            mtimes: [None, None, None],
        };
        w.mtimes = w.current_mtimes();
        w
    }

    fn current_mtimes(&self) -> [Option<SystemTime>; 3] {
        let m = |p: &Path| std::fs::metadata(p).and_then(|m| m.modified()).ok();
        [m(&self.config_path), m(&self.overrides_path), m(&self.symbols_path)]
    }

    /// Returns true if any watched file changed since the last call.
    pub fn changed(&mut self) -> bool {
        let cur = self.current_mtimes();
        if cur != self.mtimes {
            self.mtimes = cur;
            true
        } else {
            false
        }
    }

    pub fn load_all(&self) -> Result<(Config, Overrides, SymbolLists)> {
        Ok((
            Config::load(&self.config_path)?,
            Overrides::load(&self.overrides_path)?,
            SymbolLists::load(&self.symbols_path)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overrides_merge_and_validate() {
        let base = StrategyParams::default();
        let mut ov = Overrides::default();
        let mut g = serde_json::Map::new();
        g.insert("min_spread_bps".into(), serde_json::json!(15.0));
        ov.per_symbol.insert("*".into(), g);
        let mut s = serde_json::Map::new();
        s.insert("quote_mode".into(), serde_json::json!("improve"));
        s.insert("unknown_key".into(), serde_json::json!(1));
        ov.per_symbol.insert("BTCUSDT".into(), s);
        let p = ov.resolve(&base, "BTCUSDT").unwrap();
        assert_eq!(p.min_spread_bps, 15.0);
        assert_eq!(p.quote_mode, "improve");
        let q = ov.resolve(&base, "ETHUSDT").unwrap();
        assert_eq!(q.quote_mode, "join");
        assert_eq!(q.min_spread_bps, 15.0);
        let mut bad = serde_json::Map::new();
        bad.insert("quote_mode".into(), serde_json::json!("weird"));
        assert!(base.with_overrides(&bad).is_err());
    }

    #[test]
    fn default_config_parses() {
        let cfg: Config = toml::from_str("").unwrap();
        assert_eq!(cfg.exchange.category, "linear");
        assert_eq!(cfg.strategy.min_spread_bps, 10.0);
    }
}
