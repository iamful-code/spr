//! `spr` command line: live paper trading on Bybit, synthetic simulation, replay of
//! recorded data and a few utilities.

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use spr_core::bybit;
use spr_core::config::{Config, ConfigWatcher};
use spr_core::engine::Engine;
use spr_core::replay::{parse_time, run_replay, ReplayRequest};
use spr_core::simfeed::{SimFeed, SimParams};
use spr_core::store::{null_store, open_db, open_store};
use spr_core::types::{now_ms, MarketEvent, SymbolMeta};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Parser)]
#[command(name = "spr", version, about = "Spread-capture paper-trading bot for Bybit linear perpetuals")]
struct Cli {
    /// Base strategy config (overrides.json and symbols.json are looked up next to it)
    #[arg(short, long, global = true, default_value = "config/strategy.toml")]
    config: PathBuf,
    /// Log filter, e.g. info, debug, spr_core=debug
    #[arg(long, global = true)]
    log: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Live paper trading on the Bybit public feed
    Run {
        /// Instrument list JSON (from `spr symbols --out`) used when the REST API is unreachable
        #[arg(long)]
        instruments_file: Option<PathBuf>,
        /// Restrict to these symbols (comma separated); overrides [exchange].symbols
        #[arg(long, value_delimiter = ',')]
        symbols: Vec<String>,
        /// Subscribe to at most N symbols (highest 24h turnover first)
        #[arg(long)]
        max_symbols: Option<usize>,
        /// Do not write market data files (SQLite is still written)
        #[arg(long)]
        no_record: bool,
        /// Stop after this many seconds (default: run until Ctrl-C)
        #[arg(long)]
        duration_secs: Option<u64>,
    },
    /// Synthetic market: same engine, no network
    Sim {
        #[arg(long, default_value_t = 20)]
        symbols: usize,
        #[arg(long, default_value_t = 600)]
        duration_secs: u64,
        /// Time acceleration; 0 = as fast as possible
        #[arg(long, default_value_t = 1.0)]
        speed: f64,
        #[arg(long, default_value_t = 42)]
        seed: u64,
        /// Share of informed (toxic) flow, 0..1
        #[arg(long, default_value_t = 0.4)]
        toxicity: f64,
        #[arg(long)]
        no_record: bool,
    },
    /// Replay recorded market data through the engine and print JSON metrics
    Replay {
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        #[arg(long, value_delimiter = ',')]
        symbols: Vec<String>,
        /// JSON object of strategy parameters applied to every replayed symbol
        #[arg(long)]
        params_json: Option<String>,
        #[arg(long)]
        md_dir: Option<PathBuf>,
        /// Also write fills/orders/snapshots of the replay into the database as a run
        #[arg(long)]
        record: bool,
    },
    /// Fetch the instrument list (or load it from a file) and print/save it
    Symbols {
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        instruments_file: Option<PathBuf>,
    },
    /// Create the SQLite schema so the dashboard can start before the first run
    InitDb,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let filter = cli.log.clone().or_else(|| std::env::var("RUST_LOG").ok()).unwrap_or_else(|| "info".into());
    // the legacy Windows console prints ANSI colour codes as garbage; SPR_COLOR=1 forces them on
    let ansi = std::env::var("SPR_COLOR").map(|v| v == "1").unwrap_or(!cfg!(windows));
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::new(filter)).with_target(false).with_ansi(ansi).with_writer(std::io::stderr).init();

    match cli.cmd {
        Cmd::Run { instruments_file, symbols, max_symbols, no_record, duration_secs } => {
            let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
            rt.block_on(cmd_run(&cli.config, instruments_file, symbols, max_symbols, no_record, duration_secs))
        }
        Cmd::Sim { symbols, duration_secs, speed, seed, toxicity, no_record } => cmd_sim(&cli.config, symbols, duration_secs, speed, seed, toxicity, no_record),
        Cmd::Replay { from, to, symbols, params_json, md_dir, record } => cmd_replay(&cli.config, &from, &to, symbols, params_json, md_dir, record),
        Cmd::Symbols { out, instruments_file } => {
            let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
            rt.block_on(cmd_symbols(&cli.config, out, instruments_file))
        }
        Cmd::InitDb => {
            let cfg = Config::load(&cli.config)?;
            open_db(Path::new(&cfg.storage.db_path))?;
            println!("schema ready in {}", cfg.storage.db_path);
            Ok(())
        }
    }
}

// ------------------------------------------------------------------------------------
// run
// ------------------------------------------------------------------------------------

async fn load_instruments(cfg: &Config, config_dir: &Path, instruments_file: Option<PathBuf>) -> Result<Vec<SymbolMeta>> {
    let client = bybit::rest::client(15)?;
    match bybit::rest::fetch_instruments(&client, &cfg.exchange.rest_url, &cfg.exchange.category, &cfg.exchange.quote_coin).await {
        Ok(v) if !v.is_empty() => {
            tracing::info!("instruments-info: {} {} instruments", v.len(), cfg.exchange.category);
            return Ok(v);
        }
        Ok(_) => tracing::warn!("instruments-info returned no instruments"),
        Err(e) => tracing::warn!("instruments-info failed: {e:#}"),
    }
    let fallback = instruments_file.unwrap_or_else(|| config_dir.join("instruments.json"));
    if fallback.exists() {
        let v = bybit::rest::load_instruments_file(&fallback)?;
        tracing::info!("loaded {} instruments from {}", v.len(), fallback.display());
        Ok(v)
    } else {
        bail!("no instruments: REST unreachable and {} does not exist (create it with `spr symbols --out ...` on a machine where the API works)", fallback.display())
    }
}

fn config_dir(config_path: &Path) -> PathBuf {
    config_path.parent().filter(|p| !p.as_os_str().is_empty()).map(|p| p.to_path_buf()).unwrap_or_else(|| PathBuf::from("."))
}

fn select_symbols(mut metas: Vec<SymbolMeta>, cfg: &Config, cli_symbols: &[String], max_symbols: Option<usize>) -> Vec<SymbolMeta> {
    let want: Vec<&String> = if !cli_symbols.is_empty() { cli_symbols.iter().collect() } else { cfg.exchange.symbols.iter().collect() };
    if !want.is_empty() {
        metas.retain(|m| want.contains(&&m.name));
    }
    // highest turnover first, so a cap keeps the liquid names
    metas.sort_by(|a, b| b.turnover_24h.partial_cmp(&a.turnover_24h).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.name.cmp(&b.name)));
    let cap = max_symbols.unwrap_or(cfg.exchange.max_symbols);
    if cap > 0 && metas.len() > cap {
        metas.truncate(cap);
    }
    for (i, m) in metas.iter_mut().enumerate() {
        m.id = i as u32;
    }
    metas
}

async fn cmd_run(config_path: &Path, instruments_file: Option<PathBuf>, symbols: Vec<String>, max_symbols: Option<usize>, no_record: bool, duration_secs: Option<u64>) -> Result<()> {
    let watcher = ConfigWatcher::new(config_path);
    let (mut cfg, overrides, lists) = watcher.load_all()?;
    if no_record {
        cfg.recording.enabled = false;
    }
    let mut metas = load_instruments(&cfg, &config_dir(config_path), instruments_file).await?;

    let client = bybit::rest::client(15)?;
    match bybit::rest::fetch_tickers(&client, &cfg.exchange.rest_url, &cfg.exchange.category).await {
        Ok(t) => {
            for m in metas.iter_mut() {
                if let Some(v) = t.get(&m.name) {
                    m.turnover_24h = *v;
                }
            }
            tracing::info!("tickers: turnover for {} symbols", t.len());
        }
        Err(e) => tracing::warn!("tickers failed (turnover filter disabled until it works): {e:#}"),
    }
    let metas = select_symbols(metas, &cfg, &symbols, max_symbols);
    if metas.is_empty() {
        bail!("no symbols selected");
    }
    tracing::info!("trading {} symbols: {}", metas.len(), metas.iter().take(10).map(|m| m.name.as_str()).collect::<Vec<_>>().join(","));

    let run_id = now_ms();
    let store = open_store(Path::new(&cfg.storage.db_path), Path::new(&cfg.storage.md_dir), run_id, metas.clone(), cfg.recording.enabled)?;
    let mut engine = Engine::new(cfg.clone(), overrides, lists, metas.clone(), store.handle.clone(), Some(watcher), run_id, "live", run_id);

    let (tx, mut rx) = tokio::sync::mpsc::channel::<MarketEvent>(65_536);
    let handles = bybit::ws::spawn_connections(&cfg.exchange, &metas, tx.clone());

    // periodic turnover refresh
    let tick_tx = tx.clone();
    let rest_url = cfg.exchange.rest_url.clone();
    let category = cfg.exchange.category.clone();
    let refresh = cfg.exchange.tickers_refresh_secs.max(30);
    let name_to_id: std::collections::HashMap<String, u32> = metas.iter().map(|m| (m.name.clone(), m.id)).collect();
    let ticker_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(refresh)).await;
            if let Ok(t) = bybit::rest::fetch_tickers(&client, &rest_url, &category).await {
                let v: Vec<(u32, f64)> = t.iter().filter_map(|(n, v)| name_to_id.get(n).map(|id| (*id, *v))).collect();
                if tick_tx.send(MarketEvent::Turnover(v)).await.is_err() {
                    return;
                }
            }
        }
    });
    drop(tx);

    let deadline = duration_secs.map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
    let mut timer = tokio::time::interval(Duration::from_millis(50));
    let mut status = tokio::time::interval(Duration::from_secs(10));
    status.tick().await;
    tracing::info!("run {run_id} started; Ctrl-C to stop");
    loop {
        tokio::select! {
            ev = rx.recv() => {
                match ev {
                    Some(ev) => engine.on_market(ev, now_ms()),
                    None => break,
                }
            }
            _ = timer.tick() => engine.on_time(now_ms()),
            _ = status.tick() => tracing::info!("{}", engine.status_line()),
            _ = tokio::signal::ctrl_c() => { tracing::info!("Ctrl-C"); break; }
            _ = async { match deadline { Some(d) => tokio::time::sleep_until(d).await, None => std::future::pending::<()>().await } } => { tracing::info!("duration elapsed"); break; }
        }
    }
    for h in handles {
        h.abort();
    }
    ticker_task.abort();
    engine.finish(now_ms());
    let summary = engine.summary();
    store.close();
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

// ------------------------------------------------------------------------------------
// sim
// ------------------------------------------------------------------------------------

fn cmd_sim(config_path: &Path, n_symbols: usize, duration_secs: u64, speed: f64, seed: u64, toxicity: f64, no_record: bool) -> Result<()> {
    let watcher = ConfigWatcher::new(config_path);
    let (mut cfg, overrides, lists) = watcher.load_all()?;
    if no_record {
        cfg.recording.enabled = false;
    }
    let start = now_ms();
    let mut feed = SimFeed::new(&SimParams { n_symbols, seed, step_ms: 100, toxicity }, start);
    let metas = feed.metas();
    let run_id = start;
    let store = open_store(Path::new(&cfg.storage.db_path), Path::new(&cfg.storage.md_dir), run_id, metas.clone(), cfg.recording.enabled)?;
    let mut engine = Engine::new(cfg, overrides, lists, metas, store.handle.clone(), Some(watcher), run_id, "sim", start);
    tracing::info!("sim run {run_id}: {n_symbols} symbols, {duration_secs}s, speed {speed}, toxicity {toxicity}");

    let end = start + duration_secs as i64 * 1000;
    let wall_start = std::time::Instant::now();
    let mut buf = Vec::with_capacity(4096);
    let mut next_status = start + 10_000;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let stop = stop.clone();
        ctrlc_handler(move || stop.store(true, std::sync::atomic::Ordering::Relaxed));
    }
    while feed.t < end && !stop.load(std::sync::atomic::Ordering::Relaxed) {
        feed.step(&mut buf);
        let now = feed.t;
        for ev in buf.drain(..) {
            engine.on_market(ev, now);
        }
        engine.on_time(now);
        if now >= next_status {
            next_status = now + 10_000;
            tracing::info!("t+{}s {}", (now - start) / 1000, engine.status_line());
        }
        if speed > 0.0 {
            let sim_elapsed = Duration::from_millis(((now - start) as f64 / speed) as u64);
            let wall = wall_start.elapsed();
            if sim_elapsed > wall {
                std::thread::sleep(sim_elapsed - wall);
            }
        }
    }
    engine.finish(feed.t);
    let summary = engine.summary();
    store.close();
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

fn ctrlc_handler(f: impl Fn() + Send + 'static) {
    // tokio's signal handling needs a runtime; keep a tiny one on a side thread
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");
        rt.block_on(async {
            if tokio::signal::ctrl_c().await.is_ok() {
                f();
            }
        });
    });
}

// ------------------------------------------------------------------------------------
// replay
// ------------------------------------------------------------------------------------

fn cmd_replay(config_path: &Path, from: &str, to: &str, symbols: Vec<String>, params_json: Option<String>, md_dir: Option<PathBuf>, record: bool) -> Result<()> {
    let watcher = ConfigWatcher::new(config_path);
    let (cfg, overrides, lists) = watcher.load_all()?;
    let params = match params_json {
        Some(s) => {
            let v: serde_json::Value = serde_json::from_str(&s).context("--params-json must be a JSON object")?;
            match v {
                serde_json::Value::Object(m) => Some(m),
                _ => bail!("--params-json must be a JSON object"),
            }
        }
        None => None,
    };
    let req = ReplayRequest { md_dir: md_dir.unwrap_or_else(|| PathBuf::from(&cfg.storage.md_dir)), from_ms: parse_time(from)?, to_ms: parse_time(to)?, symbols, params };
    if req.to_ms <= req.from_ms {
        bail!("--to must be after --from");
    }
    let run_id = now_ms();
    let store = if record { Some(open_store(Path::new(&cfg.storage.db_path), Path::new(&cfg.storage.md_dir), run_id, vec![], false)?) } else { None };
    let handle = store.as_ref().map(|s| s.handle.clone()).unwrap_or_else(null_store);
    let report = run_replay(cfg, overrides, lists, &req, handle, run_id)?;
    if let Some(s) = store {
        s.close();
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

// ------------------------------------------------------------------------------------
// symbols
// ------------------------------------------------------------------------------------

async fn cmd_symbols(config_path: &Path, out: Option<PathBuf>, instruments_file: Option<PathBuf>) -> Result<()> {
    let cfg = Config::load(config_path)?;
    let mut metas = load_instruments(&cfg, &config_dir(config_path), instruments_file).await?;
    let client = bybit::rest::client(15)?;
    if let Ok(t) = bybit::rest::fetch_tickers(&client, &cfg.exchange.rest_url, &cfg.exchange.category).await {
        for m in metas.iter_mut() {
            if let Some(v) = t.get(&m.name) {
                m.turnover_24h = *v;
            }
        }
    }
    metas.sort_by(|a, b| b.turnover_24h.partial_cmp(&a.turnover_24h).unwrap());
    println!("{:<20} {:>12} {:>10} {:>16}", "symbol", "tick", "qty_step", "turnover_24h");
    for m in metas.iter().take(40) {
        println!("{:<20} {:>12} {:>10} {:>16.0}", m.name, m.tick_size, m.qty_step, m.turnover_24h);
    }
    println!("... {} instruments total", metas.len());
    if let Some(p) = out {
        bybit::rest::save_instruments_file(&p, &metas)?;
        println!("saved to {}", p.display());
    }
    Ok(())
}
