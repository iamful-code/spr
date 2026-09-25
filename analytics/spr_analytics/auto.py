"""Autonomous improvement loop. Runs next to the core and the dashboard:

* every `analyze_every` minutes: diagnostics on the current run -> recommendations
  (and, with `apply`, parameter changes written to overrides.json / symbols.json);
* every `pairs_every` minutes: offline pair scoring -> symbols.json;
* every `optimize_every` minutes (0 = off): Optuna over `spr replay` on the last
  `optimize_hours` of recorded data for the symbols that actually traded.

Guards against runaway self-tuning: a (symbol, parameter) pair is changed at most once
per `cooldown` minutes, and after any change the diagnostics only look at fills made
since that change, so every adjustment is judged on fresh evidence before the next one.
"""

from __future__ import annotations

import json
import os
import time
import traceback
from dataclasses import dataclass, field

from . import db, diagnostics, optimizer, pairs


@dataclass
class AutoConfig:
    db_path: str = db.DEFAULT_DB
    md_dir: str = db.DEFAULT_MD_DIR
    config_dir: str = "config"
    config_path: str = "config/strategy.toml"
    bin_path: str | None = None
    analyze_every_min: float = 10.0
    pairs_every_min: float = 60.0
    optimize_every_min: float = 0.0
    optimize_hours: float = 6.0
    optimize_trials: int = 30
    optimize_max_symbols: int = 20
    apply: bool = False
    min_n: int = 30
    cooldown_min: float = 60.0
    allow_top: int = 0
    state_path: str = field(default="")
    once: bool = False

    def __post_init__(self):
        if not self.state_path:
            self.state_path = os.path.join(self.config_dir, "auto_state.json")


def _log(msg: str) -> None:
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


def _load_state(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
        except (OSError, json.JSONDecodeError):
            pass
    return {"applied": {}, "last_apply_by_run": {}}


def _save_state(path: str, state: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def step_analyze(cfg: AutoConfig, state: dict) -> dict:
    conn = db.connect(cfg.db_path)
    try:
        run_id = db.latest_trading_run_id(conn)
        if run_id is None:
            _log("analyze: no runs yet")
            return {}
        since = state.get("last_apply_by_run", {}).get(str(run_id)) if cfg.apply else None
        res = diagnostics.run_diagnostics(conn, cfg.md_dir, run_id, min_n=cfg.min_n, since_ts=since)
        recs = res["recommendations"]
        s = res["summary"]
        db.write_recommendations(conn, run_id, recs)
        _log(f"analyze run {run_id}: {s['n']} round trips since {'last change' if since else 'start'}, net {s['net_pnl']:.2f} USDT, win {s['win_rate']:.0%}, {len(recs)} recommendations")
        for r in recs:
            _log(f"  [{r['severity']}] {r['rule']} {r.get('symbol') or '*'}: {r.get('param')} {r.get('current_value')} -> {r.get('suggested_value')}")
        if cfg.apply and recs:
            now = db.now_ms()
            cooldown_ms = cfg.cooldown_min * 60_000
            fresh = []
            for r in recs:
                if not r.get("param") or r.get("suggested_value") is None:
                    continue
                key = f"{r.get('symbol') or '*'}:{r['param']}"
                last = state.setdefault("applied", {}).get(key, 0)
                if now - last < cooldown_ms:
                    continue
                fresh.append(r)
            if fresh:
                out = diagnostics.apply_recommendations(fresh, os.path.join(cfg.config_dir, "overrides.json"), os.path.join(cfg.config_dir, "symbols.json"))
                for r in fresh:
                    state["applied"][f"{r.get('symbol') or '*'}:{r['param']}"] = now
                state.setdefault("last_apply_by_run", {})[str(run_id)] = now
                _save_state(cfg.state_path, state)
                _log(f"  applied {out['n_overrides']} overrides, deny={out['symbols'].get('deny')}; next evidence window starts now")
            else:
                _log("  nothing applied (all suggestions within cooldown)")
        return res
    finally:
        conn.close()


def step_pairs(cfg: AutoConfig) -> None:
    conn = db.connect(cfg.db_path)
    try:
        df = pairs.score_pairs(conn, lookback_hours=24.0)
        if len(df) == 0:
            _log("pairs: no statistics yet")
            return
        db.write_pair_scores(conn, df)
        out = pairs.write_symbols_json(df, os.path.join(cfg.config_dir, "symbols.json"), allow_top=cfg.allow_top)
        _log(f"pairs: scored {len(df)} symbols, deny={out['deny']}, allow={len(out['allow'])}")
    finally:
        conn.close()


def step_optimize(cfg: AutoConfig) -> None:
    conn = db.connect(cfg.db_path)
    try:
        to_ms = db.now_ms()
        from_ms = to_ms - int(cfg.optimize_hours * 3.6e6)
        traded = db.read_df(conn, "SELECT symbol, count(*) n FROM fills WHERE ts >= ? GROUP BY symbol ORDER BY n DESC LIMIT ?", (from_ms, cfg.optimize_max_symbols))
        symbols = list(traded["symbol"]) if len(traded) else []
        if not symbols:
            _log("optimize: no fills in the window, skipping")
            return
        _log(f"optimize: {len(symbols)} symbols, last {cfg.optimize_hours:g} h, {cfg.optimize_trials} trials")
        row = optimizer.optimize(
            conn,
            config=cfg.config_path,
            from_ms=from_ms,
            to_ms=to_ms,
            symbols=symbols,
            n_trials=cfg.optimize_trials,
            apply=cfg.apply,
            overrides_path=os.path.join(cfg.config_dir, "overrides.json"),
            bin_path=cfg.bin_path,
            md_dir=cfg.md_dir,
            log=lambda m: _log("  " + m),
        )
        _log(f"optimize: valid {row['valid_score']:.2f} vs baseline {row['baseline_valid_score']:.2f}; applied={row['applied']}")
    finally:
        conn.close()


def run_auto(cfg: AutoConfig) -> None:
    state = _load_state(cfg.state_path)
    _log(f"auto loop: analyze every {cfg.analyze_every_min:g} min, pairs every {cfg.pairs_every_min:g} min, optimize every {cfg.optimize_every_min:g} min, apply={cfg.apply}, min_n={cfg.min_n}, cooldown={cfg.cooldown_min:g} min")
    next_analyze = 0.0
    next_pairs = 0.0
    next_opt = time.time() + (cfg.optimize_every_min * 60 if cfg.optimize_every_min > 0 else 0)
    while True:
        now = time.time()
        try:
            if now >= next_analyze:
                step_analyze(cfg, state)
                next_analyze = now + cfg.analyze_every_min * 60
            if cfg.pairs_every_min > 0 and now >= next_pairs:
                step_pairs(cfg)
                next_pairs = now + cfg.pairs_every_min * 60
            if cfg.optimize_every_min > 0 and now >= next_opt:
                step_optimize(cfg)
                next_opt = time.time() + cfg.optimize_every_min * 60
        except KeyboardInterrupt:
            raise
        except Exception:  # keep the loop alive; the core does not depend on us
            _log("step failed:\n" + traceback.format_exc())
        if cfg.once:
            return
        candidates = [next_analyze]
        if cfg.pairs_every_min > 0:
            candidates.append(next_pairs)
        if cfg.optimize_every_min > 0:
            candidates.append(next_opt)
        wait = max(5.0, min(candidates) - time.time())
        try:
            time.sleep(wait)
        except KeyboardInterrupt:
            _log("stopped")
            return
