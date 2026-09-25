"""Parameter optimizer: Optuna (TPE) over `spr replay` on recorded market data with a
walk-forward split. Parameters are searched on the first part of the period and the
best set is checked on the held-out tail against the parameters currently in use; only
a set that beats the baseline on the held-out data by a margin is written to
config/overrides.json (per symbol, or "*" when optimizing all symbols at once)."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import dataclass

from . import db

DEFAULT_BIN = os.environ.get("SPR_BIN", os.path.join("core", "target", "release", "spr"))


@dataclass
class Objective:
    lam_drawdown: float = 0.5
    mu_inventory: float = 0.01

    def __call__(self, summary: dict) -> float:
        return float(summary.get("net_pnl", 0.0)) - self.lam_drawdown * float(summary.get("max_drawdown", 0.0)) - self.mu_inventory * float(summary.get("avg_abs_inventory", 0.0))


def find_binary(path: str | None = None) -> str:
    cand = path or DEFAULT_BIN
    if os.path.exists(cand):
        return cand
    found = shutil.which("spr")
    if found:
        return found
    raise FileNotFoundError(f"spr binary not found at {cand}; build it with `cargo build --release` in core/ or set SPR_BIN")


def replay(bin_path: str, config: str, from_ms: int, to_ms: int, symbols: list[str], params: dict | None, md_dir: str | None = None) -> dict:
    cmd = [bin_path, "--config", config, "--log", "error", "replay", "--from", str(from_ms), "--to", str(to_ms)]
    if symbols:
        cmd += ["--symbols", ",".join(symbols)]
    if params:
        cmd += ["--params-json", json.dumps(params)]
    if md_dir:
        cmd += ["--md-dir", md_dir]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"replay failed: {res.stderr.strip()[:500]}")
    report = json.loads(res.stdout)
    return report["summary"]


def suggest_params(trial, base: dict) -> dict:
    """Search space around the strategy parameters. Keeps every value valid for
    StrategyParams::validate in the core."""
    order_notional = trial.suggest_float("order_notional_usd", 50.0, 400.0, log=True)
    return {
        "min_spread_bps": trial.suggest_float("min_spread_bps", 4.0, 40.0, log=True),
        "min_edge_bps": trial.suggest_float("min_edge_bps", 0.0, 10.0),
        "quote_mode": trial.suggest_categorical("quote_mode", ["join", "improve"]),
        "order_notional_usd": order_notional,
        "max_position_notional_usd": order_notional * trial.suggest_float("position_cap_mult", 1.0, 5.0),
        "inventory_skew_bps": trial.suggest_float("inventory_skew_bps", 0.0, 15.0),
        "min_requote_ms": trial.suggest_int("min_requote_ms", 100, 3000, log=True),
        "max_order_age_ms": trial.suggest_int("max_order_age_ms", 2000, 60000, log=True),
        "max_hold_secs": trial.suggest_int("max_hold_secs", 30, 600, log=True),
        "stale_exit_mode": trial.suggest_categorical("stale_exit_mode", ["improve", "taker"]),
        "max_vol_bps": trial.suggest_float("max_vol_bps", 10.0, 80.0),
        "toxicity_imbalance": trial.suggest_float("toxicity_imbalance", 0.3, 0.95),
    }


def optimize(
    conn: sqlite3.Connection,
    config: str,
    from_ms: int,
    to_ms: int,
    symbols: list[str],
    n_trials: int = 40,
    train_frac: float = 0.7,
    objective: Objective | None = None,
    min_improvement: float = 1.0,
    apply: bool = False,
    overrides_path: str = "config/overrides.json",
    bin_path: str | None = None,
    md_dir: str | None = None,
    seed: int = 0,
    log=print,
) -> dict:
    import optuna

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    obj = objective or Objective()
    binp = find_binary(bin_path)
    split = from_ms + int((to_ms - from_ms) * train_frac)
    log(f"train [{from_ms}, {split}) valid [{split}, {to_ms}] symbols={symbols or 'all'} trials={n_trials}")

    baseline_valid = replay(binp, config, split, to_ms, symbols, None, md_dir)
    baseline_score = obj(baseline_valid)
    log(f"baseline valid: score={baseline_score:.3f} net={baseline_valid['net_pnl']:.3f} dd={baseline_valid['max_drawdown']:.3f} fills={baseline_valid['n_fills']}")

    def trial_fn(trial):
        params = suggest_params(trial, {})
        s = replay(binp, config, from_ms, split, symbols, params, md_dir)
        trial.set_user_attr("net_pnl", s["net_pnl"])
        trial.set_user_attr("n_fills", s["n_fills"])
        return obj(s)

    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=seed))
    study.optimize(trial_fn, n_trials=n_trials, show_progress_bar=False)
    best_params = suggest_params(_FrozenTrialProxy(study.best_trial), {})
    train_score = float(study.best_value)
    valid = replay(binp, config, split, to_ms, symbols, best_params, md_dir)
    valid_score = obj(valid)
    log(f"best train score={train_score:.3f}; valid score={valid_score:.3f} net={valid['net_pnl']:.3f} dd={valid['max_drawdown']:.3f} fills={valid['n_fills']}")

    improved = valid_score > baseline_score + min_improvement
    applied = False
    note = "valid better than baseline" if improved else "no improvement on held-out data (overfit or nothing to gain)"
    if improved and apply:
        _write_overrides(overrides_path, symbols, best_params)
        applied = True
        note += "; written to overrides.json"
    row = {
        "symbol": ",".join(symbols) if symbols else "*",
        "n_trials": n_trials,
        "train_from": from_ms,
        "train_to": split,
        "valid_from": split,
        "valid_to": to_ms,
        "best_params": best_params,
        "train_score": train_score,
        "valid_score": valid_score,
        "baseline_valid_score": baseline_score,
        "applied": applied,
        "note": note,
    }
    row["id"] = db.write_optimizer_run(conn, row)
    log(note)
    return row


class _FrozenTrialProxy:
    """Re-run `suggest_params` on a finished trial to rebuild the derived values."""

    def __init__(self, trial):
        self.p = trial.params

    def suggest_float(self, name, *a, **k):
        return self.p[name]

    def suggest_int(self, name, *a, **k):
        return self.p[name]

    def suggest_categorical(self, name, *a, **k):
        return self.p[name]


def _write_overrides(path: str, symbols: list[str], params: dict) -> None:
    ov = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            text = fh.read().strip()
            ov = json.loads(text) if text else {}
    keys = symbols or ["*"]
    for k in keys:
        cur = ov.setdefault(k, {})
        for pk, pv in params.items():
            cur[pk] = round(pv, 4) if isinstance(pv, float) else pv
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(ov, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


if __name__ == "__main__":  # pragma: no cover
    print("use: python -m spr_analytics optimize ...", file=sys.stderr)
