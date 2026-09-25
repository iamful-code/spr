"""Offline pair scoring: the same score the core uses online (spread × √trades / (1+vol)),
computed over a longer history, blended with what the symbol actually earned. Writes
config/symbols.json (allow / deny / scores) which the core hot-reloads."""

from __future__ import annotations

import json
import os
import sqlite3

import numpy as np
import pandas as pd

from . import db, roundtrips


def score_pairs(conn: sqlite3.Connection, lookback_hours: float = 24.0, min_roundtrips: int = 20) -> pd.DataFrame:
    since = db.now_ms() - int(lookback_hours * 3.6e6)
    stats = db.read_df(
        conn,
        "SELECT symbol, spread_med_bps, trades_per_min, vol_bps, turnover_24h, eligible FROM symbol_stats WHERE ts >= ?",
        (since,),
    )
    if len(stats) == 0:
        return pd.DataFrame(columns=["symbol", "spread_med_bps", "trades_per_min", "vol_bps", "turnover_24h", "eligible_share", "score", "realized_pnl", "n_roundtrips", "win_rate", "verdict"])
    g = stats.groupby("symbol")
    df = pd.DataFrame({
        "spread_med_bps": g["spread_med_bps"].median(),
        "trades_per_min": g["trades_per_min"].mean(),
        "vol_bps": g["vol_bps"].median(),
        "turnover_24h": g["turnover_24h"].max(),
        "eligible_share": g["eligible"].mean(),
    }).reset_index()
    df["score"] = np.where(
        (df["spread_med_bps"] > 0) & (df["trades_per_min"] > 0),
        df["spread_med_bps"] * np.sqrt(df["trades_per_min"].clip(lower=0)) / (1.0 + df["vol_bps"]),
        0.0,
    )
    fills = db.fills(conn, since_ts=since)
    rts = roundtrips.match_roundtrips(fills)
    ps = roundtrips.per_symbol(rts)[["symbol", "n", "net_pnl", "win_rate"]].rename(columns={"n": "n_roundtrips", "net_pnl": "realized_pnl"}) if len(rts) else pd.DataFrame(columns=["symbol", "n_roundtrips", "realized_pnl", "win_rate"])
    df = df.merge(ps, on="symbol", how="left")
    df["n_roundtrips"] = df["n_roundtrips"].fillna(0).astype(int)
    df["realized_pnl"] = df["realized_pnl"].fillna(0.0)
    df["win_rate"] = df["win_rate"].fillna(np.nan)

    def verdict(r) -> str:
        if r.n_roundtrips >= min_roundtrips and r.realized_pnl < 0 and (r.win_rate or 0) < 0.45:
            return "deny"
        if r.score > 0 and r.eligible_share >= 0.3:
            return "good"
        return "neutral"

    df["verdict"] = [verdict(r) for r in df.itertuples()]
    # experience-weighted score: profitable history lifts, losses lower it
    adj = np.where(df["n_roundtrips"] >= min_roundtrips, np.clip(df["realized_pnl"] / 10.0, -0.5, 0.5), 0.0)
    df["score"] = (df["score"] * (1.0 + adj)).clip(lower=0.0)
    return df.sort_values("score", ascending=False).reset_index(drop=True)


def write_symbols_json(df: pd.DataFrame, path: str, allow_top: int = 0, keep_existing_deny: bool = True) -> dict:
    existing = {}
    if keep_existing_deny and os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            text = fh.read().strip()
            existing = json.loads(text) if text else {}
    deny = sorted(set(existing.get("deny", [])) | set(df[df["verdict"] == "deny"]["symbol"]))
    allow = list(df[df["verdict"] != "deny"].head(allow_top)["symbol"]) if allow_top > 0 else []
    scores = {r.symbol: round(float(r.score), 4) for r in df.itertuples() if r.score > 0}
    out = {"allow": allow, "deny": deny, "scores": scores}
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)
    return out
