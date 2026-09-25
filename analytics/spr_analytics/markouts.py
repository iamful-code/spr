"""Markouts: how the mid price moved after each of our fills. Signed from our point
of view, in bps of the fill price: negative means the market moved against us right
after we traded (adverse selection: we were picked off by informed flow)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from . import md

HORIZONS_S = (1, 5, 30, 60)


def compute_markouts(fills: pd.DataFrame, md_dir: str, horizons: tuple[int, ...] = HORIZONS_S) -> pd.DataFrame:
    """Returns `fills` with extra columns mid0 and mo_{h}s (bps) for each horizon.
    Fills without recorded market data get NaN."""
    if fills is None or len(fills) == 0:
        out = fills.copy() if fills is not None else pd.DataFrame()
        for h in horizons:
            out[f"mo_{h}s"] = np.nan
        return out
    out = fills.copy()
    out["mid0"] = (out["bid"].astype(float) + out["ask"].astype(float)) * 0.5
    bad = ~(out["mid0"] > 0)
    out.loc[bad, "mid0"] = out.loc[bad, "price"].astype(float)
    for h in horizons:
        out[f"mo_{h}s"] = np.nan
    max_h = max(horizons)
    for symbol, g in out.groupby("symbol", sort=False):
        lo = int(g["ts"].min())
        hi = int(g["ts"].max()) + max_h * 1000 + 1000
        series = md.bbo_series(md_dir, symbol, lo - 1000, hi)
        if len(series) == 0:
            continue
        ts = series["ts"].to_numpy(dtype=np.int64)
        mids = series["mid"].to_numpy(dtype=np.float64)
        sgn = np.where(g["side"].to_numpy() == "Buy", 1.0, -1.0)
        mid0 = g["mid0"].to_numpy(dtype=np.float64)
        for h in horizons:
            target = g["ts"].to_numpy(dtype=np.int64) + h * 1000
            idx = np.searchsorted(ts, target, side="right") - 1
            ok = idx >= 0
            later = np.full(len(g), np.nan)
            later[ok] = mids[idx[ok]]
            # only trust points that are not too stale (no quote for > 5 min means no data)
            stale = ok & (target - ts[np.clip(idx, 0, len(ts) - 1)] > 300_000)
            later[stale] = np.nan
            out.loc[g.index, f"mo_{h}s"] = (later - mid0) / mid0 * 1e4 * sgn
    return out


def summarize_markouts(df: pd.DataFrame, by: tuple[str, ...] = ("symbol",), horizons: tuple[int, ...] = HORIZONS_S) -> pd.DataFrame:
    cols = [f"mo_{h}s" for h in horizons if f"mo_{h}s" in df.columns]
    if len(df) == 0 or not cols:
        return pd.DataFrame(columns=list(by) + ["n"] + cols)
    g = df.groupby(list(by))
    out = g[cols].mean()
    out.insert(0, "n", g.size())
    if "spread_bps_at_place" in df.columns:
        out["half_spread_bps"] = g["spread_bps_at_place"].mean() / 2.0
    return out.reset_index()
