"""FIFO matching of fills into round trips (one entry lot matched with one exit lot).

A round trip is the unit the diagnostics reason about: it has an entry and an exit
price, a holding time, the spread that was quoted when the entry was placed and the
fees of both legs, so the net capture in bps is directly comparable with the quoted
spread."""

from __future__ import annotations

from collections import deque

import numpy as np
import pandas as pd

RT_COLUMNS = [
    "symbol", "direction", "qty", "entry_ts", "exit_ts", "entry_price", "exit_price", "gross_pnl", "fees", "net_pnl",
    "hold_secs", "captured_bps", "net_bps", "entry_spread_bps", "exit_spread_bps", "entry_purpose", "exit_purpose",
    "entry_order_id", "exit_order_id", "entry_queue_ahead", "param_version",
]


def match_roundtrips(fills: pd.DataFrame) -> pd.DataFrame:
    """fills: DataFrame with the columns of the `fills` table, any order."""
    if fills is None or len(fills) == 0:
        return pd.DataFrame(columns=RT_COLUMNS)
    fills = fills.sort_values(["ts", "id"] if "id" in fills.columns else ["ts"], kind="stable")
    rows: list[dict] = []
    for symbol, g in fills.groupby("symbol", sort=False):
        open_lots: deque[dict] = deque()
        open_dir = 0  # +1 long lots, -1 short lots
        for f in g.itertuples(index=False):
            sgn = 1 if f.side == "Buy" else -1
            qty = float(f.qty)
            fee_per_unit = float(f.fee) / qty if qty > 0 else 0.0
            lot = {
                "qty": qty, "price": float(f.price), "ts": int(f.ts), "fee_per_unit": fee_per_unit, "spread": float(f.spread_bps_at_place or 0.0),
                "purpose": f.purpose, "order_id": int(f.order_id), "queue": float(f.queue_ahead_initial or 0.0), "pv": int(f.param_version or 0),
            }
            if open_dir == 0 or sgn == open_dir:
                open_lots.append(lot)
                open_dir = sgn
                continue
            # closing against FIFO lots
            remaining = qty
            while remaining > 1e-12 and open_lots:
                head = open_lots[0]
                take = min(remaining, head["qty"])
                gross = (lot["price"] - head["price"]) * take * open_dir
                fees = (head["fee_per_unit"] + lot["fee_per_unit"]) * take
                notional = head["price"] * take
                rows.append({
                    "symbol": symbol,
                    "direction": "long" if open_dir > 0 else "short",
                    "qty": take,
                    "entry_ts": head["ts"],
                    "exit_ts": lot["ts"],
                    "entry_price": head["price"],
                    "exit_price": lot["price"],
                    "gross_pnl": gross,
                    "fees": fees,
                    "net_pnl": gross - fees,
                    "hold_secs": (lot["ts"] - head["ts"]) / 1000.0,
                    "captured_bps": gross / notional * 1e4 if notional > 0 else 0.0,
                    "net_bps": (gross - fees) / notional * 1e4 if notional > 0 else 0.0,
                    "entry_spread_bps": head["spread"],
                    "exit_spread_bps": lot["spread"],
                    "entry_purpose": head["purpose"],
                    "exit_purpose": lot["purpose"],
                    "entry_order_id": head["order_id"],
                    "exit_order_id": lot["order_id"],
                    "entry_queue_ahead": head["queue"],
                    "param_version": head["pv"],
                })
                head["qty"] -= take
                remaining -= take
                if head["qty"] <= 1e-12:
                    open_lots.popleft()
            if remaining > 1e-12:
                # flipped: leftover opens the other direction
                lot["qty"] = remaining
                open_lots.append(lot)
                open_dir = sgn
            elif not open_lots:
                open_dir = 0
    df = pd.DataFrame(rows, columns=RT_COLUMNS)
    return df.sort_values("exit_ts", kind="stable").reset_index(drop=True)


def summarize(rts: pd.DataFrame) -> dict:
    if len(rts) == 0:
        return {"n": 0, "gross_pnl": 0.0, "fees": 0.0, "net_pnl": 0.0, "win_rate": 0.0, "avg_net_bps": 0.0, "avg_hold_secs": 0.0, "p90_hold_secs": 0.0}
    return {
        "n": int(len(rts)),
        "gross_pnl": float(rts["gross_pnl"].sum()),
        "fees": float(rts["fees"].sum()),
        "net_pnl": float(rts["net_pnl"].sum()),
        "win_rate": float((rts["net_pnl"] > 0).mean()),
        "avg_net_bps": float(rts["net_bps"].mean()),
        "avg_captured_bps": float(rts["captured_bps"].mean()),
        "avg_quoted_spread_bps": float(rts["entry_spread_bps"].mean()),
        "avg_hold_secs": float(rts["hold_secs"].mean()),
        "p90_hold_secs": float(np.percentile(rts["hold_secs"], 90)),
    }


def per_symbol(rts: pd.DataFrame) -> pd.DataFrame:
    if len(rts) == 0:
        return pd.DataFrame(columns=["symbol", "n", "gross_pnl", "fees", "net_pnl", "win_rate", "avg_net_bps", "avg_captured_bps", "avg_quoted_spread_bps", "avg_hold_secs", "p90_hold_secs"])
    g = rts.groupby("symbol")
    out = pd.DataFrame({
        "n": g.size(),
        "gross_pnl": g["gross_pnl"].sum(),
        "fees": g["fees"].sum(),
        "net_pnl": g["net_pnl"].sum(),
        "win_rate": g["net_pnl"].apply(lambda s: float((s > 0).mean())),
        "avg_net_bps": g["net_bps"].mean(),
        "avg_captured_bps": g["captured_bps"].mean(),
        "avg_quoted_spread_bps": g["entry_spread_bps"].mean(),
        "avg_hold_secs": g["hold_secs"].mean(),
        "p90_hold_secs": g["hold_secs"].quantile(0.9),
    })
    return out.reset_index().sort_values("net_pnl", ascending=False).reset_index(drop=True)
