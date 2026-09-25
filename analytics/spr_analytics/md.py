"""Reader for the binary market-data files written by the Rust recorder.

Layout: data/md/YYYYMMDD/run{run_id}_bbo.bin, run{run_id}_trd.bin, run{run_id}_symbols.json.
Records are little-endian and fixed size (see core/src/recorder.rs)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np
import pandas as pd

BBO_DTYPE = np.dtype([("ts", "<i8"), ("sym", "<u4"), ("pad", "<u4"), ("bid", "<f8"), ("ask", "<f8"), ("bid_qty", "<f8"), ("ask_qty", "<f8")])
TRD_DTYPE = np.dtype([("ts", "<i8"), ("sym", "<u4"), ("side", "u1"), ("pad", "u1", (3,)), ("price", "<f8"), ("qty", "<f8")])
DAY_MS = 86_400_000


@dataclass
class Segment:
    day: str
    day_start_ms: int
    run_id: int
    dir: str
    symbols: list[dict]

    @property
    def names(self) -> list[str]:
        return [m["name"] for m in self.symbols]

    @property
    def bbo_path(self) -> str:
        return os.path.join(self.dir, f"run{self.run_id}_bbo.bin")

    @property
    def trd_path(self) -> str:
        return os.path.join(self.dir, f"run{self.run_id}_trd.bin")


def _day_start(name: str) -> int | None:
    try:
        return int(datetime.strptime(name, "%Y%m%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    except ValueError:
        return None


def list_segments(md_dir: str, from_ms: int = 0, to_ms: int = 1 << 62) -> list[Segment]:
    out: list[Segment] = []
    if not os.path.isdir(md_dir):
        return out
    for day in sorted(os.listdir(md_dir)):
        start = _day_start(day)
        if start is None or start + DAY_MS <= from_ms or start > to_ms:
            continue
        d = os.path.join(md_dir, day)
        for f in os.listdir(d):
            if f.startswith("run") and f.endswith("_symbols.json"):
                run_id = int(f[3:-len("_symbols.json")])
                with open(os.path.join(d, f), encoding="utf-8") as fh:
                    symbols = json.load(fh)
                out.append(Segment(day, start, run_id, d, symbols))
    out.sort(key=lambda s: (s.day_start_ms, s.run_id))
    return out


def load_bbo(seg: Segment, symbols: list[str] | None = None, from_ms: int = 0, to_ms: int = 1 << 62) -> pd.DataFrame:
    if not os.path.exists(seg.bbo_path):
        return pd.DataFrame(columns=["ts", "symbol", "bid", "ask", "bid_qty", "ask_qty", "mid"])
    arr = np.fromfile(seg.bbo_path, dtype=BBO_DTYPE)
    return _frame(arr, seg, symbols, from_ms, to_ms, ["bid", "ask", "bid_qty", "ask_qty"], mid=True)


def load_trades(seg: Segment, symbols: list[str] | None = None, from_ms: int = 0, to_ms: int = 1 << 62) -> pd.DataFrame:
    if not os.path.exists(seg.trd_path):
        return pd.DataFrame(columns=["ts", "symbol", "side", "price", "qty"])
    arr = np.fromfile(seg.trd_path, dtype=TRD_DTYPE)
    df = _frame(arr, seg, symbols, from_ms, to_ms, ["side", "price", "qty"])
    if len(df):
        df["side"] = np.where(df["side"].to_numpy() == 0, "Buy", "Sell")
    return df


def _frame(arr: np.ndarray, seg: Segment, symbols: list[str] | None, from_ms: int, to_ms: int, cols: list[str], mid: bool = False) -> pd.DataFrame:
    names = np.array(seg.names, dtype=object)
    mask = (arr["ts"] >= from_ms) & (arr["ts"] <= to_ms)
    if symbols is not None:
        ids = np.array([i for i, n in enumerate(seg.names) if n in set(symbols)], dtype=np.uint32)
        mask &= np.isin(arr["sym"], ids)
    arr = arr[mask]
    df = pd.DataFrame({"ts": arr["ts"], "symbol": names[arr["sym"]]})
    for c in cols:
        df[c] = arr[c]
    if mid:
        df["mid"] = (df["bid"] + df["ask"]) * 0.5
    return df


def bbo_series(md_dir: str, symbol: str, from_ms: int, to_ms: int) -> pd.DataFrame:
    """Top-of-book series of one symbol across all segments in the period, sorted by ts."""
    parts = [load_bbo(seg, [symbol], from_ms, to_ms) for seg in list_segments(md_dir, from_ms, to_ms)]
    parts = [p for p in parts if len(p)]
    if not parts:
        return pd.DataFrame(columns=["ts", "symbol", "bid", "ask", "bid_qty", "ask_qty", "mid"])
    return pd.concat(parts, ignore_index=True).sort_values("ts", kind="stable").reset_index(drop=True)


def recorded_symbols(md_dir: str, from_ms: int = 0, to_ms: int = 1 << 62) -> list[str]:
    names: set[str] = set()
    for seg in list_segments(md_dir, from_ms, to_ms):
        names.update(seg.names)
    return sorted(names)
