"""SQLite access. The schema is created by the Rust core (`spr init-db`); this module only
reads it and writes the three analytics tables (recommendations, optimizer_runs, pair_scores)."""

from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Iterable

import pandas as pd

DEFAULT_DB = os.environ.get("SPR_DB", "data/spr.db")
DEFAULT_MD_DIR = os.environ.get("SPR_MD_DIR", "data/md")


def now_ms() -> int:
    return int(time.time() * 1000)


def connect(path: str = DEFAULT_DB, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    else:
        conn = sqlite3.connect(path, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def read_df(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=tuple(params))


def runs(conn: sqlite3.Connection) -> pd.DataFrame:
    return read_df(conn, "SELECT run_id, started_ts, ended_ts, mode FROM runs ORDER BY started_ts DESC")


def latest_run_id(conn: sqlite3.Connection, mode: str | None = None) -> int | None:
    if mode:
        row = conn.execute("SELECT run_id FROM runs WHERE mode = ? ORDER BY started_ts DESC LIMIT 1", (mode,)).fetchone()
    else:
        row = conn.execute("SELECT run_id FROM runs ORDER BY started_ts DESC LIMIT 1").fetchone()
    return int(row[0]) if row else None


def fills(conn: sqlite3.Connection, run_id: int | None = None, symbol: str | None = None, since_ts: int | None = None) -> pd.DataFrame:
    sql = "SELECT * FROM fills WHERE 1=1"
    params: list[Any] = []
    if run_id is not None:
        sql += " AND run_id = ?"
        params.append(run_id)
    if symbol:
        sql += " AND symbol = ?"
        params.append(symbol)
    if since_ts:
        sql += " AND ts >= ?"
        params.append(since_ts)
    sql += " ORDER BY ts, id"
    return read_df(conn, sql, params)


def orders(conn: sqlite3.Connection, run_id: int | None = None, since_ts: int | None = None) -> pd.DataFrame:
    sql = "SELECT * FROM orders WHERE 1=1"
    params: list[Any] = []
    if run_id is not None:
        sql += " AND run_id = ?"
        params.append(run_id)
    if since_ts:
        sql += " AND ts_done >= ?"
        params.append(since_ts)
    return read_df(conn, sql + " ORDER BY ts_done", params)


def latest_trading_run_id(conn: sqlite3.Connection) -> int | None:
    """Most recent live or sim run (replays are excluded)."""
    row = conn.execute("SELECT run_id FROM runs WHERE mode != 'replay' ORDER BY started_ts DESC LIMIT 1").fetchone()
    return int(row[0]) if row else None


def symbol_stats(conn: sqlite3.Connection, run_id: int | None = None, since_ts: int | None = None) -> pd.DataFrame:
    sql = "SELECT * FROM symbol_stats WHERE 1=1"
    params: list[Any] = []
    if run_id is not None:
        sql += " AND run_id = ?"
        params.append(run_id)
    if since_ts:
        sql += " AND ts >= ?"
        params.append(since_ts)
    return read_df(conn, sql + " ORDER BY ts", params)


def pnl_snapshots(conn: sqlite3.Connection, run_id: int, limit: int | None = None) -> pd.DataFrame:
    df = read_df(conn, "SELECT * FROM pnl_snapshots WHERE run_id = ? ORDER BY ts", (run_id,))
    if limit and len(df) > limit:
        # thin evenly, keep the last point
        step = max(1, len(df) // limit)
        df = pd.concat([df.iloc[::step], df.iloc[[-1]]]).drop_duplicates("ts")
    return df


def latest_params(conn: sqlite3.Connection, run_id: int) -> dict:
    row = conn.execute("SELECT params_json FROM param_versions WHERE run_id = ? ORDER BY version DESC LIMIT 1", (run_id,)).fetchone()
    if not row:
        return {}
    try:
        return json.loads(row[0])
    except json.JSONDecodeError:
        return {}


def strategy_params_for(params_blob: dict, symbol: str) -> dict:
    """Resolve the effective parameters of a symbol from a param_versions blob."""
    base = dict(params_blob.get("strategy", {}))
    ov = params_blob.get("overrides", {}) or {}
    for key in ("*", symbol):
        for k, v in (ov.get(key) or {}).items():
            if k in base:
                base[k] = v
    return base


# -------------------------------------------------------------------------------------
# writes
# -------------------------------------------------------------------------------------

def write_recommendations(conn: sqlite3.Connection, run_id: int | None, recs: list[dict]) -> int:
    ts = now_ms()
    with conn:
        conn.execute("UPDATE recommendations SET status = 'superseded' WHERE status = 'open' AND (run_id = ? OR ? IS NULL)", (run_id, run_id))
        conn.executemany(
            "INSERT INTO recommendations(created_ts, run_id, symbol, rule, severity, message, param, current_value, suggested_value, evidence_json, status)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')",
            [
                (
                    ts,
                    run_id,
                    r.get("symbol"),
                    r["rule"],
                    r.get("severity", "info"),
                    r["message"],
                    r.get("param"),
                    _s(r.get("current_value")),
                    _s(r.get("suggested_value")),
                    json.dumps(r.get("evidence", {}), ensure_ascii=False, default=float),
                )
                for r in recs
            ],
        )
    return len(recs)


def write_pair_scores(conn: sqlite3.Connection, rows: pd.DataFrame) -> int:
    ts = now_ms()
    with conn:
        conn.executemany(
            "INSERT INTO pair_scores(created_ts, symbol, score, spread_med_bps, trades_per_min, vol_bps, realized_pnl, n_roundtrips, verdict)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (ts, r.symbol, float(r.score), float(r.spread_med_bps), float(r.trades_per_min), float(r.vol_bps), float(r.realized_pnl), int(r.n_roundtrips), r.verdict)
                for r in rows.itertuples()
            ],
        )
    return len(rows)


def write_optimizer_run(conn: sqlite3.Connection, row: dict) -> int:
    with conn:
        cur = conn.execute(
            "INSERT INTO optimizer_runs(created_ts, symbol, n_trials, train_from, train_to, valid_from, valid_to, best_params_json, train_score, valid_score, baseline_valid_score, applied, note)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                now_ms(),
                row.get("symbol"),
                row.get("n_trials"),
                row.get("train_from"),
                row.get("train_to"),
                row.get("valid_from"),
                row.get("valid_to"),
                json.dumps(row.get("best_params", {}), ensure_ascii=False),
                row.get("train_score"),
                row.get("valid_score"),
                row.get("baseline_valid_score"),
                1 if row.get("applied") else 0,
                row.get("note"),
            ),
        )
        return int(cur.lastrowid)


def _s(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, float):
        return f"{v:.6g}"
    return str(v)
