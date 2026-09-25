from __future__ import annotations

import json
import os
import sqlite3
import threading

import numpy as np
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, RedirectResponse

from .. import db, roundtrips

DB_PATH = os.environ.get("SPR_DB", "data/spr.db")
STATIC = os.path.join(os.path.dirname(__file__), "static")

app = FastAPI(title="SPR dashboard", docs_url="/api/docs")
_cache_lock = threading.Lock()
_rt_cache: dict[int, tuple[int, dict]] = {}


def conn() -> sqlite3.Connection:
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=503, detail=f"database {DB_PATH} does not exist yet: start the core or run `spr init-db`")
    return db.connect(DB_PATH, readonly=True)


def rows(cur) -> list[dict]:
    return [dict(r) for r in cur.fetchall()]


def resolve_run(c: sqlite3.Connection, run_id: int | None) -> int:
    rid = run_id or db.latest_run_id(c)
    if rid is None:
        raise HTTPException(status_code=404, detail="no runs yet")
    return rid


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC, "index.html"))


PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.2.min.js"


@app.get("/static/plotly.min.js")
def plotly_js():
    """Serve Plotly from the installed `plotly` package (works offline); fall back to the CDN."""
    try:
        import plotly  # noqa: WPS433

        path = os.path.join(os.path.dirname(plotly.__file__), "package_data", "plotly.min.js")
        if os.path.exists(path):
            return FileResponse(path, media_type="application/javascript")
    except ImportError:
        pass
    return RedirectResponse(PLOTLY_CDN)


@app.get("/api/runs")
def api_runs():
    with conn() as c:
        return rows(c.execute("SELECT run_id, started_ts, ended_ts, mode FROM runs ORDER BY started_ts DESC LIMIT 50"))


@app.get("/api/summary")
def api_summary(run_id: int | None = None):
    with conn() as c:
        rid = resolve_run(c, run_id)
        run = c.execute("SELECT * FROM runs WHERE run_id = ?", (rid,)).fetchone()
        snap = c.execute("SELECT * FROM pnl_snapshots WHERE run_id = ? ORDER BY ts DESC LIMIT 1", (rid,)).fetchone()
        n_fills = c.execute("SELECT count(*) FROM fills WHERE run_id = ?", (rid,)).fetchone()[0]
        n_orders = c.execute("SELECT count(*), sum(status='filled'), sum(status='rejected_post_only') FROM orders WHERE run_id = ?", (rid,)).fetchone()
        n_symbols = c.execute("SELECT count(DISTINCT symbol) FROM symbol_stats WHERE run_id = ?", (rid,)).fetchone()[0]
        pv = c.execute("SELECT version, ts FROM param_versions WHERE run_id = ? ORDER BY version DESC LIMIT 1", (rid,)).fetchone()
        n_recs = c.execute("SELECT count(*) FROM recommendations WHERE status = 'open'").fetchone()[0]
        last_event = c.execute("SELECT ts, level, msg FROM events WHERE run_id = ? ORDER BY ts DESC LIMIT 1", (rid,)).fetchone()
        decided = (n_orders[0] or 0) - (n_orders[2] or 0)
        return {
            "run": dict(run) if run else None,
            "snapshot": dict(snap) if snap else None,
            "n_fills": n_fills,
            "n_orders": n_orders[0] or 0,
            "fill_ratio": (n_orders[1] or 0) / decided if decided else 0.0,
            "n_symbols": n_symbols,
            "param_version": dict(pv) if pv else None,
            "open_recommendations": n_recs,
            "last_event": dict(last_event) if last_event else None,
            "now": db.now_ms(),
        }


@app.get("/api/equity")
def api_equity(run_id: int | None = None, limit: int = Query(800, le=5000)):
    with conn() as c:
        rid = resolve_run(c, run_id)
        df = db.pnl_snapshots(c, rid, limit)
        cols = ["ts", "equity", "realized", "fees", "unrealized", "gross_exposure", "open_positions", "n_live_orders", "n_active_symbols", "max_drawdown"]
        return {k: df[k].tolist() for k in cols if k in df.columns}


@app.get("/api/symbols")
def api_symbols(run_id: int | None = None):
    with conn() as c:
        rid = resolve_run(c, run_id)
        stats = rows(c.execute(
            "SELECT s.* FROM symbol_stats s JOIN (SELECT symbol, max(ts) mts FROM symbol_stats WHERE run_id = ? GROUP BY symbol) m"
            " ON s.symbol = m.symbol AND s.ts = m.mts WHERE s.run_id = ? ORDER BY s.active DESC, s.score DESC",
            (rid, rid),
        ))
        pos = {r["symbol"]: r for r in rows(c.execute("SELECT * FROM position_state WHERE run_id = ?", (rid,)))}
        pnl = {r["symbol"]: r for r in rows(c.execute("SELECT symbol, sum(realized_pnl) realized, sum(fee) fees, count(*) n_fills FROM fills WHERE run_id = ? GROUP BY symbol", (rid,)))}
        for s in stats:
            p = pos.get(s["symbol"])
            q = pnl.get(s["symbol"])
            s["position_qty"] = p["qty"] if p else 0.0
            s["avg_price"] = p["avg_price"] if p else 0.0
            s["unrealized"] = ((s["bid"] + s["ask"]) / 2 - p["avg_price"]) * p["qty"] if p and p["qty"] and s["bid"] and s["ask"] else 0.0
            s["realized"] = q["realized"] if q else 0.0
            s["fees"] = q["fees"] if q else 0.0
            s["n_fills"] = q["n_fills"] if q else 0
            s["net"] = s["realized"] - s["fees"]
        return stats


@app.get("/api/fills")
def api_fills(run_id: int | None = None, limit: int = Query(200, le=5000), symbol: str | None = None):
    with conn() as c:
        rid = resolve_run(c, run_id)
        if symbol:
            return rows(c.execute("SELECT * FROM fills WHERE run_id = ? AND symbol = ? ORDER BY ts DESC LIMIT ?", (rid, symbol, limit)))
        return rows(c.execute("SELECT * FROM fills WHERE run_id = ? ORDER BY ts DESC LIMIT ?", (rid, limit)))


@app.get("/api/roundtrips")
def api_roundtrips(run_id: int | None = None, limit: int = Query(300, le=5000)):
    with conn() as c:
        rid = resolve_run(c, run_id)
        n_fills = c.execute("SELECT count(*) FROM fills WHERE run_id = ?", (rid,)).fetchone()[0]
        with _cache_lock:
            cached = _rt_cache.get(rid)
        if cached and cached[0] == n_fills:
            return cached[1]
        fills = db.fills(c, rid)
    rts = roundtrips.match_roundtrips(fills)
    summary = roundtrips.summarize(rts)
    per_sym = roundtrips.per_symbol(rts)
    hist = {"bins": [], "counts": []}
    if len(rts):
        counts, edges = np.histogram(rts["net_bps"].clip(-50, 50), bins=40)
        hist = {"bins": [float(x) for x in (edges[:-1] + edges[1:]) / 2], "counts": [int(x) for x in counts]}
    # cumulative net pnl by exit time
    cum = {"ts": rts["exit_ts"].tolist(), "net": rts["net_pnl"].cumsum().round(6).tolist()} if len(rts) else {"ts": [], "net": []}
    out = {
        "summary": summary,
        "per_symbol": json.loads(per_sym.to_json(orient="records")) if len(per_sym) else [],
        "recent": json.loads(rts.tail(limit).iloc[::-1].to_json(orient="records")) if len(rts) else [],
        "hist": hist,
        "cum": cum,
    }
    with _cache_lock:
        _rt_cache[rid] = (n_fills, out)
    return out


@app.get("/api/recommendations")
def api_recommendations(status: str = "open", limit: int = Query(200, le=2000)):
    with conn() as c:
        if status == "all":
            return rows(c.execute("SELECT * FROM recommendations ORDER BY created_ts DESC, id LIMIT ?", (limit,)))
        return rows(c.execute("SELECT * FROM recommendations WHERE status = ? ORDER BY created_ts DESC, id LIMIT ?", (status, limit)))


@app.get("/api/optimizer")
def api_optimizer(limit: int = Query(30, le=500)):
    with conn() as c:
        out = rows(c.execute("SELECT * FROM optimizer_runs ORDER BY created_ts DESC LIMIT ?", (limit,)))
        for r in out:
            try:
                r["best_params"] = json.loads(r.pop("best_params_json") or "{}")
            except json.JSONDecodeError:
                r["best_params"] = {}
        return out


@app.get("/api/pair_scores")
def api_pair_scores():
    with conn() as c:
        last = c.execute("SELECT max(created_ts) FROM pair_scores").fetchone()[0]
        if not last:
            return []
        return rows(c.execute("SELECT * FROM pair_scores WHERE created_ts = ? ORDER BY score DESC", (last,)))


@app.get("/api/events")
def api_events(run_id: int | None = None, limit: int = Query(100, le=2000)):
    with conn() as c:
        rid = resolve_run(c, run_id)
        return rows(c.execute("SELECT ts, level, msg FROM events WHERE run_id = ? ORDER BY ts DESC LIMIT ?", (rid, limit)))


@app.get("/api/params")
def api_params(run_id: int | None = None):
    with conn() as c:
        rid = resolve_run(c, run_id)
        return db.latest_params(c, rid)
