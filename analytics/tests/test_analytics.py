import json
import os
import sqlite3

import numpy as np
import pandas as pd
import pytest

from spr_analytics import db, diagnostics, markouts, md, pairs, roundtrips

SCHEMA = None


def core_schema() -> str:
    """The SQL schema lives in the Rust source; read it so the tests use the real one."""
    here = os.path.dirname(__file__)
    path = os.path.join(here, "..", "..", "core", "src", "store.rs")
    text = open(path, encoding="utf-8").read()
    start = text.index('pub const SCHEMA: &str = r#"') + len('pub const SCHEMA: &str = r#"')
    end = text.index('"#;', start)
    return text[start:end]


def make_fill(order_id, symbol, side, price, qty, ts, purpose="entry", spread=20.0, queue=0.0, fee_rate=0.0002, pv=1):
    return dict(order_id=order_id, symbol=symbol, side=side, price=price, qty=qty, fee=price * qty * fee_rate, ts=ts, is_maker=1, purpose=purpose,
                bid=price - 0.01 if side == "Buy" else price - 0.02, ask=price + 0.02 if side == "Buy" else price + 0.01, placed_ts=ts - 500,
                mid_at_place=price, spread_bps_at_place=spread, queue_ahead_initial=queue, inventory_before=0.0, param_version=pv, realized_pnl=0.0)


def test_fifo_roundtrips_long_short_and_flip():
    fills = pd.DataFrame([
        make_fill(1, "AUSDT", "Buy", 100.0, 2.0, 1000),
        make_fill(2, "AUSDT", "Buy", 102.0, 2.0, 2000),
        make_fill(3, "AUSDT", "Sell", 103.0, 3.0, 3000, purpose="exit"),
        make_fill(4, "AUSDT", "Sell", 103.0, 3.0, 4000, purpose="exit"),  # closes 1, opens short 2
        make_fill(5, "AUSDT", "Buy", 101.0, 2.0, 9000, purpose="exit"),   # closes the short
        make_fill(6, "BUSDT", "Sell", 10.0, 5.0, 1500),
        make_fill(7, "BUSDT", "Buy", 9.9, 5.0, 2500, purpose="exit"),
    ])
    fills["id"] = range(1, len(fills) + 1)
    rts = roundtrips.match_roundtrips(fills)
    a = rts[rts.symbol == "AUSDT"]
    # lots: 2@100 -> 103 (+6), 1@102 -> 103 (+1), 1@102 -> 103 (+1), short 2@103 -> 101 (+4)
    assert len(a) == 4
    assert pytest.approx(a["gross_pnl"].sum()) == 12.0
    assert list(a["direction"]) == ["long", "long", "long", "short"]
    assert pytest.approx(a.iloc[-1]["hold_secs"]) == 5.0
    b = rts[rts.symbol == "BUSDT"].iloc[0]
    assert b["direction"] == "short" and pytest.approx(b["gross_pnl"]) == 0.5
    s = roundtrips.summarize(rts)
    assert s["n"] == 5 and s["win_rate"] == 1.0
    ps = roundtrips.per_symbol(rts)
    assert set(ps["symbol"]) == {"AUSDT", "BUSDT"}


def write_md(tmp_path, run_id, symbols, bbo_rows, trd_rows, day="20240101"):
    d = tmp_path / "md" / day
    d.mkdir(parents=True)
    with open(d / f"run{run_id}_symbols.json", "w") as fh:
        json.dump([{"id": i, "name": n, "base_coin": "X", "quote_coin": "USDT", "tick_size": 0.01, "qty_step": 1, "min_qty": 1, "max_qty": 1e9, "min_notional": 5, "price_scale": 2, "turnover_24h": 0} for i, n in enumerate(symbols)], fh)
    arr = np.zeros(len(bbo_rows), dtype=md.BBO_DTYPE)
    for i, (t, s, bid, ask) in enumerate(bbo_rows):
        arr[i] = (t, s, 0, bid, ask, 1.0, 2.0)
    arr.tofile(d / f"run{run_id}_bbo.bin")
    tr = np.zeros(len(trd_rows), dtype=md.TRD_DTYPE)
    for i, (t, s, side, p, q) in enumerate(trd_rows):
        tr[i] = (t, s, side, (0, 0, 0), p, q)
    tr.tofile(d / f"run{run_id}_trd.bin")
    return tmp_path / "md"


def test_md_reader_and_markouts(tmp_path):
    t0 = 1_704_067_200_000  # 2024-01-01 UTC
    bbo = [(t0 + i * 1000, 0, 100.0 + i * 0.1, 100.02 + i * 0.1) for i in range(120)]  # rising market
    trd = [(t0 + 500, 0, 1, 100.0, 3.0), (t0 + 1500, 1, 0, 5.0, 10.0)]
    md_dir = write_md(tmp_path, 7, ["AUSDT", "BUSDT"], bbo, trd)
    segs = md.list_segments(str(md_dir), t0, t0 + 200_000)
    assert len(segs) == 1 and segs[0].names == ["AUSDT", "BUSDT"]
    df = md.load_bbo(segs[0])
    assert len(df) == 120 and df["symbol"].iloc[0] == "AUSDT"
    tr = md.load_trades(segs[0])
    assert list(tr["side"]) == ["Sell", "Buy"] and tr["symbol"].iloc[1] == "BUSDT"
    series = md.bbo_series(str(md_dir), "AUSDT", t0, t0 + 60_000)
    assert len(series) == 61 and series["ts"].is_monotonic_increasing
    # a buy at t0+10s in a market that rises 0.1 per second: markouts are positive, ~+10 bps per second
    fills = pd.DataFrame([make_fill(1, "AUSDT", "Buy", 101.0, 1.0, t0 + 10_000), make_fill(2, "AUSDT", "Sell", 101.0, 1.0, t0 + 10_000)])
    fills["bid"], fills["ask"] = 100.99, 101.01
    mo = markouts.compute_markouts(fills, str(md_dir))
    assert mo.loc[0, "mo_5s"] > 40 and mo.loc[0, "mo_1s"] > 5
    assert mo.loc[1, "mo_5s"] < -40  # the sell is adversely selected
    summ = markouts.summarize_markouts(mo, by=("side",))
    assert set(summ["side"]) == {"Buy", "Sell"}


def make_db(tmp_path):
    path = tmp_path / "t.db"
    conn = sqlite3.connect(path)
    conn.executescript(core_schema())
    conn.row_factory = sqlite3.Row
    return conn, str(path)


def test_diagnostics_adverse_selection_and_losing_symbol(tmp_path):
    conn, path = make_db(tmp_path)
    t0 = 1_704_067_200_000
    run_id = 1
    conn.execute("INSERT INTO runs(run_id, started_ts, ended_ts, mode) VALUES (?, ?, ?, ?)", (run_id, t0, t0 + 7_200_000, "sim"))
    conn.execute("INSERT INTO param_versions(run_id, version, ts, params_json) VALUES (?, 1, ?, ?)", (run_id, t0, json.dumps({"strategy": {"min_spread_bps": 10.0, "toxicity_imbalance": 0.6, "order_notional_usd": 100.0, "quote_mode": "join", "max_hold_secs": 120, "max_position_notional_usd": 300, "inventory_skew_bps": 4, "min_edge_bps": 3}, "overrides": {}})))
    conn.execute("INSERT INTO pnl_snapshots(run_id, ts, n_active_symbols) VALUES (?, ?, ?)", (run_id, t0 + 7_000_000, 1))
    # market falls steadily: every buy is picked off
    bbo = [(t0 + i * 1000, 0, 100.0 - i * 0.01, 100.02 - i * 0.01) for i in range(7200)]
    md_dir = write_md(tmp_path, run_id, ["BADUSDT"], bbo, [])
    rows = []
    oid = 1
    for k in range(40):
        t = t0 + 60_000 + k * 120_000
        px = 100.0 - (t - t0) / 1000 * 0.01
        rows.append(make_fill(oid, "BADUSDT", "Buy", px, 1.0, t, spread=20.0)); oid += 1
        rows.append(make_fill(oid, "BADUSDT", "Sell", px - 0.05, 1.0, t + 30_000, purpose="exit", spread=20.0)); oid += 1
    cols = list(rows[0].keys())
    conn.executemany(f"INSERT INTO fills(run_id, {', '.join(cols)}) VALUES ({run_id}, {', '.join('?' * len(cols))})", [tuple(r[c] for c in cols) for r in rows])
    conn.commit()
    res = diagnostics.run_diagnostics(conn, str(md_dir), run_id, min_n=30)
    rules = {r["rule"] for r in res["recommendations"]}
    assert "adverse_selection" in rules
    assert "losing_symbol" in rules
    bd = res["breakdowns"]
    assert set(bd) >= {"by_queue", "by_side", "by_hour"}
    assert bd["by_side"]["n"].sum() == 40 and bd["by_side"].iloc[0]["mo_5s"] < 0
    assert list(bd["by_queue"]["queue_bucket"]) == ["0 (внутри спреда)"]
    n = db.write_recommendations(conn, run_id, res["recommendations"])
    assert n == len(res["recommendations"])
    assert conn.execute("SELECT count(*) FROM recommendations WHERE status='open'").fetchone()[0] == n
    # apply writes overrides and deny list
    out = diagnostics.apply_recommendations(res["recommendations"], str(tmp_path / "overrides.json"), str(tmp_path / "symbols.json"))
    assert "BADUSDT" in out["symbols"]["deny"]
    assert out["overrides"]["BADUSDT"]["min_spread_bps"] == pytest.approx(12.5)
    assert db.strategy_params_for(db.latest_params(conn, run_id), "BADUSDT")["min_spread_bps"] == 10.0
    # evidence window after the last change: nothing left to judge
    res2 = diagnostics.run_diagnostics(conn, str(md_dir), run_id, min_n=30, since_ts=t0 + 7_100_000)
    assert res2["summary"]["n"] == 0 and not [r for r in res2["recommendations"] if r["rule"] in ("adverse_selection", "losing_symbol")]


def test_auto_loop_once_applies_with_cooldown(tmp_path, monkeypatch):
    from spr_analytics import auto

    conn, path = make_db(tmp_path)
    t0 = 1_704_067_200_000
    conn.execute("INSERT INTO runs(run_id, started_ts, ended_ts, mode) VALUES (1, ?, ?, 'sim')", (t0, t0 + 7_200_000))
    conn.execute("INSERT INTO param_versions(run_id, version, ts, params_json) VALUES (1, 1, ?, ?)", (t0, json.dumps({"strategy": {"min_spread_bps": 10.0, "toxicity_imbalance": 0.6}, "overrides": {}})))
    bbo = [(t0 + i * 1000, 0, 100.0 - i * 0.01, 100.02 - i * 0.01) for i in range(7200)]
    md_dir = write_md(tmp_path, 1, ["BADUSDT"], bbo, [])
    rows = []
    oid = 1
    for k in range(40):
        t = t0 + 60_000 + k * 120_000
        px = 100.0 - (t - t0) / 1000 * 0.01
        rows.append(make_fill(oid, "BADUSDT", "Buy", px, 1.0, t)); oid += 1
        rows.append(make_fill(oid, "BADUSDT", "Sell", px - 0.05, 1.0, t + 30_000, purpose="exit")); oid += 1
    cols = list(rows[0].keys())
    conn.executemany(f"INSERT INTO fills(run_id, {', '.join(cols)}) VALUES (1, {', '.join('?' * len(cols))})", [tuple(r[c] for c in cols) for r in rows])
    conn.commit()
    conn.close()
    cfg = auto.AutoConfig(db_path=path, md_dir=str(md_dir), config_dir=str(tmp_path / "cfg"), apply=True, min_n=30, pairs_every_min=0, once=True)
    auto.run_auto(cfg)
    ov = json.load(open(tmp_path / "cfg" / "overrides.json"))
    assert ov["BADUSDT"]["min_spread_bps"] == pytest.approx(12.5)
    state = json.load(open(cfg.state_path))
    assert "BADUSDT:min_spread_bps" in state["applied"]
    # second pass: within cooldown and no fills after the change -> nothing new applied
    auto.run_auto(cfg)
    ov2 = json.load(open(tmp_path / "cfg" / "overrides.json"))
    assert ov2 == ov


def test_pairs_scoring(tmp_path):
    conn, path = make_db(tmp_path)
    now = db.now_ms()
    for i in range(10):
        conn.execute("INSERT INTO symbol_stats(run_id, ts, symbol, spread_med_bps, spread_mean_bps, vol_bps, trades_per_min, turnover_24h, eligible, reason, score, active, bid, ask, position_qty, quote_reason)"
                     " VALUES (1, ?, 'GOODUSDT', 20, 21, 5, 30, 1e7, 1, 'ok', 50, 1, 1, 1.002, 0, 'ok')", (now - i * 60_000,))
        conn.execute("INSERT INTO symbol_stats(run_id, ts, symbol, spread_med_bps, spread_mean_bps, vol_bps, trades_per_min, turnover_24h, eligible, reason, score, active, bid, ask, position_qty, quote_reason)"
                     " VALUES (1, ?, 'THINUSDT', 2, 2, 5, 3, 1e6, 0, 'spread_low', 0, 0, 1, 1.0002, 0, 'inactive')", (now - i * 60_000,))
    conn.commit()
    conn.execute("INSERT INTO symbol_stats(run_id, ts, symbol, spread_med_bps, spread_mean_bps, vol_bps, trades_per_min, turnover_24h, eligible, reason, score, active, bid, ask, position_qty, quote_reason, markout_bps, markout_n)"
                 " VALUES (1, ?, 'TOXICUSDT', 25, 25, 5, 40, 1e7, 1, 'ok', 60, 1, 1, 1.0025, 0, 'ok', -8.0, 50)", (now,))
    conn.commit()
    df = pairs.score_pairs(conn, lookback_hours=1)
    assert list(df["symbol"])[0] == "GOODUSDT"
    assert df.iloc[0]["verdict"] == "good"
    assert df[df["symbol"] == "TOXICUSDT"].iloc[0]["verdict"] == "deny"
    out = pairs.write_symbols_json(df, str(tmp_path / "symbols.json"), allow_top=1)
    assert out["allow"] == ["GOODUSDT"] and "GOODUSDT" in out["scores"]
    assert out["deny"] == ["TOXICUSDT"]
    assert db.write_pair_scores(conn, df) == 3
