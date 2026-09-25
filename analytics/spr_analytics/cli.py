"""Command line: python -m spr_analytics <analyze|pairs|optimize|report|clean>"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timezone

import pandas as pd

from . import db, diagnostics, md, optimizer, pairs, roundtrips


def parse_time(s: str) -> int:
    s = s.strip()
    if s.lstrip("-").isdigit():
        n = int(s)
        return n * 1000 if n < 100_000_000_000 else n
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return int(datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp() * 1000)
        except ValueError:
            pass
    return int(datetime.fromisoformat(s).timestamp() * 1000)


def fmt_ts(ms: int | None) -> str:
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def add_common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--db", default=db.DEFAULT_DB, help="SQLite database (default data/spr.db)")
    p.add_argument("--md-dir", default=db.DEFAULT_MD_DIR, help="market data directory (default data/md)")
    p.add_argument("--config-dir", default="config", help="where overrides.json / symbols.json live")


def cmd_analyze(a) -> int:
    conn = db.connect(a.db)
    run_id = a.run_id or db.latest_run_id(conn)
    if run_id is None:
        print("no runs in the database yet", file=sys.stderr)
        return 1
    res = diagnostics.run_diagnostics(conn, a.md_dir, run_id, min_n=a.min_n)
    s = res["summary"]
    print(f"run {run_id}: {s['n']} round trips over {res['hours']:.2f} h, gross {s['gross_pnl']:.2f}, fees {s['fees']:.2f}, net {s['net_pnl']:.2f} USDT, win rate {s['win_rate']:.0%}")
    if s["n"]:
        print(f"  avg net {s['avg_net_bps']:.2f} bps per round trip, captured {s['avg_captured_bps']:.2f} of quoted {s['avg_quoted_spread_bps']:.2f} bps; hold avg {s['avg_hold_secs']:.0f}s p90 {s['p90_hold_secs']:.0f}s")
    with pd.option_context("display.width", 200, "display.max_rows", 60, "display.float_format", "{:.3f}".format):
        if len(res["per_symbol"]):
            print("\nper symbol:")
            print(res["per_symbol"].head(30).to_string(index=False))
        if len(res["markouts"]):
            print("\nmarkouts after entries (bps, negative = adverse selection):")
            print(res["markouts"].to_string(index=False))
    recs = res["recommendations"]
    n = db.write_recommendations(conn, run_id, recs)
    print(f"\n{n} recommendations written")
    for r in recs:
        tgt = f"{r['param']} {r.get('current_value')} -> {r.get('suggested_value')}" if r.get("param") else ""
        print(f"  [{r['severity']}] {r['rule']}: {r['message']} {tgt}")
    if a.apply and recs:
        out = diagnostics.apply_recommendations(recs, os.path.join(a.config_dir, "overrides.json"), os.path.join(a.config_dir, "symbols.json"))
        print(f"applied {out['n_overrides']} parameter overrides; deny list: {out['symbols'].get('deny')}")
    return 0


def cmd_pairs(a) -> int:
    conn = db.connect(a.db)
    df = pairs.score_pairs(conn, lookback_hours=a.hours, min_roundtrips=a.min_roundtrips)
    if len(df) == 0:
        print("no symbol statistics in the lookback window", file=sys.stderr)
        return 1
    with pd.option_context("display.width", 200, "display.max_rows", 80, "display.float_format", "{:.3f}".format):
        print(df.head(a.top).to_string(index=False))
    db.write_pair_scores(conn, df)
    out_path = a.out or os.path.join(a.config_dir, "symbols.json")
    out = pairs.write_symbols_json(df, out_path, allow_top=a.allow_top)
    print(f"\nwritten {out_path}: allow={len(out['allow'])} deny={out['deny']} scores={len(out['scores'])}")
    return 0


def cmd_optimize(a) -> int:
    conn = db.connect(a.db)
    from_ms, to_ms = parse_time(a.__dict__["from"]), parse_time(a.to)
    symbols = [s for s in (a.symbols or "").split(",") if s]
    row = optimizer.optimize(
        conn,
        config=a.config,
        from_ms=from_ms,
        to_ms=to_ms,
        symbols=symbols,
        n_trials=a.trials,
        train_frac=a.train_frac,
        objective=optimizer.Objective(a.lam, a.mu),
        min_improvement=a.min_improvement,
        apply=a.apply,
        overrides_path=os.path.join(a.config_dir, "overrides.json"),
        bin_path=a.bin,
        md_dir=a.md_dir,
        seed=a.seed,
    )
    print(json.dumps({k: v for k, v in row.items()}, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_report(a) -> int:
    conn = db.connect(a.db, readonly=True)
    runs = db.runs(conn)
    if len(runs) == 0:
        print("no runs")
        return 0
    print("runs:")
    for r in runs.head(10).itertuples():
        print(f"  {r.run_id}  {r.mode:6}  {fmt_ts(r.started_ts)} -> {fmt_ts(r.ended_ts)}")
    run_id = a.run_id or int(runs.iloc[0].run_id)
    snap = conn.execute("SELECT * FROM pnl_snapshots WHERE run_id = ? ORDER BY ts DESC LIMIT 1", (run_id,)).fetchone()
    if snap:
        print(f"\nrun {run_id} at {fmt_ts(snap['ts'])}: equity {snap['equity']:.2f} realized {snap['realized']:.2f} fees {snap['fees']:.2f} unrealized {snap['unrealized']:.2f} positions {snap['open_positions']} live orders {snap['n_live_orders']} active {snap['n_active_symbols']} dd {snap['max_drawdown']:.2f}{' HALTED' if snap['halted'] else ''}")
    fills = db.fills(conn, run_id)
    rts = roundtrips.match_roundtrips(fills)
    print(f"fills {len(fills)}, round trips {len(rts)}")
    if len(rts):
        with pd.option_context("display.width", 200, "display.float_format", "{:.3f}".format):
            print(roundtrips.per_symbol(rts).head(20).to_string(index=False))
    recs = db.read_df(conn, "SELECT severity, rule, symbol, message FROM recommendations WHERE status='open' ORDER BY created_ts DESC LIMIT 20")
    if len(recs):
        print("\nopen recommendations:")
        for r in recs.itertuples():
            print(f"  [{r.severity}] {r.rule}: {r.message}")
    return 0


def cmd_clean(a) -> int:
    segs = md.list_segments(a.md_dir)
    cutoff = db.now_ms() - a.keep_days * 86_400_000
    removed = 0
    for seg in segs:
        if seg.day_start_ms + 86_400_000 < cutoff:
            shutil.rmtree(seg.dir, ignore_errors=True)
            removed += 1
    print(f"removed {removed} day directories older than {a.keep_days} days")
    return 0


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="spr_analytics", description="SPR analytics: diagnostics, pair scoring, optimizer")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("analyze", help="round trips, markouts, diagnostics -> recommendations")
    add_common(s)
    s.add_argument("--run-id", type=int)
    s.add_argument("--min-n", type=int, default=30, help="minimum sample size for a rule to fire")
    s.add_argument("--apply", action="store_true", help="write suggested parameters into overrides.json / symbols.json")
    s.set_defaults(fn=cmd_analyze)

    s = sub.add_parser("pairs", help="offline pair scoring -> symbols.json")
    add_common(s)
    s.add_argument("--hours", type=float, default=24.0)
    s.add_argument("--min-roundtrips", type=int, default=20)
    s.add_argument("--allow-top", type=int, default=0, help="restrict the core to the N best symbols (0 = no allow list)")
    s.add_argument("--top", type=int, default=40, help="rows to print")
    s.add_argument("--out")
    s.set_defaults(fn=cmd_pairs)

    s = sub.add_parser("optimize", help="Optuna over spr replay with walk-forward validation")
    add_common(s)
    s.add_argument("--from", required=True)
    s.add_argument("--to", required=True)
    s.add_argument("--symbols", help="comma separated; empty = all recorded symbols (writes the '*' override)")
    s.add_argument("--trials", type=int, default=40)
    s.add_argument("--train-frac", type=float, default=0.7)
    s.add_argument("--lam", type=float, default=0.5, help="drawdown penalty")
    s.add_argument("--mu", type=float, default=0.01, help="average inventory penalty per USDT")
    s.add_argument("--min-improvement", type=float, default=1.0, help="required gain of the objective on held-out data")
    s.add_argument("--apply", action="store_true")
    s.add_argument("--config", default="config/strategy.toml")
    s.add_argument("--bin", help="path to the spr binary")
    s.add_argument("--seed", type=int, default=0)
    s.set_defaults(fn=cmd_optimize)

    s = sub.add_parser("report", help="text summary of the latest run")
    add_common(s)
    s.add_argument("--run-id", type=int)
    s.set_defaults(fn=cmd_report)

    s = sub.add_parser("clean", help="delete market data older than N days")
    add_common(s)
    s.add_argument("--keep-days", type=int, default=14)
    s.set_defaults(fn=cmd_clean)

    a = p.parse_args(argv)
    sys.exit(a.fn(a))
