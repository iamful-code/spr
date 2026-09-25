"""Rule-based trade diagnostics. Each rule looks at what actually happened (round trips,
markouts, order outcomes) and turns a statistically supported pattern into a concrete
parameter change. The result is stored in `recommendations` and shown on the
dashboard; `--apply` writes the suggested values into config/overrides.json, which the
core picks up within five seconds."""

from __future__ import annotations

import json
import os
import sqlite3

import numpy as np
import pandas as pd

from . import db, markouts, roundtrips

RULES_DOC = {
    "back_of_queue": "Исполнения из хвоста очереди токсичны, а из головы — нет: нужен приоритет в очереди",
    "inside_too_tight": "Внутренние котировки чистые по markout'у, но захваченный спред не покрывает комиссии: сохранять большую долю спреда",
    "adverse_selection": "Markout через 5 с после входа хуже, чем −0.5 × полуспреда: нас переезжает информированный поток",
    "low_fill_ratio": "Мало исполнений при большой очереди впереди: котировки не доходят до сделки",
    "stale_holding": "Позиции держатся почти до max_hold_secs и выходы убыточны: инвентарь копится",
    "losing_symbol": "Символ стабильно убыточен на большом числе кругов",
    "spread_leakage": "Реализованный спред заметно меньше котируемого: цена уходит между входом и выходом",
    "fee_heavy": "Комиссии съедают больше половины валового результата",
    "too_few_trades": "Слишком мало кругов за час: спред-порог или фильтр пар слишком жёсткие",
    "kill_switch": "Сработал дневной стоп-лосс",
}


QUEUE_BINS = [-0.01, 0.01, 1.0, 3.0, float("inf")]
QUEUE_LABELS = ["0 (внутри спреда)", "<1x ордера", "1-3x", ">3x"]


def markout_breakdowns(mo: pd.DataFrame, params_blob: dict, horizons=markouts.HORIZONS_S) -> dict[str, pd.DataFrame]:
    """Entry markouts sliced by queue position, side, hour of day (UTC) and by the
    reference signal at placement. Shows where the adverse selection comes from."""
    if mo is None or len(mo) == 0 or "mo_5s" not in mo.columns:
        return {}
    df = mo[mo["purpose"] == "entry"].copy()
    if len(df) == 0:
        return {}
    notional = df["symbol"].map(lambda s: float(db.strategy_params_for(params_blob, s).get("order_notional_usd", 100.0)))
    df["queue_x"] = df["queue_ahead_initial"].astype(float).fillna(0.0) * df["price"].astype(float) / notional
    df["queue_bucket"] = pd.cut(df["queue_x"], bins=QUEUE_BINS, labels=QUEUE_LABELS)
    df["hour_utc"] = ((df["ts"] // 3_600_000) % 24).astype(int)
    out = {
        "by_queue": markouts.summarize_markouts(df, by=("queue_bucket",), horizons=horizons),
        "by_side": markouts.summarize_markouts(df, by=("side",), horizons=horizons),
        "by_hour": markouts.summarize_markouts(df, by=("hour_utc",), horizons=horizons),
    }
    if "ref_dev_bps" in df.columns and df["ref_dev_bps"].notna().any():
        # did the reference venue say the price was moving against the side we quoted?
        sgn = np.where(df["side"] == "Buy", 1.0, -1.0)
        rd = df["ref_dev_bps"].astype(float).fillna(0.0) * sgn
        df["ref_signal"] = pd.cut(rd, bins=[-np.inf, -2.0, 2.0, np.inf], labels=["против нас", "нейтрально", "за нас"])
        out["by_ref"] = markouts.summarize_markouts(df, by=("ref_signal",), horizons=horizons)
    for k, v in out.items():
        if "queue_bucket" in v.columns:
            v["queue_bucket"] = v["queue_bucket"].astype(str)
        if "ref_signal" in v.columns:
            v["ref_signal"] = v["ref_signal"].astype(str)
        out[k] = v[v["n"] > 0].reset_index(drop=True)
    return out


def _round_sig(x: float, sig: int = 3) -> float:
    if x == 0 or not np.isfinite(x):
        return x
    return float(f"{x:.{sig}g}")


def run_diagnostics(conn: sqlite3.Connection, md_dir: str, run_id: int, min_n: int = 30, since_ts: int | None = None) -> dict:
    """Compute everything and return {"recommendations": [...], "per_symbol": DataFrame, "markouts": DataFrame, "summary": dict}.
    `since_ts` restricts the evidence to fills and orders after that moment (the auto loop passes
    the time of its last parameter change, so each change is judged on fresh data only)."""
    fills = db.fills(conn, run_id, since_ts=since_ts)
    orders = db.orders(conn, run_id, since_ts=since_ts)
    params_blob = db.latest_params(conn, run_id)
    run = conn.execute("SELECT started_ts, ended_ts FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    started = int(run["started_ts"]) if run else 0
    if since_ts:
        started = max(started, int(since_ts))
    last_ts = int(max(fills["ts"].max() if len(fills) else 0, run["ended_ts"] or 0 if run else 0, db.now_ms() if run and not run["ended_ts"] else 0))
    hours = max((last_ts - started) / 3.6e6, 1e-6) if started else 1e-6

    rts = roundtrips.match_roundtrips(fills)
    per_sym = roundtrips.per_symbol(rts)
    summary = roundtrips.summarize(rts)
    mo = markouts.compute_markouts(fills, md_dir) if len(fills) else fills
    mo_entry = mo[mo["purpose"] == "entry"] if len(mo) else mo
    mo_sym = markouts.summarize_markouts(mo_entry, by=("symbol",)) if len(mo_entry) else pd.DataFrame()
    breakdowns = markout_breakdowns(mo, params_blob) if len(mo) else {}

    recs: list[dict] = []
    symbols = sorted(set(fills["symbol"])) if len(fills) else []
    for symbol in symbols:
        p = db.strategy_params_for(params_blob, symbol)
        srt = rts[rts["symbol"] == symbol]
        n_rt = len(srt)
        # 1. adverse selection
        if len(mo_sym) and symbol in set(mo_sym["symbol"]):
            row = mo_sym[mo_sym["symbol"] == symbol].iloc[0]
            if row["n"] >= min_n and np.isfinite(row.get("mo_5s", np.nan)):
                half = float(row.get("half_spread_bps", 0.0) or 0.0)
                if row["mo_5s"] < -0.5 * half and row["mo_5s"] < -0.5:
                    cur = float(p.get("min_spread_bps", 10.0))
                    recs.append({
                        "symbol": symbol, "rule": "adverse_selection", "severity": "high",
                        "message": f"{symbol}: markout 5с после входа {row['mo_5s']:.2f} bps при полуспреде {half:.2f} bps (n={int(row['n'])}). Поток токсичен: поднять min_spread_bps и ужесточить фильтр дисбаланса.",
                        "param": "min_spread_bps", "current_value": cur, "suggested_value": _round_sig(cur * 1.25),
                        "evidence": {"mo_1s": row.get("mo_1s"), "mo_5s": row["mo_5s"], "mo_30s": row.get("mo_30s"), "half_spread_bps": half, "n": int(row["n"])},
                    })
                    tox = float(p.get("toxicity_imbalance", 0.6))
                    recs.append({
                        "symbol": symbol, "rule": "adverse_selection", "severity": "medium",
                        "message": f"{symbol}: блокировать сторону при меньшем дисбалансе потока.",
                        "param": "toxicity_imbalance", "current_value": tox, "suggested_value": _round_sig(max(0.3, tox - 0.1)),
                        "evidence": {"mo_5s": row["mo_5s"]},
                    })
        # 2. fill ratio vs queue
        so = orders[orders["symbol"] == symbol] if len(orders) else orders
        decided = so[so["status"] != "rejected_post_only"] if len(so) else so
        if len(decided) >= min_n * 3:
            fill_ratio = float((decided["status"] == "filled").mean())
            qa = decided["queue_ahead_initial"].astype(float) * decided["price"].astype(float)
            order_notional = float(p.get("order_notional_usd", 100.0))
            if fill_ratio < 0.05 and qa.mean() > 3 * order_notional:
                mode = p.get("quote_mode", "join")
                if mode in ("join", "improve"):
                    recs.append({
                        "symbol": symbol, "rule": "low_fill_ratio", "severity": "medium",
                        "message": f"{symbol}: исполняется {fill_ratio:.1%} ордеров, впереди в очереди в среднем {qa.mean():.0f} USDT. Котировать только внутри спреда (inside).",
                        "param": "quote_mode", "current_value": mode, "suggested_value": "inside",
                        "evidence": {"fill_ratio": fill_ratio, "queue_ahead_usd": float(qa.mean()), "n_orders": int(len(decided))},
                    })
                else:
                    rq = int(p.get("min_requote_ms", 300))
                    recs.append({
                        "symbol": symbol, "rule": "low_fill_ratio", "severity": "low",
                        "message": f"{symbol}: исполняется {fill_ratio:.1%} ордеров; реже переставлять котировки, чтобы не терять место в очереди.",
                        "param": "min_requote_ms", "current_value": rq, "suggested_value": min(rq * 2, 5000),
                        "evidence": {"fill_ratio": fill_ratio, "queue_ahead_usd": float(qa.mean())},
                    })
        if n_rt >= min_n:
            # 3. stale holding
            max_hold = float(p.get("max_hold_secs", 120))
            p90 = float(np.percentile(srt["hold_secs"], 90))
            stale_exits = srt[srt["exit_purpose"] == "stale_exit"]
            if p90 > 0.8 * max_hold and len(stale_exits) >= 5 and stale_exits["net_pnl"].sum() < 0:
                cap = float(p.get("max_position_notional_usd", 300.0))
                order_notional = float(p.get("order_notional_usd", 100.0))
                recs.append({
                    "symbol": symbol, "rule": "stale_holding", "severity": "medium",
                    "message": f"{symbol}: p90 удержания {p90:.0f}с при лимите {max_hold:.0f}с, принудительные выходы дали {stale_exits['net_pnl'].sum():.2f} USDT. Уменьшить лимит позиции и сильнее скашивать котировки.",
                    "param": "max_position_notional_usd", "current_value": cap, "suggested_value": _round_sig(max(order_notional, cap * 0.7)),
                    "evidence": {"p90_hold_secs": p90, "n_stale_exits": int(len(stale_exits)), "stale_exit_pnl": float(stale_exits["net_pnl"].sum())},
                })
                skew = float(p.get("inventory_skew_bps", 4.0))
                recs.append({
                    "symbol": symbol, "rule": "stale_holding", "severity": "low",
                    "message": f"{symbol}: сильнее сдвигать котировки против накопленной позиции.",
                    "param": "inventory_skew_bps", "current_value": skew, "suggested_value": _round_sig(skew * 1.5),
                    "evidence": {"p90_hold_secs": p90},
                })
            # 4. losing symbol
            net = float(srt["net_pnl"].sum())
            wr = float((srt["net_pnl"] > 0).mean())
            if n_rt >= max(min_n, 20) and net < 0 and wr < 0.45:
                recs.append({
                    "symbol": symbol, "rule": "losing_symbol", "severity": "high",
                    "message": f"{symbol}: {n_rt} кругов, итог {net:.2f} USDT, доля прибыльных {wr:.0%}. Исключить символ (deny).",
                    "param": "deny", "current_value": "", "suggested_value": symbol,
                    "evidence": {"n": n_rt, "net_pnl": net, "win_rate": wr},
                })
            # 5. spread leakage
            quoted = float(srt["entry_spread_bps"].mean())
            captured = float(srt["captured_bps"].mean())
            if quoted > 0 and captured < 0.5 * quoted and captured < 2.0:
                cur = float(p.get("min_spread_bps", 10.0))
                recs.append({
                    "symbol": symbol, "rule": "spread_leakage", "severity": "medium",
                    "message": f"{symbol}: котируемый спред {quoted:.1f} bps, реализованный за круг {captured:.1f} bps. Цена уходит между входом и выходом: расширить порог спреда или проверить задержку.",
                    "param": "min_spread_bps", "current_value": cur, "suggested_value": _round_sig(cur * 1.2),
                    "evidence": {"quoted_bps": quoted, "captured_bps": captured, "n": n_rt},
                })
            # 5b. inside quotes are clean but capture too little of the spread
            if p.get("quote_mode") == "inside":
                sym_mo = mo_sym[mo_sym["symbol"] == symbol] if len(mo_sym) else mo_sym
                mo5 = float(sym_mo.iloc[0]["mo_5s"]) if len(sym_mo) and np.isfinite(sym_mo.iloc[0].get("mo_5s", np.nan)) else 0.0
                fee_bps = 4.0
                frac = float(p.get("inside_spread_frac", 0.8))
                if captured < 1.5 * fee_bps and mo5 > -1.0 and frac < 0.95:
                    recs.append({
                        "symbol": symbol, "rule": "inside_too_tight", "severity": "medium",
                        "message": f"{symbol}: внутри спреда исполнения чистые (markout 5с {mo5:.1f} bps), но захват {captured:.1f} bps за круг едва покрывает комиссии. Держать большую долю спреда.",
                        "param": "inside_spread_frac", "current_value": frac, "suggested_value": _round_sig(min(0.95, frac + 0.1)),
                        "evidence": {"captured_bps": captured, "markout_5s": mo5, "n": n_rt},
                    })
            # 6. fee heavy
            gross = float(srt["gross_pnl"].sum())
            fees = float(srt["fees"].sum())
            if gross > 0 and fees > 0.5 * gross:
                on = float(p.get("order_notional_usd", 100.0))
                recs.append({
                    "symbol": symbol, "rule": "fee_heavy", "severity": "low",
                    "message": f"{symbol}: комиссии {fees:.2f} из валовых {gross:.2f} USDT. Увеличить размер ордера и целевой edge.",
                    "param": "order_notional_usd", "current_value": on, "suggested_value": _round_sig(on * 1.5),
                    "evidence": {"gross": gross, "fees": fees},
                })
                edge = float(p.get("min_edge_bps", 3.0))
                recs.append({
                    "symbol": symbol, "rule": "fee_heavy", "severity": "low",
                    "message": f"{symbol}: требовать больший чистый edge за круг.",
                    "param": "min_edge_bps", "current_value": edge, "suggested_value": _round_sig(edge + 1.0),
                    "evidence": {"gross": gross, "fees": fees},
                })
    # 7. too few trades overall (global rule)
    n_active = conn.execute("SELECT n_active_symbols FROM pnl_snapshots WHERE run_id = ? ORDER BY ts DESC LIMIT 1", (run_id,)).fetchone()
    n_active = int(n_active[0]) if n_active and n_active[0] else 0
    rt_per_hour = len(rts) / hours
    if hours >= 1.0 and n_active > 0 and rt_per_hour < 2.0 * n_active:
        base = params_blob.get("strategy", {})
        cur = float(base.get("min_spread_bps", 10.0))
        recs.append({
            "symbol": None, "rule": "too_few_trades", "severity": "low",
            "message": f"{rt_per_hour:.1f} кругов в час на {n_active} активных символов. Снизить порог спреда и расширить набор пар.",
            "param": "min_spread_bps", "current_value": cur, "suggested_value": _round_sig(max(2 * 2.0 + 1.0, cur * 0.85)),
            "evidence": {"roundtrips_per_hour": rt_per_hour, "n_active": n_active, "hours": hours},
        })
    # 9. back of the queue is toxic, front is fine -> queue priority (global)
    bq = breakdowns.get("by_queue")
    if bq is not None and len(bq) >= 2:
        back = bq[bq["queue_bucket"].isin([">3x", "1-3x"])]
        front = bq[bq["queue_bucket"].isin(["0 (внутри спреда)", "<1x ордера"])]
        if len(back) and len(front) and back["n"].sum() >= min_n and front["n"].sum() >= min_n:
            mb = float((back["mo_5s"] * back["n"]).sum() / back["n"].sum())
            mf = float((front["mo_5s"] * front["n"]).sum() / front["n"].sum())
            if mb < -1.0 and mf - mb > 2.0:
                base = params_blob.get("strategy", {})
                if base.get("quote_mode", "join") in ("join", "improve"):
                    recs.append({
                        "symbol": None, "rule": "back_of_queue", "severity": "high",
                        "message": f"Входы из хвоста очереди дают markout {mb:.1f} bps, из головы {mf:.1f} bps. Нужен приоритет: котировать только внутри спреда (inside).",
                        "param": "quote_mode", "current_value": base.get("quote_mode", "join"), "suggested_value": "inside",
                        "evidence": {"markout_back_bps": mb, "markout_front_bps": mf, "n_back": int(back["n"].sum()), "n_front": int(front["n"].sum())},
                    })
    # 8. kill switch
    ks = conn.execute("SELECT ts, msg FROM events WHERE run_id = ? AND msg LIKE 'KILL SWITCH%' ORDER BY ts DESC LIMIT 1", (run_id,)).fetchone()
    if ks:
        recs.append({"symbol": None, "rule": "kill_switch", "severity": "high", "message": f"Сработал дневной стоп: {ks['msg']}", "param": None, "evidence": {"ts": int(ks["ts"])}})

    return {"recommendations": recs, "per_symbol": per_sym, "markouts": mo_sym, "breakdowns": breakdowns, "summary": summary, "roundtrips": rts, "hours": hours}


def apply_recommendations(recs: list[dict], overrides_path: str, symbols_path: str) -> dict:
    """Write suggested parameter values into overrides.json (per symbol, "*" for global) and
    deny-listed symbols into symbols.json. Returns what was written."""
    ov = _load_json(overrides_path, {})
    sy = _load_json(symbols_path, {"allow": [], "deny": [], "scores": {}})
    changed_ov = 0
    for r in recs:
        param = r.get("param")
        if not param or r.get("suggested_value") is None:
            continue
        if param == "deny":
            if r["suggested_value"] not in sy.setdefault("deny", []):
                sy["deny"].append(r["suggested_value"])
            continue
        key = r.get("symbol") or "*"
        bucket = ov.setdefault(key, {})
        if param in bucket and _is_num(bucket[param]) and _is_num(r["suggested_value"]) and _is_num(r.get("current_value")):
            # two rules touched the same parameter: keep the stronger move away from the current value
            cur = float(r["current_value"])
            if abs(float(bucket[param]) - cur) >= abs(float(r["suggested_value"]) - cur):
                continue
        bucket[param] = r["suggested_value"]
        changed_ov += 1
    _dump_json(overrides_path, ov)
    _dump_json(symbols_path, sy)
    return {"overrides": ov, "symbols": sy, "n_overrides": changed_ov}


def _is_num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _load_json(path: str, default):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            text = fh.read().strip()
            if text:
                return json.loads(text)
    return default


def _dump_json(path: str, obj) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)
