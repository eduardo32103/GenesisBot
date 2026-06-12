from __future__ import annotations

import csv
import math
import statistics
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.mt5.mt5_forward_profile_degradation_registry import (
    forward_profile_degradation,
    forward_profile_degradation_registry_status,
)
from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore
from services.mt5.mt5_research_rejection_registry import (
    research_rejection,
    research_rejection_registry_status,
)
from services.mt5.mt5_symbol_cost_model import build_symbol_cost_model


FAMILY = "volatility_compression_breakout"
SCAN_VERSION = "2026-06-12.mt5_volatility_compression_breakout_feature_scan.v1"

_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CSV_DIRS = (
    _REPO_ROOT / "data" / "backtests" / "multisymbol",
    _REPO_ROOT / "data" / "backtests",
)
DEFAULT_SYMBOLS = ("BTCUSD", "ETHUSD", "EURUSD", "GBPUSD", "US500", "USTEC", "XAUUSD")
DEFAULT_TIMEFRAMES = ("M15", "M30", "H1")
DEFAULT_MAX_ROWS_PER_FILE = 5000
DEFAULT_MAX_EVALUATIONS = 240
RECENT_FRACTION = 0.25

MIN_TOTAL_CLOSED = 50
MIN_RECENT_CLOSED = 20
MIN_TOTAL_PF = 1.15
MIN_RECENT_PF = 1.15
MIN_SPREAD_X2_PF = 0.95
MIN_REMOVE_BEST_5_PF = 1.0

_VARIANTS: tuple[dict[str, Any], ...] = (
    {"mode": "atr_compression_breakout", "compression": "atr"},
    {"mode": "bollinger_bandwidth_compression_breakout", "compression": "bollinger"},
    {"mode": "donchian_narrow_range_breakout", "compression": "donchian"},
    {"mode": "nr7_breakout", "compression": "nr7"},
    {"mode": "nr10_breakout", "compression": "nr10"},
    {"mode": "range_percentile_compression_breakout", "compression": "range_percentile"},
    {"mode": "atr_compression_trend_filter", "compression": "atr", "trend_filter": True},
    {"mode": "bollinger_compression_session_filter", "compression": "bollinger", "session_filter": True},
    {"mode": "donchian_fast_loss_cut", "compression": "donchian", "fast_loss_cut": True},
    {"mode": "nr7_trailing_defensive", "compression": "nr7", "trailing_defensive": True},
    {
        "mode": "range_percentile_trend_session",
        "compression": "range_percentile",
        "trend_filter": True,
        "session_filter": True,
    },
)


def run_volatility_compression_breakout_feature_scan(
    *,
    csv_dirs: list[str | Path] | str | Path | None = None,
    symbols: list[str] | str | None = None,
    timeframes: list[str] | str | None = None,
    max_rows_per_file: int = DEFAULT_MAX_ROWS_PER_FILE,
    max_evaluations: int = DEFAULT_MAX_EVALUATIONS,
    evaluations: list[dict[str, Any]] | None = None,
    persistent_events: dict[str, Any] | None = None,
    load_persistent: bool = True,
    store: MT5PersistentIntelligenceStore | Any | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    requested_symbols = _requested_symbols(symbols)
    requested_timeframes = _requested_timeframes(timeframes)
    max_rows = max(500, min(int(max_rows_per_file or DEFAULT_MAX_ROWS_PER_FILE), 20000))
    max_evals = max(1, min(int(max_evaluations or DEFAULT_MAX_EVALUATIONS), 1000))
    persistent = _load_persistent_lessons(persistent_events, load_persistent=load_persistent, store=store)
    rejection_registry = research_rejection_registry_status()
    degradation_registry = forward_profile_degradation_registry_status()

    scanned_csvs: list[str] = []
    missing_csvs: list[str] = []
    evaluated_rows: list[dict[str, Any]] = []

    sources = evaluations if evaluations is not None else _csv_evaluations(
        _requested_csv_dirs(csv_dirs),
        requested_symbols,
        requested_timeframes,
        max_rows_per_file=max_rows,
    )
    if evaluations is None:
        available_pairs = {(str(item.get("symbol")), str(item.get("timeframe"))) for item in sources}
        missing_csvs = _missing_expected_pairs(requested_symbols, requested_timeframes, available_pairs)

    for source in sources:
        if len(evaluated_rows) >= max_evals:
            break
        if isinstance(source.get("precomputed_result"), dict):
            evaluated_rows.append(_finalize_precomputed(source["precomputed_result"]))
            continue
        symbol = _symbol(source.get("symbol"))
        timeframe = _timeframe(source.get("timeframe"))
        if symbol not in requested_symbols or timeframe not in requested_timeframes:
            continue
        bars = list(source.get("bars") or [])
        csv_path = str(source.get("csv_path") or "")
        if csv_path:
            scanned_csvs.append(csv_path)
        indicator_rows = _indicator_rows(bars)
        for variant in _VARIANTS:
            if len(evaluated_rows) >= max_evals:
                break
            evaluated_rows.append(_evaluate_variant(symbol, timeframe, indicator_rows, variant, csv_path=csv_path))

    evaluated_rows = _dedupe_evaluations(evaluated_rows)
    evaluated_rows.sort(key=_ranking_key)
    deep_candidates = [row for row in evaluated_rows if row["candidate_status"] == "deep_validation_candidate"]
    top_feature_edges = (deep_candidates + [row for row in evaluated_rows if row["candidate_status"] == "near_miss"])[:10]
    near_misses = [row for row in evaluated_rows if row["candidate_status"] == "near_miss"][:10]
    rejected = [row for row in evaluated_rows if row["candidate_status"] == "rejected"]
    recommended = deep_candidates[0] if deep_candidates else None
    recommendation = "deep_validation_candidate_found" if recommended else "continue_research"

    return {
        "ok": True,
        "status": "volatility_compression_breakout_feature_scan_ready",
        "scan_version": SCAN_VERSION,
        "family": FAMILY,
        "mode": "ohlc_fast_feature_scan",
        "max_rows_per_file": max_rows,
        "max_evaluations": max_evals,
        "scanned_csvs": _dedupe(scanned_csvs),
        "missing_csvs": missing_csvs,
        "lessons_loaded": len(persistent.get("research_lessons") or []),
        "db_state": persistent.get("db_state") or {},
        "rejected_families_loaded": int(rejection_registry.get("count") or 0),
        "degraded_profiles_loaded": int(degradation_registry.get("count") or 0),
        "evaluations_count": len(evaluated_rows),
        "top_feature_edges": top_feature_edges,
        "near_misses": near_misses,
        "rejected_summary": _rejected_summary(rejected),
        "top_rejected": _top_rejected(rejected),
        "deep_validation_candidates": deep_candidates[:10],
        "recommended_next_candidate": recommended,
        "recommended_next_script": _recommended_next_script(recommended),
        "recommendation": recommendation,
        "recommended_next_research_phase": "single_candidate_deep_validation" if recommended else "continue_fast_edge_factory",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "paper_rotation_applied": False,
        "applies_to_real_trading": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "runtime_mutated": False,
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }


def _csv_evaluations(
    csv_dirs: list[Path],
    symbols: set[str],
    timeframes: set[str],
    *,
    max_rows_per_file: int,
) -> list[dict[str, Any]]:
    paths = _available_ohlc_paths(csv_dirs)
    evaluations: list[dict[str, Any]] = []
    for symbol in sorted(symbols):
        for timeframe in sorted(timeframes):
            path = paths.get((symbol, timeframe))
            if not path:
                continue
            evaluations.append(
                {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "csv_path": str(path),
                    "bars": _read_bars(path, max_rows_per_file=max_rows_per_file),
                }
            )
    return evaluations


def _available_ohlc_paths(csv_dirs: list[Path]) -> dict[tuple[str, str], Path]:
    best: dict[tuple[str, str], Path] = {}
    for root in csv_dirs:
        if not root.exists():
            continue
        for path in sorted(root.glob("*.csv")):
            parsed = _parse_csv_name(path)
            if not parsed:
                continue
            symbol, timeframe = parsed
            if timeframe not in DEFAULT_TIMEFRAMES:
                continue
            current = best.get((symbol, timeframe))
            if current is None or path.stat().st_size > current.stat().st_size:
                best[(symbol, timeframe)] = path
    return best


def _evaluate_variant(
    symbol: str,
    timeframe: str,
    rows: list[dict[str, Any]],
    variant: dict[str, Any],
    *,
    csv_path: str,
) -> dict[str, Any]:
    mode = str(variant["mode"])
    profile = f"{FAMILY}|mode={mode}"
    data_quality = _data_quality(rows)
    trades = _trades(symbol, rows, variant) if data_quality == "ok" else []
    metrics = _trade_metrics(rows, trades)
    return _finalize_row(
        {
            **metrics,
            "symbol": symbol,
            "timeframe": timeframe,
            "family": FAMILY,
            "profile": profile,
            "mode": mode,
            "csv_path": csv_path,
            "bars_loaded": len(rows),
            "source_identity_resolved": True,
            "data_quality": data_quality,
        }
    )


def _finalize_precomputed(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload.setdefault("family", FAMILY)
    payload.setdefault("profile", f"{FAMILY}|mode={payload.get('mode') or 'injected'}")
    payload.setdefault("source_identity_resolved", "unknown_profile" not in str(payload.get("profile") or "").casefold())
    payload.setdefault("data_quality", "ok")
    return _finalize_row(payload)


def _finalize_row(row: dict[str, Any]) -> dict[str, Any]:
    symbol = _symbol(row.get("symbol"))
    timeframe = _timeframe(row.get("timeframe"))
    family = str(row.get("family") or FAMILY)
    profile = str(row.get("profile") or family)
    degraded = forward_profile_degradation(symbol, timeframe, profile)
    rejected = research_rejection(symbol, timeframe, profile, family, FAMILY)
    sibling = _sibling_risk(symbol, timeframe, profile)
    reasons = _gate_reasons(row, degraded=bool(degraded), rejected=bool(rejected), sibling=sibling)
    status = _candidate_status(reasons, row)
    return {
        "candidate_id": _candidate_id(symbol, timeframe, profile),
        "symbol": symbol,
        "timeframe": timeframe,
        "family": family,
        "profile": profile,
        "mode": row.get("mode") or "",
        "csv_path": row.get("csv_path") or "",
        "bars_loaded": int(_number(row.get("bars_loaded"))),
        "source_identity_resolved": _bool(row.get("source_identity_resolved")),
        "total_closed": int(_number(row.get("total_closed") or row.get("closed"))),
        "recent_closed": int(_number(row.get("recent_closed"))),
        "total_pf": _number(row.get("total_pf") or row.get("profit_factor")),
        "recent_pf": _number(row.get("recent_pf") or row.get("recent_profit_factor")),
        "expectancy": _number(row.get("expectancy")),
        "recent_expectancy": _number(row.get("recent_expectancy")),
        "spread_x2_pf": _optional_number(row.get("spread_x2_pf")),
        "remove_best_5_pf": _optional_number(row.get("remove_best_5_pf")),
        "max_drawdown": _number(row.get("max_drawdown")),
        "single_trade_dependency": _optional_bool(row.get("single_trade_dependency")),
        "fragile_regime_dependency": _optional_bool(row.get("fragile_regime_dependency")),
        "data_quality": row.get("data_quality") or "ok",
        "degraded_by_registry": bool(degraded),
        "degradation_reason": degraded.get("degradation_reason") or "",
        "rejected_by_research_registry": bool(rejected),
        "research_rejection_reason": rejected.get("rejection_reason") or "",
        "sibling_risk": sibling,
        "sibling_risk_reason": "eth_m30_volatility_breakout_cluster_sibling" if sibling else "",
        "rejection_reasons": reasons,
        "rejection_signature": ",".join(sorted(reasons)),
        "candidate_status": status,
        "recommended_next_action": "run_single_deep_validation" if status == "deep_validation_candidate" else "skip_or_continue_research",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _trades(symbol: str, rows: list[dict[str, Any]], variant: dict[str, Any]) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    if len(rows) < 240:
        return trades
    spread_cost = _spread_cost(symbol, rows)
    for index in range(220, len(rows) - 9):
        previous = rows[index - 1]
        current = rows[index]
        if not _compressed(previous, variant):
            continue
        side = _breakout_side(previous, current, variant)
        if not side:
            continue
        if variant.get("trend_filter") and not _trend_filter(current, side):
            continue
        if variant.get("session_filter") and not _session_filter(current):
            continue
        trades.append(_trade_return(rows, index, side, spread_cost=spread_cost, variant=variant, spread_multiplier=1.0))
    return trades


def _indicator_rows(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    ema50: float | None = None
    ema200: float | None = None
    prev_close: float | None = None
    tr_values: deque[float] = deque(maxlen=14)
    atr_history: deque[float] = deque(maxlen=120)
    bb_history: deque[float] = deque(maxlen=120)
    donchian_history: deque[float] = deque(maxlen=120)
    range_history: deque[float] = deque(maxlen=120)
    close_history: deque[float] = deque(maxlen=20)
    range_history_short: deque[float] = deque(maxlen=20)
    for bar in bars:
        timestamp = _parse_time(bar.get("time"))
        open_price = _number_or_none(bar.get("open"))
        high = _number_or_none(bar.get("high"))
        low = _number_or_none(bar.get("low"))
        close = _number_or_none(bar.get("close"))
        if timestamp is None or open_price is None or high is None or low is None or close is None:
            continue
        true_range = _true_range(high, low, prev_close)
        tr_values.append(true_range)
        atr = _average_float(tr_values)
        close_history.append(close)
        range_history_short.append(high - low)
        ema50 = _ema(close, ema50, 50)
        ema200 = _ema(close, ema200, 200)
        bb_width = _bb_width(close_history)
        donchian_high = max((row["high"] for row in rows[-20:]), default=high)
        donchian_low = min((row["low"] for row in rows[-20:]), default=low)
        donchian_width = (donchian_high - donchian_low) / max(abs(close), 0.00000001)
        range_width = (max(close_history) - min(close_history)) / max(abs(close), 0.00000001) if len(close_history) >= 10 else 0.0
        range_size = high - low
        row = {
            "time": timestamp,
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "ema50": ema50,
            "ema200": ema200,
            "atr": atr,
            "atr_pct": _percentile_rank(atr, atr_history),
            "bb_width": bb_width,
            "bb_width_pct": _percentile_rank(bb_width, bb_history),
            "donchian_high": donchian_high,
            "donchian_low": donchian_low,
            "donchian_width": donchian_width,
            "donchian_width_pct": _percentile_rank(donchian_width, donchian_history),
            "range_width": range_width,
            "range_width_pct": _percentile_rank(range_width, range_history),
            "nr7": len(range_history_short) >= 7 and range_size <= min(list(range_history_short)[-7:]),
            "nr10": len(range_history_short) >= 10 and range_size <= min(list(range_history_short)[-10:]),
            "hour": timestamp.hour,
        }
        rows.append(row)
        if atr > 0:
            atr_history.append(atr)
        if bb_width > 0:
            bb_history.append(bb_width)
        if donchian_width > 0:
            donchian_history.append(donchian_width)
        if range_width > 0:
            range_history.append(range_width)
        prev_close = close
    return rows


def _compressed(row: dict[str, Any], variant: dict[str, Any]) -> bool:
    compression = str(variant.get("compression") or "")
    if compression == "atr":
        return float(row.get("atr_pct") or 1.0) <= 0.22
    if compression == "bollinger":
        return float(row.get("bb_width_pct") or 1.0) <= 0.22
    if compression == "donchian":
        return float(row.get("donchian_width_pct") or 1.0) <= 0.22
    if compression == "nr7":
        return bool(row.get("nr7"))
    if compression == "nr10":
        return bool(row.get("nr10"))
    if compression == "range_percentile":
        return float(row.get("range_width_pct") or 1.0) <= 0.22
    return False


def _breakout_side(previous: dict[str, Any], current: dict[str, Any], variant: dict[str, Any]) -> str:
    compression = str(variant.get("compression") or "")
    if compression in {"donchian", "range_percentile", "atr", "bollinger"}:
        high = float(previous.get("donchian_high") or previous.get("high") or 0.0)
        low = float(previous.get("donchian_low") or previous.get("low") or 0.0)
    else:
        high = float(previous.get("high") or 0.0)
        low = float(previous.get("low") or 0.0)
    close = float(current.get("close") or 0.0)
    if close > high:
        return "long"
    if close < low:
        return "short"
    return ""


def _trend_filter(row: dict[str, Any], side: str) -> bool:
    close = float(row.get("close") or 0.0)
    ema50 = float(row.get("ema50") or 0.0)
    ema200 = float(row.get("ema200") or 0.0)
    if side == "long":
        return close > ema50 > ema200
    return close < ema50 < ema200


def _session_filter(row: dict[str, Any]) -> bool:
    hour = int(row.get("hour") or 0)
    return 7 <= hour <= 20


def _trade_return(
    rows: list[dict[str, Any]],
    index: int,
    side: str,
    *,
    spread_cost: float,
    variant: dict[str, Any],
    spread_multiplier: float,
) -> dict[str, Any]:
    entry = float(rows[index]["close"])
    direction = 1.0 if side == "long" else -1.0
    atr = max(float(rows[index].get("atr") or 0.0), abs(entry) * 0.0001)
    stop_distance = atr * 1.2
    stop_return = -stop_distance / max(abs(entry), 0.00000001)
    future = rows[index + 1 : index + 9]
    stopped = False
    if side == "long":
        stopped = any(float(row["low"]) <= entry - stop_distance for row in future)
        favorable = max((float(row["high"]) - entry) / max(abs(entry), 0.00000001) for row in future)
    else:
        stopped = any(float(row["high"]) >= entry + stop_distance for row in future)
        favorable = max((entry - float(row["low"])) / max(abs(entry), 0.00000001) for row in future)
    close_2 = float(rows[index + 2]["close"])
    close_8 = float(rows[index + 8]["close"])
    return_2 = direction * (close_2 - entry) / max(abs(entry), 0.00000001)
    raw_return = stop_return if stopped else direction * (close_8 - entry) / max(abs(entry), 0.00000001)
    if variant.get("fast_loss_cut") and return_2 < 0:
        raw_return = max(return_2, stop_return)
    if variant.get("trailing_defensive") and favorable > 0 and raw_return < favorable * 0.5:
        raw_return = favorable * 0.5
    pnl = raw_return - spread_cost * spread_multiplier
    return {
        "index": index,
        "side": side,
        "pnl": pnl,
        "pnl_spread_x2": raw_return - spread_cost * 2.0,
        "regime": _regime(rows[index]),
    }


def _trade_metrics(rows: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, Any]:
    recent_start = int(len(rows) * (1.0 - RECENT_FRACTION)) if rows else 0
    recent = [trade for trade in trades if int(trade.get("index") or 0) >= recent_start]
    pnl = [float(trade.get("pnl") or 0.0) for trade in trades]
    recent_pnl = [float(trade.get("pnl") or 0.0) for trade in recent]
    spread_x2_pnl = [float(trade.get("pnl_spread_x2") or 0.0) for trade in trades]
    remove_best_5_pnl = _remove_best(pnl, 5)
    quarters = _quarter_pnls(trades, len(rows))
    return {
        "total_closed": len(trades),
        "recent_closed": len(recent),
        "total_pf": _profit_factor(pnl),
        "recent_pf": _profit_factor(recent_pnl),
        "expectancy": _average(pnl),
        "recent_expectancy": _average(recent_pnl),
        "spread_x2_pf": _profit_factor(spread_x2_pnl),
        "remove_best_5_pf": _profit_factor(remove_best_5_pnl),
        "max_drawdown": _max_drawdown(pnl),
        "single_trade_dependency": _single_trade_dependency(pnl),
        "fragile_regime_dependency": _fragile_regime_dependency(pnl, quarters),
    }


def _gate_reasons(row: dict[str, Any], *, degraded: bool, rejected: bool, sibling: bool) -> list[str]:
    reasons: list[str] = []
    profile = str(row.get("profile") or "")
    data_quality = str(row.get("data_quality") or "ok")
    if data_quality != "ok":
        reasons.append(data_quality)
    if "unknown_profile" in profile.casefold():
        reasons.append("unknown_profile")
    if not _bool(row.get("source_identity_resolved")):
        reasons.append("source_identity_unresolved")
    if int(_number(row.get("total_closed") or row.get("closed"))) < MIN_TOTAL_CLOSED:
        reasons.append("total_closed_below_50")
    if int(_number(row.get("recent_closed"))) < MIN_RECENT_CLOSED:
        reasons.append("recent_closed_below_20")
    if _number(row.get("total_pf") or row.get("profit_factor")) < MIN_TOTAL_PF:
        reasons.append("total_pf_below_1_15")
    if _number(row.get("recent_pf") or row.get("recent_profit_factor")) < MIN_RECENT_PF:
        reasons.append("recent_pf_below_1_15")
    if _number(row.get("expectancy")) <= 0:
        reasons.append("expectancy_not_positive")
    if _number(row.get("recent_expectancy")) <= 0:
        reasons.append("recent_expectancy_not_positive")
    if _has_value(row, "spread_x2_pf") and _number(row.get("spread_x2_pf")) < MIN_SPREAD_X2_PF:
        reasons.append("spread_x2_pf_below_0_95")
    if _has_value(row, "remove_best_5_pf") and _number(row.get("remove_best_5_pf")) < MIN_REMOVE_BEST_5_PF:
        reasons.append("remove_best_5_pf_below_1")
    if _has_value(row, "single_trade_dependency") and _bool(row.get("single_trade_dependency")):
        reasons.append("single_trade_dependency")
    if _has_value(row, "fragile_regime_dependency") and _bool(row.get("fragile_regime_dependency")):
        reasons.append("fragile_regime_dependency")
    if degraded:
        reasons.append("degraded_by_registry")
    if rejected:
        reasons.append("rejected_by_research_registry")
    if sibling:
        reasons.append("sibling_risk")
    return _dedupe(reasons)


def _candidate_status(reasons: list[str], row: dict[str, Any]) -> str:
    if not reasons:
        return "deep_validation_candidate"
    hard = {
        "degraded_by_registry",
        "rejected_by_research_registry",
        "sibling_risk",
        "unknown_profile",
        "source_identity_unresolved",
        "spread_x2_pf_below_0_95",
        "remove_best_5_pf_below_1",
        "single_trade_dependency",
        "fragile_regime_dependency",
    }
    if hard.intersection(reasons):
        return "rejected"
    if len(reasons) <= 3 and int(_number(row.get("total_closed") or row.get("closed"))) >= 30:
        return "near_miss"
    return "rejected"


def _rejected_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[f"{row.get('symbol')} {row.get('timeframe')} {row.get('mode') or row.get('profile')}"].append(row)
    summary: list[dict[str, Any]] = []
    for family, family_rows in grouped.items():
        counter = Counter(reason for row in family_rows for reason in (row.get("rejection_reasons") or []))
        summary.append(
            {
                "family": family,
                "rejected_count": len(family_rows),
                "top_rejection_reasons": [
                    {"reason": reason, "count": count}
                    for reason, count in counter.most_common(5)
                ],
            }
        )
    summary.sort(key=lambda row: (-int(row.get("rejected_count") or 0), str(row.get("family") or "")))
    return summary[:30]


def _top_rejected(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = sorted(rows, key=lambda row: (len(row.get("rejection_reasons") or []), -float(row.get("total_pf") or 0.0)))
    return rows[:10]


def _ranking_key(row: dict[str, Any]) -> tuple[int, float, int, str]:
    status_rank = {"deep_validation_candidate": 0, "near_miss": 1, "rejected": 2}.get(str(row.get("candidate_status")), 3)
    score = (
        float(row.get("total_pf") or 0.0) * 100.0
        + float(row.get("recent_pf") or 0.0) * 90.0
        + int(row.get("total_closed") or 0) * 0.4
        + int(row.get("recent_closed") or 0) * 1.2
    )
    return (status_rank, -score, -int(row.get("total_closed") or 0), str(row.get("candidate_id") or ""))


def _dedupe_evaluations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str, str]] = set()
    output: list[dict[str, Any]] = []
    for row in rows:
        key = (
            str(row.get("symbol") or ""),
            str(row.get("timeframe") or ""),
            str(row.get("profile") or ""),
            str(row.get("rejection_signature") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def _load_persistent_lessons(
    persistent_events: dict[str, Any] | None,
    *,
    load_persistent: bool,
    store: MT5PersistentIntelligenceStore | Any | None,
) -> dict[str, Any]:
    if persistent_events is not None:
        return {
            "research_lessons": _safe_list(persistent_events.get("recent_research_lessons")),
            "db_state": {"source": "injected", "db_degraded": bool(persistent_events.get("db_degraded")), **_safety()},
            **_safety(),
        }
    if not load_persistent:
        return {"research_lessons": [], "db_state": {"source": "disabled", "db_degraded": False, **_safety()}, **_safety()}
    try:
        active_store = store or MT5PersistentIntelligenceStore()
        events = active_store.recent_events(limit=30) if hasattr(active_store, "recent_events") else {}
        return {
            "research_lessons": _safe_list((events or {}).get("recent_research_lessons")),
            "db_state": {
                "source": "persistent_intelligence",
                "provider": (events or {}).get("provider") or "",
                "db_degraded": bool((events or {}).get("db_degraded")),
                "queue_depth": (events or {}).get("queue_depth", 0),
                "status_endpoints_write_free": bool((events or {}).get("status_endpoints_write_free", True)),
                **_safety(),
            },
            **_safety(),
        }
    except Exception as exc:  # pragma: no cover - defensive guard
        return {
            "research_lessons": [],
            "db_state": {"source": "persistent_intelligence", "db_degraded": True, "reason": type(exc).__name__, **_safety()},
            **_safety(),
        }


def _data_quality(rows: list[dict[str, Any]]) -> str:
    if len(rows) < 240:
        return "insufficient_bars"
    return "ok"


def _sibling_risk(symbol: str, timeframe: str, profile: str) -> bool:
    blob = f"{symbol} {timeframe} {profile} {FAMILY}".casefold()
    return symbol == "ETHUSD" and timeframe == "M30" and "breakout" in blob and "volatility" in blob


def _recommended_next_script(candidate: dict[str, Any] | None) -> str:
    if not candidate:
        return "python scripts/run_fast_edge_factory.py --run-fast-scans --max-evaluations 300"
    return f"python scripts/run_fast_edge_factory.py --deep-validate-candidate {candidate.get('candidate_id')}"


def _missing_expected_pairs(symbols: set[str], timeframes: set[str], available_pairs: set[tuple[str, str]]) -> list[str]:
    missing: list[str] = []
    for symbol in sorted(symbols):
        for timeframe in sorted(timeframes):
            if (symbol, timeframe) not in available_pairs:
                missing.append(f"{symbol}_{timeframe}")
    return missing


def _requested_csv_dirs(value: list[str | Path] | str | Path | None) -> list[Path]:
    if value is None or value == "":
        return [Path(path) for path in DEFAULT_CSV_DIRS]
    if isinstance(value, (str, Path)):
        return [Path(item.strip()) for item in str(value).split(",") if item.strip()]
    return [Path(item) for item in value]


def _requested_symbols(value: list[str] | str | None) -> set[str]:
    if value is None or value == "":
        return set(DEFAULT_SYMBOLS)
    items = value.split(",") if isinstance(value, str) else value
    return {_symbol(item) for item in items if _symbol(item)}


def _requested_timeframes(value: list[str] | str | None) -> set[str]:
    if value is None or value == "":
        return set(DEFAULT_TIMEFRAMES)
    items = value.split(",") if isinstance(value, str) else value
    return {_timeframe(item) for item in items if _timeframe(item)}


def _parse_csv_name(path: Path) -> tuple[str, str] | None:
    parts = path.stem.split("_")
    for index in range(len(parts) - 1, -1, -1):
        timeframe = _timeframe(parts[index])
        if timeframe in DEFAULT_TIMEFRAMES and index > 0:
            return _symbol("_".join(parts[:index])), timeframe
    return None


def _read_bars(path: Path, *, max_rows_per_file: int) -> list[dict[str, Any]]:
    rows: deque[dict[str, Any]] = deque(maxlen=max_rows_per_file)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            rows.append(row)
    return list(rows)


def _spread_cost(symbol: str, rows: list[dict[str, Any]]) -> float:
    first_price = float(rows[0].get("close") or 0.0) if rows else 0.0
    model = build_symbol_cost_model(symbol, first_price=first_price)
    return model.estimated_spread_price / max(abs(first_price), 0.00000001)


def _regime(row: dict[str, Any]) -> str:
    close = float(row.get("close") or 0.0)
    ema50 = float(row.get("ema50") or 0.0)
    ema200 = float(row.get("ema200") or 0.0)
    if close > ema50 > ema200:
        return "uptrend"
    if close < ema50 < ema200:
        return "downtrend"
    return "mixed"


def _quarter_pnls(trades: list[dict[str, Any]], rows_count: int) -> list[float]:
    quarters = [0.0, 0.0, 0.0, 0.0]
    if rows_count <= 0:
        return quarters
    for trade in trades:
        bucket = min(3, max(0, int((int(trade.get("index") or 0) / rows_count) * 4)))
        quarters[bucket] += float(trade.get("pnl") or 0.0)
    return quarters


def _fragile_regime_dependency(pnl: list[float], quarters: list[float]) -> bool:
    total = sum(pnl)
    if len(pnl) < 20 or total <= 0:
        return False
    if max(quarters or [0.0]) > total * 0.75:
        return True
    positive_quarters = sum(1 for value in quarters if value > 0)
    return positive_quarters <= 1


def _single_trade_dependency(pnl: list[float]) -> bool:
    wins = sorted([value for value in pnl if value > 0], reverse=True)
    if not wins:
        return False
    gross_win = sum(wins)
    return wins[0] >= gross_win * 0.45 or _profit_factor(_remove_best(pnl, 1)) < 1.0


def _remove_best(values: list[float], count: int) -> list[float]:
    output = list(values)
    for _ in range(min(count, len(output))):
        best = max(output)
        output.remove(best)
    return output


def _max_drawdown(pnl: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in pnl:
        equity += value
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return round(max_dd, 8)


def _profit_factor(values: list[float]) -> float:
    wins = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value < 0))
    if wins > 0 and losses == 0:
        return 999.0
    return round(wins / losses, 6) if losses > 0 else 0.0


def _average(values: list[float]) -> float:
    return round(sum(values) / len(values), 8) if values else 0.0


def _true_range(high: float, low: float, prev_close: float | None) -> float:
    if prev_close is None:
        return high - low
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def _bb_width(values: deque[float]) -> float:
    if len(values) < 20:
        return 0.0
    mean = sum(values) / len(values)
    stdev = statistics.pstdev(values)
    return (4.0 * stdev) / max(abs(mean), 0.00000001)


def _percentile_rank(value: float, history: deque[float]) -> float:
    if len(history) < 20:
        return 1.0
    return sum(1 for item in history if item <= value) / len(history)


def _ema(value: float, previous: float | None, period: int) -> float:
    alpha = 2.0 / (period + 1.0)
    return value if previous is None else previous + alpha * (value - previous)


def _average_float(values: deque[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _parse_time(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _has_value(row: dict[str, Any], key: str) -> bool:
    return row.get(key) not in {None, ""}


def _optional_number(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    return _number(value)


def _optional_bool(value: Any) -> bool | None:
    if value in {None, ""}:
        return None
    return _bool(value)


def _number(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def _number_or_none(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    return _number(value)


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().casefold() in {"1", "true", "yes", "y"}


def _safe_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def _candidate_id(symbol: str, timeframe: str, profile: str) -> str:
    raw = "_".join(part for part in (symbol, timeframe, profile) if part)
    return raw.casefold().replace(" ", "_").replace("|", "_").replace("/", "_")


def _symbol(value: object) -> str:
    symbol = str(value or "").upper().strip().replace(".B", "")
    if symbol in {"USTECB", "NAS100"}:
        return "USTEC"
    if symbol == "XAUUSDB":
        return "XAUUSD"
    return symbol


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
