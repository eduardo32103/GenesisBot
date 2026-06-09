from __future__ import annotations

import csv
import math
import random
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation
from services.mt5.mt5_research_rejection_registry import research_rejection
from services.mt5.mt5_symbol_cost_model import build_symbol_cost_model


SYMBOL = "EURUSD"
TIMEFRAME = "H1"
FAMILY = "session_vwap_reclaim"
DEFAULT_CSV_PATHS = (
    Path("data") / "backtests" / "multisymbol" / "EURUSD_H1_20000.csv",
    Path("data") / "backtests" / "EURUSD_H1_40000.csv",
    Path("data") / "backtests" / "EURUSD_H1_60000.csv",
    Path("data") / "backtests" / "multisymbol" / "EURUSD_H1_40000.csv",
    Path("data") / "backtests" / "multisymbol" / "EURUSD_H1_60000.csv",
)

MIN_RECENT_CLOSED = 15
MIN_TOTAL_CLOSED = 45
MIN_RECENT_PF = 1.05
MIN_TOTAL_PF = 1.15
MIN_MONTE_CARLO_STRESSED_PF = 1.05
MIN_SPREAD_X2_PF = 0.95
MIN_REMOVE_BEST_5_PF = 1.0
RECENT_FRACTION = 0.25

_REJECTED_MARKERS = (
    "volatility_breakout",
    "vol_breakout",
    "session_open_continuation",
    "ema_reclaim",
    "london_us_breakout",
    "opening_range_fakeout",
)

_VARIANTS: tuple[dict[str, Any], ...] = (
    {"mode": "baseline", "actions": ("baseline",), "min_distance_pct": 0.0, "hold_bars": 4},
    {"mode": "distance_filter", "actions": ("distance_filter",), "min_distance_pct": 0.00025, "hold_bars": 4},
    {
        "mode": "momentum_distance_filter",
        "actions": ("momentum_guard", "distance_filter"),
        "momentum": True,
        "min_distance_pct": 0.00025,
        "hold_bars": 4,
    },
    {"mode": "trend_guard", "actions": ("trend_guard",), "trend_guard": True, "hold_bars": 4},
    {"mode": "volatility_guard", "actions": ("volatility_guard",), "volatility_guard": True, "hold_bars": 4},
    {"mode": "spread_guard", "actions": ("spread_guard",), "min_distance_pct": 0.00035, "hold_bars": 4},
    {"mode": "mae_guard", "actions": ("mae_guard",), "mae_guard": 0.0012, "hold_bars": 4},
    {"mode": "fast_loss_cut", "actions": ("fast_loss_cut",), "fast_loss_cut": 0.00045, "hold_bars": 4},
    {"mode": "trailing_defensive", "actions": ("trailing_defensive",), "trailing_defensive": True, "hold_bars": 8},
    {"mode": "time_stop_guard", "actions": ("time_stop_guard",), "hold_bars": 2},
    {"mode": "session_filter_london", "actions": ("session_filter_london",), "session_hours": {7, 8, 9, 10, 11}, "hold_bars": 4},
    {"mode": "session_filter_ny", "actions": ("session_filter_ny",), "session_hours": {13, 14, 15, 16, 17}, "hold_bars": 4},
    {
        "mode": "session_filter_london_ny",
        "actions": ("session_filter_london_ny",),
        "session_hours": {7, 8, 9, 10, 11, 13, 14, 15, 16, 17},
        "hold_bars": 4,
    },
    {
        "mode": "distance_trend_guard",
        "actions": ("distance_filter", "trend_guard"),
        "min_distance_pct": 0.00025,
        "trend_guard": True,
        "hold_bars": 4,
    },
    {
        "mode": "distance_volatility_guard",
        "actions": ("distance_filter", "volatility_guard"),
        "min_distance_pct": 0.00025,
        "volatility_guard": True,
        "hold_bars": 4,
    },
    {
        "mode": "distance_london_ny",
        "actions": ("distance_filter", "session_filter_london_ny"),
        "min_distance_pct": 0.00025,
        "session_hours": {7, 8, 9, 10, 11, 13, 14, 15, 16, 17},
        "hold_bars": 4,
    },
    {
        "mode": "momentum_london_ny",
        "actions": ("momentum_guard", "session_filter_london_ny"),
        "momentum": True,
        "session_hours": {7, 8, 9, 10, 11, 13, 14, 15, 16, 17},
        "hold_bars": 4,
    },
)


def run_eurusd_h1_vwap_reclaim_hardening(body: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    body = body or {}
    if isinstance(body.get("rows"), list):
        rows = [_finalize_row(dict(row)) for row in body["rows"] if isinstance(row, dict)]
        rows.sort(key=_ranking_key)
        return _result(rows, [], [], [], started)

    csv_paths = _requested_csv_paths(body.get("csv_paths"))
    max_bars = max(500, min(int(_number(body.get("max_bars")) or 20000), 65000))
    monte_carlo_simulations = max(100, min(int(_number(body.get("monte_carlo_simulations")) or 300), 1000))
    requested_targets = _requested_list(body.get("targets"), [str(item["mode"]) for item in _VARIANTS])

    rows: list[dict[str, Any]] = []
    used_csvs: list[str] = []
    missing_csvs: list[str] = []
    errors: list[dict[str, Any]] = []
    for csv_path in csv_paths:
        if not csv_path.exists():
            missing_csvs.append(str(csv_path))
            continue
        bars = _read_bars(csv_path, max_bars=max_bars)
        if not bars:
            errors.append({"csv_path": str(csv_path), "error": "csv_bars_not_loaded"})
            continue
        used_csvs.append(str(csv_path))
        cost_model = build_symbol_cost_model(SYMBOL, resolved_symbol=SYMBOL, first_price=_number(bars[0].get("close")) or 0.0)
        for variant in _VARIANTS:
            if str(variant["mode"]) not in requested_targets:
                continue
            rows.append(
                _evaluate_variant(
                    bars,
                    variant,
                    csv_path=csv_path,
                    cost_per_trade=_cost_per_trade(cost_model, bars),
                    monte_carlo_simulations=monte_carlo_simulations,
                )
            )

    rows.sort(key=_ranking_key)
    return _result(rows, used_csvs, missing_csvs, errors, started)


def _evaluate_variant(
    bars: list[dict[str, Any]],
    variant: dict[str, Any],
    *,
    csv_path: Path,
    cost_per_trade: float,
    monte_carlo_simulations: int,
) -> dict[str, Any]:
    profile = f"eurusd_h1_vwap_reclaim_{variant['mode']}"
    data_quality = _data_quality(bars)
    trades = _trades(bars, variant, cost_per_trade=cost_per_trade) if data_quality == "ok" else []
    metrics = _metrics(trades)
    recent_metrics = _metrics(_recent_trades(trades, bars))
    monte_carlo = _monte_carlo_stress([trade["pnl"] for trade in trades], simulations=monte_carlo_simulations)
    spread_x2 = _metrics([{**trade, "pnl": trade["raw_return"] - cost_per_trade * 2.0} for trade in trades])
    remove_best = _remove_best_metrics(trades, 5)
    degraded = forward_profile_degradation(SYMBOL, TIMEFRAME, profile)
    rejected = research_rejection(SYMBOL, TIMEFRAME, profile, FAMILY, FAMILY)
    sibling = _sibling_risk(profile)

    return _finalize_row(
        {
            "symbol": SYMBOL,
            "timeframe": TIMEFRAME,
            "family": FAMILY,
            "profile": profile,
            "target_name": profile,
            "hardening_actions": list(variant.get("actions") or (variant["mode"],)),
            "csv_path": str(csv_path),
            "csv_label": csv_path.name,
            "bars_loaded": len(bars),
            "data_quality": data_quality,
            "recent_closed": recent_metrics["closed"],
            "total_closed": metrics["closed"],
            "recent_pf": recent_metrics["profit_factor"],
            "total_pf": metrics["profit_factor"],
            "expectancy": metrics["expectancy"],
            "monte_carlo_stressed_pf": monte_carlo["profit_factor"],
            "monte_carlo_stressed_expectancy": monte_carlo["expectancy"],
            "spread_x2_pf": spread_x2["profit_factor"],
            "remove_best_5_pf": remove_best["profit_factor"],
            "max_drawdown": metrics["max_drawdown"],
            "fragile_regime_dependency": _fragile_regime_dependency(trades, metrics),
            "single_trade_dependency": _single_trade_dependency(trades, metrics),
            "degraded_by_registry": bool(degraded),
            "degradation_reason": degraded.get("degradation_reason") or "",
            "rejected_by_research_registry": bool(rejected),
            "research_rejection_reason": rejected.get("rejection_reason") or "",
            "sibling_risk": sibling,
            "sibling_risk_reason": "sibling_of_failed_profile" if sibling else "",
            "cost_per_trade": cost_per_trade,
            "source": "eurusd_h1_vwap_reclaim_hardening",
        }
    )


def _trades(bars: list[dict[str, Any]], variant: dict[str, Any], *, cost_per_trade: float) -> list[dict[str, Any]]:
    rows = _session_vwap_rows(bars)
    trades: list[dict[str, Any]] = []
    max_hold = int(variant.get("hold_bars") or 4)
    for index in range(30, len(rows) - max_hold - 1):
        side = _signal_side(rows, index, variant)
        if not side:
            continue
        entry = float(rows[index]["close"])
        exit_index = _exit_index(rows, index, side, variant)
        exit_price = float(rows[exit_index]["close"])
        direction = 1.0 if side == "long" else -1.0
        raw_return = direction * (exit_price - entry) / max(abs(entry), 0.00000001)
        trades.append(
            {
                "opened_index": index,
                "closed_index": exit_index,
                "side": side,
                "raw_return": raw_return,
                "pnl": raw_return - cost_per_trade,
            }
        )
    return trades


def _signal_side(rows: list[dict[str, Any]], index: int, variant: dict[str, Any]) -> str:
    previous = rows[index - 1]
    current = rows[index]
    prev_close = float(previous["close"])
    curr_close = float(current["close"])
    prev_vwap = float(previous["session_vwap"])
    curr_vwap = float(current["session_vwap"])
    if prev_vwap <= 0 or curr_vwap <= 0 or curr_close <= 0:
        return ""
    hour = current["time"].hour if isinstance(current.get("time"), datetime) else -1
    session_hours = variant.get("session_hours")
    if isinstance(session_hours, set) and hour not in session_hours:
        return ""
    distance = abs(prev_close - prev_vwap) / max(abs(prev_close), 0.00000001)
    if distance < float(variant.get("min_distance_pct") or 0.0):
        return ""
    momentum = curr_close - prev_close
    long_signal = prev_close < prev_vwap and curr_close > curr_vwap
    short_signal = prev_close > prev_vwap and curr_close < curr_vwap
    if variant.get("momentum") and long_signal and momentum <= 0:
        long_signal = False
    if variant.get("momentum") and short_signal and momentum >= 0:
        short_signal = False
    if variant.get("trend_guard"):
        trend = _trend(rows, index)
        long_signal = long_signal and trend > 0
        short_signal = short_signal and trend < 0
    if variant.get("volatility_guard") and not _normal_volatility(rows, index):
        return ""
    if long_signal:
        return "long"
    if short_signal:
        return "short"
    return ""


def _exit_index(rows: list[dict[str, Any]], index: int, side: str, variant: dict[str, Any]) -> int:
    hold_bars = int(variant.get("hold_bars") or 4)
    direction = 1.0 if side == "long" else -1.0
    entry = float(rows[index]["close"])
    best_return = 0.0
    exit_index = min(index + hold_bars, len(rows) - 1)
    for offset in range(1, hold_bars + 1):
        current_index = min(index + offset, len(rows) - 1)
        current_return = direction * (float(rows[current_index]["close"]) - entry) / max(abs(entry), 0.00000001)
        best_return = max(best_return, current_return)
        if variant.get("fast_loss_cut") and offset == 1 and current_return < -float(variant["fast_loss_cut"]):
            return current_index
        if variant.get("mae_guard") and current_return < -float(variant["mae_guard"]):
            return current_index
        if variant.get("trailing_defensive") and best_return > 0.0009 and current_return < best_return * 0.35:
            return current_index
    return exit_index


def _finalize_row(row: dict[str, Any]) -> dict[str, Any]:
    row["symbol"] = str(row.get("symbol") or SYMBOL).upper()
    row["timeframe"] = str(row.get("timeframe") or TIMEFRAME).upper()
    row["family"] = str(row.get("family") or FAMILY)
    row["profile"] = str(row.get("profile") or row.get("target_name") or FAMILY)
    row["target_name"] = str(row.get("target_name") or row["profile"])
    row["data_quality"] = str(row.get("data_quality") or "ok")
    row["recent_closed"] = int(_number(row.get("recent_closed")) or 0)
    row["total_closed"] = int(_number(row.get("total_closed")) or 0)
    for key in (
        "recent_pf",
        "total_pf",
        "expectancy",
        "monte_carlo_stressed_pf",
        "monte_carlo_stressed_expectancy",
        "spread_x2_pf",
        "remove_best_5_pf",
        "max_drawdown",
    ):
        row[key] = float(_number(row.get(key)) or 0.0)
    row["degraded_by_registry"] = bool(row.get("degraded_by_registry") or forward_profile_degradation(row["symbol"], row["timeframe"], row["profile"]))
    rejected = research_rejection(row["symbol"], row["timeframe"], row["profile"], row["family"], FAMILY)
    row["rejected_by_research_registry"] = bool(row.get("rejected_by_research_registry") or rejected)
    row["research_rejection_reason"] = str(row.get("research_rejection_reason") or rejected.get("rejection_reason") or "")
    row["sibling_risk"] = bool(row.get("sibling_risk") or _sibling_risk(row["profile"]))
    row["rejection_reasons"] = _gate_reasons(row)
    row["candidate_status"] = "paper_forward_review_ready" if not row["rejection_reasons"] else "gate_failed"
    row["recommendation"] = "paper_forward_candidate_review" if row["candidate_status"] == "paper_forward_review_ready" else "continue_research"
    row["recommended_next_action"] = row["recommendation"]
    row["hardening_score"] = _score(row)
    return {
        **row,
        "applies_to_paper_shadow": False,
        "applies_to_real_trading": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "live_runtime_mutated": False,
        "shadow_trades_mutated": False,
        "martingale_enabled": False,
        "grid_enabled": False,
        "averaging_down_enabled": False,
        "increase_size_after_loss_enabled": False,
        **_safety(),
    }


def _gate_reasons(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if row.get("data_quality") != "ok":
        reasons.append(str(row.get("data_quality")))
    if row.get("degraded_by_registry"):
        reasons.append("degraded_by_registry")
    if row.get("rejected_by_research_registry"):
        reasons.append("research_rejection_registry")
    if row.get("sibling_risk"):
        reasons.append("sibling_risk")
    _min_gate(reasons, row, "recent_closed", MIN_RECENT_CLOSED)
    _min_gate(reasons, row, "total_closed", MIN_TOTAL_CLOSED)
    _min_gate(reasons, row, "recent_pf", MIN_RECENT_PF)
    _min_gate(reasons, row, "total_pf", MIN_TOTAL_PF)
    if float(row.get("expectancy") or 0.0) <= 0.0:
        reasons.append("expectancy_not_positive")
    _min_gate(reasons, row, "monte_carlo_stressed_pf", MIN_MONTE_CARLO_STRESSED_PF)
    if float(row.get("monte_carlo_stressed_expectancy") or 0.0) <= 0.0:
        reasons.append("monte_carlo_stressed_expectancy_not_positive")
    _min_gate(reasons, row, "spread_x2_pf", MIN_SPREAD_X2_PF)
    _min_gate(reasons, row, "remove_best_5_pf", MIN_REMOVE_BEST_5_PF)
    if _flag(row.get("fragile_regime_dependency")):
        reasons.append("fragile_regime_dependency")
    if _flag(row.get("single_trade_dependency")):
        reasons.append("single_trade_dependency")
    return reasons


def _result(
    rows: list[dict[str, Any]],
    used_csvs: list[str],
    missing_csvs: list[str],
    errors: list[dict[str, Any]],
    started: float,
) -> dict[str, Any]:
    candidates = [row for row in rows if row.get("candidate_status") == "paper_forward_review_ready"]
    best = rows[0] if rows else None
    return {
        "ok": True,
        "status": "eurusd_h1_vwap_reclaim_hardening_ready",
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "family": FAMILY,
        "csv_used": used_csvs,
        "missing_csvs": missing_csvs,
        "variants_evaluated": len(rows),
        "results": rows,
        "best_variant": best,
        "candidates": candidates,
        "recommended_candidate": candidates[0] if candidates else None,
        "recommendation": "paper_forward_candidate_review" if candidates else "continue_research",
        "errors": errors,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "live_runtime_mutated": False,
        "shadow_trades_mutated": False,
        "martingale_enabled": False,
        "grid_enabled": False,
        "averaging_down_enabled": False,
        "increase_size_after_loss_enabled": False,
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }


def _metrics(trades: list[dict[str, Any]]) -> dict[str, float | int]:
    values = [float(trade.get("pnl") or 0.0) for trade in trades]
    wins = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value < 0))
    pf = 999.0 if wins > 0 and losses == 0 else (wins / losses if losses > 0 else 0.0)
    return {
        "closed": len(values),
        "profit_factor": round(pf, 6),
        "expectancy": round(sum(values) / len(values), 8) if values else 0.0,
        "max_drawdown": round(_max_drawdown(values), 8),
    }


def _monte_carlo_stress(values: list[float], *, simulations: int) -> dict[str, float]:
    if not values:
        return {"profit_factor": 0.0, "expectancy": 0.0}
    rng = random.Random(20260609)
    pfs: list[float] = []
    expectancies: list[float] = []
    count = len(values)
    for _ in range(simulations):
        sample = [rng.choice(values) for _ in range(count)]
        stressed = sorted(sample)[: max(1, int(count * 0.85))]
        metrics = _metrics([{"pnl": value} for value in stressed])
        pfs.append(float(metrics["profit_factor"]))
        expectancies.append(float(metrics["expectancy"]))
    return {
        "profit_factor": round(_percentile(pfs, 20), 6),
        "expectancy": round(_percentile(expectancies, 20), 8),
    }


def _remove_best_metrics(trades: list[dict[str, Any]], count: int) -> dict[str, float | int]:
    values = sorted((float(trade.get("pnl") or 0.0) for trade in trades), reverse=True)[count:]
    return _metrics([{"pnl": value} for value in values])


def _recent_trades(trades: list[dict[str, Any]], bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cutoff = int(len(bars) * (1.0 - RECENT_FRACTION))
    return [trade for trade in trades if int(trade.get("opened_index") or 0) >= cutoff]


def _fragile_regime_dependency(trades: list[dict[str, Any]], total: dict[str, Any]) -> bool:
    if len(trades) < 45:
        return True
    midpoint = len(trades) // 2
    first = _metrics(trades[:midpoint])
    second = _metrics(trades[midpoint:])
    total_pf = float(total.get("profit_factor") or 0.0)
    return total_pf >= 1.15 and (float(first["profit_factor"]) < 0.9 or float(second["profit_factor"]) < 0.9)


def _single_trade_dependency(trades: list[dict[str, Any]], total: dict[str, Any]) -> bool:
    if len(trades) < 10:
        return True
    values = [float(trade.get("pnl") or 0.0) for trade in trades]
    gross = sum(abs(value) for value in values)
    if gross <= 0:
        return True
    if max(values) / gross > 0.28:
        return True
    return float(_remove_best_metrics(trades, 1)["profit_factor"]) < 1.0 <= float(total.get("profit_factor") or 0.0)


def _session_vwap_rows(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    session_key = ""
    cumulative_pv = 0.0
    cumulative_volume = 0.0
    for bar in bars:
        timestamp = _parse_time(bar.get("time"))
        key = timestamp.date().isoformat() if timestamp else str(bar.get("time") or "")
        if key != session_key:
            session_key = key
            cumulative_pv = 0.0
            cumulative_volume = 0.0
        high = _number(bar.get("high")) or 0.0
        low = _number(bar.get("low")) or 0.0
        close = _number(bar.get("close")) or 0.0
        volume = _volume(bar)
        typical = (high + low + close) / 3.0
        cumulative_pv += typical * volume
        cumulative_volume += volume
        output.append(
            {
                **bar,
                "time": timestamp,
                "close": close,
                "session_vwap": cumulative_pv / cumulative_volume if cumulative_volume > 0 else 0.0,
            }
        )
    return output


def _normal_volatility(rows: list[dict[str, Any]], index: int) -> bool:
    if index < 20:
        return False
    ranges = [abs(float(rows[i]["high"]) - float(rows[i]["low"])) / max(abs(float(rows[i]["close"])), 0.00000001) for i in range(index - 20, index)]
    current = abs(float(rows[index]["high"]) - float(rows[index]["low"])) / max(abs(float(rows[index]["close"])), 0.00000001)
    average = sum(ranges) / len(ranges)
    return average * 0.55 <= current <= average * 1.9


def _trend(rows: list[dict[str, Any]], index: int) -> float:
    if index < 24:
        return 0.0
    fast = sum(float(rows[i]["close"]) for i in range(index - 6, index)) / 6.0
    slow = sum(float(rows[i]["close"]) for i in range(index - 24, index)) / 24.0
    return fast - slow


def _data_quality(bars: list[dict[str, Any]]) -> str:
    if len(bars) < 100:
        return "insufficient_bars"
    if not any(_volume(bar) > 0.0 for bar in bars):
        return "missing_volume"
    if not all((_number(bar.get("high")) and _number(bar.get("low")) and _number(bar.get("close"))) for bar in bars):
        return "missing_price"
    return "ok"


def _read_bars(path: Path, *, max_bars: int) -> list[dict[str, Any]]:
    rows: deque[dict[str, Any]] = deque(maxlen=max_bars)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            parsed = dict(row)
            parsed["time"] = _parse_time(row.get("time"))
            for key in ("open", "high", "low", "close", "volume", "tick_volume"):
                if key in parsed:
                    parsed[key] = _number(parsed.get(key)) or 0.0
            rows.append(parsed)
    return list(rows)


def _cost_per_trade(cost_model: Any, bars: list[dict[str, Any]]) -> float:
    first_price = float(_number(bars[0].get("close")) or 0.0) if bars else 0.0
    spread_return = cost_model.estimated_spread_price / max(abs(first_price), 0.00000001)
    slippage_return = (cost_model.slippage_assumption * cost_model.point) / max(abs(first_price), 0.00000001)
    return round(spread_return + slippage_return, 10)


def _score(row: dict[str, Any]) -> float:
    score = 0.0
    score += min(int(row.get("recent_closed") or 0), 100) * 3.0
    score += min(int(row.get("total_closed") or 0), 250) * 0.7
    score += max(0.0, float(row.get("recent_pf") or 0.0) - 1.0) * 110.0
    score += max(0.0, float(row.get("total_pf") or 0.0) - 1.0) * 120.0
    score += max(0.0, float(row.get("monte_carlo_stressed_pf") or 0.0) - 1.0) * 180.0
    score += max(0.0, float(row.get("remove_best_5_pf") or 0.0) - 1.0) * 110.0
    score += max(0.0, float(row.get("spread_x2_pf") or 0.0) - 0.95) * 80.0
    score += max(0.0, float(row.get("expectancy") or 0.0)) * 100000.0
    score += max(0.0, float(row.get("monte_carlo_stressed_expectancy") or 0.0)) * 100000.0
    score -= len(row.get("rejection_reasons") or []) * 22.0
    return round(score, 4)


def _ranking_key(row: dict[str, Any]) -> tuple[int, float, str, str]:
    status_rank = 0 if row.get("candidate_status") == "paper_forward_review_ready" else 1
    return (status_rank, -float(row.get("hardening_score") or 0.0), str(row.get("csv_label") or ""), str(row.get("profile") or ""))


def _min_gate(reasons: list[str], row: dict[str, Any], field: str, threshold: float) -> None:
    if float(_number(row.get(field)) or 0.0) < threshold:
        reasons.append(f"{field}_below_{_threshold_label(threshold)}")


def _max_drawdown(values: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return drawdown


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = int(max(0, min(len(ordered) - 1, round((percentile / 100.0) * (len(ordered) - 1)))))
    return ordered[index]


def _requested_csv_paths(value: Any) -> list[Path]:
    if value is None or value == "":
        return _dedupe_paths([Path(path) for path in DEFAULT_CSV_PATHS])
    if isinstance(value, str):
        return _dedupe_paths([Path(item.strip()) for item in value.split(",") if item.strip()])
    if isinstance(value, (list, tuple, set)):
        return _dedupe_paths([Path(str(item)) for item in value if str(item).strip()])
    return _dedupe_paths([Path(path) for path in DEFAULT_CSV_PATHS])


def _requested_list(value: Any, default: list[str]) -> list[str]:
    if value is None or value == "":
        return default
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return default


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    unique: list[Path] = []
    for path in paths:
        marker = str(path.absolute())
        if marker in seen:
            continue
        seen.add(marker)
        unique.append(path)
    return unique


def _sibling_risk(profile: str) -> bool:
    blob = f"{SYMBOL} {TIMEFRAME} {profile} {FAMILY}".casefold()
    return any(marker in blob for marker in _REJECTED_MARKERS)


def _volume(bar: dict[str, Any]) -> float:
    return _number(bar.get("volume")) or _number(bar.get("tick_volume")) or 0.0


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


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _flag(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").casefold().strip() in {"1", "true", "yes", "y", "fragile"}


def _threshold_label(value: float) -> str:
    return str(value).replace(".", "_").rstrip("0").rstrip("_")


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
