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


SYMBOL = "USTEC"
OPERATING_TIMEFRAME = "M30"
HIGHER_TIMEFRAME = "H1"
FAMILY = "multi_timeframe_trend_pullback"
SOURCE = "ustec_m30_h1_trend_pullback_hardening"

DEFAULT_M30_PATHS = (
    Path("data") / "backtests" / "multisymbol" / "USTEC.b_M30_20000.csv",
    Path("data") / "backtests" / "multisymbol" / "USTEC_M30_20000.csv",
    Path("data") / "backtests" / "multisymbol" / "NAS100_M30_20000.csv",
)
DEFAULT_H1_PATHS = (
    Path("data") / "backtests" / "multisymbol" / "USTEC.b_H1_20000.csv",
    Path("data") / "backtests" / "multisymbol" / "USTEC_H1_20000.csv",
    Path("data") / "backtests" / "multisymbol" / "NAS100_H1_20000.csv",
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
    "session_vwap_reclaim",
)

_VARIANTS: tuple[dict[str, Any], ...] = (
    {"mode": "baseline", "actions": ("baseline",), "hold_bars": 4},
    {"mode": "rsi_filter", "actions": ("rsi_filter",), "rsi_filter": True, "hold_bars": 4},
    {"mode": "rsi_atr_filter", "actions": ("rsi_filter", "atr_filter"), "rsi_filter": True, "atr_filter": True, "hold_bars": 4},
    {"mode": "trend_strength_guard", "actions": ("trend_strength_guard",), "trend_strength_guard": True, "hold_bars": 4},
    {"mode": "pullback_depth_guard", "actions": ("pullback_depth_guard",), "pullback_depth_guard": True, "hold_bars": 4},
    {"mode": "volatility_guard", "actions": ("volatility_guard",), "atr_filter": True, "hold_bars": 4},
    {"mode": "spread_guard", "actions": ("spread_guard",), "spread_guard": True, "hold_bars": 4},
    {"mode": "mae_guard", "actions": ("mae_guard",), "mae_guard": 0.0025, "hold_bars": 4},
    {"mode": "fast_loss_cut", "actions": ("fast_loss_cut",), "fast_loss_cut": 0.0014, "hold_bars": 4},
    {"mode": "trailing_defensive", "actions": ("trailing_defensive",), "trailing_defensive": True, "hold_bars": 8},
    {"mode": "time_stop_guard", "actions": ("time_stop_guard",), "hold_bars": 2},
    {"mode": "long_only", "actions": ("long_only",), "side_filter": "long", "hold_bars": 4},
    {"mode": "short_only", "actions": ("short_only",), "side_filter": "short", "hold_bars": 4},
    {
        "mode": "rsi_trend_strength",
        "actions": ("rsi_filter", "trend_strength_guard"),
        "rsi_filter": True,
        "trend_strength_guard": True,
        "hold_bars": 4,
    },
    {
        "mode": "rsi_pullback_depth",
        "actions": ("rsi_filter", "pullback_depth_guard"),
        "rsi_filter": True,
        "pullback_depth_guard": True,
        "hold_bars": 4,
    },
    {
        "mode": "atr_mae_guard",
        "actions": ("atr_filter", "mae_guard"),
        "atr_filter": True,
        "mae_guard": 0.0025,
        "hold_bars": 4,
    },
    {
        "mode": "rsi_time_stop",
        "actions": ("rsi_filter", "time_stop_guard"),
        "rsi_filter": True,
        "hold_bars": 2,
    },
)


def run_ustec_m30_h1_trend_pullback_hardening(body: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    body = body or {}
    if isinstance(body.get("rows"), list):
        rows = [_finalize_row(dict(row)) for row in body["rows"] if isinstance(row, dict)]
        rows.sort(key=_ranking_key)
        return _result(rows, [], [], [], started)

    max_bars = max(500, min(int(_number(body.get("max_bars")) or 20000), 65000))
    monte_carlo_simulations = max(100, min(int(_number(body.get("monte_carlo_simulations")) or 300), 1000))
    requested_targets = _requested_list(body.get("targets"), [str(item["mode"]) for item in _VARIANTS])
    m30_path, missing_m30 = _resolve_required_path(body.get("m30_csv_path") or body.get("m30_csv_paths"), DEFAULT_M30_PATHS, OPERATING_TIMEFRAME)
    h1_path, missing_h1 = _resolve_required_path(body.get("h1_csv_path") or body.get("h1_csv_paths"), DEFAULT_H1_PATHS, HIGHER_TIMEFRAME)
    missing_csvs = [*missing_m30, *missing_h1]
    errors: list[dict[str, Any]] = []

    if m30_path is None or h1_path is None:
        row = _missing_required_row(m30_path, h1_path, missing_csvs)
        return _result([row], [str(path) for path in (m30_path, h1_path) if path], missing_csvs, errors, started, repair_data_sources=True)

    m30_bars = _read_bars(m30_path, max_bars=max_bars)
    h1_bars = _read_bars(h1_path, max_bars=max_bars)
    if not m30_bars or not h1_bars:
        errors.append({"error": "required_csv_bars_not_loaded", "m30_csv": str(m30_path), "h1_csv": str(h1_path)})
        row = _missing_required_row(m30_path, h1_path, missing_csvs)
        return _result([row], [str(m30_path), str(h1_path)], missing_csvs, errors, started, repair_data_sources=True)

    cost_model = build_symbol_cost_model(SYMBOL, resolved_symbol="USTEC.b", first_price=_number(m30_bars[0].get("close")) or 0.0)
    cost_per_trade = _cost_per_trade(cost_model, m30_bars)
    rows: list[dict[str, Any]] = []
    for variant in _VARIANTS:
        if str(variant["mode"]) not in requested_targets:
            continue
        rows.append(
            _evaluate_variant(
                m30_bars,
                h1_bars,
                variant,
                m30_csv_path=m30_path,
                h1_csv_path=h1_path,
                cost_per_trade=cost_per_trade,
                monte_carlo_simulations=monte_carlo_simulations,
            )
        )
    rows.sort(key=_ranking_key)
    return _result(rows, [str(m30_path), str(h1_path)], missing_csvs, errors, started)


def _evaluate_variant(
    m30_bars: list[dict[str, Any]],
    h1_bars: list[dict[str, Any]],
    variant: dict[str, Any],
    *,
    m30_csv_path: Path,
    h1_csv_path: Path,
    cost_per_trade: float,
    monte_carlo_simulations: int,
) -> dict[str, Any]:
    profile = f"ustec_m30_h1_trend_pullback_{variant['mode']}"
    data_quality = _data_quality(m30_bars, h1_bars)
    trades = _trades(m30_bars, h1_bars, variant, cost_per_trade=cost_per_trade) if data_quality == "ok" else []
    metrics = _metrics(trades)
    recent_metrics = _metrics(_recent_trades(trades, m30_bars))
    monte_carlo = _monte_carlo_stress([trade["pnl"] for trade in trades], simulations=monte_carlo_simulations)
    spread_x2 = _metrics([{**trade, "pnl": trade["raw_return"] - cost_per_trade * 2.0} for trade in trades])
    remove_best = _remove_best_metrics(trades, 5)
    degraded = forward_profile_degradation(SYMBOL, OPERATING_TIMEFRAME, profile)
    rejected = research_rejection(SYMBOL, OPERATING_TIMEFRAME, profile, FAMILY, FAMILY)
    sibling = _sibling_risk(profile)
    return _finalize_row(
        {
            "symbol": SYMBOL,
            "timeframe": OPERATING_TIMEFRAME,
            "higher_timeframe": HIGHER_TIMEFRAME,
            "family": FAMILY,
            "profile": profile,
            "target_name": profile,
            "hardening_actions": list(variant.get("actions") or (variant["mode"],)),
            "m30_csv_path": str(m30_csv_path),
            "h1_csv_path": str(h1_csv_path),
            "csv_used": [str(m30_csv_path), str(h1_csv_path)],
            "m30_bars_loaded": len(m30_bars),
            "h1_bars_loaded": len(h1_bars),
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
            "source": SOURCE,
        }
    )


def _trades(
    m30_bars: list[dict[str, Any]],
    h1_bars: list[dict[str, Any]],
    variant: dict[str, Any],
    *,
    cost_per_trade: float,
) -> list[dict[str, Any]]:
    lower_rows = _indicator_rows(m30_bars)
    higher_rows = _indicator_rows(h1_bars)
    trades: list[dict[str, Any]] = []
    higher_index = 0
    hold_bars = int(variant.get("hold_bars") or 4)
    index = 205
    while index < len(lower_rows) - hold_bars - 1:
        lower = lower_rows[index]
        while higher_index + 1 < len(higher_rows) and higher_rows[higher_index + 1]["time"] <= lower["time"]:
            higher_index += 1
        side = _signal_side(lower_rows, higher_rows[higher_index], index, variant)
        if not side:
            index += 1
            continue
        exit_index = _exit_index(lower_rows, index, side, variant)
        entry = float(lower_rows[index]["close"])
        exit_price = float(lower_rows[exit_index]["close"])
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
        index = max(index + 1, exit_index + 1)
    return trades


def _signal_side(rows: list[dict[str, Any]], higher: dict[str, Any], index: int, variant: dict[str, Any]) -> str:
    side = _trend_side(higher, variant)
    side_filter = str(variant.get("side_filter") or "")
    if side_filter and side != side_filter:
        return ""
    if not side:
        return ""
    current = rows[index]
    if not _has_pullback_recovery(rows, index, side, variant):
        return ""
    if variant.get("rsi_filter") and not _passes_rsi(current, side):
        return ""
    if variant.get("atr_filter") and not _passes_atr(current):
        return ""
    if variant.get("spread_guard") and _spread_distance_too_thin(current, side):
        return ""
    return side


def _trend_side(higher: dict[str, Any], variant: dict[str, Any]) -> str:
    close = float(higher["close"])
    ema50 = float(higher["ema50"])
    ema200 = float(higher["ema200"])
    strength = abs(ema50 - ema200) / max(abs(close), 0.00000001)
    if variant.get("trend_strength_guard") and strength < 0.0025:
        return ""
    if close > ema50 > ema200:
        return "long"
    if close < ema50 < ema200:
        return "short"
    return ""


def _has_pullback_recovery(rows: list[dict[str, Any]], index: int, side: str, variant: dict[str, Any]) -> bool:
    current = rows[index]
    previous = rows[index - 1]
    lookback = rows[max(0, index - 5) : index + 1]
    depth_multiplier = 0.998 if variant.get("pullback_depth_guard") else 1.002
    if side == "long":
        pulled_back = any(
            float(row["low"]) <= float(row["ema20"]) * 1.001
            or float(row["low"]) <= float(row["ema50"]) * depth_multiplier
            for row in lookback
        )
        previous_was_pullback = float(previous["close"]) <= float(previous["ema20"]) or float(previous["low"]) <= float(previous["ema50"]) * 1.002
        reclaimed = float(current["close"]) > float(current["ema20"]) and previous_was_pullback
        broke_short_high = previous_was_pullback and float(current["high"]) > max(float(row["high"]) for row in rows[index - 3 : index])
        return bool(pulled_back and (reclaimed or broke_short_high))
    depth_multiplier = 1.002 if variant.get("pullback_depth_guard") else 0.998
    pulled_back = any(
        float(row["high"]) >= float(row["ema20"]) * 0.999
        or float(row["high"]) >= float(row["ema50"]) * depth_multiplier
        for row in lookback
    )
    previous_was_pullback = float(previous["close"]) >= float(previous["ema20"]) or float(previous["high"]) >= float(previous["ema50"]) * 0.998
    continued = float(current["close"]) < float(current["ema20"]) and previous_was_pullback
    broke_short_low = previous_was_pullback and float(current["low"]) < min(float(row["low"]) for row in rows[index - 3 : index])
    return bool(pulled_back and (continued or broke_short_low))


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
        if variant.get("trailing_defensive") and best_return > 0.0022 and current_return < best_return * 0.4:
            return current_index
    return exit_index


def _indicator_rows(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    ema20: float | None = None
    ema50: float | None = None
    ema200: float | None = None
    prev_close: float | None = None
    gains: deque[float] = deque(maxlen=14)
    losses: deque[float] = deque(maxlen=14)
    true_ranges: deque[float] = deque(maxlen=14)
    for bar in bars:
        timestamp = _parse_time(bar.get("time"))
        open_price = _number(bar.get("open"))
        high = _number(bar.get("high"))
        low = _number(bar.get("low"))
        close = _number(bar.get("close"))
        if timestamp is None or open_price is None or high is None or low is None or close is None:
            continue
        ema20 = _ema(close, ema20, 20)
        ema50 = _ema(close, ema50, 50)
        ema200 = _ema(close, ema200, 200)
        change = 0.0 if prev_close is None else close - prev_close
        gains.append(max(change, 0.0))
        losses.append(abs(min(change, 0.0)))
        true_ranges.append(_true_range(high, low, prev_close))
        rows.append(
            {
                "time": timestamp,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "ema20": ema20,
                "ema50": ema50,
                "ema200": ema200,
                "rsi": _rsi(gains, losses),
                "atr": _average_float(true_ranges),
            }
        )
        prev_close = close
    return rows


def _passes_rsi(row: dict[str, Any], side: str) -> bool:
    rsi = float(row.get("rsi") or 50.0)
    if side == "long":
        return rsi >= 48.0
    return rsi <= 52.0


def _passes_atr(row: dict[str, Any]) -> bool:
    close = float(row.get("close") or 0.0)
    atr = float(row.get("atr") or 0.0)
    return close > 0.0 and atr / close >= 0.00035


def _spread_distance_too_thin(row: dict[str, Any], side: str) -> bool:
    close = float(row.get("close") or 0.0)
    ema20 = float(row.get("ema20") or close)
    if close <= 0:
        return True
    distance = (close - ema20) / close if side == "long" else (ema20 - close) / close
    return distance < 0.0002


def _finalize_row(row: dict[str, Any]) -> dict[str, Any]:
    row["symbol"] = _symbol(row.get("symbol") or SYMBOL)
    row["timeframe"] = str(row.get("timeframe") or OPERATING_TIMEFRAME).upper()
    row["higher_timeframe"] = str(row.get("higher_timeframe") or HIGHER_TIMEFRAME).upper()
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
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
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
    *,
    repair_data_sources: bool = False,
) -> dict[str, Any]:
    candidates = [row for row in rows if row.get("candidate_status") == "paper_forward_review_ready"]
    best = rows[0] if rows else None
    recommendation = "repair_data_sources" if repair_data_sources else ("paper_forward_candidate_review" if candidates else "continue_research")
    return {
        "ok": True,
        "status": "ustec_m30_h1_trend_pullback_hardening_ready",
        "symbol": SYMBOL,
        "timeframe": OPERATING_TIMEFRAME,
        "higher_timeframe": HIGHER_TIMEFRAME,
        "family": FAMILY,
        "csv_used": used_csvs,
        "missing_csvs": missing_csvs,
        "variants_evaluated": 0 if repair_data_sources else len(rows),
        "results": [] if repair_data_sources else rows,
        "best_variant": best,
        "candidates": candidates,
        "recommended_candidate": candidates[0] if candidates else None,
        "recommendation": recommendation,
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


def _missing_required_row(m30_path: Path | None, h1_path: Path | None, missing_csvs: list[str]) -> dict[str, Any]:
    return _finalize_row(
        {
            "symbol": SYMBOL,
            "timeframe": OPERATING_TIMEFRAME,
            "higher_timeframe": HIGHER_TIMEFRAME,
            "family": FAMILY,
            "profile": "ustec_m30_h1_trend_pullback_missing_required_timeframe",
            "target_name": "ustec_m30_h1_trend_pullback_missing_required_timeframe",
            "hardening_actions": [],
            "m30_csv_path": str(m30_path or ""),
            "h1_csv_path": str(h1_path or ""),
            "missing_csvs": missing_csvs,
            "data_quality": "missing_required_timeframe",
            "source": SOURCE,
        }
    )


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


def _data_quality(m30_bars: list[dict[str, Any]], h1_bars: list[dict[str, Any]]) -> str:
    if len(m30_bars) < 220 or len(h1_bars) < 220:
        return "insufficient_bars"
    if not _has_price_columns(m30_bars) or not _has_price_columns(h1_bars):
        return "missing_price"
    return "ok"


def _has_price_columns(bars: list[dict[str, Any]]) -> bool:
    return all(
        _number(bar.get("open")) is not None
        and _number(bar.get("high")) is not None
        and _number(bar.get("low")) is not None
        and _number(bar.get("close")) is not None
        for bar in bars[: min(len(bars), 50)]
    )


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


def _resolve_required_path(value: Any, defaults: tuple[Path, ...], timeframe: str) -> tuple[Path | None, list[str]]:
    requested = _requested_paths(value, defaults)
    missing: list[str] = []
    for path in requested:
        if path.exists():
            return path, missing
        missing.append(f"{timeframe}:{path}")
    return None, missing


def _requested_paths(value: Any, default: tuple[Path, ...]) -> list[Path]:
    if value is None or value == "":
        return _dedupe_paths([Path(path) for path in default])
    if isinstance(value, str):
        return _dedupe_paths([Path(item.strip()) for item in value.split(",") if item.strip()])
    if isinstance(value, (list, tuple, set)):
        return _dedupe_paths([Path(str(item)) for item in value if str(item).strip()])
    return _dedupe_paths([Path(path) for path in default])


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


def _ranking_key(row: dict[str, Any]) -> tuple[int, float, str]:
    status_rank = 0 if row.get("candidate_status") == "paper_forward_review_ready" else 1
    return (status_rank, -float(row.get("hardening_score") or 0.0), str(row.get("profile") or ""))


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


def _sibling_risk(profile: str) -> bool:
    blob = f"{SYMBOL} {OPERATING_TIMEFRAME} {profile} {FAMILY}".casefold()
    return any(marker in blob for marker in _REJECTED_MARKERS)


def _ema(value: float, previous: float | None, period: int) -> float:
    alpha = 2.0 / (period + 1.0)
    return value if previous is None else previous + alpha * (value - previous)


def _rsi(gains: deque[float], losses: deque[float]) -> float:
    if len(gains) < 14 or len(losses) < 14:
        return 50.0
    average_gain = sum(gains) / len(gains)
    average_loss = sum(losses) / len(losses)
    if average_loss == 0.0:
        return 100.0 if average_gain > 0.0 else 50.0
    rs = average_gain / average_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _true_range(high: float, low: float, previous_close: float | None) -> float:
    if previous_close is None:
        return high - low
    return max(high - low, abs(high - previous_close), abs(low - previous_close))


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


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _symbol(value: object) -> str:
    symbol = str(value or "").upper().strip().replace(".B", "")
    if symbol in {"USTECB", "NAS100"}:
        return "USTEC"
    return symbol


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
