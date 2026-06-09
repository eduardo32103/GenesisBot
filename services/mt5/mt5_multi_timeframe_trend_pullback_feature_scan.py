from __future__ import annotations

import csv
import math
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation
from services.mt5.mt5_research_intelligence_core import run_research_intelligence_core
from services.mt5.mt5_research_rejection_registry import research_rejection
from services.mt5.mt5_symbol_cost_model import build_symbol_cost_model


FAMILY = "multi_timeframe_trend_pullback"
DEFAULT_CSV_DIR = Path(__file__).resolve().parents[2] / "data" / "backtests" / "multisymbol"
DEFAULT_SYMBOLS = ("BTCUSD", "ETHUSD", "XAUUSD", "USTEC", "US500", "EURUSD", "GBPUSD")
DEFAULT_TIMEFRAMES = ("M15", "M30", "H1")
DEFAULT_MAX_ROWS_PER_FILE = 2500
DEFAULT_MAX_EVALUATIONS = 120
RECENT_FRACTION = 0.25

MIN_SIGNAL_COUNT = 50
MIN_RECENT_SIGNAL_COUNT = 15
MIN_PROFIT_FACTOR_PROXY = 1.20

HIGHER_TIMEFRAME_BY_OPERATING = {
    "M15": "H1",
    "M30": "H1",
    "H1": "H4",
}

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
    {"mode": "baseline", "rsi_filter": False, "atr_filter": False, "pullback": "ema20_or_ema50"},
    {"mode": "rsi_filter", "rsi_filter": True, "atr_filter": False, "pullback": "ema20_or_ema50"},
    {"mode": "atr_filter", "rsi_filter": False, "atr_filter": True, "pullback": "ema20_or_ema50"},
    {"mode": "rsi_atr_filter", "rsi_filter": True, "atr_filter": True, "pullback": "ema20_or_ema50"},
    {"mode": "ema50_pullback", "rsi_filter": False, "atr_filter": False, "pullback": "ema50"},
)


def run_multi_timeframe_trend_pullback_feature_scan(
    *,
    csv_dir: str | Path | None = None,
    symbols: list[str] | str | None = None,
    timeframes: list[str] | str | None = None,
    max_rows_per_file: int = DEFAULT_MAX_ROWS_PER_FILE,
    max_evaluations: int = DEFAULT_MAX_EVALUATIONS,
    run_deep_scan: bool = False,
    evaluations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    requested_symbols = _requested_symbols(symbols)
    requested_timeframes = _requested_timeframes(timeframes)
    max_rows = _max_rows(max_rows_per_file, run_deep_scan)
    max_evals = max(1, int(max_evaluations or DEFAULT_MAX_EVALUATIONS))

    evaluated_rows: list[dict[str, Any]] = []
    rejected_by_registry: list[dict[str, Any]] = []
    data_quality_issues: list[dict[str, Any]] = []
    scanned_symbols: set[str] = set()
    scanned_timeframes: set[str] = set()

    source_evaluations = evaluations if evaluations is not None else _csv_evaluations(
        Path(csv_dir) if csv_dir else DEFAULT_CSV_DIR,
        requested_symbols,
        requested_timeframes,
        max_rows_per_file=max_rows,
    )

    for source in source_evaluations:
        if len(evaluated_rows) >= max_evals:
            break
        symbol = _symbol(source.get("symbol"))
        timeframe = _timeframe(source.get("timeframe"))
        higher_timeframe = _timeframe(source.get("higher_timeframe")) or HIGHER_TIMEFRAME_BY_OPERATING.get(timeframe, "")
        if not symbol or not timeframe:
            continue
        if symbol not in requested_symbols or timeframe not in requested_timeframes:
            continue
        scanned_symbols.add(symbol)
        scanned_timeframes.add(timeframe)
        bars = list(source.get("bars") or [])
        higher_bars = list(source.get("higher_bars") or [])
        csv_path = str(source.get("csv_path") or "")
        higher_csv_path = str(source.get("higher_csv_path") or "")
        for variant in _VARIANTS:
            if len(evaluated_rows) >= max_evals:
                break
            row = _evaluate_variant(
                symbol,
                timeframe,
                higher_timeframe,
                bars,
                higher_bars,
                variant,
                csv_path=csv_path,
                higher_csv_path=higher_csv_path,
            )
            evaluated_rows.append(row)
            if row["degraded_by_registry"] or row["rejected_by_research_registry"] or row["sibling_risk"]:
                rejected_by_registry.append(row)
            if row["data_quality"] != "ok":
                data_quality_issues.append(row)

    evaluated_rows.sort(key=_ranking_key)
    top_feature_edges = [row for row in evaluated_rows if row["scan_status"] == "hardening_candidate"][:10]
    near_misses = [row for row in evaluated_rows if row["scan_status"] == "near_miss"][:10]
    recommendation = "hardening_candidate_found" if top_feature_edges else "continue_research"
    recommended_next = (
        "multi_timeframe_trend_pullback_hardening"
        if top_feature_edges
        else _fallback_next_phase()
    )

    return {
        "ok": True,
        "status": "multi_timeframe_trend_pullback_feature_scan_ready",
        "family": FAMILY,
        "mode": "deep_scan" if run_deep_scan else "fast_feature_scan",
        "run_deep_scan": bool(run_deep_scan),
        "max_rows_per_file": max_rows,
        "max_evaluations": max_evals,
        "scanned_symbols": sorted(scanned_symbols),
        "scanned_timeframes": sorted(scanned_timeframes),
        "evaluations_count": len(evaluated_rows),
        "results": evaluated_rows,
        "top_feature_edges": top_feature_edges,
        "near_misses": near_misses,
        "rejected_by_registry": rejected_by_registry,
        "data_quality_issues": data_quality_issues,
        "proxy_only": True,
        "requires_real_hardening": True,
        "hardening_required_before_candidate": True,
        "cannot_be_paper_forward_candidate": True,
        "recommendation": recommendation,
        "recommended_next_research_phase": recommended_next,
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


def _csv_evaluations(
    csv_dir: Path,
    symbols: set[str],
    timeframes: set[str],
    *,
    max_rows_per_file: int,
) -> list[dict[str, Any]]:
    if not csv_dir.exists():
        return []
    paths = _available_ohlc_paths(csv_dir)
    bars_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
    evaluations: list[dict[str, Any]] = []
    for symbol in sorted(symbols):
        for timeframe in sorted(timeframes):
            higher_timeframe = HIGHER_TIMEFRAME_BY_OPERATING.get(timeframe)
            if not higher_timeframe:
                continue
            lower_path = paths.get((symbol, timeframe))
            higher_path = paths.get((symbol, higher_timeframe))
            bars = _cached_bars(bars_cache, symbol, timeframe, lower_path, max_rows_per_file=max_rows_per_file)
            higher_bars = _cached_bars(bars_cache, symbol, higher_timeframe, higher_path, max_rows_per_file=max_rows_per_file)
            evaluations.append(
                {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "higher_timeframe": higher_timeframe,
                    "bars": bars,
                    "higher_bars": higher_bars,
                    "csv_path": str(lower_path or ""),
                    "higher_csv_path": str(higher_path or ""),
                }
            )
    return evaluations


def _available_ohlc_paths(csv_dir: Path) -> dict[tuple[str, str], Path]:
    paths: dict[tuple[str, str], Path] = {}
    for path in sorted(csv_dir.glob("*.csv")):
        parsed = _parse_csv_name(path)
        if not parsed:
            continue
        paths.setdefault(parsed, path)
    return paths


def _cached_bars(
    cache: dict[tuple[str, str], list[dict[str, Any]]],
    symbol: str,
    timeframe: str,
    path: Path | None,
    *,
    max_rows_per_file: int,
) -> list[dict[str, Any]]:
    key = (symbol, timeframe)
    if key in cache:
        return cache[key]
    cache[key] = _read_bars(path, max_rows_per_file=max_rows_per_file) if path else []
    return cache[key]


def _evaluate_variant(
    symbol: str,
    timeframe: str,
    higher_timeframe: str,
    bars: list[dict[str, Any]],
    higher_bars: list[dict[str, Any]],
    variant: dict[str, Any],
    *,
    csv_path: str,
    higher_csv_path: str,
) -> dict[str, Any]:
    mode = str(variant["mode"])
    profile = f"{FAMILY}|mode={mode}|higher={higher_timeframe}"
    degraded = forward_profile_degradation(symbol, timeframe, profile)
    rejected = research_rejection(symbol, timeframe, profile, FAMILY, FAMILY)
    sibling = _sibling_risk(symbol, timeframe, profile)
    quality = _data_quality(bars, higher_bars)
    signals = _signals(symbol, bars, higher_bars, variant) if quality == "ok" else []
    metrics = _signal_metrics(symbol, signals, bars)
    reasons = _rejection_reasons(
        metrics,
        data_quality=quality,
        degraded=bool(degraded),
        rejected=bool(rejected),
        sibling=sibling,
    )
    status = _scan_status(reasons, metrics)
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "higher_timeframe": higher_timeframe,
        "family": FAMILY,
        "profile": profile,
        "mode": mode,
        "csv_path": csv_path,
        "higher_csv_path": higher_csv_path,
        "bars_loaded": len(bars),
        "higher_bars_loaded": len(higher_bars),
        "signal_count": metrics["signal_count"],
        "recent_signal_count": metrics["recent_signal_count"],
        "long_count": metrics["long_count"],
        "short_count": metrics["short_count"],
        "recent_long_count": metrics["recent_long_count"],
        "recent_short_count": metrics["recent_short_count"],
        "forward_return_1_bar": metrics["forward_return_1_bar"],
        "forward_return_2_bar": metrics["forward_return_2_bar"],
        "forward_return_4_bar": metrics["forward_return_4_bar"],
        "forward_return_8_bar": metrics["forward_return_8_bar"],
        "win_rate_proxy": metrics["win_rate_proxy"],
        "expectancy_proxy": metrics["expectancy_proxy"],
        "profit_factor_proxy": metrics["profit_factor_proxy"],
        "recent_expectancy_proxy": metrics["recent_expectancy_proxy"],
        "spread_cost_estimate": metrics["spread_cost_estimate"],
        "data_quality": quality,
        "degraded_by_registry": bool(degraded),
        "degradation_reason": degraded.get("degradation_reason") or "",
        "rejected_by_research_registry": bool(rejected),
        "research_rejection_reason": rejected.get("rejection_reason") or "",
        "sibling_risk": sibling,
        "sibling_risk_reason": "sibling_of_failed_profile" if sibling else "",
        "rejection_reasons": reasons,
        "scan_status": status,
        "proxy_only": True,
        "requires_real_hardening": True,
        "hardening_required_before_candidate": True,
        "cannot_be_paper_forward_candidate": True,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _signals(
    symbol: str,
    bars: list[dict[str, Any]],
    higher_bars: list[dict[str, Any]],
    variant: dict[str, Any],
) -> list[dict[str, Any]]:
    lower_rows = _indicator_rows(bars)
    higher_rows = _indicator_rows(higher_bars)
    if not lower_rows or not higher_rows:
        return []
    signals: list[dict[str, Any]] = []
    higher_index = 0
    spread_cost = _spread_cost(symbol, lower_rows)
    for index in range(205, len(lower_rows) - 8):
        lower = lower_rows[index]
        while higher_index + 1 < len(higher_rows) and higher_rows[higher_index + 1]["time"] <= lower["time"]:
            higher_index += 1
        higher = higher_rows[higher_index]
        side = _trend_side(higher)
        if not side:
            continue
        if not _has_pullback_recovery(lower_rows, index, side, variant):
            continue
        if variant.get("rsi_filter") and not _passes_rsi(lower, side):
            continue
        if variant.get("atr_filter") and not _passes_atr(lower):
            continue
        signals.append(_signal_return(lower_rows, index, side, spread_cost))
    return signals


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


def _trend_side(higher: dict[str, Any]) -> str:
    close = float(higher["close"])
    ema50 = float(higher["ema50"])
    ema200 = float(higher["ema200"])
    if close > ema50 > ema200:
        return "long"
    if close < ema50 < ema200:
        return "short"
    return ""


def _has_pullback_recovery(rows: list[dict[str, Any]], index: int, side: str, variant: dict[str, Any]) -> bool:
    current = rows[index]
    previous = rows[index - 1]
    lookback = rows[max(0, index - 4) : index + 1]
    ema20 = float(current["ema20"])
    ema50 = float(current["ema50"])
    pullback_mode = str(variant.get("pullback") or "ema20_or_ema50")
    if side == "long":
        if pullback_mode == "ema50":
            pulled_back = any(float(row["low"]) <= float(row["ema50"]) * 1.002 for row in lookback)
        else:
            pulled_back = any(float(row["low"]) <= float(row["ema20"]) * 1.001 or float(row["low"]) <= float(row["ema50"]) * 1.002 for row in lookback)
        previous_was_pullback = float(previous["close"]) <= float(previous["ema20"]) or float(previous["low"]) <= float(previous["ema50"]) * 1.002
        reclaimed = float(current["close"]) > ema20 and previous_was_pullback
        broke_short_high = previous_was_pullback and float(current["high"]) > max(float(row["high"]) for row in rows[index - 3 : index])
        reclaimed = reclaimed or broke_short_high
        return bool(pulled_back and reclaimed)
    if pullback_mode == "ema50":
        pulled_back = any(float(row["high"]) >= float(row["ema50"]) * 0.998 for row in lookback)
    else:
        pulled_back = any(float(row["high"]) >= float(row["ema20"]) * 0.999 or float(row["high"]) >= float(row["ema50"]) * 0.998 for row in lookback)
    previous_was_pullback = float(previous["close"]) >= float(previous["ema20"]) or float(previous["high"]) >= float(previous["ema50"]) * 0.998
    continued = float(current["close"]) < ema20 and previous_was_pullback
    broke_short_low = previous_was_pullback and float(current["low"]) < min(float(row["low"]) for row in rows[index - 3 : index])
    continued = continued or broke_short_low
    return bool(pulled_back and continued)


def _passes_rsi(row: dict[str, Any], side: str) -> bool:
    rsi = float(row.get("rsi") or 50.0)
    if side == "long":
        return rsi >= 48.0
    return rsi <= 52.0


def _passes_atr(row: dict[str, Any]) -> bool:
    close = float(row.get("close") or 0.0)
    atr = float(row.get("atr") or 0.0)
    return close > 0.0 and atr / close >= 0.00035


def _signal_return(rows: list[dict[str, Any]], index: int, side: str, spread_cost: float) -> dict[str, Any]:
    close = float(rows[index]["close"])
    direction = 1.0 if side == "long" else -1.0
    returns: dict[str, float] = {}
    for horizon in (1, 2, 4, 8):
        future_close = float(rows[index + horizon]["close"])
        raw_return = direction * (future_close - close) / max(abs(close), 0.00000001)
        returns[f"return_{horizon}"] = raw_return - spread_cost
    return {"index": index, "side": side, **returns}


def _signal_metrics(symbol: str, signals: list[dict[str, Any]], bars: list[dict[str, Any]]) -> dict[str, Any]:
    recent_start = int(len(bars) * (1.0 - RECENT_FRACTION))
    recent = [signal for signal in signals if int(signal["index"]) >= recent_start]
    primary = [float(signal.get("return_4") or 0.0) for signal in signals]
    recent_primary = [float(signal.get("return_4") or 0.0) for signal in recent]
    return {
        "signal_count": len(signals),
        "recent_signal_count": len(recent),
        "long_count": sum(1 for signal in signals if signal["side"] == "long"),
        "short_count": sum(1 for signal in signals if signal["side"] == "short"),
        "recent_long_count": sum(1 for signal in recent if signal["side"] == "long"),
        "recent_short_count": sum(1 for signal in recent if signal["side"] == "short"),
        "forward_return_1_bar": _average(signal.get("return_1") for signal in signals),
        "forward_return_2_bar": _average(signal.get("return_2") for signal in signals),
        "forward_return_4_bar": _average(primary),
        "forward_return_8_bar": _average(signal.get("return_8") for signal in signals),
        "win_rate_proxy": _win_rate(primary),
        "expectancy_proxy": _average(primary),
        "profit_factor_proxy": _profit_factor(primary),
        "recent_expectancy_proxy": _average(recent_primary),
        "spread_cost_estimate": round(_spread_cost(symbol, _indicator_rows(bars[:2])), 8),
    }


def _rejection_reasons(
    metrics: dict[str, Any],
    *,
    data_quality: str,
    degraded: bool,
    rejected: bool,
    sibling: bool,
) -> list[str]:
    reasons: list[str] = []
    if data_quality != "ok":
        reasons.append(data_quality)
    if degraded:
        reasons.append("degradation_registry")
    if rejected:
        reasons.append("research_rejection_registry")
    if sibling:
        reasons.append("sibling_risk")
    if int(metrics["signal_count"]) < MIN_SIGNAL_COUNT:
        reasons.append("signal_count_below_50")
    if int(metrics["recent_signal_count"]) < MIN_RECENT_SIGNAL_COUNT:
        reasons.append("recent_signal_count_below_15")
    if float(metrics["profit_factor_proxy"]) < MIN_PROFIT_FACTOR_PROXY:
        reasons.append("profit_factor_proxy_below_1_20")
    if float(metrics["expectancy_proxy"]) <= 0.0:
        reasons.append("expectancy_proxy_not_positive")
    if float(metrics["recent_expectancy_proxy"]) <= 0.0:
        reasons.append("recent_expectancy_proxy_not_positive")
    return reasons


def _scan_status(reasons: list[str], metrics: dict[str, Any]) -> str:
    if any(reason in reasons for reason in ("degradation_registry", "research_rejection_registry", "sibling_risk")):
        return "excluded_by_registry_or_sibling_risk"
    if not reasons:
        return "hardening_candidate"
    soft = {"signal_count_below_50", "recent_signal_count_below_15", "profit_factor_proxy_below_1_20"}
    if set(reasons) <= soft and int(metrics["signal_count"]) >= 30:
        return "near_miss"
    return "feature_gate_failed"


def _data_quality(bars: list[dict[str, Any]], higher_bars: list[dict[str, Any]]) -> str:
    if not higher_bars:
        return "missing_higher_timeframe"
    if len(bars) < 220:
        return "insufficient_bars"
    if len(higher_bars) < 220:
        return "insufficient_higher_timeframe_bars"
    if not _has_price_columns(bars):
        return "missing_price"
    if not _has_price_columns(higher_bars):
        return "missing_higher_timeframe_price"
    return "ok"


def _has_price_columns(bars: list[dict[str, Any]]) -> bool:
    return all(
        _number(bar.get("open")) is not None
        and _number(bar.get("high")) is not None
        and _number(bar.get("low")) is not None
        and _number(bar.get("close")) is not None
        for bar in bars[: min(len(bars), 50)]
    )


def _read_bars(path: Path, *, max_rows_per_file: int) -> list[dict[str, Any]]:
    rows: deque[dict[str, Any]] = deque(maxlen=max_rows_per_file)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            rows.append(row)
    return list(rows)


def _parse_csv_name(path: Path) -> tuple[str, str] | None:
    parts = path.stem.split("_")
    if len(parts) < 3:
        return None
    timeframe = _timeframe(parts[-2])
    if timeframe not in {"M15", "M30", "H1", "H4"}:
        return None
    return _symbol("_".join(parts[:-2])), timeframe


def _fallback_next_phase() -> str:
    intelligence = run_research_intelligence_core(load_default_sources=True)
    queue = intelligence.get("priority_queue") or []
    for item in queue:
        family = str(item.get("family_name") or "")
        if family and family != FAMILY:
            return f"design_{family}_processed_feature_scan"
    return "continue_new_family_edge_discovery"


def _ranking_key(row: dict[str, Any]) -> tuple[int, float, int, str, str]:
    status_rank = {"hardening_candidate": 0, "near_miss": 1, "feature_gate_failed": 2}.get(row["scan_status"], 3)
    return (
        status_rank,
        -float(row["profit_factor_proxy"]),
        -int(row["signal_count"]),
        row["symbol"],
        row["timeframe"],
    )


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


def _max_rows(value: int, run_deep_scan: bool) -> int:
    default = 20000 if run_deep_scan else DEFAULT_MAX_ROWS_PER_FILE
    return max(250, int(value or default))


def _sibling_risk(symbol: str, timeframe: str, profile: str) -> bool:
    blob = f"{symbol} {timeframe} {profile} {FAMILY}".casefold()
    return any(marker in blob for marker in _REJECTED_MARKERS)


def _spread_cost(symbol: str, rows: list[dict[str, Any]]) -> float:
    first_price = float(rows[0].get("close") or 0.0) if rows else 0.0
    model = build_symbol_cost_model(symbol, first_price=first_price)
    return model.estimated_spread_price / max(abs(first_price), 0.00000001)


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


def _average(values: Any) -> float:
    clean = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    return round(sum(clean) / len(clean), 8) if clean else 0.0


def _profit_factor(values: list[float]) -> float:
    wins = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value < 0))
    if wins > 0 and losses == 0:
        return 999.0
    return round(wins / losses, 6) if losses > 0 else 0.0


def _win_rate(values: list[float]) -> float:
    return round(100.0 * sum(1 for value in values if value > 0) / len(values), 4) if values else 0.0


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
    if symbol == "USTECB" or symbol == "NAS100":
        return "USTEC"
    if symbol == "XAUUSDB":
        return "XAUUSD"
    return symbol


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
