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


FAMILY = "session_vwap_reclaim"
DEFAULT_CSV_DIR = Path(__file__).resolve().parents[2] / "data" / "backtests" / "multisymbol"
DEFAULT_SYMBOLS = ("BTCUSD", "ETHUSD", "XAUUSD", "USTEC", "US500", "EURUSD", "GBPUSD")
DEFAULT_TIMEFRAMES = ("M15", "M30", "H1")
DEFAULT_MAX_ROWS_PER_FILE = 2500
DEFAULT_MAX_EVALUATIONS = 80
RECENT_FRACTION = 0.25

MIN_SIGNAL_COUNT = 50
MIN_RECENT_SIGNAL_COUNT = 15
MIN_PROFIT_FACTOR_PROXY = 1.15

_DEFAULT_EXCLUDED_PAIRS = {("ETHUSD", "M30")}
_REJECTED_MARKERS = (
    "volatility_breakout",
    "vol_breakout",
    "session_open_continuation",
    "ema_reclaim",
    "london_us_breakout",
    "opening_range_fakeout",
)
_VARIANTS: tuple[dict[str, Any], ...] = (
    {"mode": "baseline", "momentum": False, "min_distance_pct": 0.0},
    {"mode": "momentum_filter", "momentum": True, "min_distance_pct": 0.0},
    {"mode": "distance_filter", "momentum": False, "min_distance_pct": 0.00025},
    {"mode": "momentum_distance_filter", "momentum": True, "min_distance_pct": 0.00025},
)


def run_session_vwap_reclaim_feature_scan(
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
        run_deep_scan=run_deep_scan,
    )

    for source in source_evaluations:
        if len(evaluated_rows) >= max_evals:
            break
        symbol = _symbol(source.get("symbol"))
        timeframe = _timeframe(source.get("timeframe"))
        if not symbol or not timeframe:
            continue
        if symbol not in requested_symbols or timeframe not in requested_timeframes:
            continue
        if not run_deep_scan and (symbol, timeframe) in _DEFAULT_EXCLUDED_PAIRS:
            rejected_by_registry.append(
                {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "profile": FAMILY,
                    "candidate_status": "skipped_default_failed_cluster_pair",
                    "rejection_reason": "eth_m30_failed_cluster_avoided_by_default",
                    **_safety(),
                }
            )
            continue
        bars = list(source.get("bars") or [])
        csv_path = str(source.get("csv_path") or "")
        for variant in _VARIANTS:
            if len(evaluated_rows) >= max_evals:
                break
            row = _evaluate_variant(symbol, timeframe, bars, variant, csv_path=csv_path)
            evaluated_rows.append(row)
            scanned_symbols.add(symbol)
            scanned_timeframes.add(timeframe)
            if row["degraded_by_registry"] or row["rejected_by_research_registry"] or row["sibling_risk"]:
                rejected_by_registry.append(row)
            if row["data_quality"] != "ok":
                data_quality_issues.append(row)

    evaluated_rows.sort(key=_ranking_key)
    top_feature_edges = [row for row in evaluated_rows if row["scan_status"] == "hardening_candidate"][:10]
    near_misses = [row for row in evaluated_rows if row["scan_status"] == "near_miss"][:10]
    recommendation = "hardening_candidate_found" if top_feature_edges else "continue_research"
    recommended_next = (
        "session_vwap_reclaim_hardening"
        if top_feature_edges
        else _fallback_next_phase()
    )

    return {
        "ok": True,
        "status": "session_vwap_reclaim_feature_scan_ready",
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
    run_deep_scan: bool,
) -> list[dict[str, Any]]:
    evaluations: list[dict[str, Any]] = []
    if not csv_dir.exists():
        return evaluations
    for path in sorted(csv_dir.glob("*.csv")):
        parsed = _parse_csv_name(path)
        if not parsed:
            continue
        symbol, timeframe = parsed
        if symbol not in symbols or timeframe not in timeframes:
            continue
        if not run_deep_scan and (symbol, timeframe) in _DEFAULT_EXCLUDED_PAIRS:
            evaluations.append({"symbol": symbol, "timeframe": timeframe, "bars": [], "csv_path": str(path)})
            continue
        evaluations.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "bars": _read_bars(path, max_rows_per_file=max_rows_per_file),
                "csv_path": str(path),
            }
        )
    return evaluations


def _evaluate_variant(
    symbol: str,
    timeframe: str,
    bars: list[dict[str, Any]],
    variant: dict[str, Any],
    *,
    csv_path: str,
) -> dict[str, Any]:
    mode = str(variant["mode"])
    profile = f"{FAMILY}|mode={mode}"
    degraded = forward_profile_degradation(symbol, timeframe, profile)
    rejected = research_rejection(symbol, timeframe, profile, FAMILY, FAMILY)
    sibling = _sibling_risk(symbol, timeframe, profile)
    quality = _data_quality(bars)
    signals = _signals(bars, variant) if quality == "ok" else []
    metrics = _signal_metrics(symbol, signals, bars)
    reasons = _rejection_reasons(metrics, data_quality=quality, degraded=bool(degraded), rejected=bool(rejected), sibling=sibling)
    status = _scan_status(reasons, metrics)
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "family": FAMILY,
        "profile": profile,
        "mode": mode,
        "csv_path": csv_path,
        "bars_loaded": len(bars),
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
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _signals(bars: list[dict[str, Any]], variant: dict[str, Any]) -> list[dict[str, Any]]:
    vwap_rows = _session_vwap_rows(bars)
    signals: list[dict[str, Any]] = []
    for index in range(1, len(vwap_rows) - 8):
        previous = vwap_rows[index - 1]
        current = vwap_rows[index]
        prev_close = previous["close"]
        curr_close = current["close"]
        prev_vwap = previous["session_vwap"]
        curr_vwap = current["session_vwap"]
        if prev_vwap <= 0 or curr_vwap <= 0 or curr_close <= 0:
            continue
        distance = abs(prev_close - prev_vwap) / max(abs(prev_close), 0.00000001)
        if distance < float(variant.get("min_distance_pct") or 0.0):
            continue
        momentum = curr_close - prev_close
        long_signal = prev_close < prev_vwap and curr_close > curr_vwap
        short_signal = prev_close > prev_vwap and curr_close < curr_vwap
        if variant.get("momentum") and long_signal and momentum <= 0:
            long_signal = False
        if variant.get("momentum") and short_signal and momentum >= 0:
            short_signal = False
        if long_signal:
            signals.append(_signal_return(vwap_rows, index, "long"))
        elif short_signal:
            signals.append(_signal_return(vwap_rows, index, "short"))
    return signals


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


def _signal_return(rows: list[dict[str, Any]], index: int, side: str) -> dict[str, Any]:
    close = float(rows[index]["close"])
    direction = 1.0 if side == "long" else -1.0
    returns: dict[str, float] = {}
    for horizon in (1, 2, 4, 8):
        future_close = float(rows[index + horizon]["close"])
        returns[f"return_{horizon}"] = direction * (future_close - close) / max(abs(close), 0.00000001)
    return {"index": index, "side": side, **returns}


def _signal_metrics(symbol: str, signals: list[dict[str, Any]], bars: list[dict[str, Any]]) -> dict[str, Any]:
    recent_start = int(len(bars) * (1.0 - RECENT_FRACTION))
    recent = [signal for signal in signals if int(signal["index"]) >= recent_start]
    primary = [float(signal.get("return_4") or 0.0) for signal in signals]
    recent_primary = [float(signal.get("return_4") or 0.0) for signal in recent]
    first_price = _number(bars[0].get("close")) if bars else 0.0
    cost_model = build_symbol_cost_model(symbol, first_price=first_price)
    spread_cost = cost_model.estimated_spread_price / max(abs(float(first_price or 0.0)), 0.00000001)
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
        "spread_cost_estimate": round(spread_cost, 8),
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
        reasons.append("profit_factor_proxy_below_1_15")
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
    soft = {"signal_count_below_50", "recent_signal_count_below_15", "profit_factor_proxy_below_1_15"}
    if set(reasons) <= soft and int(metrics["signal_count"]) >= 30:
        return "near_miss"
    return "feature_gate_failed"


def _data_quality(bars: list[dict[str, Any]]) -> str:
    if len(bars) < 20:
        return "insufficient_bars"
    if not any(_volume(bar) > 0.0 for bar in bars):
        return "missing_volume"
    if not all((_number(bar.get("high")) and _number(bar.get("low")) and _number(bar.get("close"))) for bar in bars):
        return "missing_price"
    return "ok"


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
    if timeframe not in DEFAULT_TIMEFRAMES:
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
    return max(100, int(value or default))


def _sibling_risk(symbol: str, timeframe: str, profile: str) -> bool:
    blob = f"{symbol} {timeframe} {profile} {FAMILY}".casefold()
    return any(marker in blob for marker in _REJECTED_MARKERS)


def _volume(bar: dict[str, Any]) -> float:
    return _number(bar.get("volume")) or _number(bar.get("tick_volume")) or 0.0


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
    if symbol == "USTECB":
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
