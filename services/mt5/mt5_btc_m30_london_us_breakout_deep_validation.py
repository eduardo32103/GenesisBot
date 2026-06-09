from __future__ import annotations

import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import services.mt5.mt5_recent_first_research as recent_first
from services.mt5.mt5_backtester import _load_bars, _metrics, _number, _safety, _settings
from services.mt5.mt5_capital_preservation_optimizer import _depends_on_single_trade, _monte_carlo_stress
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation
from services.mt5.mt5_multi_symbol_recent_first import (
    MultiSymbolConfig,
    _first_csv_price,
    _remove_best_metrics,
    _simulate_multi_symbol,
    _split_metrics,
    _spread_stress_metrics,
)
from services.mt5.mt5_recent_first_research import RecentFirstVariant, _fragile_dependency
from services.mt5.mt5_strategy_research_v2 import _features_by_index
from services.mt5.mt5_symbol_cost_model import build_symbol_cost_model


SYMBOL = "BTCUSD"
TIMEFRAME = "M30"
FAMILY = "recent_london_us_breakout"
CONCEPT = "opening_range_fakeout"

DEFAULT_CSV_PATHS = (
    Path("data") / "backtests" / "multisymbol" / "BTCUSD_M30_20000.csv",
    Path("data") / "backtests" / "BTCUSD_M30_40000.csv",
    Path("data") / "backtests" / "BTCUSD_M30_60000.csv",
    Path("data") / "backtests" / "multisymbol" / "BTCUSD_M30_40000.csv",
    Path("data") / "backtests" / "multisymbol" / "BTCUSD_M30_60000.csv",
)

MIN_RECENT_CLOSED = 15
MIN_TOTAL_CLOSED = 45
MIN_RECENT_PF = 1.05
MIN_TOTAL_PF = 1.15
MIN_MONTE_CARLO_STRESSED_PF = 1.05
MIN_SPREAD_X2_PF = 0.95
MIN_REMOVE_BEST_5_PF = 1.0

_CUSTOM_SESSION_HOURS: dict[str, set[int]] = {
    "strict_london_us": set(range(8, 20)),
    "london_us_overlap": {13, 14, 15, 16},
}


@dataclass(frozen=True)
class BtcM30LondonUsBreakoutDeepConfig:
    target_name: str
    config: MultiSymbolConfig
    hardening_actions: tuple[str, ...]


def run_btc_m30_london_us_breakout_deep_validation(body: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    body = body or {}
    if isinstance(body.get("rows"), list):
        rows = [_finalize_row(dict(row)) for row in body["rows"] if isinstance(row, dict)]
        rows.sort(key=_ranking_key)
        return _result(rows, [], [], [], started)

    timeout_seconds = max(1.0, float(_number(body.get("per_evaluation_timeout_seconds")) or 6.0))
    monte_carlo_simulations = max(100, min(int(_number(body.get("monte_carlo_simulations")) or 250), 1000))
    max_bars = max(500, min(int(_number(body.get("max_bars")) or 60000), 65000))
    requested_targets = _requested_list(body.get("targets"), [item.target_name for item in _deep_configs()])
    configs = [item for item in _deep_configs() if item.target_name in requested_targets]
    csv_paths = _requested_csv_paths(body.get("csv_paths"))

    errors: list[dict[str, Any]] = []
    warnings: list[str] = []
    rows: list[dict[str, Any]] = []
    used_csvs: list[str] = []
    missing_csvs: list[str] = []
    for csv_path in csv_paths:
        if not csv_path.exists():
            missing_csvs.append(str(csv_path))
            continue
        used_csvs.append(str(csv_path))
        evaluated_rows, evaluated_warnings, evaluated_errors = _evaluate_csv(
            csv_path,
            configs,
            max_bars=max_bars,
            timeout_seconds=timeout_seconds,
            monte_carlo_simulations=monte_carlo_simulations,
            spread_points=_number(body.get("spread_points")),
        )
        rows.extend(evaluated_rows)
        warnings.extend(evaluated_warnings)
        errors.extend(evaluated_errors)

    rows.sort(key=_ranking_key)
    export_readiness = _export_readiness(missing_csvs, used_csvs)
    return _result(rows, used_csvs, missing_csvs, errors, started, warnings=warnings, export_readiness=export_readiness)


def _evaluate_csv(
    csv_path: Path,
    configs: list[BtcM30LondonUsBreakoutDeepConfig],
    *,
    max_bars: int,
    timeout_seconds: float,
    monte_carlo_simulations: int,
    spread_points: float | None,
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    warnings: list[str] = []
    errors: list[dict[str, Any]] = []
    cost_model = build_symbol_cost_model(
        SYMBOL,
        resolved_symbol=SYMBOL,
        first_price=_first_csv_price(csv_path),
        broker_spread_points=spread_points,
    )
    settings_body = {
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "csv_path": str(csv_path),
        "max_bars": max_bars,
        "spread_points": cost_model.spread_points,
        "point": cost_model.point,
        "commission": cost_model.commission_assumption,
        "slippage_points": cost_model.slippage_assumption,
        "save_results": False,
        "source": "mt5_csv",
        "timeout_seconds": timeout_seconds,
    }
    settings = replace(
        _settings(settings_body, get_mt5_config()),
        max_bars=max_bars,
        timeout_seconds=max(1.0, min(timeout_seconds, 30.0)),
        point=cost_model.point,
        spread_points=cost_model.spread_points,
        commission=cost_model.commission_assumption,
        slippage_points=cost_model.slippage_assumption,
    )
    bars, load_warnings = _load_bars(settings_body, settings)
    warnings.extend(load_warnings)
    bars = bars[-settings.max_bars :]
    if not bars:
        return [], warnings, [{"csv_path": str(csv_path), "error": "csv_bars_not_loaded"}]

    features_by_index = _features_by_index(bars)
    rows: list[dict[str, Any]] = []
    original_sessions = dict(recent_first._SESSION_HOURS)
    recent_first._SESSION_HOURS.update(_CUSTOM_SESSION_HOURS)
    try:
        for deep_config in configs:
            row = _evaluate_config(
                settings,
                bars,
                features_by_index,
                deep_config,
                csv_path=csv_path,
                timeout_seconds=timeout_seconds,
                monte_carlo_simulations=monte_carlo_simulations,
                cost_model=cost_model.as_dict(),
            )
            rows.append(row)
    finally:
        recent_first._SESSION_HOURS.clear()
        recent_first._SESSION_HOURS.update(original_sessions)
    return rows, warnings, errors


def _evaluate_config(
    settings: Any,
    bars: list[dict[str, Any]],
    features_by_index: dict[int, dict[str, Any]],
    deep_config: BtcM30LondonUsBreakoutDeepConfig,
    *,
    csv_path: Path,
    timeout_seconds: float,
    monte_carlo_simulations: int,
    cost_model: dict[str, Any],
) -> dict[str, Any]:
    started = time.monotonic()
    trades, blocked, signals, state = _simulate_multi_symbol(
        settings,
        bars,
        deep_config.config,
        started,
        timeout_seconds=timeout_seconds,
        features_by_index=features_by_index,
    )
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    total = _metrics(closed, initial_balance=settings.initial_balance)
    split = _split_metrics(settings, bars, closed)
    monte_carlo = _monte_carlo_stress(
        closed,
        initial_balance=settings.initial_balance,
        max_drawdown_limit=5000.0,
        simulations=monte_carlo_simulations,
    )
    spread_x2 = _spread_stress_metrics(settings, bars, deep_config.config, features_by_index, timeout_seconds, 2.0)
    remove_best_5 = _remove_best_metrics(settings, closed, 5)
    fragile = _fragile_dependency(total, split)
    single_trade_dependency = _depends_on_single_trade(total)
    window_results = _window_results(settings, bars, closed)
    chosen_window = _choose_validation_window(window_results)
    return _finalize_row(
        {
            "symbol": SYMBOL,
            "timeframe": TIMEFRAME,
            "family": FAMILY,
            "concept": CONCEPT,
            "target_name": deep_config.target_name,
            "profile": deep_config.target_name,
            "hardening_actions": list(deep_config.hardening_actions),
            "csv_path": str(csv_path),
            "csv_label": csv_path.name,
            "bars_loaded": len(bars),
            "first_bar_time": str(bars[0].get("time") or "") if bars else "",
            "last_bar_time": str(bars[-1].get("time") or "") if bars else "",
            "recent_closed": chosen_window["closed"],
            "recent_pf": chosen_window["profit_factor"],
            "recent_expectancy": chosen_window["expectancy"],
            "validation_window": chosen_window["window"],
            "total_closed": total["closed"],
            "total_pf": total["profit_factor"],
            "expectancy": total["expectancy"],
            "monte_carlo_stressed_pf": monte_carlo.get("profit_factor_stressed", 0.0),
            "monte_carlo_stressed_expectancy": monte_carlo.get("expectancy_stressed", 0.0),
            "spread_x2_pf": spread_x2["profit_factor"],
            "remove_best_5_pf": remove_best_5["profit_factor"],
            "max_drawdown": total["max_drawdown"],
            "fragile_regime_dependency": fragile,
            "single_trade_dependency": single_trade_dependency,
            "window_results": window_results,
            "window_stability": _window_stability(window_results),
            "blocked_reason_count": len(blocked),
            "generated_signal_count": signals.get("generated", 0),
            "actionable_signal_count": signals.get("actionable", 0),
            "risk_governor_blocks": state.get("risk_governor_blocks", 0),
            "cost_model": cost_model,
            "source": "btc_m30_london_us_breakout_deep_validation",
        }
    )


def _finalize_row(row: dict[str, Any]) -> dict[str, Any]:
    row["symbol"] = str(row.get("symbol") or SYMBOL).upper()
    row["timeframe"] = str(row.get("timeframe") or TIMEFRAME).upper()
    row["family"] = str(row.get("family") or FAMILY)
    row["concept"] = str(row.get("concept") or CONCEPT)
    row["profile"] = str(row.get("profile") or row.get("target_name") or FAMILY)
    row["target_name"] = str(row.get("target_name") or row["profile"])
    explicit_window_results = isinstance(row.get("window_results"), list) and bool(row.get("window_results"))
    row["window_results"] = _coerce_window_results(row)
    row["window_stability"] = row.get("window_stability") or _window_stability(row["window_results"])
    chosen_window = _choose_validation_window(row["window_results"])
    row["validation_window"] = str(row.get("validation_window") or chosen_window["window"])
    if explicit_window_results:
        row["recent_closed"] = int(_number(chosen_window.get("closed")) or 0)
        row["recent_pf"] = float(_number(chosen_window.get("profit_factor")) or 0.0)
        row["recent_expectancy"] = float(_number(chosen_window.get("expectancy")) or 0.0)
    else:
        row["recent_closed"] = int(_number(row.get("recent_closed")) or chosen_window["closed"])
        row["recent_pf"] = float(_number(row.get("recent_pf")) or chosen_window["profit_factor"])
    row["total_closed"] = int(_number(row.get("total_closed")) or 0)
    row["total_pf"] = float(_number(row.get("total_pf")) or 0.0)
    row["expectancy"] = float(_number(row.get("expectancy") or row.get("total_expectancy")) or 0.0)
    row["monte_carlo_stressed_pf"] = float(_number(row.get("monte_carlo_stressed_pf")) or 0.0)
    row["monte_carlo_stressed_expectancy"] = float(_number(row.get("monte_carlo_stressed_expectancy")) or 0.0)
    row["spread_x2_pf"] = float(_number(row.get("spread_x2_pf")) or 0.0)
    row["remove_best_5_pf"] = float(_number(row.get("remove_best_5_pf")) or 0.0)
    row["max_drawdown"] = float(_number(row.get("max_drawdown") or row.get("total_max_drawdown")) or 0.0)
    row["degraded_by_registry"] = bool(forward_profile_degradation(row["symbol"], row["timeframe"], row["profile"]))
    row["sibling_risk"] = False
    row["sibling_of_degraded_profile"] = ""
    row["sibling_risk_reason"] = ""
    row["rejection_reasons"] = _gate_reasons(row)
    row["candidate_status"] = "paper_forward_review_ready" if not row["rejection_reasons"] else "gate_failed"
    row["candidate"] = row["candidate_status"] == "paper_forward_review_ready"
    row["recommendation"] = "paper_forward_candidate_review" if row["candidate"] else "continue_research"
    row["recommended_next_action"] = row["recommendation"]
    row["deep_validation_score"] = _score(row)
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


def _coerce_window_results(row: dict[str, Any]) -> list[dict[str, Any]]:
    value = row.get("window_results")
    if isinstance(value, list) and value:
        windows: list[dict[str, Any]] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            window = dict(item)
            closed = int(_number(window.get("closed")) or 0)
            pf = float(_number(window.get("profit_factor")) or 0.0)
            expectancy = float(_number(window.get("expectancy")) or 0.0)
            window["closed"] = closed
            window["profit_factor"] = pf
            window["expectancy"] = expectancy
            window["passes_recent_gate"] = closed >= MIN_RECENT_CLOSED and pf >= MIN_RECENT_PF and expectancy > 0.0
            windows.append(window)
        return windows
    return [
        {
            "window": str(row.get("validation_window") or "synthetic_recent"),
            "closed": int(_number(row.get("recent_closed")) or 0),
            "profit_factor": float(_number(row.get("recent_pf")) or 0.0),
            "expectancy": float(_number(row.get("recent_expectancy")) or 0.0),
            "passes_recent_gate": (
                int(_number(row.get("recent_closed")) or 0) >= MIN_RECENT_CLOSED
                and float(_number(row.get("recent_pf")) or 0.0) >= MIN_RECENT_PF
            ),
        }
    ]


def _gate_reasons(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if row.get("degraded_by_registry"):
        reasons.append("degraded_by_registry")
    if row.get("sibling_risk"):
        reasons.append("sibling_risk")
    _min_gate(reasons, row, "recent_closed", MIN_RECENT_CLOSED)
    _min_gate(reasons, row, "total_closed", MIN_TOTAL_CLOSED)
    _min_gate(reasons, row, "recent_pf", MIN_RECENT_PF)
    _min_gate(reasons, row, "total_pf", MIN_TOTAL_PF)
    if float(_number(row.get("expectancy")) or 0.0) <= 0.0:
        reasons.append("expectancy_not_positive")
    _min_gate(reasons, row, "monte_carlo_stressed_pf", MIN_MONTE_CARLO_STRESSED_PF)
    if float(_number(row.get("monte_carlo_stressed_expectancy")) or 0.0) <= 0.0:
        reasons.append("monte_carlo_stressed_expectancy_not_positive")
    _min_gate(reasons, row, "spread_x2_pf", MIN_SPREAD_X2_PF)
    _min_gate(reasons, row, "remove_best_5_pf", MIN_REMOVE_BEST_5_PF)
    if _flag(row.get("fragile_regime_dependency")):
        reasons.append("fragile_regime_dependency")
    if _flag(row.get("single_trade_dependency")):
        reasons.append("single_trade_dependency")
    return reasons


def _min_gate(reasons: list[str], row: dict[str, Any], field: str, threshold: float) -> None:
    if float(_number(row.get(field)) or 0.0) < threshold:
        reasons.append(f"{field}_below_{_threshold_label(threshold)}")


def _window_results(settings: Any, bars: list[dict[str, Any]], trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    windows: list[tuple[str, int, int]] = [
        ("total_sample", 0, len(bars)),
        ("recent_25_pct", int(len(bars) * 0.75), len(bars)),
        ("recent_15_pct", int(len(bars) * 0.85), len(bars)),
        ("recent_10_pct", int(len(bars) * 0.90), len(bars)),
    ]
    for days in (30, 60, 90):
        cutoff = _cutoff_index_for_days(bars, days)
        if cutoff is not None:
            windows.append((f"last_{days}_days", cutoff, len(bars)))

    results: list[dict[str, Any]] = []
    seen: set[str] = set()
    for name, start, end in windows:
        if name in seen:
            continue
        seen.add(name)
        metrics = _metrics(
            [trade for trade in trades if start <= int(_number(trade.get("opened_index")) or 0) < end],
            initial_balance=settings.initial_balance,
        )
        closed = int(metrics.get("closed") or 0)
        pf = float(metrics.get("profit_factor") or 0.0)
        expectancy = float(metrics.get("expectancy") or 0.0)
        results.append(
            {
                "window": name,
                "start_index": start,
                "end_index": end,
                "closed": closed,
                "profit_factor": pf,
                "expectancy": expectancy,
                "max_drawdown": metrics.get("max_drawdown") or 0.0,
                "passes_recent_gate": closed >= MIN_RECENT_CLOSED and pf >= MIN_RECENT_PF and expectancy > 0.0,
            }
        )
    return results


def _choose_validation_window(windows: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = [window for window in windows if window.get("window") != "total_sample"]
    passing = [window for window in candidates if window.get("passes_recent_gate")]
    pool = passing or candidates or windows
    return sorted(pool, key=_window_key)[0] if pool else {"window": "none", "closed": 0, "profit_factor": 0.0, "expectancy": 0.0}


def _window_key(window: dict[str, Any]) -> tuple[int, int, float, float, str]:
    closed = int(_number(window.get("closed")) or 0)
    pf = float(_number(window.get("profit_factor")) or 0.0)
    expectancy = float(_number(window.get("expectancy")) or 0.0)
    sample_penalty = int(closed < MIN_RECENT_CLOSED)
    return (sample_penalty, -closed, -pf, -expectancy, str(window.get("window") or ""))


def _window_stability(windows: list[dict[str, Any]]) -> dict[str, Any]:
    validation_windows = [window for window in windows if window.get("window") != "total_sample"]
    passing = [window for window in validation_windows if window.get("passes_recent_gate")]
    return {
        "windows_evaluated": len(windows),
        "validation_windows_evaluated": len(validation_windows),
        "passing_recent_windows": len(passing),
        "best_window": (_choose_validation_window(windows).get("window") if windows else "none"),
        "all_validation_windows": [window.get("window") for window in validation_windows],
    }


def _cutoff_index_for_days(bars: list[dict[str, Any]], days: int) -> int | None:
    if not bars:
        return None
    last = _bar_time(bars[-1])
    if last is None:
        return None
    cutoff = last - timedelta(days=days)
    for index, bar in enumerate(bars):
        value = _bar_time(bar)
        if value is not None and value >= cutoff:
            return index
    return 0


def _bar_time(bar: dict[str, Any]) -> datetime | None:
    value = bar.get("time")
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


def _score(row: dict[str, Any]) -> float:
    score = 0.0
    score += min(int(_number(row.get("recent_closed")) or 0), 80) * 4.0
    score += min(int(_number(row.get("total_closed")) or 0), 220) * 0.6
    score += max(0.0, float(_number(row.get("recent_pf")) or 0.0) - 1.0) * 80.0
    score += max(0.0, float(_number(row.get("total_pf")) or 0.0) - 1.0) * 70.0
    score += max(0.0, float(_number(row.get("monte_carlo_stressed_pf")) or 0.0) - 1.0) * 220.0
    score += max(0.0, float(_number(row.get("remove_best_5_pf")) or 0.0) - 1.0) * 120.0
    score += max(0.0, float(_number(row.get("spread_x2_pf")) or 0.0) - 0.95) * 70.0
    score += max(0.0, float(_number(row.get("expectancy")) or 0.0)) * 180.0
    score += max(0.0, float(_number(row.get("monte_carlo_stressed_expectancy")) or 0.0)) * 8.0
    stability = row.get("window_stability") if isinstance(row.get("window_stability"), dict) else {}
    score += int(stability.get("passing_recent_windows") or 0) * 30.0
    score -= len(row.get("rejection_reasons") or []) * 25.0
    if _flag(row.get("fragile_regime_dependency")):
        score -= 160.0
    if _flag(row.get("single_trade_dependency")):
        score -= 160.0
    return round(score, 4)


def _result(
    rows: list[dict[str, Any]],
    used_csvs: list[str],
    missing_csvs: list[str],
    errors: list[dict[str, Any]],
    started: float,
    *,
    warnings: list[str] | None = None,
    export_readiness: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidates = [row for row in rows if row.get("candidate_status") == "paper_forward_review_ready"]
    best = rows[0] if rows else None
    recommendation = "paper_forward_candidate_review" if candidates else "continue_research"
    return {
        "ok": True,
        "status": "btc_m30_london_us_breakout_deep_validation_ready",
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "family": FAMILY,
        "concept": CONCEPT,
        "csv_used": used_csvs,
        "missing_csvs": missing_csvs,
        "windows_evaluated": ["total_sample", "recent_25_pct", "recent_15_pct", "recent_10_pct", "last_30_days", "last_60_days", "last_90_days"],
        "variants_evaluated": len(rows),
        "results": rows,
        "best_variant": best,
        "candidates": candidates,
        "recommended_candidate": candidates[0] if candidates else None,
        "recommendation": recommendation,
        "export_readiness": export_readiness or {},
        "errors": errors,
        "warnings": warnings or [],
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


def _deep_configs() -> list[BtcM30LondonUsBreakoutDeepConfig]:
    return [
        _cfg("btc_m30_london_us_breakout_stricter_london_us_session", ("stricter_london_us_session",), session="strict_london_us", score=57.0),
        _cfg("btc_m30_london_us_breakout_baseline", ("baseline",)),
        _cfg("btc_m30_london_us_breakout_no_offsession", ("no_offsession",), session="london_us"),
        _cfg("btc_m30_london_us_breakout_overlap_only", ("london_us_overlap_only",), session="london_us_overlap"),
        _cfg(
            "btc_m30_london_us_breakout_strict_volatility",
            ("stricter_london_us_session", "volatility_guard"),
            session="strict_london_us",
            volatility="high",
            score=57.0,
        ),
        _cfg(
            "btc_m30_london_us_breakout_strict_time_stop",
            ("stricter_london_us_session", "time_stop_guard"),
            session="strict_london_us",
            time_stop=1,
            score=57.0,
        ),
        _cfg(
            "btc_m30_london_us_breakout_strict_mae",
            ("stricter_london_us_session", "mae_guard"),
            session="strict_london_us",
            mode="mae_guard",
            mae=0.62,
            score=57.0,
        ),
        _cfg(
            "btc_m30_london_us_breakout_strict_trailing",
            ("stricter_london_us_session", "trailing_defensive"),
            session="strict_london_us",
            mode="trailing_defensive",
            fast=0.45,
            trail=0.75,
            lock=0.05,
            score=57.0,
        ),
    ]


def _cfg(
    target_name: str,
    actions: tuple[str, ...],
    *,
    mode: str = "baseline",
    session: str = "london_us",
    volatility: str = "normal_high",
    trend: str = "any",
    rsi: str = "any",
    score: float = 56.0,
    rr: float = 1.05,
    time_stop: int = 2,
    atr_stop: float = 1.0,
    mae: float = 0.78,
    fast: float = 0.0,
    trail: float = 0.0,
    lock: float = 0.0,
) -> BtcM30LondonUsBreakoutDeepConfig:
    variant = RecentFirstVariant(
        family=FAMILY,
        timeframe=TIMEFRAME,
        side_mode="both",
        session_name=session,
        volatility_regime=volatility,
        trend_regime=trend,
        rsi_regime=rsi,
        score_threshold=score,
        risk_reward=rr,
        time_stop_bars=time_stop,
        atr_stop_multiplier=atr_stop,
        mae_exit_r=mae,
        momentum_loss_exit=True,
    )
    return BtcM30LondonUsBreakoutDeepConfig(
        target_name=target_name,
        config=MultiSymbolConfig(
            base=variant,
            hardening_mode=mode,
            mae_exit_r=mae,
            fast_loss_cut_r=fast,
            trailing_activation_r=trail,
            trailing_lock_r=lock,
        ),
        hardening_actions=actions,
    )


def _ranking_key(row: dict[str, Any]) -> tuple[int, int, int, float, str, str]:
    status_rank = 0 if row.get("candidate_status") == "paper_forward_review_ready" else 1
    reasons = list(row.get("rejection_reasons") or [])
    sample_penalty = int("recent_closed_below_15" in reasons)
    return (
        status_rank,
        len(reasons),
        sample_penalty,
        -float(_number(row.get("deep_validation_score")) or 0.0),
        str(row.get("csv_label") or row.get("csv_path") or ""),
        str(row.get("target_name") or ""),
    )


def _requested_csv_paths(value: Any) -> list[Path]:
    if value is None or value == "":
        return _dedupe_paths([Path(path) for path in DEFAULT_CSV_PATHS])
    if isinstance(value, str):
        return _dedupe_paths([Path(item.strip()) for item in value.split(",") if item.strip()])
    if isinstance(value, (list, tuple, set)):
        return _dedupe_paths([Path(str(item)) for item in value if str(item).strip()])
    return _dedupe_paths([Path(path) for path in DEFAULT_CSV_PATHS])


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


def _export_readiness(missing_csvs: list[str], used_csvs: list[str]) -> dict[str, Any]:
    missing_depths = [
        depth
        for depth in ("40000", "60000")
        if any(depth in path for path in missing_csvs) and not any(depth in path for path in used_csvs)
    ]
    missing_deep = [path for path in missing_csvs if any(depth in path for depth in missing_depths)]
    return {
        "needed": bool(missing_depths),
        "missing_depths": missing_depths,
        "missing_deep_csvs": missing_deep,
        "prepared_read_only": bool(missing_depths),
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _threshold_label(value: float) -> str:
    return str(value).replace(".", "_").rstrip("0").rstrip("_")


def _flag(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().casefold() in {"1", "true", "yes", "y"}


def _requested_list(value: Any, default: list[str]) -> list[str]:
    if value is None or value == "":
        return default
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return default
