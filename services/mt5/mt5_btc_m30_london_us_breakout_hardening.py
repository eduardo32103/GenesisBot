from __future__ import annotations

import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import services.mt5.mt5_recent_first_research as recent_first
from services.mt5.mt5_backtester import _load_bars, _number, _safety, _settings
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation
from services.mt5.mt5_multi_symbol_recent_first import (
    MultiSymbolConfig,
    _first_csv_price,
    evaluate_multi_symbol_config,
)
from services.mt5.mt5_recent_first_research import RecentFirstVariant
from services.mt5.mt5_strategy_research_v2 import _features_by_index
from services.mt5.mt5_symbol_cost_model import build_symbol_cost_model


SYMBOL = "BTCUSD"
TIMEFRAME = "M30"
FAMILY = "recent_london_us_breakout"
CONCEPT = "opening_range_fakeout"
DEFAULT_CSV_PATH = Path("data") / "backtests" / "multisymbol" / "BTCUSD_M30_20000.csv"

MIN_RECENT_CLOSED = 15
MIN_TOTAL_CLOSED = 45
MIN_RECENT_PF = 1.05
MIN_TOTAL_PF = 1.15
MIN_MONTE_CARLO_STRESSED_PF = 1.05
MIN_SPREAD_X2_PF = 0.95
MIN_REMOVE_BEST_5_PF = 1.0

_CUSTOM_SESSION_HOURS: dict[str, set[int]] = {
    "strict_london_us": set(range(8, 20)),
    "london_open": {7, 8, 9, 10},
    "ny_open": {13, 14, 15},
    "london_us_overlap": {13, 14, 15, 16},
}


@dataclass(frozen=True)
class BtcM30LondonUsBreakoutHardeningConfig:
    target_name: str
    config: MultiSymbolConfig
    hardening_actions: tuple[str, ...]


def run_btc_m30_london_us_breakout_hardening(body: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    body = body or {}
    if isinstance(body.get("rows"), list):
        rows = [_finalize_row(dict(row)) for row in body["rows"] if isinstance(row, dict)]
        rows.sort(key=_ranking_key)
        return _result(rows, [], [], started, Path(str(body.get("csv_path") or DEFAULT_CSV_PATH)))

    csv_path = Path(str(body.get("csv_path") or DEFAULT_CSV_PATH))
    max_bars = max(500, min(int(_number(body.get("max_bars")) or 20000), 25000))
    timeout_seconds = max(0.5, float(_number(body.get("per_evaluation_timeout_seconds")) or 2.0))
    monte_carlo_simulations = max(100, min(int(_number(body.get("monte_carlo_simulations")) or 300), 1000))
    requested_targets = _requested_list(body.get("targets"), [item.target_name for item in _hardening_configs()])
    configs = [item for item in _hardening_configs() if item.target_name in requested_targets]

    errors: list[dict[str, Any]] = []
    warnings: list[str] = []
    if not csv_path.exists():
        errors.append({"csv_path": str(csv_path), "error": "csv_not_found"})
        return _result([], errors, warnings, started, csv_path)

    cost_model = build_symbol_cost_model(
        SYMBOL,
        resolved_symbol=SYMBOL,
        first_price=_first_csv_price(csv_path),
        broker_spread_points=_number(body.get("spread_points")),
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
        timeout_seconds=max(1.0, min(timeout_seconds, 20.0)),
        point=cost_model.point,
        spread_points=cost_model.spread_points,
        commission=cost_model.commission_assumption,
        slippage_points=cost_model.slippage_assumption,
    )
    bars, load_warnings = _load_bars(settings_body, settings)
    warnings.extend(load_warnings)
    bars = bars[-settings.max_bars :]
    if not bars:
        errors.append({"csv_path": str(csv_path), "error": "csv_bars_not_loaded"})
        return _result([], errors, warnings, started, csv_path)

    features_by_index = _features_by_index(bars)
    rows: list[dict[str, Any]] = []
    original_sessions = dict(recent_first._SESSION_HOURS)
    recent_first._SESSION_HOURS.update(_CUSTOM_SESSION_HOURS)
    try:
        for hardening_config in configs:
            raw = evaluate_multi_symbol_config(
                settings,
                bars,
                hardening_config.config,
                symbol=SYMBOL,
                sample_label="BTCUSD_M30_20000",
                source_csv=str(csv_path),
                features_by_index=features_by_index,
                timeout_seconds=timeout_seconds,
                cost_model=cost_model.as_dict(),
                monte_carlo_simulations=monte_carlo_simulations,
            )
            rows.append(
                _finalize_row(
                    {
                        **raw,
                        "target_name": hardening_config.target_name,
                        "profile": hardening_config.target_name,
                        "hardening_actions": list(hardening_config.hardening_actions),
                        "source": "btc_m30_london_us_breakout_hardening",
                    }
                )
            )
    finally:
        recent_first._SESSION_HOURS.clear()
        recent_first._SESSION_HOURS.update(original_sessions)

    rows.sort(key=_ranking_key)
    return _result(rows, errors, warnings, started, csv_path)


def _finalize_row(row: dict[str, Any]) -> dict[str, Any]:
    row["symbol"] = str(row.get("symbol") or SYMBOL).upper()
    row["timeframe"] = str(row.get("timeframe") or TIMEFRAME).upper()
    row["family"] = str(row.get("family") or FAMILY)
    row["concept"] = str(row.get("concept") or CONCEPT)
    row["profile"] = str(row.get("profile") or row.get("target_name") or row.get("hardening_mode") or FAMILY)
    row["target_name"] = str(row.get("target_name") or row["profile"])
    row["expectancy"] = float(_number(row.get("expectancy") or row.get("total_expectancy")) or 0.0)
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
    row["btc_m30_london_us_breakout_score"] = _score(row)
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
    if row.get("degraded_by_registry"):
        reasons.append("degraded_by_registry")
    if row.get("sibling_risk"):
        reasons.append("sibling_risk")
    _min_gate(reasons, row, "recent_closed", MIN_RECENT_CLOSED)
    _min_gate(reasons, row, "total_closed", MIN_TOTAL_CLOSED)
    _min_gate(reasons, row, "recent_pf", MIN_RECENT_PF)
    _min_gate(reasons, row, "total_pf", MIN_TOTAL_PF)
    if float(_number(row.get("expectancy") or row.get("total_expectancy")) or 0.0) <= 0.0:
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


def _score(row: dict[str, Any]) -> float:
    score = 0.0
    score += min(int(_number(row.get("recent_closed")) or 0), 80) * 3.0
    score += min(int(_number(row.get("total_closed")) or 0), 180) * 0.8
    score += max(0.0, float(_number(row.get("recent_pf")) or 0.0) - 1.0) * 95.0
    score += max(0.0, float(_number(row.get("total_pf")) or 0.0) - 1.0) * 75.0
    score += max(0.0, float(_number(row.get("monte_carlo_stressed_pf")) or 0.0) - 1.0) * 220.0
    score += max(0.0, float(_number(row.get("spread_x2_pf")) or 0.0) - 0.95) * 75.0
    score += max(0.0, float(_number(row.get("remove_best_5_pf")) or 0.0) - 1.0) * 120.0
    score += max(0.0, float(_number(row.get("expectancy") or row.get("total_expectancy")) or 0.0)) * 220.0
    score += max(0.0, float(_number(row.get("monte_carlo_stressed_expectancy")) or 0.0)) * 20.0
    score -= len(row.get("rejection_reasons") or []) * 18.0
    if _flag(row.get("fragile_regime_dependency")):
        score -= 140.0
    if _flag(row.get("single_trade_dependency")):
        score -= 140.0
    return round(score, 4)


def _result(
    rows: list[dict[str, Any]],
    errors: list[dict[str, Any]],
    warnings: list[str],
    started: float,
    csv_path: Path,
) -> dict[str, Any]:
    candidates = [row for row in rows if row.get("candidate_status") == "paper_forward_review_ready"]
    best = rows[0] if rows else None
    recommendation = "paper_forward_candidate_review" if candidates else "continue_research"
    return {
        "ok": True,
        "status": "btc_m30_london_us_breakout_hardening_ready",
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "family": FAMILY,
        "concept": CONCEPT,
        "csv_path": str(csv_path),
        "variants_evaluated": len(rows),
        "results": rows,
        "best_variant": best,
        "candidates": candidates,
        "recommended_candidate": candidates[0] if candidates else None,
        "recommendation": recommendation,
        "errors": errors,
        "warnings": warnings,
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


def _hardening_configs() -> list[BtcM30LondonUsBreakoutHardeningConfig]:
    return [
        _cfg("btc_m30_london_us_breakout_baseline", ("baseline",)),
        _cfg("btc_m30_london_us_breakout_stricter_london_us_session", ("stricter_london_us_session",), session="strict_london_us", score=57.0),
        _cfg("btc_m30_london_us_breakout_london_open_only", ("london_open_only",), session="london_open"),
        _cfg("btc_m30_london_us_breakout_ny_open_only", ("ny_open_only",), session="ny_open"),
        _cfg("btc_m30_london_us_breakout_overlap_only", ("london_us_overlap_only",), session="london_us_overlap"),
        _cfg("btc_m30_london_us_breakout_volatility_guard", ("volatility_guard",), volatility="high"),
        _cfg("btc_m30_london_us_breakout_trend_guard", ("trend_guard",), trend="trend", score=58.0),
        _cfg("btc_m30_london_us_breakout_momentum_guard", ("momentum_guard",), score=60.0, rsi="not_extreme"),
        _cfg("btc_m30_london_us_breakout_spread_guard", ("spread_guard",), score=58.0, rsi="not_extreme", mae=0.72),
        _cfg("btc_m30_london_us_breakout_mae_guard", ("mae_guard",), mode="mae_guard", mae=0.62),
        _cfg("btc_m30_london_us_breakout_fast_loss_cut", ("fast_loss_cut",), mode="fast_loss_cut", fast=0.34, mae=0.76),
        _cfg(
            "btc_m30_london_us_breakout_trailing_defensive",
            ("trailing_defensive",),
            mode="trailing_defensive",
            fast=0.45,
            trail=0.75,
            lock=0.05,
        ),
        _cfg("btc_m30_london_us_breakout_time_stop_guard", ("time_stop_guard",), time_stop=1),
        _cfg("btc_m30_london_us_breakout_remove_low_atr", ("remove_low_atr",), volatility="high"),
        _cfg("btc_m30_london_us_breakout_remove_chop_regime", ("remove_chop_regime",), trend="trend"),
        _cfg(
            "btc_m30_london_us_breakout_strict_momentum",
            ("stricter_london_us_session", "momentum_guard"),
            session="strict_london_us",
            score=60.0,
            rsi="not_extreme",
        ),
        _cfg(
            "btc_m30_london_us_breakout_london_open_momentum",
            ("london_open_only", "momentum_guard"),
            session="london_open",
            score=60.0,
            rsi="not_extreme",
        ),
        _cfg(
            "btc_m30_london_us_breakout_overlap_volatility",
            ("london_us_overlap_only", "volatility_guard"),
            session="london_us_overlap",
            volatility="high",
        ),
        _cfg(
            "btc_m30_london_us_breakout_volatility_mae",
            ("volatility_guard", "mae_guard"),
            volatility="high",
            mode="mae_guard",
            mae=0.62,
        ),
        _cfg(
            "btc_m30_london_us_breakout_trend_momentum",
            ("trend_guard", "momentum_guard"),
            trend="trend",
            score=60.0,
            rsi="not_extreme",
        ),
        _cfg(
            "btc_m30_london_us_breakout_strict_fast_loss_cut",
            ("stricter_london_us_session", "fast_loss_cut"),
            session="strict_london_us",
            mode="fast_loss_cut",
            fast=0.34,
            mae=0.76,
        ),
        _cfg(
            "btc_m30_london_us_breakout_time_stop_mae",
            ("time_stop_guard", "mae_guard"),
            time_stop=1,
            mode="mae_guard",
            mae=0.62,
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
) -> BtcM30LondonUsBreakoutHardeningConfig:
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
    return BtcM30LondonUsBreakoutHardeningConfig(
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


def _ranking_key(row: dict[str, Any]) -> tuple[int, int, int, int, float, str]:
    status_rank = 0 if row.get("candidate_status") == "paper_forward_review_ready" else 1
    reasons = list(row.get("rejection_reasons") or [])
    sample_penalty = int(any(reason in reasons for reason in {"recent_closed_below_15", "total_closed_below_45"}))
    dependency_penalty = int(any(reason in reasons for reason in {"fragile_regime_dependency", "single_trade_dependency"}))
    return (
        status_rank,
        len(reasons),
        sample_penalty,
        dependency_penalty,
        -float(_number(row.get("btc_m30_london_us_breakout_score")) or 0.0),
        str(row.get("target_name") or ""),
    )


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
