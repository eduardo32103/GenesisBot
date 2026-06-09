from __future__ import annotations

import csv
import json
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

from services.mt5.mt5_backtester import _load_bars, _settings
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation
from services.mt5.mt5_multi_symbol_recent_first import MultiSymbolConfig, _first_csv_price, evaluate_multi_symbol_config
from services.mt5.mt5_research_rejection_registry import research_rejection, research_rejection_registry_status
from services.mt5.mt5_recent_first_research import RecentFirstVariant
from services.mt5.mt5_strategy_research_v2 import _features_by_index
from services.mt5.mt5_symbol_cost_model import build_symbol_cost_model


MIN_RECENT_CLOSED = 15
MIN_TOTAL_CLOSED = 45
MIN_RECENT_PF = 1.05
MIN_TOTAL_PF = 1.15
MIN_MONTE_CARLO_STRESSED_PF = 1.05
MIN_SPREAD_X2_PF = 0.95
MIN_REMOVE_BEST_5_PF = 1.0
MAX_RESULTS_FILE_BYTES = 2_000_000

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_RESULTS_DIR = _REPO_ROOT / "data" / "backtests" / "multisymbol"
_KNOWN_RESULT_FILENAMES = (
    "multi_symbol_recent_first_cost_calibrated_results.json",
    "multi_symbol_recent_first_cost_calibrated_results.csv",
    "multi_symbol_recent_first_results.json",
    "multi_symbol_recent_first_results.csv",
    "eth_m30_volatility_hardening_results.json",
    "eth_m30_volatility_hardening_results.csv",
    "eth_m30_capital_preservation_results.json",
    "eth_m30_capital_preservation_results.csv",
)
_DEFAULT_CSV_DIR = _DEFAULT_RESULTS_DIR

_ALLOWED_SYMBOLS = {"BTCUSD", "ETHUSD", "XAUUSD", "USTEC", "NAS100", "US500", "EURUSD", "GBPUSD"}
_ALLOWED_TIMEFRAMES = {"M15", "M30", "H1"}

_NEW_FAMILY_MAP: dict[str, str] = {
    "recent_range_reversion": "mean_reversion_after_exhaustion",
    "recent_chop_avoidance_reversal": "mean_reversion_after_exhaustion",
    "recent_momentum_pullback": "trend_pullback_continuation",
    "recent_liquidity_sweep": "liquidity_sweep_reversal",
    "recent_failed_breakout_reversal": "range_breakout_failed_retest",
    "recent_london_us_breakout": "opening_range_fakeout",
    "recent_atr_expansion_scalp": "atr_expansion_continuation",
}

_SKIPPED_FAMILY_IDEAS = (
    {
        "family": "volatility_compression_breakout",
        "reason": "not_implemented_in_current_offline_signal_set",
        "next_step": "add explicit compression setup before backtest",
    },
    {
        "family": "session_vwap_reclaim",
        "reason": "not_implemented_in_current_offline_signal_set",
        "next_step": "add VWAP feature generation before backtest",
    },
    {
        "family": "rsi_divergence_confirmation",
        "reason": "not_implemented_in_current_offline_signal_set",
        "next_step": "add divergence detector before backtest",
    },
    {
        "family": "ema_slope_pullback",
        "reason": "covered_by_recent_momentum_pullback_proxy",
        "next_step": "split into dedicated EMA-slope signal only if proxy near-misses",
    },
)

_ALIASES = {
    "symbol": ("symbol", "Symbol", "requested_symbol", "resolved_symbol", "normalized_symbol"),
    "timeframe": ("timeframe", "Timeframe"),
    "profile": ("profile", "Profile", "strategy_profile", "target_name", "experimental_registry_record"),
    "family": ("family", "Family", "strategy_family"),
    "side": ("side", "side_mode"),
    "session": ("session", "session_filter"),
    "hardening_mode": ("hardening_mode", "mode"),
    "recent_closed": ("recent_closed", "recent_trades", "closed_recent", "closed_recent_holdout"),
    "total_closed": ("total_closed", "closed", "trades", "closed_total", "total_trades"),
    "recent_pf": ("recent_pf", "recent_profit_factor", "pf_recent_holdout"),
    "total_pf": ("total_pf", "profit_factor", "profit_factor_total"),
    "expectancy": ("expectancy", "total_expectancy", "expectancy_total", "recent_expectancy"),
    "monte_carlo_stressed_pf": ("monte_carlo_stressed_pf", "mc_pf", "stressed_pf"),
    "monte_carlo_stressed_expectancy": ("monte_carlo_stressed_expectancy", "mc_expectancy", "stressed_expectancy"),
    "spread_x2_pf": ("spread_x2_pf", "spread_stress_pf"),
    "remove_best_5_pf": ("remove_best_5_pf",),
    "max_drawdown": ("max_drawdown", "total_max_drawdown"),
    "fragile_regime_dependency": ("fragile_regime_dependency", "fragile_regime", "fragile"),
    "single_trade_dependency": ("single_trade_dependency", "single_trade_dependent"),
}


def run_new_family_edge_discovery(
    *,
    rows: list[dict[str, Any]] | None = None,
    result_paths: list[str | Path] | None = None,
    search_root: str | Path | None = None,
    load_default_sources: bool = True,
    include_offline_backtests: bool = False,
    max_offline_evaluations: int = 80,
    max_bars: int = 20000,
    monte_carlo_simulations: int = 150,
    per_evaluation_timeout_seconds: float = 1.5,
) -> dict[str, Any]:
    started = time.monotonic()
    discovered_rows, loaded_sources, missing_sources, skipped_sources = _load_existing_result_rows(
        result_paths or [],
        search_root=search_root,
        load_default_sources=load_default_sources,
    )
    source_rows = discovered_rows + list(rows or [])
    offline_rows: list[dict[str, Any]] = []
    offline_errors: list[dict[str, str]] = []
    if include_offline_backtests:
        offline_rows, offline_errors = _offline_gap_backtest_rows(
            source_rows,
            csv_root=Path(search_root) if search_root else _DEFAULT_CSV_DIR,
            max_evaluations=max_offline_evaluations,
            max_bars=max_bars,
            monte_carlo_simulations=monte_carlo_simulations,
            timeout_seconds=per_evaluation_timeout_seconds,
        )
        source_rows.extend(offline_rows)
    normalized = [_normalize_row(row) for row in source_rows if _is_useful_source_row(row)]
    merged = _merge_rows(normalized)

    evaluated: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for row in merged:
        assessed = _evaluate_row(row)
        if assessed.get("candidate_status") in {
            "excluded_by_degradation_registry",
            "blocked_by_sibling_risk",
            "excluded_by_research_rejection_registry",
        }:
            excluded.append(assessed)
        elif assessed.get("candidate_pool_eligible"):
            evaluated.append(assessed)

    evaluated.sort(key=_ranking_key)
    excluded.sort(key=_excluded_key)
    candidates = [row for row in evaluated if row["candidate_status"] == "paper_forward_review_ready"]
    near_misses = [row for row in evaluated if row["candidate_status"] == "near_miss"]
    recommended = candidates[0] if candidates else None
    recommendation = "paper_forward_candidate_review" if recommended else "continue_research"

    return {
        "ok": True,
        "status": "new_family_edge_discovery_ready",
        "recommendation": recommendation,
        "recommended_candidate": recommended,
        "candidates": candidates,
        "ranking": evaluated,
        "top_near_misses": near_misses[:5],
        "excluded_by_registry_or_sibling_risk": excluded,
        "families_evaluated": sorted({row["conceptual_family"] for row in evaluated}),
        "raw_families_evaluated": sorted({row["family"] for row in evaluated}),
        "symbol_timeframes_evaluated": sorted({f"{row['symbol']} {row['timeframe']}" for row in evaluated}),
        "loaded_sources": loaded_sources,
        "missing_sources": missing_sources,
        "skipped_sources": skipped_sources,
        "offline_backtests_run": include_offline_backtests,
        "offline_evaluations": len(offline_rows),
        "offline_errors": offline_errors,
        "useful_rows": len(merged),
        "skipped_family_ideas": list(_SKIPPED_FAMILY_IDEAS),
        "research_rejection_registry": research_rejection_registry_status(),
        "next_expansion": _next_expansion(recommended, near_misses),
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


def _evaluate_row(row: dict[str, Any]) -> dict[str, Any]:
    degraded = forward_profile_degradation(row["symbol"], row["timeframe"], row["profile"])
    conceptual_family = _NEW_FAMILY_MAP.get(row["family"], "")
    rejected = research_rejection(row["symbol"], row["timeframe"], row["profile"], row["family"], conceptual_family)
    sibling_risk, sibling_of, sibling_reason = (False, "", "") if degraded or rejected else _sibling_risk(row)
    candidate_pool_eligible = bool(conceptual_family) and not _is_failed_family_cluster(row)

    assessed = {
        **row,
        "conceptual_family": conceptual_family,
        "degraded_by_registry": bool(degraded),
        "degradation_reason": degraded.get("degradation_reason") or "",
        "rejected_by_research_registry": bool(rejected),
        "research_rejection_status": rejected.get("rejection_status") or "",
        "research_rejection_reason": rejected.get("rejection_reason") or "",
        "research_rejection_registry_version": rejected.get("reviewed_at_version") or "",
        "sibling_risk": sibling_risk,
        "sibling_of_degraded_profile": sibling_of,
        "sibling_risk_reason": sibling_reason,
        "candidate_pool_eligible": candidate_pool_eligible,
    }
    rejection_reasons, shortfalls = _gate_reasons(assessed)
    if assessed["degraded_by_registry"]:
        status = "excluded_by_degradation_registry"
        next_action = "skip_degraded_profile"
    elif assessed["rejected_by_research_registry"]:
        status = "excluded_by_research_rejection_registry"
        next_action = "skip_rejected_family"
    elif assessed["sibling_risk"]:
        status = "blocked_by_sibling_risk"
        next_action = "manual_review_or_new_family_required"
    elif not candidate_pool_eligible:
        status = "excluded_correlated_or_out_of_scope_family"
        next_action = "skip_failed_or_correlated_family"
    elif not rejection_reasons:
        status = "paper_forward_review_ready"
        next_action = "paper_forward_candidate_review"
    elif _worth_near_miss(assessed, rejection_reasons):
        status = "near_miss"
        next_action = "targeted_hardening_review"
    else:
        status = "research_gate_failed"
        next_action = "continue_research"

    return {
        **assessed,
        "rejection_reasons": rejection_reasons,
        "gate_shortfalls": shortfalls,
        "candidate_status": status,
        "recommended_next_action": next_action,
        "discovery_score": _score(assessed, rejection_reasons),
        **_safety(),
    }


def _gate_reasons(row: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
    reasons: list[str] = []
    shortfalls: dict[str, Any] = {}
    if row.get("degraded_by_registry"):
        reasons.append("degraded_by_registry")
    if row.get("rejected_by_research_registry"):
        reasons.append("research_rejection_registry")
    if row.get("sibling_risk"):
        reasons.append("sibling_risk")
    _min_gate(reasons, shortfalls, row, "recent_closed", MIN_RECENT_CLOSED)
    _min_gate(reasons, shortfalls, row, "total_closed", MIN_TOTAL_CLOSED)
    _min_gate(reasons, shortfalls, row, "recent_pf", MIN_RECENT_PF)
    _min_gate(reasons, shortfalls, row, "total_pf", MIN_TOTAL_PF)
    if float(row.get("expectancy") or 0.0) <= 0.0:
        reasons.append("expectancy_not_positive")
        shortfalls["expectancy"] = {"current": float(row.get("expectancy") or 0.0), "required": "> 0"}
    _min_gate(reasons, shortfalls, row, "monte_carlo_stressed_pf", MIN_MONTE_CARLO_STRESSED_PF)
    if float(row.get("monte_carlo_stressed_expectancy") or 0.0) <= 0.0:
        reasons.append("monte_carlo_stressed_expectancy_not_positive")
        shortfalls["monte_carlo_stressed_expectancy"] = {
            "current": float(row.get("monte_carlo_stressed_expectancy") or 0.0),
            "required": "> 0",
        }
    _min_gate(reasons, shortfalls, row, "spread_x2_pf", MIN_SPREAD_X2_PF)
    _min_gate(reasons, shortfalls, row, "remove_best_5_pf", MIN_REMOVE_BEST_5_PF)
    if bool(row.get("fragile_regime_dependency")):
        reasons.append("fragile_regime_dependency")
        shortfalls["fragile_regime_dependency"] = {"current": True, "required": False}
    if bool(row.get("single_trade_dependency")):
        reasons.append("single_trade_dependency")
        shortfalls["single_trade_dependency"] = {"current": True, "required": False}
    return reasons, shortfalls


def _min_gate(
    reasons: list[str],
    shortfalls: dict[str, Any],
    row: dict[str, Any],
    field: str,
    threshold: float,
) -> None:
    current = float(row.get(field) or 0.0)
    if current >= threshold:
        return
    reasons.append(f"{field}_below_{_threshold_label(threshold)}")
    shortfalls[field] = {"current": current, "required": threshold, "missing": round(threshold - current, 6)}


def _worth_near_miss(row: dict[str, Any], rejection_reasons: list[str]) -> bool:
    hard_blockers = {
        "degraded_by_registry",
        "research_rejection_registry",
        "sibling_risk",
        "fragile_regime_dependency",
        "single_trade_dependency",
        "expectancy_not_positive",
        "monte_carlo_stressed_expectancy_not_positive",
    }
    if hard_blockers & set(rejection_reasons):
        return False
    return (
        int(row.get("recent_closed") or 0) >= 10
        and int(row.get("total_closed") or 0) >= 35
        and float(row.get("recent_pf") or 0.0) >= 1.0
        and float(row.get("total_pf") or 0.0) >= 1.05
        and float(row.get("expectancy") or 0.0) > 0.0
    )


def _score(row: dict[str, Any], rejection_reasons: list[str]) -> float:
    score = 0.0
    score += min(int(row.get("recent_closed") or 0), 80) * 3.0
    score += min(int(row.get("total_closed") or 0), 200) * 0.8
    score += float(row.get("recent_pf") or 0.0) * 35.0
    score += float(row.get("total_pf") or 0.0) * 40.0
    score += float(row.get("monte_carlo_stressed_pf") or 0.0) * 45.0
    score += float(row.get("spread_x2_pf") or 0.0) * 25.0
    score += float(row.get("remove_best_5_pf") or 0.0) * 20.0
    score += max(0.0, float(row.get("expectancy") or 0.0)) * 100.0
    score += max(0.0, float(row.get("monte_carlo_stressed_expectancy") or 0.0)) * 20.0
    score -= len(rejection_reasons) * 45.0
    if row.get("degraded_by_registry"):
        score -= 10_000.0
    if row.get("rejected_by_research_registry"):
        score -= 9_000.0
    if row.get("sibling_risk"):
        score -= 5_000.0
    return round(score, 4)


def _sibling_risk(row: dict[str, Any]) -> tuple[bool, str, str]:
    if _is_eth_m30_volatility_breakout_cluster(row):
        return True, "eth_m30_vol_breakout_chop_guard_v1", "failed_eth_m30_volatility_breakout_cluster"
    if _is_xau_m15_session_open_cluster(row):
        return True, "xau_m15_session_baseline", "failed_xau_m15_session_open_continuation_hardening"
    if _is_btc_h1_ema_reclaim_cluster(row):
        return True, "btc_h1_ema_reclaim_volatility_guard", "failed_btc_h1_ema_reclaim_hardening"
    return False, "", ""


def _is_failed_family_cluster(row: dict[str, Any]) -> bool:
    return (
        _is_eth_m30_volatility_breakout_cluster(row)
        or _is_xau_m15_session_open_cluster(row)
        or _is_btc_h1_ema_reclaim_cluster(row)
        or row["family"] in {"recent_volatility_breakout", "recent_session_open_continuation", "recent_ema_reclaim"}
    )


def _is_eth_m30_volatility_breakout_cluster(row: dict[str, Any]) -> bool:
    return row["symbol"] == "ETHUSD" and row["timeframe"] == "M30" and _contains(row, "volatility_breakout", "vol_breakout")


def _is_xau_m15_session_open_cluster(row: dict[str, Any]) -> bool:
    return row["symbol"] == "XAUUSD" and row["timeframe"] == "M15" and _contains(row, "session_open_continuation")


def _is_btc_h1_ema_reclaim_cluster(row: dict[str, Any]) -> bool:
    return row["symbol"] == "BTCUSD" and row["timeframe"] == "H1" and _contains(row, "ema_reclaim")


def _contains(row: dict[str, Any], *needles: str) -> bool:
    blob = f"{row.get('family') or ''} {row.get('profile') or ''}".casefold()
    return any(needle in blob for needle in needles)


def _next_expansion(recommended: dict[str, Any] | None, near_misses: list[dict[str, Any]]) -> list[dict[str, str]]:
    if recommended:
        return [{"action": "human_review_only", "reason": "clean_candidate_found"}]
    if near_misses:
        top = near_misses[0]
        return [
            {
                "action": "targeted_hardening_review",
                "reason": f"top near miss {top.get('symbol')} {top.get('timeframe')} {top.get('family')}",
            },
            {"action": "implement_session_vwap_reclaim", "reason": "not covered by current offline signals"},
            {"action": "implement_rsi_divergence_confirmation", "reason": "not covered by current offline signals"},
        ]
    return [
        {"action": "expand_signal_set", "reason": "no clean candidate and no hardening-grade near miss"},
        {"action": "implement_volatility_compression_breakout", "reason": "requested family is not in current offline signals"},
        {"action": "collect_more_recent_processed_results", "reason": "preserve paper-only workflow without runtime mutation"},
    ]


def _offline_gap_backtest_rows(
    source_rows: list[dict[str, Any]],
    *,
    csv_root: Path,
    max_evaluations: int,
    max_bars: int,
    monte_carlo_simulations: int,
    timeout_seconds: float,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    present_families = {
        normalized["family"]
        for row in source_rows
        if _is_useful_source_row(row)
        for normalized in [_normalize_row(row)]
    }
    missing_families = [
        family
        for family in ("recent_chop_avoidance_reversal", "recent_london_us_breakout", "recent_atr_expansion_scalp")
        if family not in present_families
    ]
    if not missing_families:
        return [], []

    rows: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    evaluations = 0
    for csv_path in _csv_targets(csv_root):
        if evaluations >= max_evaluations:
            break
        symbol = _symbol_from_csv_path(csv_path)
        timeframe = _timeframe_from_csv_path(csv_path)
        if symbol not in _ALLOWED_SYMBOLS or timeframe not in _ALLOWED_TIMEFRAMES:
            continue
        loaded = _load_offline_bars(csv_path, symbol=symbol, timeframe=timeframe, max_bars=max_bars, timeout_seconds=timeout_seconds)
        if isinstance(loaded, dict) and loaded.get("error"):
            errors.append({"csv_path": str(csv_path), "error": str(loaded["error"])})
            continue
        settings, bars, cost_model = loaded
        features_by_index = _features_by_index(bars)
        for family in missing_families:
            if evaluations >= max_evaluations:
                break
            config = _offline_config(family, timeframe)
            try:
                raw = evaluate_multi_symbol_config(
                    settings,
                    bars,
                    config,
                    symbol=symbol,
                    sample_label=f"{symbol}_{timeframe}_offline_gap",
                    source_csv=str(csv_path),
                    features_by_index=features_by_index,
                    timeout_seconds=timeout_seconds,
                    cost_model=cost_model.as_dict(),
                    monte_carlo_simulations=monte_carlo_simulations,
                )
            except Exception as exc:  # pragma: no cover - defensive around research helpers
                errors.append({"csv_path": str(csv_path), "family": family, "error": type(exc).__name__})
                continue
            evaluations += 1
            rows.append(
                {
                    **raw,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "family": family,
                    "profile": f"{family}|side=both|session={_offline_session(family)}|mode=offline_gap_baseline",
                    "side": "both",
                    "session": _offline_session(family),
                    "hardening_mode": "offline_gap_baseline",
                    "source": "offline_gap_backtest",
                    "source_csv": str(csv_path),
                }
            )
    return rows, errors


def _load_offline_bars(
    csv_path: Path,
    *,
    symbol: str,
    timeframe: str,
    max_bars: int,
    timeout_seconds: float,
) -> tuple[Any, list[dict[str, Any]], Any] | dict[str, str]:
    cost_model = build_symbol_cost_model(
        symbol,
        resolved_symbol=csv_path.name.split("_", 1)[0],
        first_price=_first_csv_price(csv_path),
        broker_spread_points=None,
    )
    settings_body = {
        "symbol": symbol,
        "timeframe": timeframe,
        "csv_path": str(csv_path),
        "max_bars": max(500, min(int(max_bars), 25000)),
        "spread_points": cost_model.spread_points,
        "point": cost_model.point,
        "commission": cost_model.commission_assumption,
        "slippage_points": cost_model.slippage_assumption,
        "save_results": False,
        "source": "mt5_csv",
        "timeout_seconds": max(0.5, timeout_seconds),
    }
    settings = replace(
        _settings(settings_body, get_mt5_config()),
        max_bars=settings_body["max_bars"],
        timeout_seconds=max(1.0, min(timeout_seconds, 20.0)),
        point=cost_model.point,
        spread_points=cost_model.spread_points,
        commission=cost_model.commission_assumption,
        slippage_points=cost_model.slippage_assumption,
    )
    bars, warnings = _load_bars(settings_body, settings)
    bars = bars[-settings.max_bars :]
    if warnings and not bars:
        return {"error": ";".join(str(item) for item in warnings)}
    if not bars:
        return {"error": "csv_bars_not_loaded"}
    return settings, bars, cost_model


def _offline_config(family: str, timeframe: str) -> MultiSymbolConfig:
    session = _offline_session(family)
    trend = "chop" if family in {"recent_chop_avoidance_reversal"} else "any"
    volatility = "normal_high" if family in {"recent_london_us_breakout", "recent_atr_expansion_scalp"} else "any"
    variant = RecentFirstVariant(
        family=family,
        timeframe=timeframe,
        side_mode="both",
        session_name=session,
        volatility_regime=volatility,
        trend_regime=trend,
        rsi_regime="any",
        score_threshold=56.0 if volatility == "normal_high" else 54.0,
        risk_reward=1.05 if volatility == "normal_high" else 0.95,
        time_stop_bars=3 if timeframe == "H1" else 2,
        atr_stop_multiplier=1.0,
        mae_exit_r=0.78,
        momentum_loss_exit=True,
    )
    return MultiSymbolConfig(base=variant, hardening_mode="baseline", mae_exit_r=0.78)


def _offline_session(family: str) -> str:
    if family == "recent_london_us_breakout":
        return "london_us"
    return "all"


def _csv_targets(csv_root: Path) -> list[Path]:
    if not csv_root.exists():
        return []
    return sorted(
        path
        for path in csv_root.glob("*.csv")
        if path.name.endswith("_20000.csv") and ("_M" in path.name or "_H1_" in path.name)
    )


def _symbol_from_csv_path(path: Path) -> str:
    raw = path.name.rsplit("_", 2)[0]
    return _symbol(raw)


def _timeframe_from_csv_path(path: Path) -> str:
    parts = path.stem.split("_")
    if len(parts) < 2:
        return ""
    return _timeframe(parts[-2])


def _ranking_key(row: dict[str, Any]) -> tuple[int, float, str, str, str]:
    ranks = {"paper_forward_review_ready": 0, "near_miss": 1, "research_gate_failed": 2}
    return (
        ranks.get(row.get("candidate_status") or "", 9),
        -float(row.get("discovery_score") or 0.0),
        str(row.get("symbol") or ""),
        str(row.get("timeframe") or ""),
        str(row.get("profile") or ""),
    )


def _excluded_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (str(row.get("symbol") or ""), str(row.get("timeframe") or ""), str(row.get("profile") or ""))


def _merge_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["symbol"], row["timeframe"], row["profile"])
        current = merged.get(key, {})
        merged[key] = _prefer_more_complete_row(current, row)
    return list(merged.values())


def _prefer_more_complete_row(current: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    if not current:
        return {k: v for k, v in candidate.items() if v not in (None, "")}
    if _row_data_score(candidate) >= _row_data_score(current):
        return {**current, **{k: v for k, v in candidate.items() if v not in (None, "")}}
    return {**candidate, **{k: v for k, v in current.items() if v not in (None, "")}}


def _row_data_score(row: dict[str, Any]) -> float:
    return sum(
        abs(_number(row.get(key)) or 0.0)
        for key in (
            "recent_closed",
            "total_closed",
            "recent_pf",
            "total_pf",
            "expectancy",
            "monte_carlo_stressed_pf",
            "monte_carlo_stressed_expectancy",
            "spread_x2_pf",
            "remove_best_5_pf",
        )
    )


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    symbol = _symbol(_get_alias(row, "symbol"))
    family = str(_get_alias(row, "family") or "").strip()
    side = str(_get_alias(row, "side") or "").strip() or "both"
    session = str(_get_alias(row, "session") or "").strip() or "all"
    hardening_mode = str(_get_alias(row, "hardening_mode") or "").strip() or "baseline"
    profile = str(_get_alias(row, "profile") or "").strip()
    if not profile:
        profile = f"{family}|side={side}|session={session}|mode={hardening_mode}"
    return {
        **row,
        "symbol": symbol,
        "timeframe": _timeframe(_get_alias(row, "timeframe")),
        "family": family,
        "profile": profile,
        "side": side,
        "session": session,
        "hardening_mode": hardening_mode,
        "recent_closed": int(_number(_get_alias(row, "recent_closed")) or 0),
        "total_closed": int(_number(_get_alias(row, "total_closed")) or 0),
        "recent_pf": float(_number(_get_alias(row, "recent_pf")) or 0.0),
        "total_pf": float(_number(_get_alias(row, "total_pf")) or 0.0),
        "expectancy": float(_number(_get_alias(row, "expectancy")) or 0.0),
        "monte_carlo_stressed_pf": float(_number(_get_alias(row, "monte_carlo_stressed_pf")) or 0.0),
        "monte_carlo_stressed_expectancy": float(_number(_get_alias(row, "monte_carlo_stressed_expectancy")) or 0.0),
        "spread_x2_pf": float(_number(_get_alias(row, "spread_x2_pf")) or 0.0),
        "remove_best_5_pf": float(_number(_get_alias(row, "remove_best_5_pf")) or 0.0),
        "max_drawdown": float(_number(_get_alias(row, "max_drawdown")) or 0.0),
        "fragile_regime_dependency": _flag(_get_alias(row, "fragile_regime_dependency")),
        "single_trade_dependency": _flag(_get_alias(row, "single_trade_dependency")),
    }


def _is_useful_source_row(row: dict[str, Any]) -> bool:
    normalized = _normalize_row(row)
    if normalized["symbol"] not in _ALLOWED_SYMBOLS:
        return False
    if normalized["timeframe"] not in _ALLOWED_TIMEFRAMES:
        return False
    if not normalized["family"] or not normalized["profile"]:
        return False
    return _row_data_score(normalized) > 0.0


def _load_existing_result_rows(
    result_paths: list[str | Path],
    *,
    search_root: str | Path | None,
    load_default_sources: bool,
) -> tuple[list[dict[str, Any]], list[str], list[str], list[dict[str, str]]]:
    paths = [Path(path) for path in result_paths]
    if search_root:
        root = Path(search_root)
        paths.extend(root / name for name in _KNOWN_RESULT_FILENAMES)
    elif load_default_sources:
        paths.extend(_DEFAULT_RESULTS_DIR / name for name in _KNOWN_RESULT_FILENAMES)
    paths = _dedupe_paths(paths)

    rows: list[dict[str, Any]] = []
    loaded: list[str] = []
    missing: list[str] = []
    skipped: list[dict[str, str]] = []
    for path in paths:
        if not path.exists():
            missing.append(str(path))
            continue
        if path.suffix.lower() not in {".csv", ".json"}:
            skipped.append({"path": str(path), "reason": "not_processed_results_file"})
            continue
        if path.stat().st_size > MAX_RESULTS_FILE_BYTES:
            skipped.append({"path": str(path), "reason": "result_file_too_large"})
            continue
        try:
            loaded_rows = _read_result_file(path)
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            skipped.append({"path": str(path), "reason": f"read_failed:{type(exc).__name__}"})
            continue
        if not loaded_rows:
            skipped.append({"path": str(path), "reason": "no_rows"})
            continue
        rows.extend({**row, "source": str(path)} for row in loaded_rows)
        loaded.append(str(path))
    return rows, loaded, missing, skipped


def _read_result_file(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        for key in ("results", "ranking", "rows", "candidates"):
            value = data.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    return []


def _get_alias(row: dict[str, Any], canonical: str) -> Any:
    for key in _ALIASES[canonical]:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return ""


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    unique: list[Path] = []
    for path in paths:
        marker = str(path.resolve()) if path.exists() else str(path.absolute())
        if marker in seen:
            continue
        seen.add(marker)
        unique.append(path)
    return unique


def _symbol(value: object) -> str:
    symbol = str(value or "").upper().strip().replace(".B", "")
    if symbol == "USTECB":
        return "USTEC"
    return symbol


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


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
