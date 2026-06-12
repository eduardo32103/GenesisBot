from __future__ import annotations

import csv
import json
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import services.mt5.mt5_recent_first_research as recent_first
from services.mt5.mt5_backtester import _load_bars, _metrics, _number, _safety, _settings
from services.mt5.mt5_capital_preservation_optimizer import (
    _depends_on_single_trade,
    _loss_streak,
    _monte_carlo_stress,
)
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation
from services.mt5.mt5_multi_symbol_recent_first import (
    MultiSymbolConfig,
    _first_csv_price,
    _remove_best_metrics,
    _simulate_multi_symbol,
    _spread_stress_metrics,
)
from services.mt5.mt5_persistent_intelligence_store import (
    MT5PersistentIntelligenceStore,
    persist_research_lesson,
)
from services.mt5.mt5_recent_first_research import RecentFirstVariant, _fragile_dependency
from services.mt5.mt5_research_rejection_registry import research_rejection
from services.mt5.mt5_strategy_research_v2 import _features_by_index
from services.mt5.mt5_symbol_cost_model import build_symbol_cost_model


VALIDATION_VERSION = "2026-06-12.mt5_btc_h1_candidate_deep_validation.v1"

SYMBOL = "BTCUSD"
TIMEFRAME = "H1"
CANDIDATE_PROFILE = "btcusd_h1_tournament_edge_candidate_paper_review_v1"
CANDIDATE_PROFILE_BEFORE = "unknown_profile"
CANDIDATE_FAMILY = "tournament_edge"

DEFAULT_CSV_PATHS = (
    Path("data") / "backtests" / "multisymbol" / "BTCUSD_H1_20000.csv",
    Path("data") / "backtests" / "BTCUSD_H1_40000.csv",
    Path("data") / "backtests" / "BTCUSD_H1_60000.csv",
    Path("data") / "backtests" / "multisymbol" / "BTCUSD_H1_40000.csv",
    Path("data") / "backtests" / "multisymbol" / "BTCUSD_H1_60000.csv",
)

PROCESSED_SOURCE_PATHS = (
    Path("data") / "backtests" / "multisymbol" / "multi_symbol_recent_first_cost_calibrated_results.csv",
    Path("data") / "backtests" / "multisymbol" / "multi_symbol_recent_first_results.csv",
    Path("data") / "backtests" / "recent_first_research_results.csv",
    Path("data") / "backtests" / "recent_first_hardening_results.csv",
)

MIN_TOTAL_CLOSED = 50
MIN_RECENT_CLOSED = 20
MIN_RECENT_PF = 1.15
MIN_TOTAL_PF = 1.15
MIN_MONTE_CARLO_STRESSED_PF = 1.05
MIN_SPREAD_X2_PF = 0.95
MIN_REMOVE_BEST_5_PF = 1.0

OBSERVED_PAPER_REVIEW_METRICS = {
    "trades_forward": 8,
    "win_rate": 75.0,
    "profit_factor": 19.72,
    "recent_profit_factor": 19.72,
    "expectancy": 55.12,
}


@dataclass(frozen=True)
class BtcH1CandidateDeepConfig:
    target_name: str
    config: MultiSymbolConfig
    hardening_actions: tuple[str, ...]
    processed_source: dict[str, Any]


def run_btc_h1_candidate_deep_validation(body: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    body = body or {}
    processed = _load_processed_sources(_requested_processed_paths(body.get("processed_source_paths")))
    persistent_memory = _load_persistent_memory(body)
    source_identity = _source_identity(processed.get("rows") or [], persistent_memory)

    if isinstance(body.get("rows"), list):
        rows = [
            _finalize_row(dict(row), source_identity=_source_identity_from_body(body, source_identity))
            for row in body["rows"]
            if isinstance(row, dict)
        ]
        rows.sort(key=_ranking_key)
        result = _result(rows, [], [], [], started, processed, persistent_memory, _source_identity_from_body(body, source_identity))
        return _attach_research_lesson(result, body)

    timeout_seconds = max(0.5, min(float(_number(body.get("per_evaluation_timeout_seconds")) or 2.0), 12.0))
    monte_carlo_simulations = max(100, min(int(_number(body.get("monte_carlo_simulations")) or 200), 1000))
    max_bars = max(500, min(int(_number(body.get("max_bars")) or 20000), 60000))
    max_configs = max(1, min(int(_number(body.get("max_configs")) or 4), 12))
    max_runtime_seconds = max(5.0, min(float(_number(body.get("max_runtime_seconds")) or 60.0), 300.0))
    configs = _deep_configs(processed.get("rows") or [], max_configs=max_configs)
    requested_targets = _requested_list(body.get("targets"), [item.target_name for item in configs])
    configs = [item for item in configs if item.target_name in requested_targets]
    csv_paths = _requested_csv_paths(body.get("csv_paths"))

    errors: list[dict[str, Any]] = []
    warnings: list[str] = []
    rows: list[dict[str, Any]] = []
    used_csvs: list[str] = []
    missing_csvs: list[str] = []

    for csv_path in csv_paths:
        if time.monotonic() - started > max_runtime_seconds:
            errors.append({"error": "max_runtime_seconds_reached", "csv_path": str(csv_path)})
            break
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
            source_identity=source_identity,
            global_started=started,
            max_runtime_seconds=max_runtime_seconds,
        )
        rows.extend(evaluated_rows)
        warnings.extend(evaluated_warnings)
        errors.extend(evaluated_errors)

    rows.sort(key=_ranking_key)
    result = _result(rows, used_csvs, missing_csvs, errors, started, processed, persistent_memory, source_identity, warnings=warnings)
    return _attach_research_lesson(result, body)


def _evaluate_csv(
    csv_path: Path,
    configs: list[BtcH1CandidateDeepConfig],
    *,
    max_bars: int,
    timeout_seconds: float,
    monte_carlo_simulations: int,
    spread_points: float | None,
    source_identity: dict[str, Any],
    global_started: float,
    max_runtime_seconds: float,
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
        return [], warnings, [{"csv_path": str(csv_path), "error": "csv_bars_not_loaded"}]

    features_by_index = _features_by_index(bars)
    rows: list[dict[str, Any]] = []
    for deep_config in configs:
        if time.monotonic() - global_started > max_runtime_seconds:
            errors.append({"csv_path": str(csv_path), "target_name": deep_config.target_name, "error": "max_runtime_seconds_reached"})
            break
        rows.append(
            _evaluate_config(
                settings,
                bars,
                features_by_index,
                deep_config,
                csv_path=csv_path,
                timeout_seconds=timeout_seconds,
                monte_carlo_simulations=monte_carlo_simulations,
                cost_model=cost_model.as_dict(),
                source_identity=source_identity,
            )
        )
    return rows, warnings, errors


def _evaluate_config(
    settings: Any,
    bars: list[dict[str, Any]],
    features_by_index: dict[int, dict[str, Any]],
    deep_config: BtcH1CandidateDeepConfig,
    *,
    csv_path: Path,
    timeout_seconds: float,
    monte_carlo_simulations: int,
    cost_model: dict[str, Any],
    source_identity: dict[str, Any],
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
    split = _quarter_split_metrics(settings, bars, closed)
    monte_carlo = _monte_carlo_stress(
        closed,
        initial_balance=settings.initial_balance,
        max_drawdown_limit=5000.0,
        simulations=monte_carlo_simulations,
    )
    spread_x1_5 = _spread_stress_metrics(settings, bars, deep_config.config, features_by_index, timeout_seconds, 1.5)
    spread_x2 = _spread_stress_metrics(settings, bars, deep_config.config, features_by_index, timeout_seconds, 2.0)
    remove_best_1 = _remove_best_metrics(settings, closed, 1)
    remove_best_5 = _remove_best_metrics(settings, closed, 5)
    fragile = _fragile_dependency(total, split)
    single_trade_dependency = _depends_on_single_trade(total)
    window_results = _window_results(settings, bars, closed)
    chosen_window = _choose_validation_window(window_results)
    return _finalize_row(
        {
            "symbol": SYMBOL,
            "timeframe": TIMEFRAME,
            "source_family": source_identity.get("source_family"),
            "source_profile": source_identity.get("candidate_profile_name"),
            "source_profile_before": source_identity.get("source_profile_before"),
            "source_identity_status": source_identity.get("source_identity_status"),
            "source_identity_resolved": source_identity.get("source_identity_resolved"),
            "family": deep_config.config.family,
            "profile": deep_config.target_name,
            "target_name": deep_config.target_name,
            "hardening_actions": list(deep_config.hardening_actions),
            "processed_source": deep_config.processed_source,
            "csv_path": str(csv_path),
            "csv_label": csv_path.name,
            "bars_loaded": len(bars),
            "first_bar_time": str(bars[0].get("time") or "") if bars else "",
            "last_bar_time": str(bars[-1].get("time") or "") if bars else "",
            "recent_closed": chosen_window["closed"],
            "recent_win_rate": chosen_window["win_rate"],
            "recent_profit_factor": chosen_window["profit_factor"],
            "recent_pf": chosen_window["profit_factor"],
            "recent_expectancy": chosen_window["expectancy"],
            "validation_window": chosen_window["window"],
            "total_closed": total["closed"],
            "win_rate": total["win_rate"],
            "profit_factor": total["profit_factor"],
            "total_pf": total["profit_factor"],
            "expectancy": total["expectancy"],
            "max_drawdown": total["max_drawdown"],
            "consecutive_losses": _loss_streak(closed),
            "monte_carlo_stressed_pf": monte_carlo.get("profit_factor_stressed", 0.0),
            "monte_carlo_stressed_expectancy": monte_carlo.get("expectancy_stressed", 0.0),
            "monte_carlo_p95_drawdown": monte_carlo.get("max_drawdown_p95", 0.0),
            "monte_carlo_fail_reasons": list(monte_carlo.get("fail_reasons") or []),
            "spread_x1_5_pf": spread_x1_5["profit_factor"],
            "spread_x2_pf": spread_x2["profit_factor"],
            "remove_best_1_pf": remove_best_1["profit_factor"],
            "remove_best_5_pf": remove_best_5["profit_factor"],
            "fragile_regime_dependency": fragile,
            "single_trade_dependency": single_trade_dependency,
            "sample_stability_score": _sample_stability_score(window_results),
            "cost_model_confidence": str(cost_model.get("cost_model_confidence") or ""),
            "cost_model": cost_model,
            "window_metrics": window_results,
            "window_results": window_results,
            "blocked_reason_count": len(blocked),
            "generated_signal_count": signals.get("generated", 0),
            "actionable_signal_count": signals.get("actionable", 0),
            "risk_governor_blocks": state.get("risk_governor_blocks", 0),
            "source": "btc_h1_candidate_deep_validation",
        },
        source_identity=source_identity,
    )


def _finalize_row(row: dict[str, Any], *, source_identity: dict[str, Any]) -> dict[str, Any]:
    row["symbol"] = str(row.get("symbol") or SYMBOL).upper()
    row["timeframe"] = str(row.get("timeframe") or TIMEFRAME).upper()
    row["family"] = str(row.get("family") or CANDIDATE_FAMILY)
    row["profile"] = str(row.get("profile") or row.get("target_name") or CANDIDATE_PROFILE)
    row["target_name"] = str(row.get("target_name") or row["profile"])
    row["source_family"] = str(row.get("source_family") or source_identity.get("source_family") or CANDIDATE_FAMILY)
    row["source_profile"] = str(row.get("source_profile") or source_identity.get("candidate_profile_name") or CANDIDATE_PROFILE)
    row["source_profile_before"] = str(row.get("source_profile_before") or source_identity.get("source_profile_before") or CANDIDATE_PROFILE_BEFORE)
    row["source_identity_status"] = str(row.get("source_identity_status") or source_identity.get("source_identity_status") or "")
    row["source_identity_resolved"] = _flag(row.get("source_identity_resolved", source_identity.get("source_identity_resolved")))
    explicit_window_results = isinstance(row.get("window_results") or row.get("window_metrics"), list) and bool(row.get("window_results") or row.get("window_metrics"))
    row["window_metrics"] = _coerce_window_results(row)
    row["window_results"] = row["window_metrics"]
    chosen_window = _choose_validation_window(row["window_metrics"])
    row["validation_window"] = str(row.get("validation_window") or chosen_window["window"])
    if explicit_window_results:
        row["recent_closed"] = int(_number(chosen_window.get("closed")) or 0)
        row["recent_win_rate"] = float(_number(chosen_window.get("win_rate")) or 0.0)
        row["recent_profit_factor"] = float(_number(chosen_window.get("profit_factor")) or 0.0)
        row["recent_pf"] = row["recent_profit_factor"]
        row["recent_expectancy"] = float(_number(chosen_window.get("expectancy")) or 0.0)
    else:
        row["recent_closed"] = int(_number(row.get("recent_closed")) or chosen_window["closed"])
        row["recent_win_rate"] = float(_number(row.get("recent_win_rate")) or chosen_window["win_rate"])
        row["recent_profit_factor"] = float(_number(row.get("recent_profit_factor") or row.get("recent_pf")) or chosen_window["profit_factor"])
        row["recent_pf"] = row["recent_profit_factor"]
        row["recent_expectancy"] = float(_number(row.get("recent_expectancy")) or chosen_window["expectancy"])
    row["total_closed"] = int(_number(row.get("total_closed")) or row.get("closed") or 0)
    row["win_rate"] = float(_number(row.get("win_rate")) or 0.0)
    row["profit_factor"] = float(_number(row.get("profit_factor") or row.get("total_pf")) or 0.0)
    row["total_pf"] = row["profit_factor"]
    row["expectancy"] = float(_number(row.get("expectancy") or row.get("total_expectancy")) or 0.0)
    row["max_drawdown"] = float(_number(row.get("max_drawdown") or row.get("total_max_drawdown")) or 0.0)
    row["consecutive_losses"] = int(_number(row.get("consecutive_losses")) or 0)
    row["monte_carlo_stressed_pf"] = float(_number(row.get("monte_carlo_stressed_pf")) or 0.0)
    row["monte_carlo_stressed_expectancy"] = float(_number(row.get("monte_carlo_stressed_expectancy")) or 0.0)
    row["monte_carlo_p95_drawdown"] = float(_number(row.get("monte_carlo_p95_drawdown")) or 0.0)
    row["spread_x1_5_pf"] = float(_number(row.get("spread_x1_5_pf")) or 0.0)
    row["spread_x2_pf"] = float(_number(row.get("spread_x2_pf")) or 0.0)
    row["remove_best_1_pf"] = float(_number(row.get("remove_best_1_pf")) or 0.0)
    row["remove_best_5_pf"] = float(_number(row.get("remove_best_5_pf")) or 0.0)
    row["sample_stability_score"] = float(_number(row.get("sample_stability_score")) or _sample_stability_score(row["window_metrics"]))
    row["cost_model_confidence"] = str(row.get("cost_model_confidence") or "unknown")
    row["registry_degraded"] = bool(forward_profile_degradation(row["symbol"], row["timeframe"], row["profile"]))
    row["degraded_by_registry"] = row["registry_degraded"]
    research_rejected = research_rejection(row["symbol"], row["timeframe"], row["profile"], row["family"])
    row["rejected_by_research_registry"] = bool(research_rejected)
    row["research_rejection_registry"] = row["rejected_by_research_registry"]
    row["research_rejection_reason"] = str(research_rejected.get("rejection_reason") or "")
    row["sibling_risk"] = _flag(row.get("sibling_risk"))
    row["sibling_of_degraded_profile"] = str(row.get("sibling_of_degraded_profile") or "")
    row["sibling_risk_reason"] = str(row.get("sibling_risk_reason") or "")
    row["rejection_reasons"] = _gate_reasons(row)
    row["candidate_status"] = "paper_observation_review_ready" if not row["rejection_reasons"] else "gate_failed"
    row["paper_observation_ready"] = row["candidate_status"] == "paper_observation_review_ready"
    row["recommendation"] = "paper_observation_review" if row["paper_observation_ready"] else "continue_research"
    row["recommended_next_action"] = row["recommendation"]
    row["deep_validation_score"] = _score(row)
    return {
        **row,
        "requires_human_approval": True,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "paper_rotation_applied": False,
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
    if not _flag(row.get("source_identity_resolved")):
        reasons.append("source_identity_unresolved")
    if row.get("registry_degraded") or row.get("degraded_by_registry"):
        reasons.append("registry_degraded")
    if row.get("rejected_by_research_registry") or row.get("research_rejection_registry"):
        reasons.append("rejected_by_research_registry")
    if row.get("sibling_risk"):
        reasons.append("sibling_risk")
    _min_gate(reasons, row, "total_closed", MIN_TOTAL_CLOSED)
    _min_gate(reasons, row, "recent_closed", MIN_RECENT_CLOSED)
    _min_gate(reasons, row, "recent_profit_factor", MIN_RECENT_PF)
    _min_gate(reasons, row, "profit_factor", MIN_TOTAL_PF)
    if float(_number(row.get("expectancy")) or 0.0) <= 0.0:
        reasons.append("expectancy_not_positive")
    if float(_number(row.get("recent_expectancy")) or 0.0) <= 0.0:
        reasons.append("recent_expectancy_not_positive")
    _min_gate(reasons, row, "monte_carlo_stressed_pf", MIN_MONTE_CARLO_STRESSED_PF)
    if float(_number(row.get("monte_carlo_stressed_expectancy")) or 0.0) <= 0.0:
        reasons.append("monte_carlo_stressed_expectancy_not_positive")
    _min_gate(reasons, row, "spread_x2_pf", MIN_SPREAD_X2_PF)
    _min_gate(reasons, row, "remove_best_5_pf", MIN_REMOVE_BEST_5_PF)
    if _flag(row.get("single_trade_dependency")):
        reasons.append("single_trade_dependency")
    if _flag(row.get("fragile_regime_dependency")):
        reasons.append("fragile_regime_dependency")
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
    if len(bars) >= 1000:
        split = int(len(bars) * 0.70)
        windows.extend(
            [
                ("out_of_sample_train_70", 0, split),
                ("out_of_sample_test_30", split, len(bars)),
            ]
        )

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
                "win_rate": float(metrics.get("win_rate") or 0.0),
                "profit_factor": pf,
                "expectancy": expectancy,
                "max_drawdown": metrics.get("max_drawdown") or 0.0,
                "passes_recent_gate": closed >= MIN_RECENT_CLOSED and pf >= MIN_RECENT_PF and expectancy > 0.0,
            }
        )
    return results


def _coerce_window_results(row: dict[str, Any]) -> list[dict[str, Any]]:
    value = row.get("window_results") if isinstance(row.get("window_results"), list) else row.get("window_metrics")
    if isinstance(value, list) and value:
        windows: list[dict[str, Any]] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            window = dict(item)
            closed = int(_number(window.get("closed")) or 0)
            win_rate = float(_number(window.get("win_rate")) or 0.0)
            pf = float(_number(window.get("profit_factor")) or 0.0)
            expectancy = float(_number(window.get("expectancy")) or 0.0)
            window["closed"] = closed
            window["win_rate"] = win_rate
            window["profit_factor"] = pf
            window["expectancy"] = expectancy
            window["passes_recent_gate"] = closed >= MIN_RECENT_CLOSED and pf >= MIN_RECENT_PF and expectancy > 0.0
            windows.append(window)
        return windows
    return [
        {
            "window": str(row.get("validation_window") or "synthetic_recent"),
            "closed": int(_number(row.get("recent_closed")) or 0),
            "win_rate": float(_number(row.get("recent_win_rate")) or 0.0),
            "profit_factor": float(_number(row.get("recent_profit_factor") or row.get("recent_pf")) or 0.0),
            "expectancy": float(_number(row.get("recent_expectancy")) or 0.0),
            "passes_recent_gate": (
                int(_number(row.get("recent_closed")) or 0) >= MIN_RECENT_CLOSED
                and float(_number(row.get("recent_profit_factor") or row.get("recent_pf")) or 0.0) >= MIN_RECENT_PF
                and float(_number(row.get("recent_expectancy")) or 0.0) > 0.0
            ),
        }
    ]


def _choose_validation_window(windows: list[dict[str, Any]]) -> dict[str, Any]:
    validation = [
        window
        for window in windows
        if str(window.get("window") or "") not in {"total_sample", "out_of_sample_train_70"}
    ]
    passing = [window for window in validation if window.get("passes_recent_gate")]
    pool = passing or validation or windows
    return sorted(pool, key=_window_key)[0] if pool else {"window": "none", "closed": 0, "win_rate": 0.0, "profit_factor": 0.0, "expectancy": 0.0}


def _window_key(window: dict[str, Any]) -> tuple[int, int, float, float, str]:
    closed = int(_number(window.get("closed")) or 0)
    pf = float(_number(window.get("profit_factor")) or 0.0)
    expectancy = float(_number(window.get("expectancy")) or 0.0)
    sample_penalty = int(closed < MIN_RECENT_CLOSED)
    return (sample_penalty, -closed, -pf, -expectancy, str(window.get("window") or ""))


def _sample_stability_score(windows: list[dict[str, Any]]) -> float:
    validation = [
        window
        for window in windows
        if str(window.get("window") or "") not in {"total_sample", "out_of_sample_train_70"}
    ]
    if not validation:
        return 0.0
    passing = [window for window in validation if window.get("passes_recent_gate")]
    pfs = [float(_number(window.get("profit_factor")) or 0.0) for window in validation if int(_number(window.get("closed")) or 0) > 0]
    pf_penalty = (max(pfs) - min(pfs)) if len(pfs) >= 2 else 0.0
    score = (len(passing) / max(1, len(validation))) * 100.0
    score -= min(40.0, pf_penalty * 10.0)
    return round(max(0.0, score), 4)


def _quarter_split_metrics(settings: Any, bars: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    ranges = recent_first._quarter_ranges(len(bars))
    return {
        name: _metrics(
            [trade for trade in trades if start <= int(_number(trade.get("opened_index")) or 0) < end],
            initial_balance=settings.initial_balance,
        )
        for name, (start, end) in ranges.items()
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


def _deep_configs(processed_rows: list[dict[str, Any]], *, max_configs: int = 4) -> list[BtcH1CandidateDeepConfig]:
    configs: list[BtcH1CandidateDeepConfig] = []
    seen: set[tuple[str, str]] = set()
    for row in sorted(processed_rows, key=_processed_rank):
        family = str(row.get("family") or "").strip()
        if not family:
            continue
        hardening_mode = str(row.get("hardening_mode") or "baseline").strip() or "baseline"
        key = (family, hardening_mode)
        if key in seen:
            continue
        seen.add(key)
        configs.append(_cfg_from_processed(row, ordinal=len(configs) + 1))
        if len(configs) >= max_configs:
            return configs
    fallback = [
        ("recent_momentum_pullback", "baseline"),
        ("recent_liquidity_sweep", "baseline"),
        ("recent_failed_breakout_reversal", "mae_guard"),
        ("recent_range_reversion", "baseline"),
    ]
    for family, mode in fallback:
        key = (family, mode)
        if key in seen:
            continue
        seen.add(key)
        configs.append(_cfg(f"btcusd_h1_{family}_{mode}_deep_validation", family=family, mode=mode, actions=(mode,)))
        if len(configs) >= max_configs:
            break
    return configs


def _cfg_from_processed(row: dict[str, Any], *, ordinal: int) -> BtcH1CandidateDeepConfig:
    family = str(row.get("family") or "recent_liquidity_sweep").strip()
    mode = str(row.get("hardening_mode") or "baseline").strip() or "baseline"
    target = f"btcusd_h1_{family}_{mode}_source_{ordinal}_deep_validation"
    return _cfg(
        target,
        family=family,
        mode=mode,
        actions=(mode,),
        side=str(row.get("side") or "both"),
        session=str(row.get("session") or "all"),
        volatility=str(row.get("volatility_regime") or row.get("volatility") or "any"),
        trend=str(row.get("regime") or row.get("trend_regime") or "any"),
        rsi=str(row.get("rsi_regime") or "any"),
        score=float(_number(row.get("score_threshold") or row.get("score") or _variant_param(row.get("variant_id"), "score")) or 55.0),
        rr=float(_number(row.get("risk_reward") or _variant_param(row.get("variant_id"), "rr")) or 1.05),
        time_stop=int(_number(row.get("time_stop_bars") or _variant_param(row.get("variant_id"), "ts")) or 3),
        atr_stop=float(_number(row.get("atr_stop_multiplier") or _variant_param(row.get("variant_id"), "atr")) or 1.0),
        mae=float(_number(row.get("mae_exit_r") or _variant_param(row.get("variant_id"), "mae")) or 0.78),
        processed_source=row,
    )


def _cfg(
    target_name: str,
    *,
    family: str,
    mode: str,
    actions: tuple[str, ...],
    side: str = "both",
    session: str = "all",
    volatility: str = "any",
    trend: str = "any",
    rsi: str = "any",
    score: float = 55.0,
    rr: float = 1.05,
    time_stop: int = 3,
    atr_stop: float = 1.0,
    mae: float = 0.78,
    fast: float = 0.0,
    trail: float = 0.0,
    lock: float = 0.0,
    processed_source: dict[str, Any] | None = None,
) -> BtcH1CandidateDeepConfig:
    if mode == "fast_loss_cut" and fast <= 0:
        fast = 0.34
    if mode == "trailing_defensive" and trail <= 0:
        fast = fast or 0.45
        trail = 0.75
        lock = 0.05
    variant = RecentFirstVariant(
        family=family,
        timeframe=TIMEFRAME,
        side_mode=side or "both",
        session_name=session or "all",
        volatility_regime=volatility or "any",
        trend_regime=trend or "any",
        rsi_regime=rsi or "any",
        score_threshold=score,
        risk_reward=rr,
        time_stop_bars=max(1, int(time_stop or 3)),
        atr_stop_multiplier=max(0.1, float(atr_stop or 1.0)),
        mae_exit_r=max(0.1, float(mae or 0.78)),
        momentum_loss_exit=True,
    )
    return BtcH1CandidateDeepConfig(
        target_name=target_name,
        config=MultiSymbolConfig(
            base=variant,
            hardening_mode=mode,
            mae_exit_r=variant.mae_exit_r,
            fast_loss_cut_r=fast,
            trailing_activation_r=trail,
            trailing_lock_r=lock,
        ),
        hardening_actions=actions,
        processed_source=processed_source or {},
    )


def _load_processed_sources(paths: list[Path]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    loaded: list[str] = []
    missing: list[str] = []
    errors: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            missing.append(str(path))
            continue
        try:
            source_rows = _read_processed_path(path)
        except Exception as exc:  # pragma: no cover - defensive parser guard
            errors.append({"source": str(path), "error": type(exc).__name__})
            continue
        useful = [_normalize_processed_row(row, path) for row in source_rows]
        useful = [row for row in useful if _processed_row_useful(row)]
        rows.extend(useful)
        loaded.append(str(path))
    return {
        "loaded_sources": loaded,
        "missing_sources": missing,
        "errors": errors,
        "rows": rows,
        "useful_rows": len(rows),
        **_safety(),
    }


def _read_processed_path(path: Path) -> list[dict[str, Any]]:
    if path.suffix.casefold() == ".csv":
        with path.open("r", newline="", encoding="utf-8") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    if path.suffix.casefold() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            for key in ("results", "rows", "candidates"):
                values = payload.get(key)
                if isinstance(values, list):
                    return [row for row in values if isinstance(row, dict)]
    return []


def _normalize_processed_row(row: dict[str, Any], path: Path) -> dict[str, Any]:
    sample_label = str(row.get("sample_label") or "")
    symbol = str(row.get("symbol") or "").upper().strip()
    if not symbol and "BTCUSD" in sample_label.upper():
        symbol = "BTCUSD"
    timeframe = str(row.get("timeframe") or "").upper().strip()
    if not timeframe and "H1" in sample_label.upper():
        timeframe = "H1"
    return {
        **row,
        "symbol": symbol,
        "timeframe": timeframe,
        "family": str(row.get("family") or row.get("profile") or "").strip(),
        "profile": str(row.get("profile") or row.get("family") or "").strip(),
        "hardening_mode": str(row.get("hardening_mode") or "baseline").strip() or "baseline",
        "recent_closed": int(_number(row.get("recent_closed") or row.get("recent_trades") or 0) or 0),
        "total_closed": int(_number(row.get("total_closed") or row.get("closed") or row.get("trades") or 0) or 0),
        "recent_pf": float(_number(row.get("recent_pf") or row.get("recent_profit_factor") or 0) or 0.0),
        "total_pf": float(_number(row.get("total_pf") or row.get("profit_factor") or 0) or 0.0),
        "expectancy": float(_number(row.get("expectancy") or row.get("total_expectancy") or 0) or 0.0),
        "monte_carlo_stressed_pf": float(_number(row.get("monte_carlo_stressed_pf") or row.get("mc_pf") or 0) or 0.0),
        "remove_best_5_pf": float(_number(row.get("remove_best_5_pf") or 0) or 0.0),
        "spread_x2_pf": float(_number(row.get("spread_x2_pf") or 0) or 0.0),
        "source_path": str(path),
    }


def _processed_row_useful(row: dict[str, Any]) -> bool:
    if str(row.get("symbol") or "").upper() != SYMBOL:
        return False
    if str(row.get("timeframe") or "").upper() != TIMEFRAME:
        return False
    return bool(str(row.get("family") or "").strip()) and (int(row.get("recent_closed") or 0) > 0 or int(row.get("total_closed") or 0) > 0)


def _processed_rank(row: dict[str, Any]) -> tuple[float, int, float, float]:
    score = float(_number(row.get("multi_symbol_score") or row.get("research_score") or row.get("hardening_score")) or 0.0)
    total_closed = int(_number(row.get("total_closed")) or 0)
    recent_pf = float(_number(row.get("recent_pf")) or 0.0)
    total_pf = float(_number(row.get("total_pf")) or 0.0)
    return (-score, -total_closed, -recent_pf, -total_pf)


def _source_identity(processed_rows: list[dict[str, Any]], persistent_memory: dict[str, Any]) -> dict[str, Any]:
    exact_processed = _exact_processed_match(processed_rows)
    exact_persistent = _exact_persistent_match(persistent_memory)
    resolved = bool(exact_processed or exact_persistent)
    family = str((exact_processed or exact_persistent or {}).get("family") or CANDIDATE_FAMILY)
    profile = str((exact_processed or exact_persistent or {}).get("profile") or CANDIDATE_PROFILE)
    return {
        "candidate_profile_name": CANDIDATE_PROFILE,
        "source_profile_before": CANDIDATE_PROFILE_BEFORE,
        "source_family": family,
        "source_profile": profile,
        "source_identity_resolved": resolved,
        "source_identity_status": (
            "resolved_from_processed_or_persistent_memory"
            if resolved
            else "unresolved_unknown_profile_from_tournament_shadow_grouping"
        ),
        "entry_logic": "processed_or_persistent_match" if resolved else "unresolved_from_unknown_profile_no_local_exact_processed_row",
        "exit_logic": "processed_or_persistent_match" if resolved else "unresolved_from_unknown_profile_no_local_exact_processed_row",
        "why_unknown_profile": (
            "strategy_tournament groups closed shadow trades by strategy_profile/profile and falls back to unknown_profile when neither field is present"
        ),
        "observed_paper_review_metrics": dict(OBSERVED_PAPER_REVIEW_METRICS),
        "exact_processed_source_match_found": bool(exact_processed),
        "exact_persistent_shadow_match_found": bool(exact_persistent),
        "matching_processed_source": exact_processed or {},
        "matching_persistent_source": exact_persistent or {},
        **_safety(),
    }


def _source_identity_from_body(body: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    value = body.get("source_identity")
    if not isinstance(value, dict):
        return fallback
    return {**fallback, **value}


def _exact_processed_match(rows: list[dict[str, Any]]) -> dict[str, Any]:
    for row in rows:
        profile_blob = " ".join(str(row.get(key) or "") for key in ("profile", "family", "variant_id"))
        if CANDIDATE_PROFILE in profile_blob:
            return row
        if int(_number(row.get("total_closed")) or 0) == OBSERVED_PAPER_REVIEW_METRICS["trades_forward"] and abs(float(_number(row.get("total_pf")) or 0.0) - OBSERVED_PAPER_REVIEW_METRICS["profit_factor"]) <= 0.01:
            return row
    return {}


def _exact_persistent_match(memory: dict[str, Any]) -> dict[str, Any]:
    events = memory.get("recent_events") if isinstance(memory.get("recent_events"), dict) else memory
    if not isinstance(events, dict):
        return {}
    for key in ("recent_shadow_events", "recent_decisions", "recent_research_lessons"):
        values = events.get(key) if isinstance(events.get(key), list) else []
        for row in values:
            if not isinstance(row, dict):
                continue
            if str(row.get("symbol") or "").upper() != SYMBOL or str(row.get("timeframe") or "").upper() != TIMEFRAME:
                continue
            profile = str(row.get("profile") or row.get("strategy_profile") or row.get("candidate_profile_name") or "")
            if profile in {CANDIDATE_PROFILE, CANDIDATE_PROFILE_BEFORE, "unknown_profile"}:
                return {"family": row.get("family") or CANDIDATE_FAMILY, "profile": profile or CANDIDATE_PROFILE, "persistent_row": row}
    return {}


def _load_persistent_memory(body: dict[str, Any]) -> dict[str, Any]:
    if isinstance(body.get("persistent_memory"), dict):
        return {"source": "injected", "recent_events": body["persistent_memory"], **_safety()}
    if body.get("load_persistent_memory") is False:
        return {"source": "disabled", "recent_events": {}, **_safety()}
    try:
        store = body.get("store") if body.get("store") is not None else MT5PersistentIntelligenceStore()
        if hasattr(store, "recent_events"):
            recent = store.recent_events(limit=20)
        else:
            recent = {}
        return {"source": "persistent_intelligence", "recent_events": recent, "db_degraded": bool((recent or {}).get("db_degraded")), **_safety()}
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {"source": "persistent_intelligence", "recent_events": {}, "db_degraded": True, "reason": type(exc).__name__, **_safety()}


def _attach_research_lesson(result: dict[str, Any], body: dict[str, Any]) -> dict[str, Any]:
    lesson = _research_lesson(result)
    result["research_lesson"] = lesson
    if result.get("recommendation") != "continue_research":
        result["research_lesson_persisted"] = False
        result["research_lesson_write"] = {"attempted": False, "reason": "candidate_not_rejected"}
        return result
    if body.get("persist_research_lesson") is False:
        result["research_lesson_persisted"] = False
        result["research_lesson_write"] = {"attempted": False, "reason": "persist_research_lesson_disabled"}
        return result
    try:
        store = body.get("store") if body.get("store") is not None else MT5PersistentIntelligenceStore()
        if hasattr(store, "healthcheck"):
            health = store.healthcheck(write_test_event=False)
            if not (health.get("db_available") and health.get("tables_ready") and not health.get("db_degraded")):
                result["research_lesson_persisted"] = False
                result["research_lesson_write"] = {"attempted": False, "reason": "persistent_intelligence_not_ready_no_write", "health": _compact_health(health), **_safety()}
                return result
        write = persist_research_lesson(lesson, critical=False, store=store)
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        write = {"ok": False, "db_degraded": True, "reason": type(exc).__name__, **_safety()}
    result["research_lesson_persisted"] = bool(write.get("ok"))
    result["research_lesson_write"] = {"attempted": True, **dict(write or {})}
    return result


def _research_lesson(result: dict[str, Any]) -> dict[str, Any]:
    best = result.get("best_variant") if isinstance(result.get("best_variant"), dict) else {}
    rejections = list(best.get("rejection_reasons") or result.get("aggregate_rejection_reasons") or [])
    if not rejections:
        rejections = ["no_rejection"]
    summary = (
        f"BTCUSD H1 paper candidate deep validation: source_status={result.get('source_identity_status')}; "
        f"best={best.get('target_name') or 'none'}; recommendation={result.get('recommendation')}; "
        f"rejections={','.join(rejections[:6])}."
    )
    return {
        "family": "btc_h1_tournament_edge_candidate",
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "lesson_type": "paper_candidate_deep_validation",
        "failure_pattern": rejections[0],
        "summary": summary[:500],
        "avoid_next": [
            "do_not_activate_btc_h1_candidate_without_source_identity_and_20_recent_closed_trades",
            "do_not_promote_unknown_profile_tournament_edge_to_runtime",
        ],
        "recommended_next_research_phase": "continue_research" if result.get("recommendation") == "continue_research" else "human_paper_observation_review",
        **_safety(),
    }


def _result(
    rows: list[dict[str, Any]],
    used_csvs: list[str],
    missing_csvs: list[str],
    errors: list[dict[str, Any]],
    started: float,
    processed: dict[str, Any],
    persistent_memory: dict[str, Any],
    source_identity: dict[str, Any],
    *,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    candidates = [row for row in rows if row.get("candidate_status") == "paper_observation_review_ready"]
    best = rows[0] if rows else None
    recommendation = "paper_observation_review" if candidates else "continue_research"
    aggregate_rejections = sorted({reason for row in rows for reason in (row.get("rejection_reasons") or [])})
    return {
        "ok": True,
        "status": "btc_h1_candidate_deep_validation_ready",
        "validation_version": VALIDATION_VERSION,
        "mode": "offline_paper_only",
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "candidate_profile_name": CANDIDATE_PROFILE,
        "candidate_profile_before": CANDIDATE_PROFILE_BEFORE,
        "source_family": source_identity.get("source_family"),
        "source_profile": source_identity.get("source_profile"),
        "source_identity_status": source_identity.get("source_identity_status"),
        "source_identity_resolved": bool(source_identity.get("source_identity_resolved")),
        "source_identity": source_identity,
        "observed_paper_review_metrics": dict(OBSERVED_PAPER_REVIEW_METRICS),
        "csv_used": used_csvs,
        "missing_csvs": missing_csvs,
        "processed_sources_loaded": processed.get("loaded_sources") or [],
        "processed_sources_missing": processed.get("missing_sources") or [],
        "useful_processed_rows": processed.get("useful_rows") or 0,
        "persistent_memory_source": persistent_memory.get("source"),
        "persistent_memory_db_degraded": bool(persistent_memory.get("db_degraded")),
        "windows_evaluated": [
            "total_sample",
            "recent_25_pct",
            "recent_15_pct",
            "recent_10_pct",
            "last_30_days",
            "last_60_days",
            "last_90_days",
            "out_of_sample_train_70",
            "out_of_sample_test_30",
        ],
        "variants_evaluated": len(rows),
        "results": rows,
        "best_variant": best,
        "candidates": candidates,
        "recommended_candidate": candidates[0] if candidates else None,
        "recommendation": recommendation,
        "paper_observation_ready": bool(candidates),
        "requires_human_approval": True,
        "aggregate_rejection_reasons": aggregate_rejections,
        "errors": errors,
        "warnings": warnings or [],
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "paper_rotation_applied": False,
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
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }


def _score(row: dict[str, Any]) -> float:
    score = 0.0
    score += min(int(_number(row.get("recent_closed")) or 0), 100) * 4.0
    score += min(int(_number(row.get("total_closed")) or 0), 250) * 0.8
    score += max(0.0, float(_number(row.get("recent_profit_factor")) or 0.0) - 1.0) * 85.0
    score += max(0.0, float(_number(row.get("profit_factor")) or 0.0) - 1.0) * 80.0
    score += max(0.0, float(_number(row.get("expectancy")) or 0.0)) * 180.0
    score += max(0.0, float(_number(row.get("recent_expectancy")) or 0.0)) * 180.0
    score += max(0.0, float(_number(row.get("monte_carlo_stressed_pf")) or 0.0) - 1.0) * 220.0
    score += max(0.0, float(_number(row.get("monte_carlo_stressed_expectancy")) or 0.0)) * 8.0
    score += max(0.0, float(_number(row.get("spread_x2_pf")) or 0.0) - 0.95) * 70.0
    score += max(0.0, float(_number(row.get("remove_best_5_pf")) or 0.0) - 1.0) * 130.0
    score += float(_number(row.get("sample_stability_score")) or 0.0) * 1.2
    score -= len(row.get("rejection_reasons") or []) * 35.0
    if _flag(row.get("fragile_regime_dependency")):
        score -= 160.0
    if _flag(row.get("single_trade_dependency")):
        score -= 160.0
    if not _flag(row.get("source_identity_resolved")):
        score -= 220.0
    return round(score, 4)


def _ranking_key(row: dict[str, Any]) -> tuple[int, int, int, int, float, str, str]:
    status_rank = 0 if row.get("candidate_status") == "paper_observation_review_ready" else 1
    reasons = list(row.get("rejection_reasons") or [])
    source_penalty = int("source_identity_unresolved" in reasons)
    sample_penalty = int(any(reason in reasons for reason in {"recent_closed_below_20", "total_closed_below_50"}))
    return (
        status_rank,
        source_penalty,
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


def _requested_processed_paths(value: Any) -> list[Path]:
    if value is None or value == "":
        return _dedupe_paths([Path(path) for path in PROCESSED_SOURCE_PATHS])
    if isinstance(value, str):
        return _dedupe_paths([Path(item.strip()) for item in value.split(",") if item.strip()])
    if isinstance(value, (list, tuple, set)):
        return _dedupe_paths([Path(str(item)) for item in value if str(item).strip()])
    return _dedupe_paths([Path(path) for path in PROCESSED_SOURCE_PATHS])


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


def _variant_param(value: Any, key: str) -> str:
    text = str(value or "")
    marker = f"{key}="
    if marker not in text:
        return ""
    return text.split(marker, 1)[1].split("|", 1)[0]


def _threshold_label(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return str(value).replace(".", "_").rstrip("0").rstrip("_")


def _flag(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().casefold() in {"1", "true", "yes", "y"}


def _compact_health(health: dict[str, Any]) -> dict[str, Any]:
    return {
        "db_available": bool(health.get("db_available")),
        "tables_ready": bool(health.get("tables_ready")),
        "db_degraded": bool(health.get("db_degraded")),
        "queue_depth": health.get("queue_depth", 0),
        "recommendation": health.get("recommendation", ""),
        **_safety(),
    }
