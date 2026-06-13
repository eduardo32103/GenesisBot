from __future__ import annotations

import random
import statistics
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation
from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore
from services.mt5.mt5_research_rejection_registry import research_rejection
from services.mt5.mt5_symbol_cost_model import build_symbol_cost_model
from services.mt5.mt5_volatility_compression_breakout_feature_scan import (
    _breakout_side,
    _compressed,
    _indicator_rows,
    _parse_time,
    _read_bars,
    _regime,
    _session_filter,
    _spread_cost,
    _trend_filter,
)


VALIDATION_VERSION = "2026-06-12.mt5_xau_m15_volatility_compression_deep_validation.v1"

SYMBOL = "XAUUSD"
TIMEFRAME = "M15"
FAMILY = "volatility_compression_breakout"
CANDIDATE_PROFILE = f"{FAMILY}|mode=nr7_trailing_defensive"

_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CSV_PATHS = (
    _REPO_ROOT / "data" / "backtests" / "multisymbol" / "XAUUSD.b_M15_20000.csv",
    _REPO_ROOT / "data" / "backtests" / "multisymbol" / "XAUUSD_M15_20000.csv",
    _REPO_ROOT / "data" / "backtests" / "XAUUSD_M15_40000.csv",
    _REPO_ROOT / "data" / "backtests" / "XAUUSD_M15_60000.csv",
)

MIN_TOTAL_CLOSED = 50
MIN_RECENT_CLOSED = 20
MIN_TOTAL_PF = 1.15
MIN_RECENT_PF = 1.15
MIN_MONTE_CARLO_STRESSED_PF = 1.05
MIN_SPREAD_X2_PF = 0.95
MIN_REMOVE_BEST_5_PF = 1.0

_VARIANTS: tuple[dict[str, Any], ...] = (
    {"mode": "nr7_trailing_defensive", "compression": "nr7", "trailing_defensive": True},
    {"mode": "nr7_fast_loss_cut", "compression": "nr7", "fast_loss_cut": True},
    {"mode": "nr7_no_trailing", "compression": "nr7"},
    {"mode": "nr7_fixed_R", "compression": "nr7", "fixed_r": True},
    {"mode": "nr10_trailing_defensive", "compression": "nr10", "trailing_defensive": True},
    {"mode": "atr_compression_equivalent", "compression": "atr", "trailing_defensive": True},
    {"mode": "session_filter", "compression": "nr7", "trailing_defensive": True, "session_filter": True},
    {
        "mode": "spread_cost_stress",
        "compression": "nr7",
        "trailing_defensive": True,
        "base_spread_multiplier": 1.5,
    },
)


def run_xau_m15_volatility_compression_deep_validation(
    *,
    csv_paths: list[str | Path] | str | Path | None = None,
    max_bars: int = 20000,
    monte_carlo_simulations: int = 250,
    variant_results: list[dict[str, Any]] | None = None,
    load_persistent: bool = False,
    store: MT5PersistentIntelligenceStore | Any | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    requested_paths = _requested_csv_paths(csv_paths)
    max_rows = max(500, min(int(_number(max_bars) or 20000), 60000))
    simulations = max(50, min(int(_number(monte_carlo_simulations) or 250), 1000))
    persistent_state = _load_persistent_state(load_persistent=load_persistent, store=store)

    if variant_results is not None:
        rows = [_finalize_variant_row(dict(row), simulations=simulations) for row in variant_results]
        rows.sort(key=_ranking_key)
        return _result(
            rows,
            source_csvs_used=[],
            missing_csvs=[],
            errors=[],
            started=started,
            persistent_state=persistent_state,
            max_bars=max_rows,
            monte_carlo_simulations=simulations,
        )

    rows: list[dict[str, Any]] = []
    source_csvs_used: list[str] = []
    missing_csvs: list[str] = []
    errors: list[dict[str, Any]] = []

    for csv_path in requested_paths:
        if not csv_path.exists():
            missing_csvs.append(str(csv_path))
            continue
        try:
            bars = _read_bars(csv_path, max_rows_per_file=max_rows)
            indicator_rows = _indicator_rows(bars)
        except Exception as exc:  # pragma: no cover - defensive guard for corrupt local files
            errors.append({"csv_path": str(csv_path), "error": type(exc).__name__})
            continue
        source_csvs_used.append(str(csv_path))
        rows.extend(_evaluate_source(csv_path, indicator_rows, simulations=simulations))

    rows.sort(key=_ranking_key)
    return _result(
        rows,
        source_csvs_used=source_csvs_used,
        missing_csvs=missing_csvs,
        errors=errors,
        started=started,
        persistent_state=persistent_state,
        max_bars=max_rows,
        monte_carlo_simulations=simulations,
    )


def _evaluate_source(csv_path: Path, rows: list[dict[str, Any]], *, simulations: int) -> list[dict[str, Any]]:
    if len(rows) < 240:
        return [
            _finalize_variant_row(
                {
                    "mode": variant["mode"],
                    "profile": f"{FAMILY}|mode={variant['mode']}",
                    "csv_path": str(csv_path),
                    "source_identity_resolved": True,
                    "data_quality": "insufficient_bars",
                    "bars_loaded": len(rows),
                    "total_closed": 0,
                    "recent_closed": 0,
                },
                simulations=simulations,
            )
            for variant in _VARIANTS
        ]

    cost_model = build_symbol_cost_model(
        SYMBOL,
        resolved_symbol="XAUUSD.b" if ".b" in csv_path.name.casefold() else SYMBOL,
        first_price=float(rows[0].get("close") or 0.0),
    )
    evaluated: list[dict[str, Any]] = []
    for variant in _VARIANTS:
        trades = _simulate_trades(rows, variant)
        evaluated.append(
            _finalize_variant_row(
                {
                    "mode": variant["mode"],
                    "profile": f"{FAMILY}|mode={variant['mode']}",
                    "csv_path": str(csv_path),
                    "csv_label": csv_path.name,
                    "bars_loaded": len(rows),
                    "first_bar_time": str(rows[0].get("time") or ""),
                    "last_bar_time": str(rows[-1].get("time") or ""),
                    "source_identity_resolved": True,
                    "data_quality": "ok",
                    "cost_model_confidence": cost_model.cost_model_confidence,
                    "cost_model": cost_model.as_dict(),
                    **_metrics_from_trades(rows, trades, simulations=simulations),
                },
                simulations=simulations,
            )
        )
    return evaluated


def _simulate_trades(rows: list[dict[str, Any]], variant: dict[str, Any]) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    spread_cost = _spread_cost(SYMBOL, rows)
    base_multiplier = float(_number(variant.get("base_spread_multiplier")) or 1.0)
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
        trades.append(
            _trade_return(
                rows,
                index,
                side,
                spread_cost=spread_cost,
                variant=variant,
                base_spread_multiplier=base_multiplier,
            )
        )
    return trades


def _trade_return(
    rows: list[dict[str, Any]],
    index: int,
    side: str,
    *,
    spread_cost: float,
    variant: dict[str, Any],
    base_spread_multiplier: float,
) -> dict[str, Any]:
    entry = float(rows[index]["close"])
    direction = 1.0 if side == "long" else -1.0
    atr = max(float(rows[index].get("atr") or 0.0), abs(entry) * 0.0001)
    stop_distance = atr * 1.2
    risk_return = stop_distance / max(abs(entry), 0.00000001)
    stop_return = -risk_return
    future = rows[index + 1 : index + 9]
    raw_return = direction * (float(rows[index + 8]["close"]) - entry) / max(abs(entry), 0.00000001)
    favorable = 0.0
    if side == "long":
        stop_price = entry - stop_distance
        target_price = entry + stop_distance * 1.5
        for offset, row in enumerate(future, start=1):
            low = float(row["low"])
            high = float(row["high"])
            close = float(row["close"])
            if low <= stop_price:
                raw_return = (stop_price - entry) / max(abs(entry), 0.00000001)
                break
            if variant.get("fixed_r") and high >= target_price:
                raw_return = risk_return * 1.5
                break
            favorable = max(favorable, (high - entry) / max(abs(entry), 0.00000001))
            if variant.get("fast_loss_cut") and offset == 2:
                return_2 = (close - entry) / max(abs(entry), 0.00000001)
                if return_2 < 0.0:
                    raw_return = max(return_2, stop_return)
                    break
            if variant.get("trailing_defensive") and favorable > 0.0:
                stop_price = max(stop_price, entry + favorable * 0.5 * entry)
    else:
        stop_price = entry + stop_distance
        target_price = entry - stop_distance * 1.5
        for offset, row in enumerate(future, start=1):
            low = float(row["low"])
            high = float(row["high"])
            close = float(row["close"])
            if high >= stop_price:
                raw_return = (entry - stop_price) / max(abs(entry), 0.00000001)
                break
            if variant.get("fixed_r") and low <= target_price:
                raw_return = risk_return * 1.5
                break
            favorable = max(favorable, (entry - low) / max(abs(entry), 0.00000001))
            if variant.get("fast_loss_cut") and offset == 2:
                return_2 = (entry - close) / max(abs(entry), 0.00000001)
                if return_2 < 0.0:
                    raw_return = max(return_2, stop_return)
                    break
            if variant.get("trailing_defensive") and favorable > 0.0:
                stop_price = min(stop_price, entry - favorable * 0.5 * entry)
    pnl = raw_return - spread_cost * base_spread_multiplier
    return {
        "index": index,
        "time": rows[index].get("time"),
        "side": side,
        "pnl": pnl,
        "pnl_spread_x1_5": raw_return - spread_cost * 1.5,
        "pnl_spread_x2": raw_return - spread_cost * 2.0,
        "regime": _regime(rows[index]),
    }


def _metrics_from_trades(
    rows: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    *,
    simulations: int,
) -> dict[str, Any]:
    pnl = [float(trade.get("pnl") or 0.0) for trade in trades]
    spread_x1_5 = [float(trade.get("pnl_spread_x1_5") or 0.0) for trade in trades]
    spread_x2 = [float(trade.get("pnl_spread_x2") or 0.0) for trade in trades]
    windows = _window_metrics(rows, trades)
    recent = _window_by_name(windows, "recent_25_pct")
    monte_carlo = _monte_carlo_stress(pnl, simulations=simulations)
    quarters = _quarter_pnls(trades, len(rows))
    return {
        "total_closed": len(trades),
        "recent_closed": int(recent.get("closed") or 0),
        "win_rate": _win_rate(pnl),
        "recent_win_rate": float(recent.get("win_rate") or 0.0),
        "profit_factor": _profit_factor(pnl),
        "total_pf": _profit_factor(pnl),
        "recent_pf": float(recent.get("profit_factor") or 0.0),
        "recent_profit_factor": float(recent.get("profit_factor") or 0.0),
        "expectancy": _average(pnl),
        "recent_expectancy": float(recent.get("expectancy") or 0.0),
        "max_drawdown": _max_drawdown(pnl),
        "consecutive_losses": _consecutive_losses(pnl),
        "monte_carlo_stressed_pf": monte_carlo["monte_carlo_stressed_pf"],
        "monte_carlo_stressed_expectancy": monte_carlo["monte_carlo_stressed_expectancy"],
        "monte_carlo_p95_drawdown": monte_carlo["monte_carlo_p95_drawdown"],
        "spread_x1_5_pf": _profit_factor(spread_x1_5),
        "spread_x2_pf": _profit_factor(spread_x2),
        "remove_best_1_pf": _profit_factor(_remove_best(pnl, 1)),
        "remove_best_5_pf": _profit_factor(_remove_best(pnl, 5)),
        "single_trade_dependency": _single_trade_dependency(pnl),
        "fragile_regime_dependency": _fragile_regime_dependency(pnl, quarters, windows),
        "sample_stability_score": _sample_stability_score(windows),
        "metrics_by_window": windows,
        "window_metrics": windows,
    }


def _finalize_variant_row(row: dict[str, Any], *, simulations: int) -> dict[str, Any]:
    mode = str(row.get("mode") or _mode_from_profile(row.get("profile")) or "nr7_trailing_defensive")
    profile = str(row.get("profile") or f"{FAMILY}|mode={mode}")
    row["symbol"] = _symbol(row.get("symbol") or SYMBOL)
    row["timeframe"] = _timeframe(row.get("timeframe") or TIMEFRAME)
    row["family"] = str(row.get("family") or FAMILY)
    row["mode"] = mode
    row["profile"] = profile
    row["candidate_profile"] = profile
    row["source_identity_resolved"] = _bool(row.get("source_identity_resolved", "unknown_profile" not in profile.casefold()))
    row["source_identity_status"] = "resolved_local_ohlc" if row["source_identity_resolved"] else "unresolved"
    row["data_quality"] = str(row.get("data_quality") or "ok")
    row["total_closed"] = int(_number(row.get("total_closed") or row.get("closed")))
    row["recent_closed"] = int(_number(row.get("recent_closed")))
    row["win_rate"] = float(_number(row.get("win_rate")))
    row["recent_win_rate"] = float(_number(row.get("recent_win_rate")))
    row["profit_factor"] = float(_number(row.get("profit_factor") or row.get("total_pf")))
    row["total_pf"] = row["profit_factor"]
    row["recent_pf"] = float(_number(row.get("recent_pf") or row.get("recent_profit_factor")))
    row["recent_profit_factor"] = row["recent_pf"]
    row["expectancy"] = float(_number(row.get("expectancy")))
    row["recent_expectancy"] = float(_number(row.get("recent_expectancy")))
    row["max_drawdown"] = float(_number(row.get("max_drawdown")))
    row["consecutive_losses"] = int(_number(row.get("consecutive_losses")))
    row["monte_carlo_stressed_pf"] = float(_number(row.get("monte_carlo_stressed_pf")))
    row["monte_carlo_stressed_expectancy"] = float(_number(row.get("monte_carlo_stressed_expectancy")))
    row["monte_carlo_p95_drawdown"] = float(_number(row.get("monte_carlo_p95_drawdown")))
    row["spread_x1_5_pf"] = float(_number(row.get("spread_x1_5_pf")))
    row["spread_x2_pf"] = float(_number(row.get("spread_x2_pf")))
    row["remove_best_1_pf"] = float(_number(row.get("remove_best_1_pf")))
    row["remove_best_5_pf"] = float(_number(row.get("remove_best_5_pf")))
    row["single_trade_dependency"] = _bool(row.get("single_trade_dependency"))
    row["fragile_regime_dependency"] = _bool(row.get("fragile_regime_dependency"))
    row["sample_stability_score"] = float(_number(row.get("sample_stability_score")))
    row["cost_model_confidence"] = str(row.get("cost_model_confidence") or "unknown")
    row["metrics_by_window"] = _coerce_windows(row)
    row["window_metrics"] = row["metrics_by_window"]
    if row["metrics_by_window"] and not row["sample_stability_score"]:
        row["sample_stability_score"] = _sample_stability_score(row["metrics_by_window"])
    row["registry_hit"] = bool(forward_profile_degradation(row["symbol"], row["timeframe"], profile))
    row["degraded_by_registry"] = row["registry_hit"]
    rejected = research_rejection(row["symbol"], row["timeframe"], profile, row["family"], FAMILY)
    row["research_rejection_registry"] = bool(rejected)
    row["rejected_by_research_registry"] = bool(rejected)
    row["research_rejection_reason"] = str(rejected.get("rejection_reason") or "")
    row["sibling_risk"] = _sibling_risk(row["symbol"], row["timeframe"], profile)
    row["sibling_risk_reason"] = "similar_to_degraded_forward_profile" if row["sibling_risk"] else ""
    row["gates"] = _gates(row)
    row["rejection_reasons"] = [name for name, gate in row["gates"].items() if not gate["passed"]]
    row["candidate_status"] = "paper_observation_review_ready" if not row["rejection_reasons"] else "gate_failed"
    row["paper_observation_ready"] = row["candidate_status"] == "paper_observation_review_ready"
    row["recommendation"] = "paper_observation_review" if row["paper_observation_ready"] else "continue_research"
    row["deep_validation_score"] = _score(row)
    return {
        **row,
        "validation_version": VALIDATION_VERSION,
        "monte_carlo_simulations": simulations,
        "requires_human_approval": True,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "paper_rotation_applied": False,
        "applies_to_real_trading": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "runtime_mutated": False,
        "raw_ohlc_persisted": False,
        "raw_trades_persisted": False,
        **_safety(),
    }


def _result(
    rows: list[dict[str, Any]],
    *,
    source_csvs_used: list[str],
    missing_csvs: list[str],
    errors: list[dict[str, Any]],
    started: float,
    persistent_state: dict[str, Any],
    max_bars: int,
    monte_carlo_simulations: int,
) -> dict[str, Any]:
    best = rows[0] if rows else None
    ready = [row for row in rows if row.get("paper_observation_ready")]
    recommended = ready[0] if ready else None
    selected = recommended or best
    recommendation = "paper_observation_review" if recommended else "continue_research"
    payload = _compact_payload(selected, recommendation=recommendation) if selected else _empty_payload(recommendation)
    return {
        "ok": True,
        "status": "xau_m15_volatility_compression_deep_validation_ready",
        "validation_version": VALIDATION_VERSION,
        "candidate_profile": CANDIDATE_PROFILE,
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "family": FAMILY,
        "max_bars": max_bars,
        "monte_carlo_simulations": monte_carlo_simulations,
        "source_csvs_used": _dedupe(source_csvs_used),
        "missing_csvs": _dedupe(missing_csvs),
        "variants_tested": [row.get("mode") for row in rows],
        "variant_results": [_public_variant_row(row) for row in rows],
        "best_variant": _public_variant_row(selected) if selected else None,
        "metrics_by_window": selected.get("metrics_by_window") if selected else [],
        "monte_carlo_stressed_pf": selected.get("monte_carlo_stressed_pf") if selected else 0.0,
        "spread_x2_pf": selected.get("spread_x2_pf") if selected else 0.0,
        "remove_best_5_pf": selected.get("remove_best_5_pf") if selected else 0.0,
        "gates": selected.get("gates") if selected else {},
        "rejection_reasons": selected.get("rejection_reasons") if selected else ["no_variant_results"],
        "recommendation": recommendation,
        "recommended_candidate": _candidate_label(recommended),
        "paper_observation_ready": bool(recommended),
        "requires_human_approval": True,
        "compact_persistence_payload": payload,
        "compact_persistence_ready": True,
        "csv_payload_included": False,
        "raw_trades_included": False,
        "raw_ohlc_persisted": False,
        "raw_trades_persisted": False,
        "persistent_state": persistent_state,
        "errors": errors,
        "duration_ms": int((time.monotonic() - started) * 1000),
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "paper_rotation_applied": False,
        "applies_to_real_trading": False,
        "runtime_mutated": False,
        **_safety(),
    }


def _public_variant_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    keys = (
        "symbol",
        "timeframe",
        "family",
        "profile",
        "mode",
        "csv_label",
        "bars_loaded",
        "total_closed",
        "recent_closed",
        "win_rate",
        "recent_win_rate",
        "total_pf",
        "recent_pf",
        "expectancy",
        "recent_expectancy",
        "max_drawdown",
        "consecutive_losses",
        "monte_carlo_stressed_pf",
        "monte_carlo_stressed_expectancy",
        "monte_carlo_p95_drawdown",
        "spread_x1_5_pf",
        "spread_x2_pf",
        "remove_best_1_pf",
        "remove_best_5_pf",
        "single_trade_dependency",
        "fragile_regime_dependency",
        "sample_stability_score",
        "cost_model_confidence",
        "source_identity_resolved",
        "registry_hit",
        "degraded_by_registry",
        "research_rejection_registry",
        "sibling_risk",
        "rejection_reasons",
        "candidate_status",
        "recommendation",
        "paper_observation_ready",
    )
    return {key: row.get(key) for key in keys}


def _compact_payload(row: dict[str, Any] | None, *, recommendation: str) -> dict[str, Any]:
    if not row:
        return _empty_payload(recommendation)
    payload_type = "paper_observation_candidate_review" if row.get("paper_observation_ready") else "research_lesson"
    lesson_type = "deep_validation_passed" if row.get("paper_observation_ready") else "deep_validation_failed"
    return {
        "payload_type": payload_type,
        "lesson_type": lesson_type,
        "validation_version": VALIDATION_VERSION,
        "symbol": row.get("symbol"),
        "timeframe": row.get("timeframe"),
        "family": row.get("family"),
        "candidate_profile": row.get("profile"),
        "best_variant": row.get("mode"),
        "recommendation": recommendation,
        "paper_observation_ready": bool(row.get("paper_observation_ready")),
        "requires_human_approval": True,
        "summary": {
            "total_closed": row.get("total_closed"),
            "recent_closed": row.get("recent_closed"),
            "total_pf": row.get("total_pf"),
            "recent_pf": row.get("recent_pf"),
            "expectancy": row.get("expectancy"),
            "recent_expectancy": row.get("recent_expectancy"),
            "monte_carlo_stressed_pf": row.get("monte_carlo_stressed_pf"),
            "spread_x2_pf": row.get("spread_x2_pf"),
            "remove_best_5_pf": row.get("remove_best_5_pf"),
            "sample_stability_score": row.get("sample_stability_score"),
        },
        "gates": row.get("gates") or {},
        "rejection_reasons": row.get("rejection_reasons") or [],
        "avoid_next": [] if row.get("paper_observation_ready") else [row.get("profile")],
        "csv_payload_included": False,
        "raw_trades_included": False,
        "applies_to_real_trading": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        **_safety(),
    }


def _empty_payload(recommendation: str) -> dict[str, Any]:
    return {
        "payload_type": "research_lesson",
        "lesson_type": "deep_validation_no_data",
        "validation_version": VALIDATION_VERSION,
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "family": FAMILY,
        "candidate_profile": CANDIDATE_PROFILE,
        "recommendation": recommendation,
        "paper_observation_ready": False,
        "requires_human_approval": True,
        "summary": {},
        "gates": {},
        "rejection_reasons": ["no_variant_results"],
        "csv_payload_included": False,
        "raw_trades_included": False,
        "applies_to_real_trading": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        **_safety(),
    }


def _gates(row: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        "source_identity_resolved": _gate(bool(row.get("source_identity_resolved")), row.get("source_identity_resolved"), True),
        "data_quality_ok": _gate(str(row.get("data_quality") or "ok") == "ok", row.get("data_quality"), "ok"),
        "total_closed": _min_gate(row.get("total_closed"), MIN_TOTAL_CLOSED),
        "recent_closed": _min_gate(row.get("recent_closed"), MIN_RECENT_CLOSED),
        "total_pf": _min_gate(row.get("total_pf"), MIN_TOTAL_PF),
        "recent_pf": _min_gate(row.get("recent_pf"), MIN_RECENT_PF),
        "expectancy_positive": _gate(float(_number(row.get("expectancy"))) > 0.0, row.get("expectancy"), ">0"),
        "recent_expectancy_positive": _gate(float(_number(row.get("recent_expectancy"))) > 0.0, row.get("recent_expectancy"), ">0"),
        "monte_carlo_stressed_pf": _min_gate(row.get("monte_carlo_stressed_pf"), MIN_MONTE_CARLO_STRESSED_PF),
        "monte_carlo_stressed_expectancy_positive": _gate(
            float(_number(row.get("monte_carlo_stressed_expectancy"))) > 0.0,
            row.get("monte_carlo_stressed_expectancy"),
            ">0",
        ),
        "spread_x2_pf": _min_gate(row.get("spread_x2_pf"), MIN_SPREAD_X2_PF),
        "remove_best_5_pf": _min_gate(row.get("remove_best_5_pf"), MIN_REMOVE_BEST_5_PF),
        "single_trade_dependency_false": _gate(not bool(row.get("single_trade_dependency")), row.get("single_trade_dependency"), False),
        "fragile_regime_dependency_false": _gate(not bool(row.get("fragile_regime_dependency")), row.get("fragile_regime_dependency"), False),
        "no_registry_hit": _gate(not bool(row.get("registry_hit")), row.get("registry_hit"), False),
        "no_degradation_hit": _gate(not bool(row.get("degraded_by_registry")), row.get("degraded_by_registry"), False),
        "no_research_rejection_hit": _gate(
            not bool(row.get("research_rejection_registry")),
            row.get("research_rejection_registry"),
            False,
        ),
        "no_sibling_risk": _gate(not bool(row.get("sibling_risk")), row.get("sibling_risk"), False),
        "not_unknown_profile": _gate("unknown_profile" not in str(row.get("profile") or "").casefold(), row.get("profile"), "known_profile"),
    }


def _min_gate(actual: Any, minimum: float) -> dict[str, Any]:
    value = float(_number(actual))
    return _gate(value >= minimum, value, f">={minimum:g}")


def _gate(passed: bool, actual: Any, required: Any) -> dict[str, Any]:
    return {"passed": bool(passed), "actual": actual, "required": required}


def _window_metrics(rows: list[dict[str, Any]], trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    windows: list[tuple[str, int, int]] = [
        ("total_sample", 0, len(rows)),
        ("recent_25_pct", int(len(rows) * 0.75), len(rows)),
        ("recent_15_pct", int(len(rows) * 0.85), len(rows)),
        ("recent_10_pct", int(len(rows) * 0.90), len(rows)),
    ]
    for days in (30, 60, 90):
        cutoff = _cutoff_index_for_days(rows, days)
        if cutoff is not None:
            windows.append((f"last_{days}_days", cutoff, len(rows)))
    if len(rows) >= 1000:
        split = int(len(rows) * 0.70)
        windows.extend((("walk_forward_train_70", 0, split), ("walk_forward_test_30", split, len(rows))))

    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for name, start, end in windows:
        if name in seen:
            continue
        seen.add(name)
        selected = [trade for trade in trades if start <= int(trade.get("index") or 0) < end]
        pnl = [float(trade.get("pnl") or 0.0) for trade in selected]
        output.append(
            {
                "window": name,
                "start_index": start,
                "end_index": end,
                "closed": len(selected),
                "win_rate": _win_rate(pnl),
                "profit_factor": _profit_factor(pnl),
                "expectancy": _average(pnl),
                "max_drawdown": _max_drawdown(pnl),
                "consecutive_losses": _consecutive_losses(pnl),
                "passes_recent_gate": len(selected) >= MIN_RECENT_CLOSED
                and _profit_factor(pnl) >= MIN_RECENT_PF
                and _average(pnl) > 0.0,
            }
        )
    return output


def _coerce_windows(row: dict[str, Any]) -> list[dict[str, Any]]:
    value = row.get("metrics_by_window") if isinstance(row.get("metrics_by_window"), list) else row.get("window_metrics")
    if isinstance(value, list) and value:
        output: list[dict[str, Any]] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            window = dict(item)
            window["closed"] = int(_number(window.get("closed")))
            window["win_rate"] = float(_number(window.get("win_rate")))
            window["profit_factor"] = float(_number(window.get("profit_factor")))
            window["expectancy"] = float(_number(window.get("expectancy")))
            window["passes_recent_gate"] = (
                window["closed"] >= MIN_RECENT_CLOSED
                and window["profit_factor"] >= MIN_RECENT_PF
                and window["expectancy"] > 0.0
            )
            output.append(window)
        return output
    return [
        {
            "window": "recent_25_pct",
            "closed": int(_number(row.get("recent_closed"))),
            "win_rate": float(_number(row.get("recent_win_rate"))),
            "profit_factor": float(_number(row.get("recent_pf") or row.get("recent_profit_factor"))),
            "expectancy": float(_number(row.get("recent_expectancy"))),
            "passes_recent_gate": int(_number(row.get("recent_closed"))) >= MIN_RECENT_CLOSED
            and float(_number(row.get("recent_pf") or row.get("recent_profit_factor"))) >= MIN_RECENT_PF
            and float(_number(row.get("recent_expectancy"))) > 0.0,
        }
    ]


def _monte_carlo_stress(values: list[float], *, simulations: int) -> dict[str, float]:
    if not values:
        return {
            "monte_carlo_stressed_pf": 0.0,
            "monte_carlo_stressed_expectancy": 0.0,
            "monte_carlo_p95_drawdown": 0.0,
        }
    rng = random.Random(73115)
    pfs: list[float] = []
    expectancies: list[float] = []
    drawdowns: list[float] = []
    sample_size = len(values)
    for _ in range(simulations):
        sampled = [values[rng.randrange(sample_size)] for _ in range(sample_size)]
        pfs.append(_profit_factor(sampled))
        expectancies.append(_average(sampled))
        drawdowns.append(_max_drawdown(sampled))
    return {
        "monte_carlo_stressed_pf": _percentile(pfs, 0.10),
        "monte_carlo_stressed_expectancy": _percentile(expectancies, 0.10),
        "monte_carlo_p95_drawdown": _percentile(drawdowns, 0.95),
    }


def _quarter_pnls(trades: list[dict[str, Any]], rows_count: int) -> list[float]:
    quarters = [0.0, 0.0, 0.0, 0.0]
    if rows_count <= 0:
        return quarters
    for trade in trades:
        bucket = min(3, max(0, int((int(trade.get("index") or 0) / rows_count) * 4)))
        quarters[bucket] += float(trade.get("pnl") or 0.0)
    return quarters


def _fragile_regime_dependency(pnl: list[float], quarters: list[float], windows: list[dict[str, Any]]) -> bool:
    total = sum(pnl)
    if len(pnl) < 20 or total <= 0:
        return False
    if max(quarters or [0.0]) > total * 0.75:
        return True
    positive_quarters = sum(1 for value in quarters if value > 0)
    if positive_quarters <= 1:
        return True
    validation = [row for row in windows if str(row.get("window") or "").startswith(("recent_", "last_", "walk_forward_test"))]
    active = [row for row in validation if int(row.get("closed") or 0) > 0]
    if len(active) >= 3:
        weak = sum(1 for row in active if float(row.get("profit_factor") or 0.0) < 0.9 or float(row.get("expectancy") or 0.0) <= 0.0)
        return weak > len(active) // 2
    return False


def _single_trade_dependency(pnl: list[float]) -> bool:
    wins = sorted([value for value in pnl if value > 0.0], reverse=True)
    if not wins:
        return False
    gross_win = sum(wins)
    return wins[0] >= gross_win * 0.45 or _profit_factor(_remove_best(pnl, 1)) < 1.0


def _sample_stability_score(windows: list[dict[str, Any]]) -> float:
    validation = [
        row
        for row in windows
        if str(row.get("window") or "") not in {"total_sample", "walk_forward_train_70"}
        and int(row.get("closed") or 0) > 0
    ]
    if not validation:
        return 0.0
    passing = [row for row in validation if row.get("passes_recent_gate")]
    pfs = [float(row.get("profit_factor") or 0.0) for row in validation]
    dispersion = statistics.pstdev(pfs) if len(pfs) >= 2 else 0.0
    score = (len(passing) / max(1, len(validation))) * 100.0
    score -= min(45.0, dispersion * 12.0)
    return round(max(0.0, score), 4)


def _profit_factor(values: list[float]) -> float:
    wins = sum(value for value in values if value > 0.0)
    losses = abs(sum(value for value in values if value < 0.0))
    if wins > 0.0 and losses == 0.0:
        return 999.0
    return round(wins / losses, 6) if losses > 0.0 else 0.0


def _average(values: list[float]) -> float:
    return round(sum(values) / len(values), 8) if values else 0.0


def _win_rate(values: list[float]) -> float:
    if not values:
        return 0.0
    return round((sum(1 for value in values if value > 0.0) / len(values)) * 100.0, 4)


def _max_drawdown(values: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return round(max_dd, 8)


def _consecutive_losses(values: list[float]) -> int:
    current = 0
    worst = 0
    for value in values:
        if value < 0.0:
            current += 1
            worst = max(worst, current)
        else:
            current = 0
    return worst


def _remove_best(values: list[float], count: int) -> list[float]:
    output = list(values)
    for _ in range(min(count, len(output))):
        output.remove(max(output))
    return output


def _window_by_name(windows: list[dict[str, Any]], name: str) -> dict[str, Any]:
    for row in windows:
        if row.get("window") == name:
            return row
    return {}


def _cutoff_index_for_days(rows: list[dict[str, Any]], days: int) -> int | None:
    if not rows:
        return None
    last = _time_from_row(rows[-1])
    if last is None:
        return None
    cutoff = last - timedelta(days=days)
    for index, row in enumerate(rows):
        value = _time_from_row(row)
        if value is not None and value >= cutoff:
            return index
    return 0


def _time_from_row(row: dict[str, Any]) -> datetime | None:
    value = row.get("time")
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return _parse_time(value)


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * max(0.0, min(1.0, fraction))))
    return round(ordered[index], 8)


def _score(row: dict[str, Any]) -> float:
    passed = sum(1 for gate in (row.get("gates") or {}).values() if gate.get("passed"))
    failed = len(row.get("rejection_reasons") or [])
    return round(
        passed * 1000.0
        - failed * 250.0
        + float(row.get("total_pf") or 0.0) * 100.0
        + float(row.get("recent_pf") or 0.0) * 120.0
        + float(row.get("monte_carlo_stressed_pf") or 0.0) * 130.0
        + float(row.get("sample_stability_score") or 0.0),
        6,
    )


def _ranking_key(row: dict[str, Any]) -> tuple[int, int, float, str]:
    status_rank = 0 if row.get("paper_observation_ready") else 1
    return (
        status_rank,
        len(row.get("rejection_reasons") or []),
        -float(row.get("deep_validation_score") or 0.0),
        str(row.get("profile") or ""),
    )


def _load_persistent_state(
    *,
    load_persistent: bool,
    store: MT5PersistentIntelligenceStore | Any | None,
) -> dict[str, Any]:
    if not load_persistent:
        return {"source": "disabled", "research_lessons_loaded": 0, **_safety()}
    try:
        active_store = store or MT5PersistentIntelligenceStore()
        events = active_store.recent_events(limit=20) if hasattr(active_store, "recent_events") else {}
        lessons = events.get("recent_research_lessons") if isinstance(events, dict) else []
        return {
            "source": "persistent_intelligence_read_only",
            "research_lessons_loaded": len(lessons) if isinstance(lessons, list) else 0,
            "db_degraded": bool(events.get("db_degraded")) if isinstance(events, dict) else False,
            "queue_depth": events.get("queue_depth", 0) if isinstance(events, dict) else 0,
            **_safety(),
        }
    except Exception as exc:  # pragma: no cover - DB should never break local validation
        return {
            "source": "persistent_intelligence_read_only",
            "research_lessons_loaded": 0,
            "db_degraded": True,
            "error_category": type(exc).__name__,
            **_safety(),
        }


def _requested_csv_paths(value: list[str | Path] | str | Path | None) -> list[Path]:
    if value is None or value == "":
        return [Path(path) for path in DEFAULT_CSV_PATHS]
    if isinstance(value, (str, Path)):
        return [Path(item.strip()) for item in str(value).split(",") if item.strip()]
    return [Path(item) for item in value]


def _candidate_label(row: dict[str, Any] | None) -> str:
    if not row:
        return "none"
    return f"{row.get('symbol')} {row.get('timeframe')} {row.get('profile')}"


def _mode_from_profile(value: object) -> str:
    text = str(value or "")
    marker = "mode="
    if marker not in text:
        return ""
    return text.split(marker, 1)[1].split("|", 1)[0].strip()


def _sibling_risk(symbol: str, timeframe: str, profile: str) -> bool:
    blob = f"{symbol} {timeframe} {profile} {FAMILY}".casefold()
    return symbol == "ETHUSD" and timeframe == "M30" and "volatility" in blob and "breakout" in blob


def _symbol(value: object) -> str:
    symbol = str(value or "").upper().strip().replace(".B", "")
    if symbol == "XAUUSDB":
        return "XAUUSD"
    return symbol


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _number(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().casefold() in {"1", "true", "yes", "y"}


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
