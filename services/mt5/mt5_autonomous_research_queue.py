from __future__ import annotations

import time
from typing import Any

from services.mt5.mt5_btc_h1_candidate_deep_validation import run_btc_h1_candidate_deep_validation
from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation_registry_status
from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore
from services.mt5.mt5_research_intelligence_core import run_research_intelligence_core
from services.mt5.mt5_research_rejection_registry import research_rejection_registry_status
from services.mt5.mt5_robust_candidate_harvester import run_robust_candidate_harvester


QUEUE_VERSION = "2026-06-12.mt5_autonomous_research_queue.v1"

MIN_TOTAL_CLOSED = 50
MIN_RECENT_CLOSED = 20
MIN_TOTAL_PF = 1.15
MIN_RECENT_PF = 1.15
MIN_MC_STRESSED_PF = 1.05
MIN_SPREAD_X2_PF = 0.95
MIN_REMOVE_BEST_5_PF = 1.0

_KNOWN_FAILED_FAMILIES: tuple[dict[str, str], ...] = (
    {
        "label": "ETHUSD M30 volatility breakout cluster",
        "reason": "degraded_or_sibling_risk",
        "marker": "ethusd m30 volatility_breakout vol_breakout",
    },
    {
        "label": "BTCUSD H1 tournament_edge_candidate",
        "reason": "deep_validation_failed_source_identity_and_robustness",
        "marker": "btcusd h1 tournament_edge_candidate",
    },
    {
        "label": "BTCUSD H1 recent_liquidity_sweep",
        "reason": "deep_validation_failed_monte_carlo_and_dependency",
        "marker": "btcusd h1 recent_liquidity_sweep",
    },
    {
        "label": "BTCUSD M30 London-US/opening-range failed cluster",
        "reason": "deep_sample_validation_failed",
        "marker": "btcusd m30 london_us_breakout opening_range_fakeout",
    },
    {
        "label": "XAUUSD M15 recent_session_open_continuation",
        "reason": "hardening_failed_mc_and_remove_best",
        "marker": "xauusd m15 recent_session_open_continuation",
    },
    {
        "label": "EURUSD H1 session_vwap_reclaim",
        "reason": "proxy_false_positive_after_costs_and_mc_failure",
        "marker": "eurusd h1 session_vwap_reclaim",
    },
    {
        "label": "USTEC M30/H1 trend_pullback failed variant",
        "reason": "proxy_false_positive_after_monte_carlo_failure",
        "marker": "ustec m30 h1 trend_pullback",
    },
    {
        "label": "unknown_profile",
        "reason": "source_identity_unresolved",
        "marker": "unknown_profile",
    },
    {
        "label": "trades_forward_below_20",
        "reason": "sample_too_small",
        "marker": "trades_forward_below_20",
    },
    {
        "label": "recent_closed_below_20",
        "reason": "sample_too_small",
        "marker": "recent_closed_below_20",
    },
    {
        "label": "monte_carlo_fragility",
        "reason": "robustness_gate_failed",
        "marker": "monte_carlo_fragility",
    },
    {
        "label": "remove_best_dependency",
        "reason": "top_trade_dependency",
        "marker": "remove_best_dependency",
    },
    {
        "label": "single_trade_dependency",
        "reason": "dependency_gate_failed",
        "marker": "single_trade_dependency",
    },
    {
        "label": "fragile_regime_dependency",
        "reason": "regime_dependency_gate_failed",
        "marker": "fragile_regime_dependency",
    },
)

_QUEUE_SEEDS: tuple[dict[str, Any], ...] = (
    {
        "family_name": "volatility_compression_breakout",
        "priority_score": 100,
        "recommended_next_script": "design_volatility_compression_breakout_processed_feature_scan",
        "recommended_next_action": "design_fast_processed_feature_scan_no_activation",
    },
    {
        "family_name": "multi_timeframe_trend_pullback",
        "priority_score": 92,
        "recommended_next_script": "python scripts/run_multi_timeframe_trend_pullback_feature_scan.py",
        "recommended_next_action": "scan_non_rejected_symbol_timeframe_variants_only",
        "scope_note": "exclude USTEC M30/H1 rejected cluster",
    },
    {
        "family_name": "rsi_divergence_confirmation",
        "priority_score": 86,
        "recommended_next_script": "design_rsi_divergence_confirmation_processed_feature_scan",
        "recommended_next_action": "design_fast_processed_feature_scan_no_activation",
    },
    {
        "family_name": "liquidity_sweep_reversal_v2",
        "priority_score": 80,
        "recommended_next_script": "design_liquidity_sweep_reversal_v2_processed_feature_scan",
        "recommended_next_action": "scan_only_if_not_btc_h1_recent_liquidity_sweep_or_sibling",
    },
    {
        "family_name": "atr_expansion_continuation_v2",
        "priority_score": 76,
        "recommended_next_script": "design_atr_expansion_continuation_v2_processed_feature_scan",
        "recommended_next_action": "design_fast_processed_feature_scan_no_activation",
    },
    {
        "family_name": "mean_reversion_after_news_shock",
        "priority_score": 70,
        "recommended_next_script": "design_mean_reversion_after_news_shock_processed_feature_scan",
        "recommended_next_action": "scan_only_if_processed_event_features_exist",
    },
    {
        "family_name": "session_filter_high_sample_low_dependency",
        "priority_score": 66,
        "recommended_next_script": "design_session_filter_high_sample_low_dependency_scan",
        "recommended_next_action": "prefer_high_sample_low_dependency_session_variants",
    },
)


def run_autonomous_research_queue(
    *,
    run_fast_scans: bool = False,
    run_deep_validation: bool = False,
    candidate: str = "",
    max_evaluations: int = 100,
    processed_source_paths: list[str] | None = None,
    persistent_events: dict[str, Any] | None = None,
    load_persistent: bool = True,
    store: MT5PersistentIntelligenceStore | Any | None = None,
    harvester_result: dict[str, Any] | None = None,
    intelligence_result: dict[str, Any] | None = None,
    rejection_registry: dict[str, Any] | None = None,
    degradation_registry: dict[str, Any] | None = None,
    deep_validation_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    safe_max = max(1, min(int(max_evaluations or 100), 500))
    persistent = _load_persistent_research_lessons(
        persistent_events,
        load_persistent=load_persistent,
        store=store,
    )
    rejection = rejection_registry or research_rejection_registry_status()
    degradation = degradation_registry or forward_profile_degradation_registry_status()

    scans_run: list[str] = []
    deep_result: dict[str, Any] | None = None
    if run_deep_validation:
        deep_result = deep_validation_result or _run_single_deep_validation(candidate)
        scans_run.extend(deep_result.get("scans_run") or [])

    intelligence = intelligence_result or _load_intelligence_core_readonly()
    if intelligence.get("status"):
        scans_run.append("research_intelligence_core_readonly")

    harvester = harvester_result or run_robust_candidate_harvester(
        processed_source_paths=processed_source_paths,
        persistent_events=persistent.get("recent_events") or {},
        load_persistent=False,
        max_candidates=max(1, min(safe_max, 25)),
    )
    scans_run.append("robust_candidate_harvester_processed_sources")
    if run_fast_scans:
        scans_run.append("fast_processed_candidate_gate_scan")

    avoided = _avoided_families(rejection, degradation)
    next_hypotheses = _next_hypotheses(intelligence, avoided)
    candidate_rows = _candidate_rows(harvester)
    considered = candidate_rows[:safe_max]
    evaluated_candidates = [_evaluate_queue_candidate(row) for row in considered]
    approved = [row for row in evaluated_candidates if row.get("candidate_status") == "queue_candidate_ready"]
    approved.sort(key=_candidate_rank)
    top_candidate = approved[0] if approved else None
    recommendation = "paper_forward_candidate_review" if top_candidate else "continue_research"
    next_phase = (
        "run_single_candidate_deep_validation"
        if top_candidate
        else _next_research_phase(next_hypotheses, intelligence)
    )
    recommended_next_script = (
        f"python scripts/run_autonomous_research_queue.py --run-deep-validation --candidate {top_candidate.get('candidate_id')}"
        if top_candidate
        else _recommended_next_script(next_hypotheses)
    )

    if run_deep_validation and deep_result and not deep_result.get("ok", True):
        recommendation = "continue_research"
        next_phase = str(deep_result.get("reason") or "deep_validation_not_run")
        recommended_next_script = _recommended_next_script(next_hypotheses)

    result = {
        "ok": True,
        "status": "autonomous_research_queue_ready",
        "queue_version": QUEUE_VERSION,
        "research_queue_state": _queue_state(
            run_fast_scans=run_fast_scans,
            run_deep_validation=run_deep_validation,
            deep_result=deep_result,
        ),
        "mode": (
            "single_candidate_deep_validation"
            if run_deep_validation
            else "fast_processed_scans"
            if run_fast_scans
            else "dry_run_plan_only"
        ),
        "db_state": persistent.get("db_state") or {},
        "lessons_loaded": len(persistent.get("research_lessons") or []),
        "rejected_families_loaded": int(rejection.get("count") or len(rejection.get("research_rejections") or [])),
        "degraded_profiles_loaded": int(degradation.get("count") or len(degradation.get("degraded_profiles") or [])),
        "avoided_families": avoided,
        "next_hypotheses": next_hypotheses,
        "next_research_plan": _next_research_plan(next_hypotheses),
        "scans_run": _dedupe(scans_run),
        "heavy_backtests_run": bool(run_deep_validation and deep_result and deep_result.get("deep_validation_run")),
        "offline_backtests_run": bool(run_deep_validation and deep_result and deep_result.get("deep_validation_run")),
        "max_evaluations": safe_max,
        "candidate_evaluations_considered": len(considered),
        "max_evaluations_respected": len(considered) <= safe_max,
        "candidates_found": len(approved),
        "top_candidate": top_candidate,
        "evaluated_candidates": evaluated_candidates,
        "harvester_summary": _harvester_summary(harvester),
        "intelligence_summary": _intelligence_summary(intelligence),
        "deep_validation_result": deep_result or {},
        "recommendation": recommendation,
        "recommended_next_research_phase": next_phase,
        "recommended_next_script": recommended_next_script,
        "research_lesson": _research_lesson(recommendation, next_phase, evaluated_candidates),
        "research_lesson_persisted": False,
        "research_lesson_write": {"attempted": False, "reason": "dry_run_no_write", **_safety()},
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
    return result


def _load_persistent_research_lessons(
    persistent_events: dict[str, Any] | None,
    *,
    load_persistent: bool,
    store: MT5PersistentIntelligenceStore | Any | None,
) -> dict[str, Any]:
    if persistent_events is not None:
        lessons = _safe_list(persistent_events.get("recent_research_lessons"))
        return {
            "source": "injected",
            "recent_events": persistent_events,
            "research_lessons": lessons,
            "db_state": {
                "source": "injected",
                "db_degraded": bool(persistent_events.get("db_degraded")),
                "status_endpoints_write_free": True,
                **_safety(),
            },
            **_safety(),
        }
    if not load_persistent:
        return {
            "source": "disabled",
            "recent_events": {},
            "research_lessons": [],
            "db_state": {
                "source": "disabled",
                "db_degraded": False,
                "status_endpoints_write_free": True,
                **_safety(),
            },
            **_safety(),
        }
    try:
        active_store = store or MT5PersistentIntelligenceStore()
        events = active_store.recent_events(limit=50) if hasattr(active_store, "recent_events") else {}
        lessons = _safe_list((events or {}).get("recent_research_lessons"))
        return {
            "source": "persistent_intelligence",
            "recent_events": events or {},
            "research_lessons": lessons,
            "db_state": {
                "source": "persistent_intelligence",
                "provider": (events or {}).get("provider") or "",
                "db_degraded": bool((events or {}).get("db_degraded")),
                "queue_depth": (events or {}).get("queue_depth", 0),
                "failed_writes": (events or {}).get("failed_writes", 0),
                "queued_writes": (events or {}).get("queued_writes", 0),
                "status_endpoints_write_free": bool((events or {}).get("status_endpoints_write_free", True)),
                **_safety(),
            },
            **_safety(),
        }
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {
            "source": "persistent_intelligence",
            "recent_events": {},
            "research_lessons": [],
            "db_state": {
                "source": "persistent_intelligence",
                "db_degraded": True,
                "reason": type(exc).__name__,
                "status_endpoints_write_free": True,
                **_safety(),
            },
            **_safety(),
        }


def _load_intelligence_core_readonly() -> dict[str, Any]:
    try:
        return run_research_intelligence_core(persist_lessons=False)
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {
            "ok": False,
            "status": "research_intelligence_core_unavailable",
            "reason": type(exc).__name__,
            "priority_queue": [],
            "next_hypotheses": [],
            "recommended_next_research_phase": "refresh_processed_results_before_research",
            **_safety(),
        }


def _run_single_deep_validation(candidate: str) -> dict[str, Any]:
    candidate_id = str(candidate or "").strip()
    if not candidate_id:
        return {
            "ok": False,
            "status": "deep_validation_candidate_required",
            "reason": "candidate_required",
            "scans_run": [],
            "deep_validation_run": False,
            **_safety(),
        }
    lowered = candidate_id.casefold()
    if "btcusd" in lowered and "h1" in lowered:
        result = run_btc_h1_candidate_deep_validation(
            {
                "targets": candidate_id,
                "max_configs": 1,
                "max_bars": 2000,
                "monte_carlo_simulations": 100,
                "per_evaluation_timeout_seconds": 1.0,
                "max_runtime_seconds": 12.0,
                "load_persistent_memory": False,
                "persist_research_lesson": False,
            }
        )
        return {
            "ok": bool(result.get("ok", True)),
            "status": result.get("status") or "deep_validation_complete",
            "candidate": candidate_id,
            "scans_run": ["btc_h1_candidate_deep_validation_single_target"],
            "deep_validation_run": True,
            "recommendation": result.get("recommendation") or "continue_research",
            "recommended_candidate": result.get("recommended_candidate"),
            "aggregate_rejection_reasons": result.get("aggregate_rejection_reasons") or [],
            **_safety(),
        }
    return {
        "ok": True,
        "status": "deep_validation_dispatch_plan_ready",
        "candidate": candidate_id,
        "reason": "no_registered_deep_validator_for_candidate",
        "scans_run": ["deep_validation_dispatch_plan_only"],
        "deep_validation_run": False,
        "recommended_next_action": "create_single_candidate_validator_before_running_heavy_backtest",
        **_safety(),
    }


def _avoided_families(rejection: dict[str, Any], degradation: dict[str, Any]) -> list[dict[str, Any]]:
    avoided: list[dict[str, Any]] = []
    for row in degradation.get("degraded_profiles") or []:
        avoided.append(
            {
                "label": f"{row.get('symbol')} {row.get('timeframe')} {row.get('profile')}",
                "reason": row.get("degradation_reason") or "degraded_profile",
                "source": "degradation_registry",
            }
        )
    for row in rejection.get("research_rejections") or []:
        patterns = ",".join(str(item) for item in row.get("family_profile_patterns") or [])
        avoided.append(
            {
                "label": f"{row.get('symbol')} {row.get('timeframe')} {patterns}",
                "reason": row.get("rejection_reason") or "research_rejection",
                "source": "research_rejection_registry",
            }
        )
    for row in _KNOWN_FAILED_FAMILIES:
        avoided.append({"label": row["label"], "reason": row["reason"], "source": "known_failed_family"})
    return _dedupe_dicts(avoided, "label")


def _next_hypotheses(intelligence: dict[str, Any], avoided: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in _QUEUE_SEEDS:
        rows.append({**row, "source": "research_queue_seed"})
    for row in intelligence.get("priority_queue") or []:
        if not isinstance(row, dict):
            continue
        family = str(row.get("family_name") or "")
        if not family:
            continue
        rows.append(
            {
                "family_name": family,
                "priority_score": int(_number(row.get("priority_score")) or 0),
                "recommended_next_action": row.get("recommended_next_action") or "design_processed_feature_scan_no_activation",
                "recommended_next_script": _script_for_family(family),
                "heavy_backtest_required": bool(row.get("heavy_backtest_required")),
                "source": "research_intelligence_core",
            }
        )
    merged: dict[str, dict[str, Any]] = {}
    for row in rows:
        family = str(row.get("family_name") or "").strip()
        if not family:
            continue
        block_reason = _hypothesis_block_reason(family, avoided)
        if block_reason:
            continue
        existing = merged.get(family)
        if existing and int(existing.get("priority_score") or 0) >= int(row.get("priority_score") or 0):
            continue
        merged[family] = {
            "hypothesis_id": family,
            "family_name": family,
            "priority_score": int(row.get("priority_score") or 0),
            "source": row.get("source") or "",
            "rejected_by_registry": False,
            "degraded_by_registry": False,
            "sibling_risk": False,
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
            "recommended_next_action": row.get("recommended_next_action") or "design_processed_feature_scan_no_activation",
            "recommended_next_script": row.get("recommended_next_script") or _script_for_family(family),
            "scope_note": row.get("scope_note") or _scope_note(family),
            "heavy_backtest_required": bool(row.get("heavy_backtest_required", True)),
            **_safety(),
        }
    output = list(merged.values())
    output.sort(key=lambda item: (-int(item.get("priority_score") or 0), str(item.get("family_name") or "")))
    return output


def _hypothesis_block_reason(family: str, avoided: list[dict[str, Any]]) -> str:
    lowered = family.casefold()
    if lowered in {"session_vwap_reclaim", "ema_slope_pullback"}:
        return "family_recently_failed_or_too_close_to_failed_proxy"
    avoid_text = " ".join(str(row.get("label") or "") for row in avoided).casefold()
    if "liquidity_sweep_reversal_v2" == lowered and "btcusd h1 recent_liquidity_sweep" in avoid_text:
        return ""
    if "multi_timeframe_trend_pullback" == lowered and "ustec m30" in avoid_text:
        return ""
    return ""


def _candidate_rows(harvester: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("top_candidates", "rejected_candidates"):
        for row in harvester.get(key) or []:
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _evaluate_queue_candidate(row: dict[str, Any]) -> dict[str, Any]:
    profile = str(row.get("profile") or row.get("target_name") or "")
    family = str(row.get("family") or row.get("source_family") or "")
    symbol = str(row.get("symbol") or "").upper().replace(".B", "")
    timeframe = str(row.get("timeframe") or "").upper()
    reasons: list[str] = []
    if "unknown_profile" in f"{profile} {family}".casefold():
        reasons.append("unknown_profile")
    if not _bool(row.get("source_identity_resolved")):
        reasons.append("source_identity_unresolved")
    _min_gate(reasons, row, "total_closed", MIN_TOTAL_CLOSED, "total_closed_below_50")
    _min_gate(reasons, row, "recent_closed", MIN_RECENT_CLOSED, "recent_closed_below_20")
    _min_gate(reasons, row, "total_pf", MIN_TOTAL_PF, "total_pf_below_1_15", aliases=("profit_factor",))
    _min_gate(reasons, row, "recent_pf", MIN_RECENT_PF, "recent_pf_below_1_15", aliases=("recent_profit_factor",))
    if _number(row.get("expectancy")) <= 0:
        reasons.append("expectancy_not_positive")
    if _number(row.get("recent_expectancy")) <= 0:
        reasons.append("recent_expectancy_not_positive")
    _min_gate(
        reasons,
        row,
        "monte_carlo_stressed_pf",
        MIN_MC_STRESSED_PF,
        "monte_carlo_stressed_pf_below_1_05",
    )
    if _number(row.get("monte_carlo_stressed_expectancy")) <= 0:
        reasons.append("monte_carlo_stressed_expectancy_not_positive")
    _min_gate(reasons, row, "spread_x2_pf", MIN_SPREAD_X2_PF, "spread_x2_pf_below_0_95")
    _min_gate(reasons, row, "remove_best_5_pf", MIN_REMOVE_BEST_5_PF, "remove_best_5_pf_below_1")
    if _bool(row.get("single_trade_dependency")):
        reasons.append("single_trade_dependency")
    if _bool(row.get("fragile_regime_dependency")):
        reasons.append("fragile_regime_dependency")
    if _bool(row.get("degraded_by_registry")) or _bool(row.get("registry_degraded")):
        reasons.append("degraded_by_registry")
    if _bool(row.get("rejected_by_research_registry")) or _bool(row.get("research_rejection_registry")):
        reasons.append("rejected_by_research_registry")
    if _bool(row.get("sibling_risk")):
        reasons.append("sibling_risk")
    reasons = _dedupe(reasons)
    status = "queue_candidate_ready" if not reasons else "queue_gate_failed"
    return {
        "candidate_id": _candidate_id(symbol, timeframe, profile or family),
        "symbol": symbol,
        "timeframe": timeframe,
        "profile": profile,
        "family": family,
        "source": row.get("source") or "",
        "source_identity_resolved": _bool(row.get("source_identity_resolved")),
        "total_closed": int(_number(row.get("total_closed"))),
        "recent_closed": int(_number(row.get("recent_closed"))),
        "total_pf": _number(row.get("total_pf") or row.get("profit_factor")),
        "recent_pf": _number(row.get("recent_pf") or row.get("recent_profit_factor")),
        "expectancy": _number(row.get("expectancy")),
        "recent_expectancy": _number(row.get("recent_expectancy")),
        "monte_carlo_stressed_pf": _number(row.get("monte_carlo_stressed_pf")),
        "monte_carlo_stressed_expectancy": _number(row.get("monte_carlo_stressed_expectancy")),
        "spread_x2_pf": _number(row.get("spread_x2_pf")),
        "remove_best_5_pf": _number(row.get("remove_best_5_pf")),
        "single_trade_dependency": _bool(row.get("single_trade_dependency")),
        "fragile_regime_dependency": _bool(row.get("fragile_regime_dependency")),
        "degraded_by_registry": _bool(row.get("degraded_by_registry") or row.get("registry_degraded")),
        "rejected_by_research_registry": _bool(row.get("rejected_by_research_registry") or row.get("research_rejection_registry")),
        "sibling_risk": _bool(row.get("sibling_risk")),
        "rejection_reasons": reasons,
        "candidate_status": status,
        "recommended_next_action": "single_candidate_deep_validation_review" if status == "queue_candidate_ready" else "skip_or_continue_research",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _min_gate(
    reasons: list[str],
    row: dict[str, Any],
    key: str,
    minimum: float,
    reason: str,
    *,
    aliases: tuple[str, ...] = (),
) -> None:
    value = row.get(key)
    for alias in aliases:
        if value in {None, ""}:
            value = row.get(alias)
    if _number(value) < minimum:
        reasons.append(reason)


def _queue_state(*, run_fast_scans: bool, run_deep_validation: bool, deep_result: dict[str, Any] | None) -> str:
    if run_deep_validation:
        if deep_result and deep_result.get("deep_validation_run"):
            return "single_candidate_deep_validation_completed"
        return "single_candidate_deep_validation_plan"
    if run_fast_scans:
        return "fast_scans_completed"
    return "dry_run_plan"


def _next_research_phase(next_hypotheses: list[dict[str, Any]], intelligence: dict[str, Any]) -> str:
    if next_hypotheses:
        return str(next_hypotheses[0].get("family_name") or "")
    return str(intelligence.get("recommended_next_research_phase") or "continue_research")


def _recommended_next_script(next_hypotheses: list[dict[str, Any]]) -> str:
    if not next_hypotheses:
        return "python scripts/run_research_intelligence_core.py"
    return str(next_hypotheses[0].get("recommended_next_script") or _script_for_family(str(next_hypotheses[0].get("family_name") or "")))


def _script_for_family(family: str) -> str:
    mapping = {
        "multi_timeframe_trend_pullback": "python scripts/run_multi_timeframe_trend_pullback_feature_scan.py",
        "volatility_compression_breakout": "design_volatility_compression_breakout_processed_feature_scan",
        "rsi_divergence_confirmation": "design_rsi_divergence_confirmation_processed_feature_scan",
        "liquidity_sweep_reversal_v2": "design_liquidity_sweep_reversal_v2_processed_feature_scan",
        "atr_expansion_continuation_v2": "design_atr_expansion_continuation_v2_processed_feature_scan",
        "mean_reversion_after_news_shock": "design_mean_reversion_after_news_shock_processed_feature_scan",
        "session_filter_high_sample_low_dependency": "design_session_filter_high_sample_low_dependency_scan",
    }
    return mapping.get(str(family or ""), "python scripts/run_research_intelligence_core.py")


def _scope_note(family: str) -> str:
    if family == "multi_timeframe_trend_pullback":
        return "Allowed only outside rejected USTEC M30/H1 cluster."
    if family == "liquidity_sweep_reversal_v2":
        return "Allowed only outside rejected BTCUSD H1 recent_liquidity_sweep cluster."
    return "Processed-source scan only until a single candidate passes robust gates."


def _next_research_plan(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "rank": index + 1,
            "family_name": row.get("family_name"),
            "recommended_next_action": row.get("recommended_next_action"),
            "recommended_next_script": row.get("recommended_next_script"),
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
            **_safety(),
        }
        for index, row in enumerate(rows[:5])
    ]


def _harvester_summary(harvester: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": harvester.get("status") or "",
        "recommendation": harvester.get("recommendation") or "",
        "loaded_sources": harvester.get("loaded_sources") or [],
        "missing_sources": harvester.get("missing_sources") or [],
        "raw_rows": harvester.get("raw_rows", 0),
        "useful_rows": harvester.get("useful_rows", 0),
        "top_candidates": len(harvester.get("top_candidates") or []),
        "rejected_candidates": len(harvester.get("rejected_candidates") or []),
        **_safety(),
    }


def _intelligence_summary(intelligence: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": intelligence.get("status") or "",
        "recommendation": intelligence.get("recommendation") or "",
        "recommended_next_research_phase": intelligence.get("recommended_next_research_phase") or "",
        "next_hypotheses": len(intelligence.get("next_hypotheses") or []),
        "priority_queue": len(intelligence.get("priority_queue") or []),
        "research_lessons_persisted": bool(intelligence.get("research_lessons_persisted")),
        **_safety(),
    }


def _research_lesson(recommendation: str, next_phase: str, evaluated: list[dict[str, Any]]) -> dict[str, Any]:
    rejection_counts: dict[str, int] = {}
    for row in evaluated:
        for reason in row.get("rejection_reasons") or []:
            rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
    return {
        "family": "autonomous_research_queue",
        "symbol": "MULTI",
        "timeframe": "MULTI",
        "lesson_type": "research_queue_plan" if recommendation == "continue_research" else "candidate_queue_review",
        "failure_pattern": ",".join(sorted(rejection_counts, key=rejection_counts.get, reverse=True)[:6]),
        "summary": f"Queue recommendation={recommendation}; next_phase={next_phase}; evaluated={len(evaluated)}.",
        "avoid_next": [row["label"] for row in _KNOWN_FAILED_FAMILIES],
        "recommended_next_research_phase": next_phase,
        **_safety(),
    }


def _candidate_rank(row: dict[str, Any]) -> tuple[float, str]:
    score = 0.0
    score += min(row.get("total_closed") or 0, 250) * 0.5
    score += min(row.get("recent_closed") or 0, 100) * 2.0
    score += max(0.0, float(row.get("recent_pf") or 0.0) - 1.0) * 100.0
    score += max(0.0, float(row.get("total_pf") or 0.0) - 1.0) * 90.0
    score += max(0.0, float(row.get("monte_carlo_stressed_pf") or 0.0) - 1.0) * 170.0
    score += max(0.0, float(row.get("remove_best_5_pf") or 0.0) - 1.0) * 120.0
    return (-score, str(row.get("candidate_id") or ""))


def _candidate_id(symbol: str, timeframe: str, profile: str) -> str:
    raw = "_".join(part for part in (symbol, timeframe, profile) if part)
    return raw.casefold().replace(" ", "_").replace("|", "_").replace("/", "_")


def _safe_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


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


def _dedupe_dicts(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    seen: set[str] = set()
    output: list[dict[str, Any]] = []
    for row in rows:
        marker = str(row.get(key) or "")
        if marker in seen:
            continue
        seen.add(marker)
        output.append(row)
    return output


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
