from __future__ import annotations

from pathlib import Path
from typing import Any

from services.mt5.mt5_paper_forward_candidate_rotation import (
    MIN_MONTE_CARLO_STRESSED_PF,
    MIN_RECENT_CLOSED,
    MIN_RECENT_PF,
    MIN_SPREAD_X2_PF,
    MIN_TOTAL_CLOSED,
    MIN_TOTAL_PF,
    run_paper_forward_candidate_rotation,
)


MIN_REMOVE_BEST_5_PF = 1.0


def run_paper_forward_research_expansion(
    *,
    rows: list[dict[str, Any]] | None = None,
    result_paths: list[str | Path] | None = None,
    search_root: str | Path | None = None,
    include_priority_candidates: bool = True,
    load_default_sources: bool = True,
) -> dict[str, Any]:
    rotation = run_paper_forward_candidate_rotation(
        rows=rows,
        result_paths=result_paths,
        search_root=search_root,
        include_priority_candidates=include_priority_candidates,
        load_default_sources=load_default_sources,
    )
    ranking = [_evaluate_research_candidate(row) for row in rotation.get("ranking") or []]
    ranking.sort(key=_research_ranking_key)

    clean = [row for row in ranking if row["candidate_status"] == "paper_forward_review_ready"]
    near_misses = [row for row in ranking if row["candidate_status"] == "near_miss_hardening_candidate"]
    excluded = [
        row
        for row in ranking
        if row["candidate_status"] in {"excluded_by_degradation_registry", "blocked_by_sibling_risk"}
    ]
    recommended = clean[0] if clean else None
    recommendation = "paper_forward_candidate_review" if recommended else "continue_research"

    return {
        "ok": True,
        "status": "paper_forward_research_expansion_ready",
        "recommendation": recommendation,
        "recommended_candidate": recommended,
        "ranking": ranking,
        "near_misses": near_misses,
        "excluded_by_registry_or_sibling_risk": excluded,
        "top_3_hardening_families": _top_hardening_families(near_misses),
        "loaded_sources": rotation.get("loaded_sources") or [],
        "missing_sources": rotation.get("missing_sources") or [],
        "skipped_sources": rotation.get("skipped_sources") or [],
        "useful_rows": rotation.get("useful_rows") or 0,
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
        **_safety(),
    }


def _evaluate_research_candidate(row: dict[str, Any]) -> dict[str, Any]:
    row = _apply_degraded_cluster_guard(row)
    failed_gates, shortfalls = _research_gate_failures(row)
    status = _candidate_status(row, failed_gates)
    action = _next_action(status)
    hardening_recommendation = _hardening_recommendation(row, failed_gates)
    return {
        **row,
        "remove_best_5_pf": row.get("remove_best_5_pf"),
        "single_trade_dependency": bool(row.get("single_trade_dependency")),
        "failed_gates": failed_gates,
        "gate_shortfalls": shortfalls,
        "candidate_status": status,
        "recommended_next_action": action,
        "hardening_recommendation": hardening_recommendation,
        "research_score": _research_score(row, failed_gates),
        **_safety(),
    }


def _apply_degraded_cluster_guard(row: dict[str, Any]) -> dict[str, Any]:
    if row.get("degraded_by_registry") or row.get("sibling_risk"):
        return row
    if not _is_eth_m30_volatility_breakout_cluster(row):
        return row
    updated = {**row}
    updated["sibling_risk"] = True
    updated["sibling_of_degraded_profile"] = "eth_m30_vol_breakout_chop_guard_v1"
    updated["sibling_risk_reason"] = "same_degraded_eth_m30_volatility_breakout_cluster"
    return updated


def _is_eth_m30_volatility_breakout_cluster(row: dict[str, Any]) -> bool:
    if row.get("symbol") != "ETHUSD" or row.get("timeframe") != "M30":
        return False
    family_blob = f"{row.get('family') or ''} {row.get('profile') or ''}".casefold()
    return "volatility_breakout" in family_blob or "vol_breakout" in family_blob


def _research_gate_failures(row: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
    failures: list[str] = []
    shortfalls: dict[str, Any] = {}

    if row.get("degraded_by_registry"):
        failures.append("degraded_by_registry")
    if row.get("sibling_risk"):
        failures.append("sibling_risk")

    _min_gate(failures, shortfalls, row, "recent_closed", MIN_RECENT_CLOSED)
    _min_gate(failures, shortfalls, row, "total_closed", MIN_TOTAL_CLOSED)
    _min_gate(failures, shortfalls, row, "recent_pf", MIN_RECENT_PF)
    _min_gate(failures, shortfalls, row, "total_pf", MIN_TOTAL_PF)
    _min_gate(failures, shortfalls, row, "monte_carlo_stressed_pf", MIN_MONTE_CARLO_STRESSED_PF)
    _min_gate(failures, shortfalls, row, "spread_x2_pf", MIN_SPREAD_X2_PF)
    if float(row.get("expectancy") or 0.0) <= 0.0:
        failures.append("expectancy_not_positive")
        shortfalls["expectancy"] = {"current": float(row.get("expectancy") or 0.0), "required": "> 0"}

    remove_best_5_pf = row.get("remove_best_5_pf")
    if remove_best_5_pf is not None and float(remove_best_5_pf or 0.0) < MIN_REMOVE_BEST_5_PF:
        failures.append("remove_best_5_pf_below_1_0")
        shortfalls["remove_best_5_pf"] = {
            "current": float(remove_best_5_pf or 0.0),
            "required": MIN_REMOVE_BEST_5_PF,
            "missing": round(MIN_REMOVE_BEST_5_PF - float(remove_best_5_pf or 0.0), 6),
        }

    if bool(row.get("fragile_regime_dependency")):
        failures.append("fragile_regime_dependency")
        shortfalls["fragile_regime_dependency"] = {"current": True, "required": False}
    if bool(row.get("single_trade_dependency")):
        failures.append("single_trade_dependency")
        shortfalls["single_trade_dependency"] = {"current": True, "required": False}

    return failures, shortfalls


def _min_gate(
    failures: list[str],
    shortfalls: dict[str, Any],
    row: dict[str, Any],
    field: str,
    threshold: float,
) -> None:
    current = float(row.get(field) or 0.0)
    if current >= threshold:
        return
    failures.append(f"{field}_below_{_threshold_label(threshold)}")
    shortfalls[field] = {
        "current": current,
        "required": threshold,
        "missing": round(threshold - current, 6),
    }


def _candidate_status(row: dict[str, Any], failed_gates: list[str]) -> str:
    if row.get("degraded_by_registry"):
        return "excluded_by_degradation_registry"
    if row.get("sibling_risk"):
        return "blocked_by_sibling_risk"
    if not failed_gates:
        return "paper_forward_review_ready"
    if _worth_hardening(row, failed_gates):
        return "near_miss_hardening_candidate"
    return "research_gate_failed"


def _next_action(status: str) -> str:
    if status == "paper_forward_review_ready":
        return "paper_forward_candidate_review"
    if status == "excluded_by_degradation_registry":
        return "skip_degraded_profile"
    if status == "blocked_by_sibling_risk":
        return "manual_review_or_new_family_required"
    if status == "near_miss_hardening_candidate":
        return "targeted_hardening_review"
    return "continue_research"


def _hardening_recommendation(row: dict[str, Any], failed_gates: list[str]) -> str:
    if row.get("degraded_by_registry") or row.get("sibling_risk"):
        return "discard_for_current_rotation"
    if not failed_gates:
        return "candidate_clean_for_human_review"
    if _worth_hardening(row, failed_gates):
        return "worth_targeted_hardening"
    return "discard_or_collect_more_evidence"


def _worth_hardening(row: dict[str, Any], failed_gates: list[str]) -> bool:
    hard_blockers = {
        "degraded_by_registry",
        "sibling_risk",
        "fragile_regime_dependency",
        "single_trade_dependency",
        "expectancy_not_positive",
    }
    if hard_blockers & set(failed_gates):
        return False
    return (
        int(row.get("recent_closed") or 0) >= 10
        and int(row.get("total_closed") or 0) >= 35
        and float(row.get("recent_pf") or 0.0) >= 1.0
        and float(row.get("total_pf") or 0.0) >= 1.10
        and float(row.get("expectancy") or 0.0) > 0.0
    )


def _research_score(row: dict[str, Any], failed_gates: list[str]) -> float:
    score = 0.0
    score += min(int(row.get("recent_closed") or 0), 80) * 3.0
    score += min(int(row.get("total_closed") or 0), 200) * 0.8
    score += float(row.get("recent_pf") or 0.0) * 35.0
    score += float(row.get("total_pf") or 0.0) * 40.0
    score += float(row.get("monte_carlo_stressed_pf") or 0.0) * 45.0
    score += float(row.get("spread_x2_pf") or 0.0) * 25.0
    score += float(row.get("expectancy") or 0.0) * 100.0
    remove_best_5_pf = row.get("remove_best_5_pf")
    if remove_best_5_pf is not None:
        score += float(remove_best_5_pf or 0.0) * 20.0
    score -= len(failed_gates) * 45.0
    if row.get("degraded_by_registry"):
        score -= 10_000.0
    if row.get("sibling_risk"):
        score -= 5_000.0
    return round(score, 4)


def _research_ranking_key(row: dict[str, Any]) -> tuple[int, float, str, str, str]:
    ranks = {
        "paper_forward_review_ready": 0,
        "near_miss_hardening_candidate": 1,
        "blocked_by_sibling_risk": 2,
        "research_gate_failed": 3,
        "excluded_by_degradation_registry": 4,
    }
    return (
        ranks.get(row.get("candidate_status") or "", 5),
        -float(row.get("research_score") or 0.0),
        str(row.get("symbol") or ""),
        str(row.get("timeframe") or ""),
        str(row.get("profile") or ""),
    )


def _top_hardening_families(near_misses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in near_misses:
        key = (str(row.get("symbol") or ""), str(row.get("timeframe") or ""), str(row.get("family") or ""))
        current = grouped.get(key)
        if current is None or float(row.get("research_score") or 0.0) > float(current.get("research_score") or 0.0):
            grouped[key] = {
                "symbol": row.get("symbol") or "",
                "timeframe": row.get("timeframe") or "",
                "family": row.get("family") or "",
                "representative_profile": row.get("profile") or "",
                "failed_gates": row.get("failed_gates") or [],
                "research_score": row.get("research_score") or 0.0,
                "recommended_next_action": "targeted_hardening_review",
            }
    return sorted(grouped.values(), key=lambda row: -float(row.get("research_score") or 0.0))[:3]


def _threshold_label(value: float) -> str:
    return str(value).replace(".", "_").rstrip("0").rstrip("_")


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
