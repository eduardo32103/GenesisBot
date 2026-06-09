from __future__ import annotations

import time
from collections import Counter
from pathlib import Path
from typing import Any

from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation_registry_status
from services.mt5.mt5_new_family_edge_discovery import run_new_family_edge_discovery
from services.mt5.mt5_paper_forward_candidate_rotation import run_paper_forward_candidate_rotation
from services.mt5.mt5_paper_forward_research_expansion import run_paper_forward_research_expansion
from services.mt5.mt5_research_rejection_registry import research_rejection_registry_status


CORE_VERSION = "2026-06-09.genesis_research_intelligence_core.v1"

_REQUIRED_METRICS = {
    "recent_closed": ">= 15",
    "total_closed": ">= 45",
    "recent_pf": ">= 1.05",
    "total_pf": ">= 1.15",
    "expectancy": "> 0",
    "monte_carlo_stressed_pf": ">= 1.05",
    "monte_carlo_stressed_expectancy": "> 0",
    "spread_x2_pf": ">= 0.95",
    "remove_best_5_pf": ">= 1.0",
    "fragile_regime_dependency": False,
    "single_trade_dependency": False,
}

_HYPOTHESIS_TEMPLATES: tuple[dict[str, Any], ...] = (
    {
        "family_name": "session_vwap_reclaim",
        "why_this_is_different": "Uses intraday fair-value reclaim instead of breakout continuation or EMA reclaim.",
        "symbols_to_test": ["US500", "NAS100", "BTCUSD", "EURUSD", "GBPUSD"],
        "timeframes_to_test": ["M15", "M30"],
        "expected_failure_risks": ["spread_sensitivity", "insufficient_recent_sample"],
        "priority_score": 92,
        "max_offline_evaluations_suggested": 36,
        "heavy_backtest_required": True,
    },
    {
        "family_name": "volatility_compression_breakout",
        "why_this_is_different": "Requires prior compression before expansion, avoiding the rejected ETH M30 volatility-breakout cluster.",
        "symbols_to_test": ["US500", "NAS100", "BTCUSD", "XAUUSD"],
        "timeframes_to_test": ["M15", "M30", "H1"],
        "expected_failure_risks": ["false_breakout_rate", "monte_carlo_fragility"],
        "priority_score": 88,
        "max_offline_evaluations_suggested": 42,
        "heavy_backtest_required": True,
    },
    {
        "family_name": "multi_timeframe_trend_pullback",
        "why_this_is_different": "Tests higher-timeframe trend alignment with lower-timeframe pullback, not a single-session breakout.",
        "symbols_to_test": ["US500", "NAS100", "EURUSD", "GBPUSD", "XAUUSD"],
        "timeframes_to_test": ["M15", "M30", "H1"],
        "expected_failure_risks": ["fragile_regime_dependency", "single_trade_dependency"],
        "priority_score": 84,
        "max_offline_evaluations_suggested": 32,
        "heavy_backtest_required": True,
    },
    {
        "family_name": "rsi_divergence_confirmation",
        "why_this_is_different": "Looks for exhaustion confirmation rather than continuation, so it is not a sibling of rejected breakout profiles.",
        "symbols_to_test": ["XAUUSD", "BTCUSD", "ETHUSD", "EURUSD", "GBPUSD"],
        "timeframes_to_test": ["M15", "M30"],
        "expected_failure_risks": ["insufficient_recent_sample", "remove_best_dependency"],
        "priority_score": 78,
        "max_offline_evaluations_suggested": 30,
        "heavy_backtest_required": True,
    },
    {
        "family_name": "atr_expansion_continuation_v2",
        "why_this_is_different": "Rebuilds expansion logic around ATR state and post-trigger validation, not the rejected London-US fakeout path.",
        "symbols_to_test": ["NAS100", "US500", "BTCUSD", "ETHUSD"],
        "timeframes_to_test": ["M30", "H1"],
        "expected_failure_risks": ["monte_carlo_fragility", "unstable_deep_sample"],
        "priority_score": 74,
        "max_offline_evaluations_suggested": 28,
        "heavy_backtest_required": True,
    },
    {
        "family_name": "liquidity_sweep_reversal_v2",
        "why_this_is_different": "Focuses on failed stop-run reversal; it should not reuse rejected EMA reclaim or London-US breakout triggers.",
        "symbols_to_test": ["BTCUSD", "US500", "NAS100", "GBPUSD"],
        "timeframes_to_test": ["M15", "M30", "H1"],
        "expected_failure_risks": ["monte_carlo_fragility", "fragile_regime_dependency"],
        "priority_score": 68,
        "max_offline_evaluations_suggested": 24,
        "heavy_backtest_required": True,
    },
    {
        "family_name": "ema_slope_pullback",
        "why_this_is_different": "Uses slope and pullback structure, but BTCUSD H1 EMA reclaim failure keeps this lower priority.",
        "symbols_to_test": ["US500", "NAS100", "EURUSD", "GBPUSD"],
        "timeframes_to_test": ["M30", "H1"],
        "expected_failure_risks": ["sibling_of_failed_profile", "total_pf_weakness"],
        "priority_score": 56,
        "max_offline_evaluations_suggested": 18,
        "heavy_backtest_required": True,
    },
    {
        "family_name": "mean_reversion_after_news_shock",
        "why_this_is_different": "Requires an event-volatility context and should only run if processed news/event features exist.",
        "symbols_to_test": ["XAUUSD", "EURUSD", "GBPUSD", "US500"],
        "timeframes_to_test": ["M15", "M30"],
        "expected_failure_risks": ["data_availability", "spread_sensitivity"],
        "priority_score": 50,
        "max_offline_evaluations_suggested": 12,
        "heavy_backtest_required": False,
    },
)

_REJECTED_FAMILY_MARKERS = (
    "volatility_breakout",
    "vol_breakout",
    "recent_session_open_continuation",
    "session_open_continuation",
    "recent_ema_reclaim",
    "ema_reclaim",
    "recent_london_us_breakout",
    "london_us_breakout",
    "opening_range_fakeout",
)


def run_research_intelligence_core(
    *,
    result_paths: list[str | Path] | None = None,
    search_root: str | Path | None = None,
    load_default_sources: bool = True,
    rotation_result: dict[str, Any] | None = None,
    expansion_result: dict[str, Any] | None = None,
    discovery_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    paths = [Path(path) for path in result_paths or []]

    rotation = rotation_result or run_paper_forward_candidate_rotation(
        result_paths=paths,
        search_root=search_root,
        load_default_sources=load_default_sources,
    )
    expansion = expansion_result or run_paper_forward_research_expansion(
        result_paths=paths,
        search_root=search_root,
        load_default_sources=load_default_sources,
    )
    discovery = discovery_result or run_new_family_edge_discovery(
        result_paths=paths,
        search_root=search_root,
        load_default_sources=load_default_sources,
        include_offline_backtests=False,
    )

    degradation_registry = forward_profile_degradation_registry_status()
    rejection_registry = research_rejection_registry_status()
    rejected_clusters = _rejected_clusters(degradation_registry, rejection_registry)
    observed_rows = _observed_rows(rotation, expansion, discovery)
    failure_patterns = _failure_patterns(rejected_clusters, observed_rows)
    avoid_next = _avoid_next(rejected_clusters, failure_patterns)
    unresolved = _unresolved_opportunities(discovery, expansion, rotation)
    research_gaps = _research_gaps(discovery, failure_patterns)
    next_hypotheses = _next_hypotheses(failure_patterns)
    priority_queue = _priority_queue(next_hypotheses)

    return {
        "ok": True,
        "status": "research_intelligence_core_ready",
        "core_version": CORE_VERSION,
        "mode": "processed_sources_and_registries_only",
        "recommendation": "research_plan_ready",
        "recommended_next_research_phase": _recommended_phase(priority_queue),
        "rejected_clusters": rejected_clusters,
        "failure_patterns": failure_patterns,
        "avoid_next": avoid_next,
        "unresolved_opportunities": unresolved,
        "research_gaps": research_gaps,
        "next_hypotheses": next_hypotheses,
        "priority_queue": priority_queue,
        "source_summaries": {
            "rotation": _source_summary(rotation),
            "research_expansion": _source_summary(expansion),
            "new_family_discovery": _source_summary(discovery),
            "degradation_registry_count": degradation_registry.get("count") or 0,
            "research_rejection_registry_count": rejection_registry.get("count") or 0,
        },
        "offline_backtests_run": False,
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


def _rejected_clusters(degradation_registry: dict[str, Any], rejection_registry: dict[str, Any]) -> list[dict[str, Any]]:
    clusters: list[dict[str, Any]] = []
    for item in degradation_registry.get("degraded_profiles") or []:
        reason = str(item.get("degradation_reason") or "")
        clusters.append(
            {
                "symbol": item.get("symbol") or "",
                "timeframe": item.get("timeframe") or "",
                "profile_or_pattern": item.get("profile") or "",
                "source": "degradation_registry",
                "rejection_status": item.get("status") or "observation_only",
                "rejection_reason": reason,
                "failure_categories": _classify_reason(reason),
                "allow_future_research": False,
                "allow_manual_override": False,
                "applies_to_real_trading": False,
            }
        )
    for item in rejection_registry.get("research_rejections") or []:
        reason = str(item.get("rejection_reason") or "")
        clusters.append(
            {
                "symbol": item.get("symbol") or "",
                "timeframe": item.get("timeframe") or "",
                "profile_or_pattern": ",".join(item.get("family_profile_patterns") or []),
                "source": "research_rejection_registry",
                "rejection_status": item.get("rejection_status") or "",
                "rejection_reason": reason,
                "failure_categories": _classify_reason(reason),
                "allow_future_research": bool(item.get("allow_future_research")),
                "allow_manual_override": bool(item.get("allow_manual_override")),
                "applies_to_real_trading": bool(item.get("applies_to_real_trading")),
            }
        )
    return clusters


def _observed_rows(rotation: dict[str, Any], expansion: dict[str, Any], discovery: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source_name, source in (
        ("candidate_rotation", rotation),
        ("research_expansion", expansion),
        ("new_family_discovery", discovery),
    ):
        for key in ("ranking", "near_misses", "top_near_misses", "excluded_by_registry_or_sibling_risk", "excluded_by_research_rejection_registry"):
            value = source.get(key)
            if not isinstance(value, list):
                continue
            for row in value:
                if isinstance(row, dict):
                    rows.append({**row, "intelligence_source": source_name})
    return rows


def _failure_patterns(rejected_clusters: list[dict[str, Any]], observed_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    examples: dict[str, list[str]] = {}
    for cluster in rejected_clusters:
        label = _cluster_label(cluster)
        for category in cluster.get("failure_categories") or []:
            counter[category] += 1
            examples.setdefault(category, []).append(label)
    for row in observed_rows:
        reasons = list(row.get("rejection_reasons") or row.get("failed_gates") or [])
        if row.get("candidate_status") == "excluded_by_research_rejection_registry":
            reasons.append(str(row.get("research_rejection_reason") or "research_rejection_registry"))
        if row.get("candidate_status") == "excluded_by_degradation_registry":
            reasons.append(str(row.get("degradation_reason") or "degraded_forward_profile"))
        if row.get("sibling_risk"):
            reasons.append(str(row.get("sibling_risk_reason") or "sibling_risk"))
        for reason in reasons:
            if _ignored_reason(str(reason)):
                continue
            for category in _classify_reason(str(reason)):
                counter[category] += 1
                examples.setdefault(category, []).append(_row_label(row))

    return [
        {
            "category": category,
            "count": count,
            "examples": _dedupe(examples.get(category, []))[:8],
        }
        for category, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def _avoid_next(rejected_clusters: list[dict[str, Any]], failure_patterns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    category_set = {row["category"] for row in failure_patterns}
    avoid: list[dict[str, Any]] = []
    for cluster in rejected_clusters:
        avoid.append(
            {
                "scope": f"{cluster['symbol']} {cluster['timeframe']} {cluster['profile_or_pattern']}",
                "reason": cluster["rejection_reason"],
                "recommended_action": "do_not_recommend_as_clean_candidate",
            }
        )
    if {"monte_carlo_fragility", "remove_best_dependency"} <= category_set:
        avoid.append(
            {
                "scope": "filter-tightened variants of failed families",
                "reason": "Monte Carlo and remove-best failures usually need a different edge, not tighter filters.",
                "recommended_action": "prefer_new_family_design",
            }
        )
    return avoid


def _unresolved_opportunities(
    discovery: dict[str, Any],
    expansion: dict[str, Any],
    rotation: dict[str, Any],
) -> list[dict[str, Any]]:
    opportunities: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for source_name, rows in (
        ("new_family_discovery", discovery.get("top_near_misses") or discovery.get("ranking") or []),
        ("research_expansion", expansion.get("near_misses") or []),
        ("candidate_rotation", rotation.get("ranking") or []),
    ):
        for row in rows:
            if not isinstance(row, dict) or _is_rejected_row(row):
                continue
            key = (str(row.get("symbol") or ""), str(row.get("timeframe") or ""), str(row.get("profile") or ""))
            if key in seen:
                continue
            seen.add(key)
            status = str(row.get("candidate_status") or "")
            reasons = list(row.get("rejection_reasons") or row.get("failed_gates") or [])
            if status == "paper_forward_review_ready":
                action = "human_review_only_no_activation"
            elif "recent_closed_below_15" in reasons and not any("monte_carlo" in str(reason) for reason in reasons):
                action = "watchlist_revisit_later"
            else:
                action = "diagnostic_only"
            opportunities.append(
                {
                    "source": source_name,
                    "symbol": row.get("symbol") or "",
                    "timeframe": row.get("timeframe") or "",
                    "profile": row.get("profile") or "",
                    "family": row.get("family") or row.get("conceptual_family") or "",
                    "candidate_status": status,
                    "rejection_reasons": reasons,
                    "recommended_action": action,
                }
            )
    return opportunities[:12]


def _research_gaps(discovery: dict[str, Any], failure_patterns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    for item in discovery.get("skipped_family_ideas") or []:
        if not isinstance(item, dict):
            continue
        gaps.append(
            {
                "gap": item.get("family") or "",
                "reason": item.get("reason") or "",
                "next_step": item.get("next_step") or "",
            }
        )
    if any(row["category"] == "unstable_deep_sample" for row in failure_patterns):
        gaps.append(
            {
                "gap": "deep_sample_stability_protocol",
                "reason": "Deep samples helped expose instability but should be a validation layer, not a candidate generator.",
                "next_step": "keep processed-source scan fast; run deep validation only after a non-rejected family near-misses.",
            }
        )
    if not gaps:
        gaps.append(
            {
                "gap": "processed_results_refresh",
                "reason": "No explicit skipped-family gaps were reported.",
                "next_step": "refresh processed summaries before any heavy offline run.",
            }
        )
    return gaps


def _next_hypotheses(failure_patterns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    categories = {row["category"] for row in failure_patterns}
    hypotheses: list[dict[str, Any]] = []
    for template in _HYPOTHESIS_TEMPLATES:
        if _is_rejected_family_name(template["family_name"]):
            continue
        priority = int(template["priority_score"])
        if "monte_carlo_fragility" in categories and template["family_name"] in {"session_vwap_reclaim", "multi_timeframe_trend_pullback"}:
            priority += 4
        if "remove_best_dependency" in categories and template["family_name"] in {"rsi_divergence_confirmation", "session_vwap_reclaim"}:
            priority += 3
        if "single_trade_dependency" in categories and template["family_name"] == "liquidity_sweep_reversal_v2":
            priority -= 8
        if "sibling_of_failed_profile" in categories and template["family_name"] == "ema_slope_pullback":
            priority -= 8
        hypotheses.append(
            {
                **template,
                "required_metrics": dict(_REQUIRED_METRICS),
                "priority_score": max(1, min(priority, 100)),
            }
        )
    hypotheses.sort(key=lambda row: (-int(row["priority_score"]), row["family_name"]))
    return hypotheses


def _priority_queue(hypotheses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "rank": index + 1,
            "family_name": item["family_name"],
            "priority_score": item["priority_score"],
            "recommended_next_action": "design_processed_feature_scan_no_activation",
            "heavy_backtest_required": item["heavy_backtest_required"],
            "max_offline_evaluations_suggested": item["max_offline_evaluations_suggested"],
        }
        for index, item in enumerate(hypotheses)
    ]


def _recommended_phase(priority_queue: list[dict[str, Any]]) -> str:
    if not priority_queue:
        return "refresh_processed_results_before_research"
    top = priority_queue[0]["family_name"]
    return f"design_{top}_processed_feature_scan"


def _classify_reason(reason: str) -> list[str]:
    text = reason.casefold()
    categories: list[str] = []
    if "recent_closed" in text or "total_closed" in text or "closed_below" in text or "recent sample" in text:
        categories.append("insufficient_recent_sample")
    if "monte_carlo" in text or "_mc_" in text or " mc" in text:
        categories.append("monte_carlo_fragility")
    if "remove_best" in text:
        categories.append("remove_best_dependency")
    if "spread" in text:
        categories.append("spread_sensitivity")
    if "fragile_regime" in text or "regime" in text:
        categories.append("fragile_regime_dependency")
    if "single_trade" in text or "dependency_gates" in text:
        categories.append("single_trade_dependency")
    if "sibling" in text:
        categories.append("sibling_of_failed_profile")
    if "degrad" in text or "early_forward_edge_failed" in text:
        categories.append("degraded_forward_profile")
    if "total_pf" in text or "total pf" in text or "failed_pf" in text or "expectancy_not_positive" in text:
        categories.append("total_pf_weakness")
    if "recent_pf" in text or "recent pf" in text:
        categories.append("recent_pf_weakness")
    if "deep" in text or "london_us_breakout_failed" in text or "opening_range_fakeout" in text:
        categories.append("unstable_deep_sample")
    return _dedupe(categories) or ["unclassified_failure"]


def _ignored_reason(reason: str) -> bool:
    return reason.casefold().strip() in {"research_rejection_registry"}


def _source_summary(source: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": source.get("status") or "",
        "recommendation": source.get("recommendation") or "",
        "useful_rows": source.get("useful_rows") or 0,
        "loaded_sources": len(source.get("loaded_sources") or []),
        "ranking_rows": len(source.get("ranking") or []),
        "offline_backtests_run": bool(source.get("offline_backtests_run")),
    }


def _is_rejected_row(row: dict[str, Any]) -> bool:
    return bool(row.get("degraded_by_registry") or row.get("rejected_by_research_registry") or row.get("sibling_risk"))


def _is_rejected_family_name(name: str) -> bool:
    lowered = name.casefold()
    return any(marker in lowered for marker in _REJECTED_FAMILY_MARKERS)


def _cluster_label(cluster: dict[str, Any]) -> str:
    return f"{cluster.get('symbol')} {cluster.get('timeframe')} {cluster.get('profile_or_pattern')}"


def _row_label(row: dict[str, Any]) -> str:
    return f"{row.get('symbol')} {row.get('timeframe')} {row.get('profile') or row.get('family')}"


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
