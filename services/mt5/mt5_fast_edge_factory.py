from __future__ import annotations

import time
from collections import Counter, defaultdict
from typing import Any

from services.mt5.mt5_autonomous_research_queue import run_autonomous_research_queue
from services.mt5.mt5_btc_h1_candidate_deep_validation import run_btc_h1_candidate_deep_validation
from services.mt5.mt5_robust_candidate_harvester import run_robust_candidate_harvester


FACTORY_VERSION = "2026-06-12.mt5_fast_edge_factory.v1"

MIN_TOTAL_CLOSED = 50
MIN_RECENT_CLOSED = 20
MIN_TOTAL_PF = 1.15
MIN_RECENT_PF = 1.15

_FACTORY_LINES: tuple[dict[str, Any], ...] = (
    {
        "family_name": "volatility_compression_breakout",
        "priority_score": 100,
        "markers": ("volatility_compression_breakout", "vol_compression_breakout"),
    },
    {
        "family_name": "rsi_divergence_confirmation",
        "priority_score": 92,
        "markers": ("rsi_divergence_confirmation", "rsi_divergence"),
    },
    {
        "family_name": "multi_timeframe_trend_pullback",
        "priority_score": 88,
        "markers": ("multi_timeframe_trend_pullback", "trend_pullback"),
    },
    {
        "family_name": "mean_reversion_after_exhaustion",
        "priority_score": 82,
        "markers": ("mean_reversion_after_exhaustion", "exhaustion_reversion"),
    },
    {
        "family_name": "atr_expansion_continuation_v2",
        "priority_score": 78,
        "markers": ("atr_expansion_continuation_v2", "atr_expansion_continuation"),
    },
    {
        "family_name": "session_filter_high_sample_low_dependency",
        "priority_score": 72,
        "markers": ("session_filter_high_sample_low_dependency", "session_filter", "session_continuation"),
    },
)

_AVOID_MARKERS: tuple[tuple[str, str], ...] = (
    ("ethusd m30 volatility_breakout", "eth_m30_volatility_breakout_cluster"),
    ("ethusd m30 vol_breakout", "eth_m30_volatility_breakout_cluster"),
    ("eth_m30_vol_breakout_chop_guard_v1", "degraded_profile"),
    ("btcusd h1 tournament_edge_candidate", "btc_h1_tournament_edge_candidate_rejected"),
    ("btcusd_h1_tournament_edge_candidate", "btc_h1_tournament_edge_candidate_rejected"),
    ("btcusd h1 recent_liquidity_sweep", "btc_h1_recent_liquidity_sweep_rejected"),
    ("btcusd_h1_recent_liquidity_sweep", "btc_h1_recent_liquidity_sweep_rejected"),
    ("btcusd m30 london_us_breakout", "btc_m30_london_us_opening_range_rejected"),
    ("btcusd m30 opening_range_fakeout", "btc_m30_london_us_opening_range_rejected"),
    ("xauusd m15 recent_session_open_continuation", "xau_m15_session_open_rejected"),
    ("eurusd h1 session_vwap_reclaim", "eurusd_h1_vwap_reclaim_rejected"),
    ("ustec m30 trend_pullback", "ustec_m30_h1_trend_pullback_rejected"),
    ("nas100 m30 trend_pullback", "ustec_m30_h1_trend_pullback_rejected"),
)


def run_fast_edge_factory(
    *,
    run_fast_scans: bool = False,
    deep_validate_candidate: str = "",
    max_evaluations: int = 300,
    processed_source_paths: list[str] | None = None,
    persistent_events: dict[str, Any] | None = None,
    load_persistent: bool = True,
    harvester_result: dict[str, Any] | None = None,
    queue_result: dict[str, Any] | None = None,
    deep_validation_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    safe_max = max(1, min(int(max_evaluations or 300), 1000))
    deep_candidate = str(deep_validate_candidate or "").strip()
    scans_run: list[str] = []

    if run_fast_scans:
        harvester = harvester_result or run_robust_candidate_harvester(
            processed_source_paths=processed_source_paths,
            persistent_events=persistent_events,
            load_persistent=load_persistent and persistent_events is None,
            max_candidates=max(1, min(safe_max, 100)),
        )
        scans_run.append("robust_candidate_harvester_processed_sources")
    else:
        harvester = harvester_result or _empty_harvester()

    queue = queue_result or run_autonomous_research_queue(
        run_fast_scans=run_fast_scans,
        max_evaluations=safe_max,
        processed_source_paths=processed_source_paths,
        persistent_events=persistent_events,
        load_persistent=load_persistent,
        harvester_result=harvester,
    )
    scans_run.append("autonomous_research_queue_readonly")

    deep_result: dict[str, Any] = {}
    if deep_candidate:
        deep_result = deep_validation_result or _deep_validate_one(deep_candidate)
        scans_run.extend(deep_result.get("scans_run") or [])

    evaluated = _evaluated_candidates(harvester, max_evaluations=safe_max) if (run_fast_scans or harvester_result) else []
    unique_evaluated = _dedupe_evaluations(evaluated)
    rejected = [row for row in unique_evaluated if row.get("candidate_status") == "rejected"]
    candidates = [
        row
        for row in unique_evaluated
        if row.get("candidate_status") in {"deep_validation_candidate", "needs_deep_validation"}
    ]
    candidates.sort(key=_candidate_rank)
    top_candidates = candidates[:10]
    recommended = top_candidates[0] if top_candidates else None
    recommendation = "deep_validation_candidate_found" if recommended else "continue_research"
    next_phase = (
        "single_candidate_deep_validation"
        if recommended
        else str(queue.get("recommended_next_research_phase") or _next_factory_line())
    )

    if deep_candidate and deep_result and not deep_result.get("ok", True):
        recommendation = "continue_research"
        next_phase = str(deep_result.get("reason") or "deep_validation_not_run")

    result = {
        "ok": True,
        "status": "fast_edge_factory_ready",
        "factory_version": FACTORY_VERSION,
        "factory_state": _factory_state(run_fast_scans=run_fast_scans, deep_candidate=deep_candidate, deep_result=deep_result),
        "mode": (
            "single_candidate_deep_validation"
            if deep_candidate
            else "fast_processed_batch"
            if run_fast_scans
            else "dry_run_plan_only"
        ),
        "db_state": queue.get("db_state") or {},
        "lessons_loaded": int(queue.get("lessons_loaded") or 0),
        "rejected_families_loaded": int(queue.get("rejected_families_loaded") or 0),
        "degraded_profiles_loaded": int(queue.get("degraded_profiles_loaded") or 0),
        "factory_lines": _factory_lines(),
        "scans_run": _dedupe(scans_run + _line_scans(run_fast_scans)),
        "heavy_backtests_run": bool(deep_result.get("deep_validation_run")),
        "offline_backtests_run": bool(deep_result.get("deep_validation_run")),
        "max_evaluations": safe_max,
        "evaluations_count": len(evaluated),
        "unique_evaluations_count": len(unique_evaluated),
        "max_evaluations_respected": len(evaluated) <= safe_max,
        "avoided_families": queue.get("avoided_families") or [],
        "rejected_summary": _rejected_summary(rejected),
        "top_rejected": _top_rejected(rejected, limit=10),
        "candidates_found": len(candidates),
        "top_candidates": top_candidates,
        "deep_validation_candidates": top_candidates,
        "recommended_next_candidate": recommended,
        "recommended_next_script": _recommended_next_script(recommended, queue),
        "recommended_next_command": _recommended_next_command(recommended),
        "recommended_next_research_phase": next_phase,
        "recommendation": recommendation,
        "deep_validation_result": deep_result,
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


def _evaluated_candidates(harvester: dict[str, Any], *, max_evaluations: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("top_candidates", "rejected_candidates"):
        for row in harvester.get(key) or []:
            if isinstance(row, dict):
                rows.append(row)
            if len(rows) >= max_evaluations:
                break
        if len(rows) >= max_evaluations:
            break
    return [_evaluate_candidate(row) for row in rows]


def _evaluate_candidate(row: dict[str, Any]) -> dict[str, Any]:
    symbol = _symbol(row.get("symbol"))
    timeframe = _timeframe(row.get("timeframe"))
    profile = str(row.get("profile") or row.get("target_name") or "")
    family = str(row.get("family") or row.get("source_family") or profile)
    text = _candidate_text(symbol, timeframe, family, profile)
    reasons: list[str] = []
    factory_line = _factory_line_for(text)
    if not factory_line:
        reasons.append("outside_fast_edge_factory_lines")
    reasons.extend(_avoid_reasons(text))
    if "unknown_profile" in text:
        reasons.append("unknown_profile")
    if not _bool(row.get("source_identity_resolved")):
        reasons.append("source_identity_unresolved")
    _min_gate(reasons, row, ("total_closed", "closed", "trades"), MIN_TOTAL_CLOSED, "total_closed_below_50")
    _min_gate(reasons, row, ("recent_closed", "recent_trades", "closed_recent"), MIN_RECENT_CLOSED, "recent_closed_below_20")
    _min_gate(reasons, row, ("total_pf", "profit_factor"), MIN_TOTAL_PF, "total_pf_below_1_15")
    _min_gate(reasons, row, ("recent_pf", "recent_profit_factor"), MIN_RECENT_PF, "recent_pf_below_1_15")
    if _number(_pick(row, ("expectancy", "total_expectancy"))) <= 0:
        reasons.append("expectancy_not_positive")
    if _number(row.get("recent_expectancy")) <= 0:
        reasons.append("recent_expectancy_not_positive")
    if _has_value(row, "spread_x2_pf") and _number(row.get("spread_x2_pf")) < 0.95:
        reasons.append("spread_x2_pf_below_0_95")
    if _has_value(row, "remove_best_5_pf") and _number(row.get("remove_best_5_pf")) < 1.0:
        reasons.append("remove_best_5_pf_below_1")
    if _has_value(row, "monte_carlo_stressed_pf") and _number(row.get("monte_carlo_stressed_pf")) < 1.05:
        reasons.append("monte_carlo_stressed_pf_below_1_05")
    if _has_value(row, "single_trade_dependency") and _bool(row.get("single_trade_dependency")):
        reasons.append("single_trade_dependency")
    if _has_value(row, "fragile_regime_dependency") and _bool(row.get("fragile_regime_dependency")):
        reasons.append("fragile_regime_dependency")
    if _bool(row.get("degraded_by_registry") or row.get("registry_degraded")):
        reasons.append("degraded_by_registry")
    if _bool(row.get("rejected_by_research_registry") or row.get("research_rejection_registry")):
        reasons.append("rejected_by_research_registry")
    if _bool(row.get("sibling_risk")):
        reasons.append("sibling_risk")

    reasons = _dedupe(reasons)
    mc_available = _has_value(row, "monte_carlo_stressed_pf")
    if reasons:
        status = "rejected"
        action = "skip_rejected_or_dead_family"
    elif mc_available:
        status = "deep_validation_candidate"
        action = "run_single_deep_validation"
    else:
        status = "needs_deep_validation"
        action = "run_single_deep_validation_missing_mc"

    return {
        "candidate_id": _candidate_id(symbol, timeframe, profile or family),
        "symbol": symbol,
        "timeframe": timeframe,
        "family": factory_line or family,
        "source_family": family,
        "profile": profile,
        "source": row.get("source") or "",
        "source_identity_resolved": _bool(row.get("source_identity_resolved")),
        "total_closed": int(_number(_pick(row, ("total_closed", "closed", "trades")))),
        "recent_closed": int(_number(_pick(row, ("recent_closed", "recent_trades", "closed_recent")))),
        "total_pf": _number(_pick(row, ("total_pf", "profit_factor"))),
        "recent_pf": _number(_pick(row, ("recent_pf", "recent_profit_factor"))),
        "expectancy": _number(_pick(row, ("expectancy", "total_expectancy"))),
        "recent_expectancy": _number(row.get("recent_expectancy")),
        "monte_carlo_stressed_pf": _optional_number(row.get("monte_carlo_stressed_pf")),
        "spread_x2_pf": _optional_number(row.get("spread_x2_pf")),
        "remove_best_5_pf": _optional_number(row.get("remove_best_5_pf")),
        "single_trade_dependency": _optional_bool(row.get("single_trade_dependency")),
        "fragile_regime_dependency": _optional_bool(row.get("fragile_regime_dependency")),
        "degraded_by_registry": _bool(row.get("degraded_by_registry") or row.get("registry_degraded")),
        "rejected_by_research_registry": _bool(row.get("rejected_by_research_registry") or row.get("research_rejection_registry")),
        "sibling_risk": _bool(row.get("sibling_risk")),
        "rejection_reasons": reasons,
        "rejection_signature": _rejection_signature(reasons),
        "candidate_status": status,
        "recommended_next_action": action,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _dedupe_evaluations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str, str, str]] = set()
    output: list[dict[str, Any]] = []
    for row in rows:
        key = (
            str(row.get("symbol") or ""),
            str(row.get("timeframe") or ""),
            str(row.get("family") or ""),
            str(row.get("profile") or ""),
            str(row.get("rejection_signature") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def _rejected_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("family") or "unknown_family")].append(row)
    summary: list[dict[str, Any]] = []
    for family, family_rows in grouped.items():
        reason_counts = Counter(reason for row in family_rows for reason in (row.get("rejection_reasons") or []))
        summary.append(
            {
                "family": family,
                "rejected_count": len(family_rows),
                "top_rejection_reasons": [
                    {"reason": reason, "count": count}
                    for reason, count in reason_counts.most_common(5)
                ],
            }
        )
    summary.sort(key=lambda row: (-int(row.get("rejected_count") or 0), str(row.get("family") or "")))
    return summary


def _top_rejected(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    rows = sorted(rows, key=lambda row: (len(row.get("rejection_reasons") or []), str(row.get("family") or ""), str(row.get("profile") or "")))
    return rows[:limit]


def _factory_state(*, run_fast_scans: bool, deep_candidate: str, deep_result: dict[str, Any]) -> str:
    if deep_candidate:
        return "single_candidate_deep_validation_completed" if deep_result.get("deep_validation_run") else "single_candidate_deep_validation_plan"
    if run_fast_scans:
        return "fast_batch_completed"
    return "dry_run_plan"


def _factory_lines() -> list[dict[str, Any]]:
    return [
        {
            "family_name": row["family_name"],
            "priority_score": row["priority_score"],
            "candidate_activated": False,
            **_safety(),
        }
        for row in _FACTORY_LINES
    ]


def _line_scans(run_fast_scans: bool) -> list[str]:
    if not run_fast_scans:
        return []
    return [f"fast_line_gate:{row['family_name']}" for row in _FACTORY_LINES]


def _factory_line_for(text: str) -> str:
    lowered = text.casefold()
    for row in _FACTORY_LINES:
        if any(marker in lowered for marker in row["markers"]):
            return str(row["family_name"])
    return ""


def _avoid_reasons(text: str) -> list[str]:
    lowered = text.casefold()
    reasons: list[str] = []
    for marker, reason in _AVOID_MARKERS:
        if marker in lowered:
            reasons.append(reason)
    return _dedupe(reasons)


def _deep_validate_one(candidate_id: str) -> dict[str, Any]:
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
            **_safety(),
        }
    return {
        "ok": True,
        "status": "deep_validation_dispatch_plan_ready",
        "candidate": candidate_id,
        "scans_run": ["deep_validation_dispatch_plan_only"],
        "deep_validation_run": False,
        "reason": "no_registered_deep_validator_for_candidate",
        **_safety(),
    }


def _recommended_next_script(candidate: dict[str, Any] | None, queue: dict[str, Any]) -> str:
    if not candidate:
        return str(queue.get("recommended_next_script") or "python scripts/run_autonomous_research_queue.py")
    return f"run_{candidate.get('candidate_id')}_deep_validation.py"


def _recommended_next_command(candidate: dict[str, Any] | None) -> str:
    if not candidate:
        return ""
    candidate_id = str(candidate.get("candidate_id") or "")
    if candidate_id.startswith("btcusd_h1"):
        return f"python scripts/run_btc_h1_candidate_deep_validation.py --targets {candidate_id}"
    return f"python scripts/run_fast_edge_factory.py --deep-validate-candidate {candidate_id}"


def _next_factory_line() -> str:
    return str(_FACTORY_LINES[0]["family_name"])


def _candidate_rank(row: dict[str, Any]) -> tuple[int, float, str]:
    status_rank = 0 if row.get("candidate_status") == "deep_validation_candidate" else 1
    score = 0.0
    score += min(int(row.get("total_closed") or 0), 250) * 0.5
    score += min(int(row.get("recent_closed") or 0), 100) * 2.0
    score += max(0.0, float(row.get("recent_pf") or 0.0) - 1.0) * 100.0
    score += max(0.0, float(row.get("total_pf") or 0.0) - 1.0) * 90.0
    if row.get("monte_carlo_stressed_pf") is not None:
        score += max(0.0, float(row.get("monte_carlo_stressed_pf") or 0.0) - 1.0) * 120.0
    if row.get("remove_best_5_pf") is not None:
        score += max(0.0, float(row.get("remove_best_5_pf") or 0.0) - 1.0) * 100.0
    return (status_rank, -score, str(row.get("candidate_id") or ""))


def _min_gate(reasons: list[str], row: dict[str, Any], keys: tuple[str, ...], minimum: float, reason: str) -> None:
    if _number(_pick(row, keys)) < minimum:
        reasons.append(reason)


def _candidate_text(symbol: str, timeframe: str, family: str, profile: str) -> str:
    return f"{symbol} {timeframe} {family} {profile}".casefold()


def _candidate_id(symbol: str, timeframe: str, profile: str) -> str:
    raw = "_".join(part for part in (symbol, timeframe, profile) if part)
    return raw.casefold().replace(" ", "_").replace("|", "_").replace("/", "_")


def _rejection_signature(reasons: list[str]) -> str:
    return ",".join(sorted(reasons))


def _pick(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in {None, ""}:
            return value
    return None


def _has_value(row: dict[str, Any], key: str) -> bool:
    return row.get(key) not in {None, ""}


def _optional_number(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    return _number(value)


def _optional_bool(value: Any) -> bool | None:
    if value in {None, ""}:
        return None
    return _bool(value)


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


def _symbol(value: Any) -> str:
    text = str(value or "").upper().strip().replace(".B", "")
    if text in {"USTECB", "NAS100"}:
        return "USTEC"
    return text


def _timeframe(value: Any) -> str:
    return str(value or "").upper().strip()


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def _empty_harvester() -> dict[str, Any]:
    return {
        "ok": True,
        "status": "robust_candidate_harvester_not_run",
        "recommendation": "continue_research",
        "loaded_sources": [],
        "missing_sources": [],
        "raw_rows": 0,
        "useful_rows": 0,
        "top_candidates": [],
        "rejected_candidates": [],
        **_safety(),
    }


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
