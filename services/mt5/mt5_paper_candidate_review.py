from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation
from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore
from services.mt5.mt5_research_rejection_registry import research_rejection


PAPER_CANDIDATE_REVIEW_VERSION = "2026-06-12.mt5_paper_candidate_review.v1"
MIN_TRADES_FORWARD_FOR_OBSERVATION = 20


def review_paper_candidate(
    candidate: dict[str, Any],
    *,
    active_profiles: list[dict[str, Any]] | dict[str, Any] | None = None,
    capital_state: str = "",
    adaptive_state: str = "",
    risk_allowed: bool | None = None,
    persist_review: bool = True,
    store: MT5PersistentIntelligenceStore | Any | None = None,
) -> dict[str, Any]:
    """Prepare a paper-only review record without activating the candidate."""

    raw = dict(candidate or {})
    symbol = _symbol(raw.get("symbol"))
    timeframe = _timeframe(raw.get("timeframe"))
    profile_before = str(raw.get("profile") or raw.get("strategy_profile") or "unknown_profile").strip() or "unknown_profile"
    profile_after = resolve_paper_review_profile_name(raw)
    family = _infer_family(raw, profile_after)
    metrics = _candidate_metrics(raw)
    degraded = bool(forward_profile_degradation(symbol, timeframe, profile_after))
    rejected = bool(research_rejection(symbol, timeframe, profile_after, family))
    sibling = bool(raw.get("sibling_risk"))
    active_context = active_context_review(
        raw,
        active_profiles=active_profiles,
        candidate_profile_name=profile_after,
    )
    gates = _review_to_observation_gates(
        metrics,
        degraded_by_registry=degraded,
        rejected_by_research_registry=rejected,
        sibling_risk=sibling,
        capital_state=capital_state,
        adaptive_state=adaptive_state,
        risk_allowed=risk_allowed,
    )
    min_sample_gate = gates[0] if gates else {}
    can_create_review = bool(symbol and timeframe and profile_after)
    persist_result: dict[str, Any] = {"attempted": False, "ok": False, "reason": "persist_review_disabled"}
    if persist_review and can_create_review:
        persist_result = _persist_paper_review(
            store or MT5PersistentIntelligenceStore(),
            symbol=symbol,
            timeframe=timeframe,
            profile=profile_after,
        )
    return {
        "ok": True,
        "status": "paper_candidate_review_ready",
        "review_version": PAPER_CANDIDATE_REVIEW_VERSION,
        "mode": "paper_review_only",
        "decision": "NO_TRADE",
        "reason": "paper_candidate_review_no_activation",
        "symbol": symbol,
        "timeframe": timeframe,
        "candidate_profile_before": profile_before,
        "candidate_profile_after": profile_after,
        "candidate_profile_name": profile_after,
        "family": family,
        **metrics,
        "active_context_status": active_context["active_context_status"],
        "active_context_review": active_context,
        "paper_candidate_review_created": can_create_review,
        "persistent_review_write": persist_result,
        "persistent_review_write_ok": bool(persist_result.get("ok")),
        "min_sample_gate": min_sample_gate,
        "review_to_observation_gates": gates,
        "review_to_observation_ready": all(bool(gate.get("passed")) for gate in gates),
        "degraded_by_registry": degraded,
        "rejected_by_research_registry": rejected,
        "sibling_risk": sibling,
        "can_activate": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "paper_rotation_applied": False,
        "applies_to_paper_shadow": False,
        "applies_to_real_trading": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def resolve_paper_review_profile_name(candidate: dict[str, Any]) -> str:
    symbol = _symbol(candidate.get("symbol")).lower() or "unknown_symbol"
    timeframe = _timeframe(candidate.get("timeframe")).lower() or "unknown_timeframe"
    raw_profile = str(candidate.get("profile") or candidate.get("strategy_profile") or "").strip()
    if raw_profile and raw_profile.casefold() not in {"unknown", "unknown_profile", "none", "null"}:
        return _slug(raw_profile)
    family = _slug(_infer_family(candidate, raw_profile) or "tournament_edge")
    variant = _slug(_variant(candidate) or "candidate")
    return _slug(f"{symbol}_{timeframe}_{family}_{variant}_paper_review_v1")


def active_context_review(
    candidate: dict[str, Any],
    *,
    active_profiles: list[dict[str, Any]] | dict[str, Any] | None = None,
    candidate_profile_name: str = "",
) -> dict[str, Any]:
    symbol = _symbol(candidate.get("symbol"))
    timeframe = _timeframe(candidate.get("timeframe"))
    rows = _active_rows(active_profiles)
    active = _matching_active_profile(rows, symbol=symbol, timeframe=timeframe)
    missing: list[str] = []
    if not active:
        missing.append("active_profile")
    else:
        if not _symbol(active.get("symbol")):
            missing.append("active_profile_symbol")
        if not _timeframe(active.get("timeframe")):
            missing.append("active_profile_timeframe")
        if not str(active.get("profile") or active.get("name") or "").strip():
            missing.append("active_profile_name")
    return {
        "active_profile_exists": bool(active),
        "active_profile_symbol": _symbol(active.get("symbol")) if active else "",
        "active_profile_timeframe": _timeframe(active.get("timeframe")) if active else "",
        "active_profile_name": str((active or {}).get("profile") or (active or {}).get("name") or ""),
        "candidate_profile_name": candidate_profile_name,
        "missing_active_context_fields": missing,
        "can_create_paper_review_context": bool(symbol and timeframe and candidate_profile_name),
        "can_activate": False,
        "active_context_status": "active_context_ready" if not missing else "paper_rotation_review_missing_active_context",
        **_safety(),
    }


def _persist_paper_review(store: Any, *, symbol: str, timeframe: str, profile: str) -> dict[str, Any]:
    payload = {
        "symbol": symbol,
        "timeframe": timeframe,
        "profile": profile,
        "status": "paper_review",
        "active": False,
        "applies_to_paper_shadow": False,
        "applies_to_real_trading": False,
        "registry_source": "paper_candidate_review",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        result = store.upsert_profile_state(payload, critical=False)
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        result = {"ok": False, "db_degraded": True, "reason": type(exc).__name__, **_safety()}
    return {"attempted": True, **dict(result or {})}


def _review_to_observation_gates(
    metrics: dict[str, Any],
    *,
    degraded_by_registry: bool,
    rejected_by_research_registry: bool,
    sibling_risk: bool,
    capital_state: str,
    adaptive_state: str,
    risk_allowed: bool | None,
) -> list[dict[str, Any]]:
    trades = int(metrics["trades_forward"])
    recent_pf = float(metrics["recent_profit_factor"])
    expectancy = float(metrics["expectancy"])
    win_rate = float(metrics["win_rate"])
    profit_factor = float(metrics["profit_factor"])
    drawdown = metrics.get("max_drawdown")
    remove_best = metrics.get("remove_best_5_pf")
    single_trade_dependency = metrics.get("single_trade_dependency")
    monte_carlo_failure = bool(metrics.get("monte_carlo_failure"))
    mc_pf = metrics.get("monte_carlo_stressed_pf")
    gates = [
        _gate(
            "min_trades_forward",
            trades >= MIN_TRADES_FORWARD_FOR_OBSERVATION,
            f"trades_forward_below_{MIN_TRADES_FORWARD_FOR_OBSERVATION}",
            trades,
            MIN_TRADES_FORWARD_FOR_OBSERVATION,
        ),
        _gate("recent_profit_factor", recent_pf >= 1.15, "recent_profit_factor_below_1_15", recent_pf, 1.15),
        _gate("expectancy_positive", expectancy > 0, "expectancy_not_positive", expectancy, ">0"),
        _gate("win_rate_or_pf_compensation", win_rate >= 45.0 or (profit_factor >= 1.5 and expectancy > 0), "win_rate_not_compensated", win_rate, ">=45_or_pf_expectancy_compensation"),
        _gate("max_drawdown_controlled", drawdown is not None and float(drawdown) <= 10.0, "max_drawdown_missing_or_above_limit", drawdown, "<=10"),
        _gate("single_trade_dependency_absent", single_trade_dependency is False, "single_trade_dependency_unknown_or_true", single_trade_dependency, False),
        _gate("remove_best_dependency_absent", remove_best is not None and float(remove_best) >= 1.0, "remove_best_5_pf_missing_or_below_1", remove_best, ">=1"),
        _gate("monte_carlo_failure_absent", not monte_carlo_failure and (mc_pf is None or float(mc_pf) >= 1.05), "monte_carlo_failure_or_below_1_05", mc_pf, ">=1.05_if_available"),
        _gate("registry_rejection_sibling_clear", not (degraded_by_registry or rejected_by_research_registry or sibling_risk), "registry_rejection_or_sibling_risk", {"degraded": degraded_by_registry, "rejected": rejected_by_research_registry, "sibling": sibling_risk}, False),
        _gate("capital_state_normal", str(capital_state or "").casefold() in {"normal", "green", "ready", "allow_paper_review"}, "capital_state_not_normal", capital_state or "unknown", "normal"),
        _gate("adaptive_governor_allows", str(adaptive_state or "").casefold() not in {"kill_switch", "blocked", "halt"} and bool(adaptive_state), "adaptive_governor_not_confirmed", adaptive_state or "unknown", "allows"),
        _gate("risk_governor_allows", risk_allowed is True, "risk_governor_not_confirmed", risk_allowed, True),
    ]
    return gates


def _gate(name: str, passed: bool, reason: str, current: object, required: object) -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "rejection_reason": "" if passed else reason,
        "current": current,
        "required": required,
        **_safety(),
    }


def _candidate_metrics(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "trades_forward": int(_number(candidate.get("trades_forward") or candidate.get("total_closed") or candidate.get("closed") or 0)),
        "win_rate": round(_number(candidate.get("win_rate") or candidate.get("recent_win_rate")), 6),
        "profit_factor": round(_number(candidate.get("profit_factor") or candidate.get("total_pf")), 6),
        "recent_profit_factor": round(_number(candidate.get("recent_profit_factor") or candidate.get("recent_pf") or candidate.get("profit_factor") or candidate.get("total_pf")), 6),
        "expectancy": round(_number(candidate.get("expectancy")), 8),
        "max_drawdown": _optional_number(candidate.get("max_drawdown") if candidate.get("max_drawdown") is not None else candidate.get("max_drawdown_pct")),
        "monte_carlo_stressed_pf": _optional_number(candidate.get("monte_carlo_stressed_pf")),
        "remove_best_5_pf": _optional_number(candidate.get("remove_best_5_pf")),
        "single_trade_dependency": candidate.get("single_trade_dependency") if isinstance(candidate.get("single_trade_dependency"), bool) else None,
        "monte_carlo_failure": bool(candidate.get("monte_carlo_failure")),
    }


def _matching_active_profile(rows: list[dict[str, Any]], *, symbol: str, timeframe: str) -> dict[str, Any]:
    for row in rows:
        if _symbol(row.get("symbol")) == symbol and _timeframe(row.get("timeframe")) == timeframe:
            return row
    return rows[0] if rows else {}


def _active_rows(value: list[dict[str, Any]] | dict[str, Any] | None) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    return []


def _infer_family(candidate: dict[str, Any], profile: object) -> str:
    for key in ("family", "profile_family", "strategy_family"):
        if candidate.get(key):
            return str(candidate.get(key))
    text = " ".join(str(value or "") for value in (profile, candidate.get("reason"), candidate.get("recommended_action"))).casefold()
    if "session_vwap" in text or "vwap" in text:
        return "session_vwap_reclaim"
    if "trend_pullback" in text:
        return "multi_timeframe_trend_pullback"
    if "ema_reclaim" in text:
        return "ema_reclaim"
    if "london_us" in text or "opening_range" in text:
        return "london_us_breakout"
    if "vol_breakout" in text or "volatility_breakout" in text:
        return "volatility_breakout"
    if "liquidity_sweep" in text:
        return "liquidity_sweep"
    return "tournament_edge"


def _variant(candidate: dict[str, Any]) -> str:
    for key in ("variant", "mode", "candidate_status"):
        value = str(candidate.get(key) or "").strip()
        if value and value.casefold() not in {"unknown", "unknown_profile", "none", "null"}:
            return value
    return "candidate"


def _slug(value: object) -> str:
    text = str(value or "").strip().casefold()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def _symbol(value: object) -> str:
    text = str(value or "").upper().strip().replace(".B", "")
    if text in {"USTECB", "NAS100"}:
        return "USTEC"
    return text


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _number(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _optional_number(value: object) -> float | None:
    if value is None or value == "":
        return None
    return _number(value)


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
