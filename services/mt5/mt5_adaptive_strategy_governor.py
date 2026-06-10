from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation
from services.mt5.mt5_paper_forward_candidate_rotation import run_paper_forward_candidate_rotation
from services.mt5.mt5_research_intelligence_core import run_research_intelligence_core
from services.mt5.mt5_research_rejection_registry import research_rejection


GOVERNOR_VERSION = "2026-06-10.mt5_adaptive_strategy_governor.v1"

DEFAULT_LIMITS = {
    "max_daily_loss_shadow": -5.0,
    "max_weekly_loss_shadow": -12.0,
    "max_consecutive_losses_global": 5,
    "max_open_shadow_trades": 3,
    "max_profile_drawdown": 3.0,
    "max_recent_error_rate": 0.75,
}

_REVIEW_STATUSES = {
    "paper_forward_candidate_review",
    "paper_forward_review_ready",
    "hardening_candidate_found",
}

_BLOCKING_GLOBAL_STATES = {
    "kill_switch",
    "pause_new_entries",
    "degrade_to_observation_only",
    "observation_only",
    "no_trade",
}

_BLOCKING_RECOMMENDED_ACTIONS = {
    "kill_switch",
    "pause_new_entries",
    "degrade_to_observation_only",
    "observation_only",
    "skip_rejected_family",
    "continue_research",
}


def run_adaptive_strategy_governor(
    *,
    closed_trades: list[dict[str, Any]] | None = None,
    open_trades: list[dict[str, Any]] | None = None,
    rotation_result: dict[str, Any] | None = None,
    intelligence_result: dict[str, Any] | None = None,
    runtime_snapshot: dict[str, Any] | None = None,
    limits: dict[str, Any] | None = None,
    load_shadow_snapshot: bool = True,
    load_rotation: bool = True,
    load_intelligence: bool = True,
) -> dict[str, Any]:
    active_limits = {**DEFAULT_LIMITS, **(limits or {})}
    shadow_snapshot = _load_shadow_snapshot(load_shadow_snapshot, closed_trades, open_trades)
    closed = list(closed_trades if closed_trades is not None else shadow_snapshot.get("closed_trades") or [])
    open_ = list(open_trades if open_trades is not None else shadow_snapshot.get("open_trades") or [])
    runtime = runtime_snapshot or {}

    profiles = _profile_states(closed, open_, active_limits)
    circuit_breakers = _circuit_breakers(closed, open_, profiles, runtime, active_limits)
    critical_breakers = [row for row in circuit_breakers if row.get("active") and row.get("critical")]
    missing_data_breakers = [row for row in circuit_breakers if row.get("active") and row.get("name") == "missing_shadow_trade_data"]

    rotation = rotation_result
    if rotation is None and load_rotation:
        rotation = _safe_rotation()
    intelligence = intelligence_result
    if intelligence is None and load_intelligence:
        intelligence = _safe_intelligence(rotation)

    rotation_candidates, rejected_candidates = _rotation_candidates(rotation or {})
    if critical_breakers:
        global_state = "kill_switch"
        recommended_next_action = "kill_switch"
    elif missing_data_breakers:
        global_state = "no_trade"
        recommended_next_action = "continue_research"
        rotation_candidates = []
    elif any(row["recommended_action"] == "degrade_to_observation_only" for row in profiles):
        global_state = "degrade_to_observation_only"
        recommended_next_action = "rotate_candidate_review" if rotation_candidates else "continue_research"
    elif any(row["recommended_action"] == "pause_new_entries" for row in profiles):
        global_state = "pause_new_entries"
        recommended_next_action = "rotate_candidate_review" if rotation_candidates else "continue_research"
    elif rotation_candidates:
        global_state = "watch"
        recommended_next_action = "rotate_candidate_review"
    else:
        global_state = "watch" if profiles else "no_trade"
        recommended_next_action = _next_research_action(intelligence)

    active_profiles = [row for row in profiles if row["active_state"] == "active"]
    paused_profiles = [row for row in profiles if row["active_state"] == "paused"]
    degraded_profiles = [row for row in profiles if row["active_state"] == "observation_only"]

    return {
        "ok": True,
        "status": "adaptive_strategy_governor_ready",
        "governor_version": GOVERNOR_VERSION,
        "mode": "paper_shadow_only",
        "decision": "NO_TRADE",
        "reason": _decision_reason(global_state, critical_breakers, missing_data_breakers),
        "global_state": global_state,
        "active_profiles": active_profiles,
        "paused_profiles": paused_profiles,
        "degraded_profiles": degraded_profiles,
        "profile_states": profiles,
        "rotation_candidates": rotation_candidates,
        "rejected_candidates": rejected_candidates,
        "circuit_breakers": circuit_breakers,
        "recommended_next_action": recommended_next_action,
        "rotation_recommendation": (rotation or {}).get("recommendation") or "",
        "research_recommendation": (intelligence or {}).get("recommended_next_research_phase") or "",
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
        "safety_state": _safety_state(),
        **_safety(),
    }


def adaptive_governor_enforcement(
    *,
    symbol: str,
    timeframe: str = "",
    profile: str = "",
    governor_result: dict[str, Any] | None = None,
    closed_trades: list[dict[str, Any]] | None = None,
    open_trades: list[dict[str, Any]] | None = None,
    runtime_snapshot: dict[str, Any] | None = None,
    limits: dict[str, Any] | None = None,
    load_shadow_snapshot: bool = True,
) -> dict[str, Any]:
    clean_symbol = _symbol(symbol)
    clean_timeframe = _timeframe(timeframe)
    clean_profile = str(profile or "").strip()
    governor = governor_result or run_adaptive_strategy_governor(
        closed_trades=closed_trades,
        open_trades=open_trades,
        runtime_snapshot=runtime_snapshot,
        limits=limits,
        load_shadow_snapshot=load_shadow_snapshot,
        load_rotation=False,
        load_intelligence=False,
    )
    circuit_breakers = governor.get("circuit_breakers") if isinstance(governor.get("circuit_breakers"), list) else []
    direct_degradation = forward_profile_degradation(clean_symbol, clean_timeframe, clean_profile) if clean_profile else {}
    direct_rejection = research_rejection(clean_symbol, clean_timeframe, clean_profile, _infer_family(clean_profile)) if clean_profile else {}
    sibling_risk = _matching_sibling_risk(governor, clean_symbol, clean_timeframe, clean_profile)

    reason = ""
    blocking_action = ""
    if _active_breaker(circuit_breakers, "max_open_shadow_trades"):
        reason = "adaptive_governor:max_open_shadow_trades"
        blocking_action = "kill_switch"
    elif str(governor.get("global_state") or "") == "kill_switch":
        reason = "adaptive_governor:kill_switch"
        blocking_action = "kill_switch"
    elif direct_degradation:
        reason = "adaptive_governor:observation_only"
        blocking_action = "observation_only"
    elif direct_rejection:
        reason = "adaptive_governor:skip_rejected_family"
        blocking_action = "skip_rejected_family"
    elif sibling_risk:
        reason = "adaptive_governor:sibling_risk"
        blocking_action = "skip_sibling_risk"
    elif str(governor.get("global_state") or "") in _BLOCKING_GLOBAL_STATES:
        state = str(governor.get("global_state") or "no_trade")
        reason = "adaptive_governor:missing_data" if state == "no_trade" else f"adaptive_governor:{state}"
        blocking_action = state
    elif str(governor.get("recommended_next_action") or "") in _BLOCKING_RECOMMENDED_ACTIONS:
        action = str(governor.get("recommended_next_action") or "")
        reason = f"adaptive_governor:{action}"
        blocking_action = action

    blocked = bool(reason)
    return {
        "ok": True,
        "status": "adaptive_governor_enforcement_ready",
        "allowed": not blocked,
        "blocked": blocked,
        "decision": "NO_TRADE" if blocked else "ALLOW_PAPER_REVIEW",
        "reason": reason,
        "blocking_action": blocking_action,
        "safe_to_open_new_shadow": not blocked,
        "paper_exploration_created": False,
        "shadow_trade_id": "",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "adaptive_governor": governor,
        "adaptive_governor_global_state": governor.get("global_state") or "",
        "adaptive_governor_recommended_next_action": governor.get("recommended_next_action") or "",
        "circuit_breakers": circuit_breakers,
        "degraded_by_registry": bool(direct_degradation),
        "degradation_reason": direct_degradation.get("degradation_reason") or "",
        "rejected_by_research_registry": bool(direct_rejection),
        "research_rejection_reason": direct_rejection.get("rejection_reason") or "",
        "sibling_risk": bool(sibling_risk),
        "sibling_risk_reason": sibling_risk.get("sibling_risk_reason") or "",
        "applies_to_real_trading": False,
        **_safety(),
    }


def _load_shadow_snapshot(
    load_shadow_snapshot: bool,
    closed_trades: list[dict[str, Any]] | None,
    open_trades: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    if closed_trades is not None or open_trades is not None or not load_shadow_snapshot:
        return {"closed_trades": closed_trades or [], "open_trades": open_trades or []}
    try:
        from services.mt5.mt5_shadow_trading import MT5ShadowTrading

        return MT5ShadowTrading().snapshot(limit=200)
    except Exception as exc:  # pragma: no cover - defensive local runtime guard
        return {
            "ok": False,
            "status": "shadow_snapshot_unavailable",
            "error": type(exc).__name__,
            "closed_trades": [],
            "open_trades": [],
            **_safety(),
        }


def _profile_states(
    closed_trades: list[dict[str, Any]],
    open_trades: list[dict[str, Any]],
    limits: dict[str, Any],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, list[dict[str, Any]]]] = defaultdict(lambda: {"closed": [], "open": []})
    for trade in closed_trades:
        grouped[_trade_key(trade)]["closed"].append(trade)
    for trade in open_trades:
        grouped[_trade_key(trade)]["open"].append(trade)

    states = [
        _profile_state(symbol, timeframe, profile, rows["closed"], rows["open"], limits)
        for (symbol, timeframe, profile), rows in grouped.items()
        if symbol and timeframe and profile
    ]
    states.sort(key=lambda row: (row["symbol"], row["timeframe"], row["profile"]))
    return states


def _profile_state(
    symbol: str,
    timeframe: str,
    profile: str,
    closed: list[dict[str, Any]],
    open_: list[dict[str, Any]],
    limits: dict[str, Any],
) -> dict[str, Any]:
    ordered = sorted(closed, key=_trade_sort_value)
    pnls = [_pnl(row) for row in ordered]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    trades_forward = len(pnls)
    win_count = len(wins)
    loss_count = len(losses)
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = _profit_factor(gross_win, gross_loss)
    expectancy = round(sum(pnls) / trades_forward, 8) if trades_forward else 0.0
    consecutive_losses = _consecutive_losses(ordered)
    max_drawdown = _max_drawdown(pnls)
    recent_error_rate = _recent_error_rate(ordered)
    degraded = forward_profile_degradation(symbol, timeframe, profile)
    rejected = research_rejection(symbol, timeframe, profile, _infer_family(profile))
    action, health, active_state = _profile_action(
        trades_forward=trades_forward,
        win_rate=_win_rate(win_count, trades_forward),
        profit_factor=profit_factor,
        expectancy=expectancy,
        consecutive_losses=consecutive_losses,
        max_drawdown=max_drawdown,
        recent_error_rate=recent_error_rate,
        rejected=bool(rejected),
        degraded=bool(degraded),
        limits=limits,
    )
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "profile": profile,
        "active_state": active_state,
        "trades_forward": trades_forward,
        "wins": win_count,
        "losses": loss_count,
        "win_rate": _win_rate(win_count, trades_forward),
        "profit_factor": profit_factor,
        "expectancy": expectancy,
        "avg_win": round(sum(wins) / len(wins), 8) if wins else 0.0,
        "avg_loss": round(sum(losses) / len(losses), 8) if losses else 0.0,
        "max_drawdown": max_drawdown,
        "consecutive_losses": consecutive_losses,
        "recent_error_rate": recent_error_rate,
        "confidence_score": _confidence_score(trades_forward, profit_factor, expectancy),
        "risk_score": _risk_score(max_drawdown, consecutive_losses, recent_error_rate, limits),
        "health_status": health,
        "recommended_action": action,
        "open_shadow_trades": len(open_),
        "degraded_by_registry": bool(degraded),
        "degradation_reason": degraded.get("degradation_reason") or "",
        "rejected_by_research_registry": bool(rejected),
        "research_rejection_reason": rejected.get("rejection_reason") or "",
        "applies_to_real_trading": False,
        **_safety(),
    }


def _profile_action(
    *,
    trades_forward: int,
    win_rate: float,
    profit_factor: float,
    expectancy: float,
    consecutive_losses: int,
    max_drawdown: float,
    recent_error_rate: float,
    rejected: bool,
    degraded: bool,
    limits: dict[str, Any],
) -> tuple[str, str, str]:
    if degraded:
        return "observation_only", "degraded", "observation_only"
    if rejected:
        return "skip_rejected_family", "rejected", "observation_only"
    if max_drawdown > float(limits["max_profile_drawdown"]):
        return "kill_switch", "critical", "paused"
    if trades_forward >= 5 and profit_factor < 0.9 and expectancy <= 0:
        return "degrade_to_observation_only", "degraded", "observation_only"
    if win_rate < 35.0 and trades_forward >= 8:
        return "degrade_to_observation_only", "degraded", "observation_only"
    if consecutive_losses >= 3:
        return "pause_new_entries", "paused", "paused"
    if recent_error_rate >= float(limits["max_recent_error_rate"]):
        return "pause_new_entries", "warning", "paused"
    if trades_forward < 5:
        return "watch", "watch", "active"
    return "healthy", "healthy", "active"


def _circuit_breakers(
    closed_trades: list[dict[str, Any]],
    open_trades: list[dict[str, Any]],
    profiles: list[dict[str, Any]],
    runtime_snapshot: dict[str, Any],
    limits: dict[str, Any],
) -> list[dict[str, Any]]:
    daily_pnl = _period_pnl(closed_trades, "day")
    weekly_pnl = _period_pnl(closed_trades, "week")
    global_consecutive_losses = _consecutive_losses(sorted(closed_trades, key=_trade_sort_value))
    max_profile_drawdown = max((float(row["max_drawdown"]) for row in profiles), default=0.0)
    breakers = [
        _breaker(
            "missing_shadow_trade_data",
            not closed_trades and not open_trades,
            False,
            "missing_data",
            "No closed or open shadow trades were available for governor scoring.",
        ),
        _breaker(
            "max_daily_loss_shadow",
            daily_pnl <= float(limits["max_daily_loss_shadow"]),
            True,
            "daily_shadow_loss_limit",
            f"daily_pnl={daily_pnl}",
        ),
        _breaker(
            "max_weekly_loss_shadow",
            weekly_pnl <= float(limits["max_weekly_loss_shadow"]),
            True,
            "weekly_shadow_loss_limit",
            f"weekly_pnl={weekly_pnl}",
        ),
        _breaker(
            "max_consecutive_losses_global",
            global_consecutive_losses >= int(limits["max_consecutive_losses_global"]),
            True,
            "global_consecutive_losses_limit",
            f"consecutive_losses={global_consecutive_losses}",
        ),
        _breaker(
            "max_open_shadow_trades",
            len(open_trades) > int(limits["max_open_shadow_trades"]),
            True,
            "open_shadow_trade_limit",
            f"open_shadow_trades={len(open_trades)}",
        ),
        _breaker(
            "max_profile_drawdown",
            max_profile_drawdown > float(limits["max_profile_drawdown"]),
            True,
            "profile_drawdown_limit",
            f"max_profile_drawdown={max_profile_drawdown}",
        ),
        _breaker(
            "stale_runtime_snapshot",
            bool(runtime_snapshot) and not _snapshot_is_recent(runtime_snapshot),
            True,
            "stale_runtime_snapshot",
            "runtime snapshot is stale or explicitly marked not recent.",
        ),
        _breaker(
            "missing_bar_context",
            bool(runtime_snapshot) and not bool(runtime_snapshot.get("runtime_snapshot_context") or runtime_snapshot.get("bar_context")),
            False,
            "missing_bar_context",
            "runtime snapshot lacks bar context.",
        ),
        _breaker(
            "data_quality_bad",
            str(runtime_snapshot.get("data_quality") or "ok").casefold() not in {"", "ok"},
            True,
            "data_quality_bad",
            f"data_quality={runtime_snapshot.get('data_quality')}",
        ),
        _breaker(
            "conflicting_signals",
            bool(runtime_snapshot.get("conflicting_signals")),
            False,
            "conflicting_signals",
            "runtime snapshot reports conflicting signals.",
        ),
        _breaker(
            "registry_block",
            any(row.get("degraded_by_registry") or row.get("rejected_by_research_registry") for row in profiles),
            False,
            "registry_block",
            "At least one profile is blocked by a persistent registry.",
        ),
    ]
    return breakers


def _rotation_candidates(rotation: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ranking = rotation.get("ranking") or []
    clean: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in ranking:
        if not isinstance(row, dict):
            continue
        if _candidate_is_blocked(row):
            rejected.append(_candidate_row(row))
            continue
        if str(row.get("candidate_status") or "") in _REVIEW_STATUSES:
            clean.append(_candidate_row(row))

    recommended = rotation.get("recommended_candidate")
    if isinstance(recommended, dict) and not _candidate_is_blocked(recommended):
        candidate = _candidate_row(recommended)
        if candidate not in clean:
            clean.insert(0, candidate)

    return clean, rejected


def _candidate_is_blocked(row: dict[str, Any]) -> bool:
    status = str(row.get("candidate_status") or "")
    action = str(row.get("recommended_next_action") or "")
    return bool(
        row.get("degraded_by_registry")
        or row.get("rejected_by_research_registry")
        or row.get("sibling_risk")
        or status in {"excluded_by_degradation_registry", "excluded_by_research_rejection_registry", "blocked_by_sibling_risk"}
        or action in {"skip_degraded_profile", "skip_rejected_family", "manual_review_or_new_family_required"}
    )


def _matching_sibling_risk(governor: dict[str, Any], symbol: str, timeframe: str, profile: str) -> dict[str, Any]:
    if not profile:
        return {}
    for key in ("rejected_candidates", "rotation_candidates"):
        value = governor.get(key)
        if not isinstance(value, list):
            continue
        for row in value:
            if not isinstance(row, dict):
                continue
            if not bool(row.get("sibling_risk")):
                continue
            if _symbol(row.get("symbol")) != symbol:
                continue
            if _timeframe(row.get("timeframe")) != timeframe:
                continue
            row_profile = str(row.get("profile") or row.get("family") or "").strip()
            if row_profile == profile:
                return row
    return {}


def _active_breaker(circuit_breakers: list[Any], name: str) -> bool:
    for row in circuit_breakers:
        if not isinstance(row, dict):
            continue
        if row.get("name") == name and row.get("active"):
            return True
    return False


def _candidate_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": _symbol(row.get("symbol")),
        "timeframe": _timeframe(row.get("timeframe")),
        "profile": str(row.get("profile") or row.get("family") or ""),
        "family": str(row.get("family") or _infer_family(row.get("profile")) or ""),
        "candidate_status": str(row.get("candidate_status") or ""),
        "recommended_next_action": str(row.get("recommended_next_action") or ""),
        "degraded_by_registry": bool(row.get("degraded_by_registry")),
        "rejected_by_research_registry": bool(row.get("rejected_by_research_registry")),
        "sibling_risk": bool(row.get("sibling_risk")),
        "research_rejection_reason": str(row.get("research_rejection_reason") or ""),
        "degradation_reason": str(row.get("degradation_reason") or ""),
        "sibling_risk_reason": str(row.get("sibling_risk_reason") or ""),
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        **_safety(),
    }


def _safe_rotation() -> dict[str, Any]:
    try:
        return run_paper_forward_candidate_rotation()
    except Exception as exc:  # pragma: no cover - defensive local runtime guard
        return {
            "ok": False,
            "status": "paper_forward_candidate_rotation_unavailable",
            "error": type(exc).__name__,
            "recommendation": "continue_research",
            "ranking": [],
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
            **_safety(),
        }


def _safe_intelligence(rotation_result: dict[str, Any] | None) -> dict[str, Any]:
    try:
        return run_research_intelligence_core(rotation_result=rotation_result)
    except Exception as exc:  # pragma: no cover - defensive local runtime guard
        return {
            "ok": False,
            "status": "research_intelligence_core_unavailable",
            "error": type(exc).__name__,
            "recommended_next_research_phase": "continue_research",
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
            **_safety(),
        }


def _decision_reason(global_state: str, critical_breakers: list[dict[str, Any]], missing_data_breakers: list[dict[str, Any]]) -> str:
    if critical_breakers:
        return f"adaptive_governor:{critical_breakers[0].get('name')}"
    if missing_data_breakers:
        return "adaptive_governor:missing_data"
    return f"adaptive_governor:{global_state}"


def _next_research_action(intelligence: dict[str, Any] | None) -> str:
    phase = str((intelligence or {}).get("recommended_next_research_phase") or "")
    return phase or "continue_research"


def _trade_key(trade: dict[str, Any]) -> tuple[str, str, str]:
    return (
        _symbol(trade.get("symbol") or trade.get("requested_symbol") or trade.get("normalized_symbol")),
        _timeframe(trade.get("timeframe") or trade.get("tf")),
        str(trade.get("strategy_profile") or trade.get("profile") or trade.get("family") or trade.get("strategy") or "unknown_profile").strip(),
    )


def _pnl(trade: dict[str, Any]) -> float:
    for key in ("pnl", "profit", "r_multiple", "net_pnl", "pnl_r"):
        value = trade.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    status = str(trade.get("status") or "").casefold()
    if status == "win":
        return 1.0
    if status == "loss":
        return -1.0
    return 0.0


def _trade_sort_value(trade: dict[str, Any]) -> str:
    return str(
        trade.get("closed_at")
        or trade.get("updated_at")
        or trade.get("opened_at")
        or trade.get("created_at")
        or trade.get("id")
        or ""
    )


def _profit_factor(gross_win: float, gross_loss: float) -> float:
    if gross_loss <= 0:
        return 999.0 if gross_win > 0 else 0.0
    return round(gross_win / gross_loss, 6)


def _win_rate(wins: int, total: int) -> float:
    return round((wins / total) * 100.0, 4) if total else 0.0


def _consecutive_losses(trades: list[dict[str, Any]]) -> int:
    count = 0
    for trade in reversed(trades):
        if _pnl(trade) < 0:
            count += 1
            continue
        break
    return count


def _max_drawdown(pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return round(max_dd, 8)


def _recent_error_rate(trades: list[dict[str, Any]]) -> float:
    recent = trades[-5:]
    if not recent:
        return 0.0
    errors = sum(1 for trade in recent if _pnl(trade) < 0 or str(trade.get("status") or "").casefold() in {"error", "failed"})
    return round(errors / len(recent), 6)


def _confidence_score(trades_forward: int, profit_factor: float, expectancy: float) -> float:
    sample = min(trades_forward / 20.0, 1.0) * 35.0
    pf_score = min(max(profit_factor, 0.0), 2.0) / 2.0 * 45.0
    expectancy_score = 20.0 if expectancy > 0 else 0.0
    return round(sample + pf_score + expectancy_score, 4)


def _risk_score(max_drawdown: float, consecutive_losses: int, recent_error_rate: float, limits: dict[str, Any]) -> float:
    drawdown_limit = max(float(limits["max_profile_drawdown"]), 0.000001)
    score = min(max_drawdown / drawdown_limit, 1.5) * 45.0
    score += min(consecutive_losses / 3.0, 1.5) * 35.0
    score += min(recent_error_rate, 1.0) * 20.0
    return round(score, 4)


def _period_pnl(trades: list[dict[str, Any]], period: str) -> float:
    now = datetime.now(timezone.utc)
    total = 0.0
    for trade in trades:
        parsed = _parse_time(trade.get("closed_at") or trade.get("updated_at") or trade.get("created_at"))
        if parsed is None:
            continue
        if period == "day" and parsed.date() != now.date():
            continue
        if period == "week" and parsed.isocalendar()[:2] != now.isocalendar()[:2]:
            continue
        total += _pnl(trade)
    return round(total, 8)


def _parse_time(value: object) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _snapshot_is_recent(snapshot: dict[str, Any]) -> bool:
    if "runtime_snapshot_recent" in snapshot:
        return bool(snapshot.get("runtime_snapshot_recent"))
    if "snapshot_recent" in snapshot:
        return bool(snapshot.get("snapshot_recent"))
    timestamp = _parse_time(snapshot.get("runtime_snapshot_at") or snapshot.get("timestamp") or snapshot.get("updated_at"))
    if timestamp is None:
        return True
    age_seconds = (datetime.now(timezone.utc) - timestamp).total_seconds()
    return age_seconds <= 3600


def _breaker(name: str, active: bool, critical: bool, reason: str, detail: str) -> dict[str, Any]:
    return {
        "name": name,
        "active": bool(active),
        "critical": bool(critical),
        "reason": reason if active else "",
        "detail": detail if active else "",
        **_safety(),
    }


def _infer_family(profile: object) -> str:
    raw = str(profile or "").casefold()
    if "vol_breakout" in raw or "volatility_breakout" in raw:
        return "volatility_breakout"
    if "session_vwap_reclaim" in raw:
        return "session_vwap_reclaim"
    if "trend_pullback" in raw:
        return "multi_timeframe_trend_pullback"
    if "session_open_continuation" in raw:
        return "session_open_continuation"
    if "ema_reclaim" in raw:
        return "ema_reclaim"
    if "london_us_breakout" in raw:
        return "london_us_breakout"
    if "opening_range_fakeout" in raw:
        return "opening_range_fakeout"
    return raw


def _symbol(value: object) -> str:
    symbol = str(value or "").upper().strip().replace(".B", "")
    if symbol == "XAUUSDB":
        return "XAUUSD"
    if symbol in {"USTECB", "NAS100"}:
        return "USTEC"
    return symbol


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _safety_state() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "applies_to_real_trading": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
    }


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
