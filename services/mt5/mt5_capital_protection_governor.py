from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore, persist_risk_event


CAPITAL_GOVERNOR_VERSION = "2026-06-10.mt5_capital_protection_governor.v1"

DEFAULT_LIMITS = {
    "max_daily_loss_pct": 3.0,
    "max_weekly_loss_pct": 7.0,
    "max_drawdown_pct": 10.0,
    "max_consecutive_losses_global": 5,
    "max_consecutive_losses_profile": 3,
    "max_open_shadow_trades": 3,
    "max_profile_exposure": 1,
    "max_stale_snapshot_minutes": 60.0,
}


def run_capital_protection_governor(
    *,
    closed_trades: list[dict[str, Any]] | None = None,
    open_trades: list[dict[str, Any]] | None = None,
    risk_events: list[dict[str, Any]] | None = None,
    decision_events: list[dict[str, Any]] | None = None,
    adaptive_state: dict[str, Any] | None = None,
    profile_performance: list[dict[str, Any]] | None = None,
    persistent_status: dict[str, Any] | None = None,
    runtime_snapshot: dict[str, Any] | None = None,
    limits: dict[str, Any] | None = None,
    load_shadow_snapshot: bool = True,
    load_persistent: bool = True,
    persist_events: bool = True,
) -> dict[str, Any]:
    active_limits = {**DEFAULT_LIMITS, **(limits or {})}
    persistent = _load_persistent(load_persistent, persistent_status)
    shadow_snapshot = _load_shadow_snapshot(load_shadow_snapshot, closed_trades, open_trades)
    closed = list(closed_trades if closed_trades is not None else shadow_snapshot.get("closed_trades") or [])
    open_ = list(open_trades if open_trades is not None else shadow_snapshot.get("open_trades") or [])
    recent_events = persistent.get("recent_events") if isinstance(persistent.get("recent_events"), dict) else {}
    risks = list(risk_events if risk_events is not None else recent_events.get("recent_risk_events") or [])
    decisions = list(decision_events if decision_events is not None else recent_events.get("recent_decisions") or [])
    adaptive = adaptive_state if adaptive_state is not None else persistent.get("adaptive_state") if isinstance(persistent.get("adaptive_state"), dict) else {}
    performances = list(profile_performance or [])

    daily_loss_pct = _period_loss_pct(closed, days=1)
    weekly_loss_pct = _period_loss_pct(closed, days=7)
    max_drawdown_pct = _max_drawdown_pct(closed, performances)
    current_drawdown_pct = _current_drawdown_pct(closed)
    open_shadow_exposure = _open_shadow_exposure(open_)
    open_profile_count = _open_profile_count(open_)
    global_consecutive_losses = _consecutive_losses(closed)
    profile_losses = _consecutive_losses_by_profile(closed)

    breakers = _circuit_breakers(
        daily_loss_pct=daily_loss_pct,
        weekly_loss_pct=weekly_loss_pct,
        current_drawdown_pct=current_drawdown_pct,
        max_drawdown_pct=max_drawdown_pct,
        open_shadow_count=len(open_),
        open_profile_count=open_profile_count,
        consecutive_losses_global=global_consecutive_losses,
        consecutive_losses_by_profile=profile_losses,
        persistent_status=persistent.get("status") if isinstance(persistent.get("status"), dict) else {},
        runtime_snapshot=runtime_snapshot or {},
        adaptive_state=adaptive or {},
        risk_events=risks,
        decision_events=decisions,
        open_trades=open_,
        limits=active_limits,
    )
    critical_breakers = [row for row in breakers if row.get("active") and row.get("critical")]
    capital_state = _capital_state(
        critical_breakers=critical_breakers,
        daily_loss_pct=daily_loss_pct,
        weekly_loss_pct=weekly_loss_pct,
        max_drawdown_pct=max_drawdown_pct,
        consecutive_losses_global=global_consecutive_losses,
        limits=active_limits,
    )
    safe_to_trade = not critical_breakers and capital_state not in {"lockdown", "kill_switch"}
    risk_budget_remaining = _risk_budget_remaining(daily_loss_pct, weekly_loss_pct, max_drawdown_pct, active_limits)
    recommended_action = _recommended_action(capital_state, critical_breakers)
    result = {
        "ok": True,
        "status": "capital_protection_governor_ready",
        "governor_version": CAPITAL_GOVERNOR_VERSION,
        "mode": "paper_shadow_only",
        "decision": "ALLOW_PAPER_REVIEW" if safe_to_trade else "NO_TRADE",
        "reason": "" if safe_to_trade else f"capital_protection:{critical_breakers[0].get('name') if critical_breakers else capital_state}",
        "capital_state": capital_state,
        "safe_to_trade": safe_to_trade,
        "daily_loss_pct": daily_loss_pct,
        "weekly_loss_pct": weekly_loss_pct,
        "current_drawdown_pct": current_drawdown_pct,
        "max_drawdown_pct": max_drawdown_pct,
        "open_shadow_exposure": open_shadow_exposure,
        "open_profile_count": open_profile_count,
        "open_shadow_trades": len(open_),
        "consecutive_losses_global": global_consecutive_losses,
        "consecutive_losses_by_profile": profile_losses,
        "risk_budget_remaining": risk_budget_remaining,
        "circuit_breakers": breakers,
        "recommended_action": recommended_action,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        "live_trading_enabled": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        **_safety(),
    }
    if persist_events:
        _persist_capital_state(result)
    return result


def capital_protection_enforcement(
    *,
    symbol: str = "",
    timeframe: str = "",
    profile: str = "",
    governor_result: dict[str, Any] | None = None,
    load_shadow_snapshot: bool = True,
    load_persistent: bool = True,
) -> dict[str, Any]:
    governor = governor_result or run_capital_protection_governor(
        load_shadow_snapshot=load_shadow_snapshot,
        load_persistent=load_persistent,
    )
    blocked = not bool(governor.get("safe_to_trade"))
    reason = str(governor.get("reason") or "capital_protection:blocked") if blocked else ""
    return {
        "ok": True,
        "status": "capital_protection_enforcement_ready",
        "symbol": _symbol(symbol),
        "timeframe": _timeframe(timeframe),
        "profile": str(profile or ""),
        "allowed": not blocked,
        "blocked": blocked,
        "decision": "NO_TRADE" if blocked else "ALLOW_PAPER_REVIEW",
        "reason": reason,
        "capital_state": governor.get("capital_state") or "",
        "safe_to_open_new_shadow": not blocked,
        "paper_exploration_created": False,
        "shadow_trade_id": "",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "capital_governor": governor,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _load_persistent(load_persistent: bool, persistent_status: dict[str, Any] | None) -> dict[str, Any]:
    if persistent_status is not None:
        return {"status": persistent_status, "recent_events": {}, "adaptive_state": {}}
    if not load_persistent:
        return {
            "status": {"db_available": True, "db_degraded": False, "tables_ready": True, "provider": "test_disabled"},
            "recent_events": {},
            "adaptive_state": {},
        }
    try:
        store = MT5PersistentIntelligenceStore()
        return {
            "status": store.healthcheck(write_test_event=False),
            "recent_events": store.recent_events(limit=50),
            "adaptive_state": store.get_adaptive_governor_state().get("state") or {},
        }
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {
            "status": {
                "db_available": False,
                "db_degraded": True,
                "tables_ready": False,
                "provider": "unavailable",
                "reason": type(exc).__name__,
            },
            "recent_events": {},
            "adaptive_state": {},
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

        return MT5ShadowTrading().snapshot(limit=500)
    except Exception:
        return {"closed_trades": [], "open_trades": []}


def _circuit_breakers(
    *,
    daily_loss_pct: float,
    weekly_loss_pct: float,
    current_drawdown_pct: float,
    max_drawdown_pct: float,
    open_shadow_count: int,
    open_profile_count: int,
    consecutive_losses_global: int,
    consecutive_losses_by_profile: dict[str, int],
    persistent_status: dict[str, Any],
    runtime_snapshot: dict[str, Any],
    adaptive_state: dict[str, Any],
    risk_events: list[dict[str, Any]],
    decision_events: list[dict[str, Any]],
    open_trades: list[dict[str, Any]],
    limits: dict[str, Any],
) -> list[dict[str, Any]]:
    max_profile_losses = max(consecutive_losses_by_profile.values(), default=0)
    max_profile_open = _max_profile_open_count(open_trades)
    stale = _runtime_snapshot_stale(runtime_snapshot, float(limits["max_stale_snapshot_minutes"]))
    registry_block = any(
        "registry" in str(row.get("reason") or row.get("risk_reason") or "").casefold()
        for row in risk_events + decision_events
        if isinstance(row, dict)
    )
    adaptive_global = str(adaptive_state.get("global_state") or adaptive_state.get("capital_state") or "").casefold()
    conflicting_governor = adaptive_global in {"kill_switch", "lockdown", "pause_new_entries", "degrade_to_observation_only"}
    return [
        _breaker("max_daily_loss_pct", daily_loss_pct >= float(limits["max_daily_loss_pct"]), True, f"daily_loss_pct={daily_loss_pct}"),
        _breaker("max_weekly_loss_pct", weekly_loss_pct >= float(limits["max_weekly_loss_pct"]), True, f"weekly_loss_pct={weekly_loss_pct}"),
        _breaker("max_drawdown_pct", max_drawdown_pct >= float(limits["max_drawdown_pct"]) or current_drawdown_pct >= float(limits["max_drawdown_pct"]), True, f"max_drawdown_pct={max_drawdown_pct}"),
        _breaker("max_consecutive_losses_global", consecutive_losses_global >= int(limits["max_consecutive_losses_global"]), True, f"consecutive_losses={consecutive_losses_global}"),
        _breaker("max_consecutive_losses_profile", max_profile_losses >= int(limits["max_consecutive_losses_profile"]), True, f"profile_consecutive_losses={max_profile_losses}"),
        _breaker("max_open_shadow_trades", open_shadow_count > int(limits["max_open_shadow_trades"]), True, f"open_shadow_trades={open_shadow_count}"),
        _breaker("max_profile_exposure", max_profile_open > int(limits["max_profile_exposure"]), True, f"max_profile_open_shadows={max_profile_open}"),
        _breaker("stale_runtime_snapshot", stale, True, "runtime snapshot stale"),
        _breaker("persistent_db_degraded", bool(persistent_status.get("db_degraded") or not persistent_status.get("db_available") or not persistent_status.get("tables_ready")), True, f"provider={persistent_status.get('provider') or 'unknown'}"),
        _breaker("registry_block", registry_block, False, "registry block observed in recent events"),
        _breaker("conflicting_governor_state", conflicting_governor, True if adaptive_global == "kill_switch" else False, f"adaptive_state={adaptive_global}"),
    ]


def _breaker(name: str, active: bool, critical: bool, detail: str) -> dict[str, Any]:
    return {
        "name": name,
        "active": bool(active),
        "critical": bool(critical),
        "reason": name if active else "",
        "detail": detail if active else "",
        **_safety(),
    }


def _capital_state(
    *,
    critical_breakers: list[dict[str, Any]],
    daily_loss_pct: float,
    weekly_loss_pct: float,
    max_drawdown_pct: float,
    consecutive_losses_global: int,
    limits: dict[str, Any],
) -> str:
    if critical_breakers:
        return "kill_switch"
    daily_ratio = daily_loss_pct / max(float(limits["max_daily_loss_pct"]), 0.0001)
    weekly_ratio = weekly_loss_pct / max(float(limits["max_weekly_loss_pct"]), 0.0001)
    drawdown_ratio = max_drawdown_pct / max(float(limits["max_drawdown_pct"]), 0.0001)
    pressure = max(daily_ratio, weekly_ratio, drawdown_ratio)
    if pressure >= 0.8:
        return "lockdown"
    if pressure >= 0.55 or consecutive_losses_global >= 3:
        return "defensive"
    if pressure >= 0.3 or consecutive_losses_global >= 2:
        return "caution"
    return "normal"


def _recommended_action(capital_state: str, critical_breakers: list[dict[str, Any]]) -> str:
    if critical_breakers:
        return "kill_switch"
    if capital_state == "lockdown":
        return "pause_new_entries"
    if capital_state == "defensive":
        return "reduce_paper_risk"
    if capital_state == "caution":
        return "keep_observing"
    return "allow_paper_review"


def _period_loss_pct(closed: list[dict[str, Any]], *, days: int) -> float:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    total = 0.0
    for trade in closed:
        parsed = _parse_time(trade.get("closed_at") or trade.get("updated_at") or trade.get("created_at"))
        if parsed is not None and parsed < cutoff:
            continue
        pnl = _pnl_pct(trade)
        if pnl < 0:
            total += abs(pnl)
    return round(total, 6)


def _current_drawdown_pct(closed: list[dict[str, Any]]) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for trade in sorted(closed, key=_trade_sort_value):
        equity += _pnl_pct(trade)
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return round(drawdown, 6)


def _max_drawdown_pct(closed: list[dict[str, Any]], performance: list[dict[str, Any]]) -> float:
    observed = _current_drawdown_pct(closed)
    perf = max((_float(row.get("max_drawdown") or row.get("max_drawdown_pct")) for row in performance if isinstance(row, dict)), default=0.0)
    return round(max(observed, perf), 6)


def _open_shadow_exposure(open_trades: list[dict[str, Any]]) -> float:
    return round(sum(max(_float(row.get("risk_pct") or row.get("exposure_pct")), 0.0) for row in open_trades), 6)


def _open_profile_count(open_trades: list[dict[str, Any]]) -> int:
    return len({_profile_key(row) for row in open_trades if _profile_key(row)})


def _max_profile_open_count(open_trades: list[dict[str, Any]]) -> int:
    counts: dict[str, int] = defaultdict(int)
    for trade in open_trades:
        key = _profile_key(trade)
        if key:
            counts[key] += 1
    return max(counts.values(), default=0)


def _consecutive_losses(closed: list[dict[str, Any]]) -> int:
    count = 0
    for trade in reversed(sorted(closed, key=_trade_sort_value)):
        if _pnl(trade) < 0:
            count += 1
            continue
        break
    return count


def _consecutive_losses_by_profile(closed: list[dict[str, Any]]) -> dict[str, int]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in closed:
        key = _profile_key(trade)
        if key:
            grouped[key].append(trade)
    return {key: _consecutive_losses(rows) for key, rows in grouped.items()}


def _risk_budget_remaining(daily: float, weekly: float, drawdown: float, limits: dict[str, Any]) -> dict[str, float]:
    return {
        "daily_loss_pct": round(max(float(limits["max_daily_loss_pct"]) - daily, 0.0), 6),
        "weekly_loss_pct": round(max(float(limits["max_weekly_loss_pct"]) - weekly, 0.0), 6),
        "drawdown_pct": round(max(float(limits["max_drawdown_pct"]) - drawdown, 0.0), 6),
    }


def _runtime_snapshot_stale(snapshot: dict[str, Any], max_minutes: float) -> bool:
    if not snapshot:
        return False
    if snapshot.get("runtime_snapshot_recent") is False:
        return True
    timestamp = _parse_time(snapshot.get("runtime_snapshot_at") or snapshot.get("timestamp") or snapshot.get("updated_at"))
    if timestamp is None:
        return False
    return (datetime.now(timezone.utc) - timestamp).total_seconds() / 60.0 > max_minutes


def _persist_capital_state(result: dict[str, Any]) -> None:
    active_breakers = [row for row in result.get("circuit_breakers") or [] if isinstance(row, dict) and row.get("active")]
    if not active_breakers:
        return
    breaker = active_breakers[0]
    result["persistent_intelligence_risk_event"] = persist_risk_event(
        {
            "symbol": "",
            "timeframe": "",
            "risk_state": result.get("capital_state") or "capital_protection",
            "allowed": bool(result.get("safe_to_trade")),
            "reason": breaker.get("reason") or result.get("reason") or "",
            "circuit_breaker": f"capital_protection:{breaker.get('name') or ''}",
            "open_shadow_count": result.get("open_shadow_trades") or 0,
            "recommended_action": result.get("recommended_action") or "NO_TRADE",
        }
    )


def _pnl(trade: dict[str, Any]) -> float:
    for key in ("pnl", "profit", "r_multiple", "net_pnl", "pnl_r"):
        value = trade.get(key)
        if value is not None:
            return _float(value)
    status = str(trade.get("status") or "").casefold()
    if status == "win":
        return 1.0
    if status == "loss":
        return -1.0
    return 0.0


def _pnl_pct(trade: dict[str, Any]) -> float:
    value = trade.get("pnl_pct")
    if value is not None:
        return _float(value)
    return _pnl(trade)


def _profile_key(trade: dict[str, Any]) -> str:
    symbol = _symbol(trade.get("symbol") or trade.get("normalized_symbol"))
    timeframe = _timeframe(trade.get("timeframe"))
    profile = str(trade.get("strategy_profile") or trade.get("profile") or trade.get("family") or "").strip()
    return "|".join(item for item in (symbol, timeframe, profile) if item)


def _trade_sort_value(trade: dict[str, Any]) -> str:
    return str(trade.get("closed_at") or trade.get("updated_at") or trade.get("opened_at") or trade.get("created_at") or "")


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


def _float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _symbol(value: object) -> str:
    return str(value or "").upper().strip().replace(".B", "")


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
