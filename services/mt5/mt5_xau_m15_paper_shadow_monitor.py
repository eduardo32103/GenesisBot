from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore
from services.mt5.mt5_risk_governor import assess_runtime_risk
from services.mt5.mt5_runtime_snapshot import append_closed_shadow_trade, get_snapshot, update_open_shadow_trade
from services.mt5.mt5_xau_m15_paper_observation_readiness import BROKER_SYMBOL, CANDIDATE_PROFILE, MIN_BARS_COUNT, SYMBOL, TIMEFRAME


MONITOR_VERSION = "2026-06-14.mt5_xau_m15_paper_shadow_monitor.v2"
MAX_RUNTIME_AGE_MINUTES = 45.0
DB_QUEUE_DEPTH_LIMIT = 3


def run_xau_m15_paper_shadow_monitor(
    *,
    apply_paper_close: bool = False,
    store: MT5PersistentIntelligenceStore | Any | None = None,
    runtime_snapshot: dict[str, Any] | None = None,
    db_state: dict[str, Any] | None = None,
    capital_state: dict[str, Any] | None = None,
    adaptive_state: dict[str, Any] | None = None,
    risk_state: dict[str, Any] | None = None,
    persist_events: bool = True,
    max_runtime_age_minutes: float = MAX_RUNTIME_AGE_MINUTES,
) -> dict[str, Any]:
    snapshot = dict(runtime_snapshot) if runtime_snapshot is not None else _runtime_snapshot()
    open_trade = _open_shadow_trade(snapshot)
    shadow_source = "runtime_memory" if open_trade else "none"
    persistent_open_count = 0
    if not open_trade:
        persistent_open = _persistent_open_shadow_trades(store)
        persistent_open_count = len(persistent_open)
        if persistent_open_count > 1:
            safety_analysis = _safety_exit_analysis(
                snapshot=snapshot,
                metrics={},
                db=_db_state(store, db_state),
                capital=_capital_state(capital_state),
                adaptive=_adaptive_state(adaptive_state),
                risk=_risk_state(snapshot, {}, risk_state),
                open_shadow_count=persistent_open_count,
                current_shadow_trade_id="",
                max_runtime_age_minutes=max_runtime_age_minutes,
            )
            return _base_result(
                monitor_state="blocked_multiple_open_shadows",
                open_shadow_count=persistent_open_count,
                exit_signal=False,
                exit_reason="multiple_open_shadows_persisted",
                paper_close_applied=False,
                shadow_status_after="blocked",
                runtime_snapshot=snapshot,
                shadow_source="persistent_intelligence_fallback",
                safety_analysis=safety_analysis,
            )
        if persistent_open_count == 1:
            open_trade = persistent_open[0]
            shadow_source = "persistent_intelligence_fallback"
            snapshot = {**snapshot, "open_shadow_trade": open_trade}
    if not open_trade:
        return _base_result(
            monitor_state="no_action",
            open_shadow_count=0,
            exit_signal=False,
            exit_reason="no_open_shadow",
            paper_close_applied=False,
            shadow_status_after="none",
            runtime_snapshot=snapshot,
            shadow_source="none",
            safety_analysis=_empty_safety_analysis(reason_detail="no_open_shadow"),
        )

    metrics = _trade_metrics(open_trade, snapshot)
    db = _db_state(store, db_state)
    capital = _capital_state(capital_state)
    adaptive = _adaptive_state(adaptive_state)
    risk = _risk_state(snapshot, open_trade, risk_state)
    safety_analysis = _safety_exit_analysis(
        snapshot=snapshot,
        metrics=metrics,
        db=db,
        capital=capital,
        adaptive=adaptive,
        risk=risk,
        open_shadow_count=1,
        current_shadow_trade_id=str(open_trade.get("shadow_trade_id") or ""),
        max_runtime_age_minutes=max_runtime_age_minutes,
    )
    exit_signal, exit_reason, apply_blocked = _exit_decision(
        trade=open_trade,
        metrics=metrics,
        safety_analysis=safety_analysis,
    )
    updated_trade = _updated_trade(open_trade, metrics, exit_reason if exit_signal else "")
    paper_close_applied = False
    shadow_status_after = "open"
    persist_result: dict[str, Any] = {"ok": True, "skipped": True}

    if apply_paper_close and exit_signal and not apply_blocked:
        closed = _closed_trade(updated_trade, metrics, exit_reason)
        update_open_shadow_trade(SYMBOL, None, timeframe=TIMEFRAME)
        append_closed_shadow_trade(SYMBOL, closed, timeframe=TIMEFRAME)
        paper_close_applied = True
        shadow_status_after = "closed"
        if persist_events:
            persist_result = _persist_close(store, closed)
    elif apply_paper_close and not exit_signal and not apply_blocked and shadow_source == "runtime_memory":
        update_open_shadow_trade(SYMBOL, updated_trade, timeframe=TIMEFRAME)

    monitor_state = "exit_applied" if paper_close_applied else "exit_pending" if exit_signal else "open_monitoring"
    if apply_blocked:
        monitor_state = "apply_blocked_missing_current_price"

    return {
        **_base_result(
            monitor_state=monitor_state,
            open_shadow_count=1,
            exit_signal=bool(exit_signal),
            exit_reason=exit_reason,
            paper_close_applied=paper_close_applied,
            shadow_status_after=shadow_status_after,
            runtime_snapshot=snapshot,
            shadow_source=shadow_source,
            safety_analysis=safety_analysis,
        ),
        "shadow_trade_id": open_trade.get("shadow_trade_id") or "",
        "side": metrics["side"],
        "entry_price": metrics["entry_price"],
        "current_price": metrics["current_price"],
        "stop_loss": metrics["stop_loss"],
        "take_profit": metrics["take_profit"],
        "unrealized_pnl": metrics["unrealized_pnl"],
        "unrealized_pnl_pct": metrics["unrealized_pnl_pct"],
        "r_multiple": metrics["r_multiple"],
        "age_minutes": metrics["age_minutes"],
        "bars_since_entry": metrics["bars_since_entry"],
        "persistent_open_shadow_count": persistent_open_count,
        "db_state": _public_db_state(db),
        "capital_state": capital,
        "adaptive_state": adaptive,
        "risk_state": risk,
        "apply_paper_close_requested": bool(apply_paper_close),
        "apply_blocked": bool(apply_blocked),
        "persist_result": persist_result,
    }


def _base_result(
    *,
    monitor_state: str,
    open_shadow_count: int,
    exit_signal: bool,
    exit_reason: str,
    paper_close_applied: bool,
    shadow_status_after: str,
    runtime_snapshot: dict[str, Any],
    shadow_source: str,
    safety_analysis: dict[str, Any] | None = None,
) -> dict[str, Any]:
    safety = safety_analysis or _empty_safety_analysis(reason_detail="")
    return {
        "ok": True,
        "status": "xau_m15_paper_shadow_monitor_ready",
        "monitor_version": MONITOR_VERSION,
        "monitor_state": monitor_state,
        "symbol": SYMBOL,
        "broker_symbol": BROKER_SYMBOL,
        "timeframe": TIMEFRAME,
        "candidate_profile": CANDIDATE_PROFILE,
        "open_shadow_count": int(open_shadow_count),
        "shadow_source": shadow_source,
        "shadow_trade_id": "",
        "side": "",
        "entry_price": 0.0,
        "current_price": 0.0,
        "stop_loss": 0.0,
        "take_profit": 0.0,
        "unrealized_pnl": 0.0,
        "unrealized_pnl_pct": 0.0,
        "r_multiple": 0.0,
        "age_minutes": 0.0,
        "bars_since_entry": 0,
        "runtime_context_available": bool(runtime_snapshot.get("runtime_snapshot_available")),
        "runtime_context_recent": bool(runtime_snapshot.get("runtime_snapshot_recent")),
        "runtime_snapshot_context": runtime_snapshot.get("runtime_snapshot_context") or "",
        "bars_count": int(_number(runtime_snapshot.get("bars_count")) or 0),
        "tick_available": isinstance(runtime_snapshot.get("last_tick"), dict),
        "exit_signal": bool(exit_signal),
        "exit_reason": exit_reason,
        **safety,
        "paper_close_applied": bool(paper_close_applied),
        "shadow_status_after": shadow_status_after,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _runtime_snapshot() -> dict[str, Any]:
    for alias in (SYMBOL, BROKER_SYMBOL, "XAUUSD.B"):
        snapshot = get_snapshot(alias, TIMEFRAME) or {}
        if snapshot:
            return dict(snapshot)
    return {}


def _open_shadow_trade(snapshot: dict[str, Any]) -> dict[str, Any]:
    trade = snapshot.get("open_shadow_trade") if isinstance(snapshot.get("open_shadow_trade"), dict) else {}
    if not trade:
        generic = get_snapshot(SYMBOL) or get_snapshot(BROKER_SYMBOL) or {}
        trade = generic.get("open_shadow_trade") if isinstance(generic.get("open_shadow_trade"), dict) else {}
    if not trade:
        return {}
    if _symbol(trade.get("symbol")) != SYMBOL:
        return {}
    if _timeframe(trade.get("timeframe")) != TIMEFRAME:
        return {}
    if str(trade.get("status") or trade.get("lifecycle_status") or "").lower() not in {"", "open"}:
        return {}
    return dict(trade)


def _persistent_open_shadow_trades(store: Any | None) -> list[dict[str, Any]]:
    active_store = store or MT5PersistentIntelligenceStore()
    if not hasattr(active_store, "_safe_select"):
        return []
    try:
        result = active_store._safe_select(
            "mt5_shadow_trades",
            params={
                "select": "shadow_trade_id,symbol,broker_symbol,timeframe,profile,strategy_profile,source,side,entry_price,stop_loss,take_profit,status,opened_at,broker_touched,order_executed,order_policy",
                "symbol": f"eq.{SYMBOL}",
                "timeframe": f"eq.{TIMEFRAME}",
                "status": "eq.open",
                "limit": "10",
            },
        )
    except Exception:
        return []
    rows = result.get("rows") if isinstance(result, dict) else []
    shadows: list[dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        shadow = _persistent_row_to_shadow(row)
        if shadow:
            shadows.append(shadow)
    return shadows


def _persistent_row_to_shadow(row: dict[str, Any]) -> dict[str, Any]:
    if _symbol(row.get("symbol")) != SYMBOL:
        return {}
    if _timeframe(row.get("timeframe")) != TIMEFRAME:
        return {}
    if str(row.get("status") or "").casefold() != "open":
        return {}
    profile = str(row.get("strategy_profile") or row.get("profile") or CANDIDATE_PROFILE)
    return {
        "shadow_trade_id": row.get("shadow_trade_id") or "",
        "symbol": SYMBOL,
        "broker_symbol": row.get("broker_symbol") or BROKER_SYMBOL,
        "timeframe": TIMEFRAME,
        "profile": row.get("profile") or profile,
        "strategy_profile": profile,
        "candidate_profile": profile,
        "source": row.get("source") or "persistent_intelligence_fallback",
        "side": str(row.get("side") or "buy").lower(),
        "entry_price": _number(row.get("entry_price")) or 0.0,
        "entry": _number(row.get("entry_price")) or 0.0,
        "stop_loss": _number(row.get("stop_loss")) or 0.0,
        "take_profit": _number(row.get("take_profit")) or 0.0,
        "initial_risk": abs((_number(row.get("entry_price")) or 0.0) - (_number(row.get("stop_loss")) or 0.0)),
        "status": "open",
        "lifecycle_status": "open",
        "opened_at": row.get("opened_at") or "",
        "paper_forward_candidate": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _trade_metrics(trade: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    side = str(trade.get("side") or trade.get("action") or "buy").lower()
    if side == "buy":
        current = _number(_tick(snapshot).get("bid")) or _last_price(snapshot)
    elif side == "sell":
        current = _number(_tick(snapshot).get("ask")) or _last_price(snapshot)
    else:
        current = _last_price(snapshot)
        side = "buy"
    entry = _number(trade.get("entry_price") or trade.get("entry")) or 0.0
    stop = _number(trade.get("stop_loss")) or 0.0
    target = _number(trade.get("take_profit")) or 0.0
    current_price = current or 0.0
    pnl = _pnl(side, entry, current_price)
    pnl_pct = (pnl / entry) * 100.0 if entry else 0.0
    initial_risk = _number(trade.get("initial_risk")) or abs(entry - stop) or 0.0
    r_multiple = pnl / initial_risk if initial_risk else 0.0
    age = _age_minutes(trade.get("opened_at") or trade.get("entry_time") or trade.get("created_at"))
    bars_since_entry = _bars_since_entry(snapshot, trade.get("opened_at") or trade.get("entry_time") or trade.get("created_at"))
    return {
        "side": side,
        "entry_price": round(entry, 6),
        "current_price": round(current_price, 6),
        "stop_loss": round(stop, 6),
        "take_profit": round(target, 6),
        "unrealized_pnl": round(pnl, 6),
        "unrealized_pnl_pct": round(pnl_pct, 6),
        "r_multiple": round(r_multiple, 6),
        "initial_risk": initial_risk,
        "age_minutes": round(age, 3),
        "bars_since_entry": bars_since_entry,
    }


def _exit_decision(
    *,
    trade: dict[str, Any],
    metrics: dict[str, Any],
    safety_analysis: dict[str, Any],
) -> tuple[bool, str, bool]:
    side = metrics["side"]
    price = float(metrics["current_price"])
    stop = float(metrics["stop_loss"])
    target = float(metrics["take_profit"])
    if side == "buy" and stop and price <= stop:
        return True, "stop_loss_hit", False
    if side == "sell" and stop and price >= stop:
        return True, "stop_loss_hit", False
    if side == "buy" and target and price >= target:
        return True, "take_profit_hit", False
    if side == "sell" and target and price <= target:
        return True, "take_profit_hit", False
    if _trailing_defensive_hit(trade, metrics):
        return True, "trailing_defensive_exit", False
    max_hold_bars = int(_number(trade.get("max_hold_bars")) or 0)
    if max_hold_bars > 0 and int(metrics.get("bars_since_entry") or 0) >= max_hold_bars:
        return True, "max_hold_bars", False
    if safety_analysis.get("should_close_paper"):
        if not metrics.get("current_price"):
            return True, "safety_exit", True
        return True, "safety_exit", False
    if safety_analysis.get("should_watch_only"):
        return False, "caution_watch", False
    return False, "", False


def _safety_exit_analysis(
    *,
    snapshot: dict[str, Any],
    metrics: dict[str, Any],
    db: dict[str, Any],
    capital: dict[str, Any],
    adaptive: dict[str, Any],
    risk: dict[str, Any],
    open_shadow_count: int,
    current_shadow_trade_id: str,
    max_runtime_age_minutes: float,
) -> dict[str, Any]:
    db_state = _db_safety_state(db)
    capital_state = _capital_safety_state(capital)
    adaptive_state = _adaptive_safety_state(adaptive)
    risk_state = _risk_safety_state(
        risk,
        open_shadow_count=open_shadow_count,
        current_shadow_trade_id=current_shadow_trade_id,
    )
    runtime_state = _runtime_safety_state(snapshot, metrics, max_runtime_age_minutes=max_runtime_age_minutes)
    open_shadow_state = _open_shadow_safety_state(open_shadow_count)

    critical_sources: list[str] = []
    unknown_sources: list[str] = []
    caution_sources: list[str] = []
    for state in (db_state, capital_state, runtime_state, open_shadow_state):
        critical_sources.extend(state["critical_reasons"])
    critical_sources.extend(adaptive_state["critical_reasons"])
    caution_sources.extend(adaptive_state["caution_reasons"])
    critical_sources.extend(risk_state["critical_reasons"])
    caution_sources.extend(risk_state["caution_reasons"])
    unknown_sources.extend(risk_state["unknown_reasons"])
    if metrics and float(metrics.get("r_multiple") or 0.0) < 0 and not critical_sources and not unknown_sources:
        caution_sources.append("small_unrealized_loss_without_critical_breaker")

    if critical_sources:
        category = "critical_safety_exit"
        reason_detail = _reason_detail(critical_sources)
        should_close = True
        should_watch = False
        close_decision_reason = f"close_paper:critical_safety_exit:{reason_detail}"
        triggered = True
        sources = critical_sources
    elif unknown_sources:
        category = "unknown_safety_exit"
        reason_detail = "missing_safety_exit_detail"
        should_close = True
        should_watch = False
        close_decision_reason = "close_paper:unknown_safety_exit:missing_safety_exit_detail"
        triggered = True
        sources = unknown_sources
    elif risk_state["risk_block_type"] == "entry_block" and risk_state["risk_block_applies_to_current_shadow"]:
        category = "entry_block_only"
        reason_detail = _reason_detail(caution_sources)
        should_close = False
        should_watch = True
        close_decision_reason = "watch_only:entry_block_current_shadow"
        triggered = False
        sources = caution_sources
    elif caution_sources:
        category = "caution_watch"
        reason_detail = _reason_detail(caution_sources)
        should_close = False
        should_watch = True
        close_decision_reason = f"watch_only:caution_watch:{reason_detail}"
        triggered = False
        sources = caution_sources
    else:
        category = "none"
        reason_detail = "no_safety_exit"
        should_close = False
        should_watch = False
        close_decision_reason = "no_close:no_safety_exit"
        triggered = False
        sources = []

    return {
        "safety_exit_triggered": bool(triggered),
        "safety_exit_category": category,
        "safety_exit_reason_detail": reason_detail,
        "safety_exit_sources": sources,
        "risk_block_type": risk_state["risk_block_type"],
        "risk_block_applies_to_current_shadow": bool(risk_state["risk_block_applies_to_current_shadow"]),
        "max_open_trades_limit": int(risk_state["max_open_trades_limit"]),
        "db_safety_state": _strip_internal_reasons(db_state),
        "capital_safety_state": _strip_internal_reasons(capital_state),
        "adaptive_safety_state": _strip_internal_reasons(adaptive_state),
        "risk_safety_state": _strip_internal_reasons(risk_state),
        "runtime_safety_state": _strip_internal_reasons(runtime_state),
        "open_shadow_safety_state": _strip_internal_reasons(open_shadow_state),
        "should_close_paper": bool(should_close),
        "should_watch_only": bool(should_watch),
        "close_decision_reason": close_decision_reason,
    }


def _empty_safety_analysis(*, reason_detail: str) -> dict[str, Any]:
    detail = reason_detail or "no_safety_exit"
    return {
        "safety_exit_triggered": False,
        "safety_exit_category": "none",
        "safety_exit_reason_detail": detail,
        "safety_exit_sources": [],
        "risk_block_type": "none",
        "risk_block_applies_to_current_shadow": False,
        "max_open_trades_limit": 1,
        "db_safety_state": {"status": "not_evaluated"},
        "capital_safety_state": {"status": "not_evaluated"},
        "adaptive_safety_state": {"status": "not_evaluated"},
        "risk_safety_state": {"status": "not_evaluated"},
        "runtime_safety_state": {"status": "not_evaluated"},
        "open_shadow_safety_state": {"status": detail},
        "should_close_paper": False,
        "should_watch_only": False,
        "close_decision_reason": f"no_close:{detail}",
    }


def _db_safety_state(db: dict[str, Any]) -> dict[str, Any]:
    queue_depth = int(_number(db.get("queue_depth")) or 0)
    reasons: list[str] = []
    if db.get("db_available") is False:
        reasons.append("db_unavailable")
    if db.get("db_degraded") is True:
        reasons.append("db_degraded")
    if db.get("tables_ready") is False:
        reasons.append("tables_not_ready")
    if queue_depth > DB_QUEUE_DEPTH_LIMIT:
        reasons.append("queue_depth_high")
    return {
        "status": "critical" if reasons else "ok",
        "db_available": bool(db.get("db_available")),
        "db_degraded": bool(db.get("db_degraded")),
        "tables_ready": bool(db.get("tables_ready")),
        "queue_depth": queue_depth,
        "queue_depth_limit": DB_QUEUE_DEPTH_LIMIT,
        "recommendation": db.get("recommendation") or "",
        "critical_reasons": reasons,
        "caution_reasons": [],
        "unknown_reasons": [],
    }


def _capital_safety_state(capital: dict[str, Any]) -> dict[str, Any]:
    state = _state_text(capital, "capital_state", "state", "status", "global_state")
    reason = str(capital.get("reason") or capital.get("recommended_action") or "").strip()
    critical: list[str] = []
    if state in {"kill_switch", "blocked", "capital_kill_switch"}:
        critical.append("capital_state_kill_switch")
    if "max_open_shadow_trades" in reason:
        critical.append("max_open_shadow_trades")
    if "max_profile_exposure" in reason:
        critical.append("max_profile_exposure")
    return {
        "status": "critical" if critical else "ok",
        "state": state or "not_provided",
        "reason": reason,
        "critical_reasons": critical,
        "caution_reasons": [],
        "unknown_reasons": [],
    }


def _adaptive_safety_state(adaptive: dict[str, Any]) -> dict[str, Any]:
    state = _state_text(adaptive, "adaptive_state", "global_state", "state", "status")
    reason = str(adaptive.get("reason") or adaptive.get("recommended_next_action") or "").strip()
    critical: list[str] = []
    caution: list[str] = []
    if state in {"kill_switch", "blocked", "halted"}:
        critical.append("adaptive_state_blocked")
    elif state == "watch":
        caution.append("adaptive_state_watch")
    return {
        "status": "critical" if critical else "watch" if caution else "ok",
        "state": state or "not_provided",
        "reason": reason,
        "critical_reasons": critical,
        "caution_reasons": caution,
        "unknown_reasons": [],
    }


def _risk_safety_state(
    risk: dict[str, Any],
    *,
    open_shadow_count: int,
    current_shadow_trade_id: str,
) -> dict[str, Any]:
    allowed = _risk_allows(risk)
    reason = str(risk.get("reason") or risk.get("risk_governor_reason") or "").strip()
    limit = int(_number(risk.get("max_open_trades") or risk.get("max_open_trades_limit")) or 1)
    critical: list[str] = []
    caution: list[str] = []
    unknown: list[str] = []
    risk_block_type = "none"
    applies_to_current_shadow = False
    if not allowed:
        if reason == "max_open_trades_reached":
            if int(open_shadow_count or 0) == 1 and current_shadow_trade_id:
                risk_block_type = "entry_block"
                applies_to_current_shadow = True
                caution.append("entry_block_current_shadow")
            elif int(open_shadow_count or 0) > limit:
                risk_block_type = "exit_block"
                critical.append("risk_governor_max_open_trades_over_limit")
            else:
                risk_block_type = "entry_block"
                caution.append("entry_block_no_current_shadow_match")
        elif _generic_safety_reason(reason):
            risk_block_type = "exit_block"
            unknown.append("risk_governor_block_without_detail")
        else:
            risk_block_type = "exit_block"
            critical.append(f"risk_governor_explicit_block:{reason}")
    return {
        "status": "critical" if critical else "unknown" if unknown else "watch" if caution else "ok",
        "allowed": bool(allowed),
        "risk_state": risk.get("risk_state") or risk.get("state") or "",
        "reason": reason,
        "risk_block_type": risk_block_type,
        "risk_block_applies_to_current_shadow": bool(applies_to_current_shadow),
        "open_shadow_count": int(open_shadow_count or 0),
        "max_open_trades_limit": limit,
        "critical_reasons": critical,
        "caution_reasons": caution,
        "unknown_reasons": unknown,
    }


def _runtime_safety_state(snapshot: dict[str, Any], metrics: dict[str, Any], *, max_runtime_age_minutes: float) -> dict[str, Any]:
    tick = _tick(snapshot)
    tick_available = isinstance(tick, dict) and bool(tick)
    bars_count = int(_number(snapshot.get("bars_count")) or 0)
    last_at = _parse_time(snapshot.get("last_tick_at") or snapshot.get("updated_at"))
    tick_age = None
    if last_at is not None:
        tick_age = round((datetime.now(timezone.utc) - last_at).total_seconds() / 60.0, 3)
    context = str(snapshot.get("runtime_snapshot_context") or "").strip()
    complete = bool(snapshot.get("runtime_snapshot_complete"))
    critical: list[str] = []
    if not bool(snapshot.get("runtime_snapshot_available")):
        critical.append("runtime_context_unavailable")
    if snapshot.get("runtime_snapshot_recent") is False:
        critical.append("runtime_context_stale")
    if context != "bar_context" or not complete:
        critical.append("runtime_context_incomplete")
    if not tick_available:
        critical.append("no_tick")
    if bars_count < MIN_BARS_COUNT:
        critical.append("no_bars_or_insufficient_bars")
    if last_at is None:
        critical.append("missing_tick_timestamp")
    elif tick_age is not None and tick_age > float(max_runtime_age_minutes):
        critical.append("runtime_context_stale")
    return {
        "status": "critical" if critical else "ok",
        "runtime_context_available": bool(snapshot.get("runtime_snapshot_available")),
        "runtime_context_recent": bool(snapshot.get("runtime_snapshot_recent")),
        "runtime_snapshot_context": context,
        "runtime_snapshot_complete": complete,
        "tick_available": tick_available,
        "bars_count": bars_count,
        "min_bars_count": MIN_BARS_COUNT,
        "latest_tick_age_minutes": tick_age,
        "current_price_available": bool(metrics.get("current_price")),
        "critical_reasons": _dedupe(critical),
        "caution_reasons": [],
        "unknown_reasons": [],
    }


def _open_shadow_safety_state(open_shadow_count: int) -> dict[str, Any]:
    critical = ["multiple_open_shadows"] if int(open_shadow_count or 0) > 1 else []
    return {
        "status": "critical" if critical else "ok",
        "open_shadow_count": int(open_shadow_count or 0),
        "critical_reasons": critical,
        "caution_reasons": [],
        "unknown_reasons": [],
    }


def _capital_state(injected: dict[str, Any] | None) -> dict[str, Any]:
    return dict(injected or {"capital_state": "normal", "reason": "not_provided"})


def _adaptive_state(injected: dict[str, Any] | None) -> dict[str, Any]:
    return dict(injected or {"adaptive_state": "normal", "reason": "not_provided"})


def _state_text(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        text = str(payload.get(key) or "").casefold().strip()
        if text:
            return text
    return ""


def _generic_safety_reason(reason: str) -> bool:
    clean = str(reason or "").casefold().strip()
    return clean in {"", "safety_exit", "unknown", "risk_block", "risk_governor_block"}


def _reason_detail(reasons: list[str]) -> str:
    return ",".join(_dedupe([str(reason) for reason in reasons if str(reason or "").strip()])) or "missing_safety_exit_detail"


def _strip_internal_reasons(state: dict[str, Any]) -> dict[str, Any]:
    public = dict(state)
    public.pop("critical_reasons", None)
    public.pop("caution_reasons", None)
    public.pop("unknown_reasons", None)
    return public


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _updated_trade(trade: dict[str, Any], metrics: dict[str, Any], exit_reason: str) -> dict[str, Any]:
    updated = dict(trade)
    pnl = float(metrics["unrealized_pnl"])
    updated.update(
        {
            "last_price": metrics["current_price"],
            "unrealized_pnl": metrics["unrealized_pnl"],
            "unrealized_pnl_pct": metrics["unrealized_pnl_pct"],
            "r_multiple": metrics["r_multiple"],
            "max_favorable_excursion": max(_number(updated.get("max_favorable_excursion")) or 0.0, pnl),
            "max_adverse_excursion": min(_number(updated.get("max_adverse_excursion")) or 0.0, pnl),
            "last_exit_reason": exit_reason,
            "updated_at": _now(),
            **_safety(),
        }
    )
    return updated


def _closed_trade(trade: dict[str, Any], metrics: dict[str, Any], exit_reason: str) -> dict[str, Any]:
    closed = _updated_trade(trade, metrics, exit_reason)
    pnl = float(metrics["unrealized_pnl"])
    closed.update(
        {
            "status": "closed",
            "lifecycle_status": "closed",
            "closed_at": _now(),
            "exit_price": metrics["current_price"],
            "exit_reason": exit_reason,
            "pnl": round(pnl, 6),
            "pnl_pct": metrics["unrealized_pnl_pct"],
            "result": "win" if pnl > 0 else "loss" if pnl < 0 else "flat",
            "source": closed.get("source") or "paper_observation_shadow_once",
            **_safety(),
        }
    )
    return closed


def _persist_close(store: Any | None, closed: dict[str, Any]) -> dict[str, Any]:
    active_store = store or MT5PersistentIntelligenceStore()
    results: dict[str, Any] = {}
    try:
        results["shadow_trade"] = active_store.record_shadow_trade(closed, critical=False)
    except Exception as exc:
        results["shadow_trade"] = {"ok": False, "error": type(exc).__name__}
    try:
        r_multiple = _number(closed.get("r_multiple")) or 0.0
        results["profile_performance"] = active_store.upsert_profile_performance(
            {
                "symbol": SYMBOL,
                "timeframe": TIMEFRAME,
                "profile": CANDIDATE_PROFILE,
                "trades_forward": 1,
                "wins": 1 if r_multiple > 0 else 0,
                "losses": 1 if r_multiple < 0 else 0,
                "expectancy": r_multiple,
                "recent_closed": 1,
                "recent_expectancy": r_multiple,
                "updated_at": _now(),
            },
            critical=False,
        )
    except Exception as exc:
        results["profile_performance"] = {"ok": False, "error": type(exc).__name__}
    if str(closed.get("exit_reason") or "") in {"stop_loss_hit", "safety_exit", "trailing_defensive_exit"}:
        try:
            results["research_lesson"] = active_store.record_research_lesson(
                {
                    "family": "volatility_compression_breakout",
                    "symbol": SYMBOL,
                    "timeframe": TIMEFRAME,
                    "lesson_type": "paper_observation_shadow_closed",
                    "failure_pattern": str(closed.get("exit_reason") or ""),
                    "summary": f"XAUUSD M15 paper observation shadow closed with r_multiple={closed.get('r_multiple')}",
                    "avoid_next": [],
                    "recommended_next_research_phase": "continue_paper_observation_review",
                },
                critical=False,
            )
        except Exception as exc:
            results["research_lesson"] = {"ok": False, "error": type(exc).__name__}
    return {"ok": True, "results": results, **_safety()}


def _db_state(store: Any | None, injected: dict[str, Any] | None) -> dict[str, Any]:
    if injected is not None:
        return dict(injected)
    try:
        active_store = store or MT5PersistentIntelligenceStore()
        return dict(active_store.healthcheck(write_test_event=False))
    except Exception as exc:
        return {"db_available": False, "db_degraded": True, "tables_ready": False, "reason": type(exc).__name__, **_safety()}


def _risk_state(snapshot: dict[str, Any], trade: dict[str, Any], injected: dict[str, Any] | None) -> dict[str, Any]:
    if injected is not None:
        return dict(injected)
    try:
        return dict(assess_runtime_risk(SYMBOL, timeframe=TIMEFRAME, tick=_tick(snapshot), open_trade=trade))
    except Exception as exc:
        return {"allowed": False, "reason": type(exc).__name__, "risk_state": "unknown", **_safety()}


def _public_db_state(db: dict[str, Any]) -> dict[str, Any]:
    return {
        "provider": db.get("provider") or "",
        "db_available": bool(db.get("db_available")),
        "db_degraded": bool(db.get("db_degraded")),
        "tables_ready": bool(db.get("tables_ready")),
        "queue_depth": int(_number(db.get("queue_depth")) or 0),
        "recommendation": db.get("recommendation") or "",
        **_safety(),
    }


def _db_healthy(db: dict[str, Any]) -> bool:
    return bool(db.get("db_available") and db.get("tables_ready") and not db.get("db_degraded"))


def _risk_allows(risk: dict[str, Any]) -> bool:
    if "allowed" in risk:
        return bool(risk.get("allowed"))
    if "risk_governor_allowed" in risk:
        return bool(risk.get("risk_governor_allowed"))
    return str(risk.get("reason") or "") == "risk_governor_pass"


def _runtime_stale(snapshot: dict[str, Any], *, max_age_minutes: float) -> bool:
    if not bool(snapshot.get("runtime_snapshot_available")):
        return True
    if not isinstance(snapshot.get("last_tick"), dict):
        return True
    if snapshot.get("runtime_snapshot_recent") is False:
        return True
    last_at = _parse_time(snapshot.get("last_tick_at") or snapshot.get("updated_at"))
    if last_at is None:
        return True
    return (datetime.now(timezone.utc) - last_at).total_seconds() / 60.0 > float(max_age_minutes)


def _trailing_defensive_hit(trade: dict[str, Any], metrics: dict[str, Any]) -> bool:
    profile = str(trade.get("candidate_profile") or trade.get("strategy_profile") or "")
    if "trailing_defensive" not in profile:
        return False
    initial_risk = float(metrics.get("initial_risk") or 0.0)
    if initial_risk <= 0:
        return False
    previous_max = _number(trade.get("max_favorable_excursion")) or 0.0
    current_pnl = float(metrics.get("unrealized_pnl") or 0.0)
    max_favorable = max(previous_max, current_pnl)
    return max_favorable >= initial_risk * 0.75 and current_pnl <= max_favorable - initial_risk * 0.5


def _bars_since_entry(snapshot: dict[str, Any], opened_at: object) -> int:
    opened = _parse_time(opened_at)
    bars = snapshot.get("ohlc_recent") if isinstance(snapshot.get("ohlc_recent"), list) else []
    if opened is None or not bars:
        return 0
    count = 0
    for bar in bars:
        if not isinstance(bar, dict):
            continue
        bar_time = _parse_time(bar.get("time") or bar.get("timestamp") or bar.get("datetime"))
        if bar_time is not None and bar_time >= opened:
            count += 1
    return count


def _age_minutes(value: object) -> float:
    opened = _parse_time(value)
    if opened is None:
        return 0.0
    return max(0.0, (datetime.now(timezone.utc) - opened).total_seconds() / 60.0)


def _last_price(snapshot: dict[str, Any]) -> float | None:
    tick = _tick(snapshot)
    return _number(tick.get("last") or tick.get("price") or snapshot.get("last") or snapshot.get("last_price"))


def _tick(snapshot: dict[str, Any]) -> dict[str, Any]:
    return snapshot.get("last_tick") if isinstance(snapshot.get("last_tick"), dict) else {}


def _pnl(side: str, entry: float, current: float) -> float:
    if not entry or not current:
        return 0.0
    return current - entry if side == "buy" else entry - current


def _parse_time(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _symbol(value: object) -> str:
    symbol = str(value or "").upper().strip().replace(".B", "")
    if symbol == "XAUUSDB":
        return "XAUUSD"
    return symbol


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
