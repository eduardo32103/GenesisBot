from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore
from services.mt5.mt5_risk_governor import assess_runtime_risk
from services.mt5.mt5_runtime_snapshot import append_closed_shadow_trade, get_snapshot, update_open_shadow_trade
from services.mt5.mt5_xau_m15_paper_observation_readiness import BROKER_SYMBOL, CANDIDATE_PROFILE, SYMBOL, TIMEFRAME


MONITOR_VERSION = "2026-06-14.mt5_xau_m15_paper_shadow_monitor.v1"
MAX_RUNTIME_AGE_MINUTES = 45.0


def run_xau_m15_paper_shadow_monitor(
    *,
    apply_paper_close: bool = False,
    store: MT5PersistentIntelligenceStore | Any | None = None,
    runtime_snapshot: dict[str, Any] | None = None,
    db_state: dict[str, Any] | None = None,
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
            return _base_result(
                monitor_state="blocked_multiple_open_shadows",
                open_shadow_count=persistent_open_count,
                exit_signal=False,
                exit_reason="multiple_open_shadows_persisted",
                paper_close_applied=False,
                shadow_status_after="blocked",
                runtime_snapshot=snapshot,
                shadow_source="persistent_intelligence_fallback",
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
        )

    metrics = _trade_metrics(open_trade, snapshot)
    db = _db_state(store, db_state)
    risk = _risk_state(snapshot, open_trade, risk_state)
    exit_signal, exit_reason, apply_blocked = _exit_decision(
        trade=open_trade,
        snapshot=snapshot,
        metrics=metrics,
        db=db,
        risk=risk,
        max_runtime_age_minutes=max_runtime_age_minutes,
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
        monitor_state = "apply_blocked_stale_runtime"

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
) -> dict[str, Any]:
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
    snapshot: dict[str, Any],
    metrics: dict[str, Any],
    db: dict[str, Any],
    risk: dict[str, Any],
    max_runtime_age_minutes: float,
) -> tuple[bool, str, bool]:
    if _runtime_stale(snapshot, max_age_minutes=max_runtime_age_minutes) or not metrics.get("current_price"):
        return True, "stale_runtime_context", True
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
    if not _db_healthy(db) or not _risk_allows(risk):
        return True, "safety_exit", False
    return False, "", False


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
