from __future__ import annotations

from typing import Any

from services.genesis.memory_store import MemoryStore
from services.mt5.mt5_autonomous_learning_status import run_autonomous_learning_status
from services.mt5.mt5_capital_protection_governor import run_capital_protection_governor
from services.mt5.mt5_persistent_db_doctor import run_persistent_db_doctor
from services.mt5.mt5_persistent_intelligence_store import (
    normalize_shadow_trade_sample_validity,
    persistent_intelligence_failed_write_summary,
    persistent_intelligence_open_shadow_trades,
    persistent_intelligence_queue_drain,
    persistent_intelligence_recent_events,
    persistent_intelligence_schema_freeze_status,
    persistent_intelligence_shadow_trade_history,
    persistent_intelligence_status,
)
from services.mt5.mt5_persistent_intelligence_bootstrap import persistent_intelligence_bootstrap_status
from services.mt5.mt5_risk_recovery import mt5_risk_recovery_status
from services.mt5.mt5_runtime_snapshot import runtime_snapshot_inventory
from services.mt5.mt5_runtime_snapshot import get_snapshot
from services.mt5.mt5_signal_router import MT5SignalRouter
from services.mt5.mt5_strategy_tournament import run_strategy_tournament
from services.mt5.mt5_xau_m15_paper_shadow_monitor import run_xau_m15_paper_shadow_monitor
from services.mt5.mt5_xau_m15_paper_observation_readiness import (
    run_xau_m15_paper_observation_cycle,
    run_xau_m15_paper_observation_readiness,
    run_xau_m15_paper_observation_shadow_once,
)
from services.mt5.mt5_xau_m15_runtime_open_shadow_backfill import run_xau_m15_runtime_open_shadow_backfill


def build_router(memory: MemoryStore | None = None) -> MT5SignalRouter:
    return MT5SignalRouter(memory=memory)


def mt5_health(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).health()


def mt5_config(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).config_payload()


def mt5_ops_status(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).ops_status(symbol=symbol)


def mt5_risk_state(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).risk_state(symbol=symbol, timeframe=timeframe)


def mt5_risk_recovery(*, memory: MemoryStore | None = None, symbol: str = "ETHUSD", timeframe: str = "M30") -> dict[str, Any]:
    return mt5_risk_recovery_status(symbol=symbol, timeframe=timeframe)


def mt5_persistent_intelligence_status(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    return _status_write_free(persistent_intelligence_status(write_test_event=False))


def mt5_persistent_intelligence_recent_events(*, memory: MemoryStore | None = None, limit: int = 10) -> dict[str, Any]:
    return _status_write_free(persistent_intelligence_recent_events(limit=limit))


def mt5_persistent_intelligence_failed_write_summary(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    return _status_write_free(persistent_intelligence_failed_write_summary())


def mt5_persistent_intelligence_queue_drain(
    payload: dict[str, Any] | None = None,
    *,
    memory: MemoryStore | None = None,
) -> dict[str, Any]:
    del memory
    body = payload if isinstance(payload, dict) else {}
    if not bool(body.get("confirm_queue_drain")):
        return {
            "ok": False,
            "status": "persistent_intelligence_queue_drain_confirmation_required",
            "reason": "confirm_queue_drain_required",
            "drain_attempted": False,
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
    try:
        max_items = max(1, min(200, int(body.get("max_items") or 50)))
    except (TypeError, ValueError):
        max_items = 50
    keep_failed = bool(body.get("keep_failed_noncritical"))
    return persistent_intelligence_queue_drain(
        max_items=max_items,
        drop_failed_noncritical=not keep_failed,
    )


def mt5_persistent_intelligence_bootstrap_status(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    return persistent_intelligence_bootstrap_status()


def mt5_persistent_db_doctor_status(
    *,
    memory: MemoryStore | None = None,
    repair: bool = False,
    apply_schema: bool = False,
    wait_for_connection: bool = False,
    max_connect_attempts: int = 10,
    verbose_sanitized: bool = False,
) -> dict[str, Any]:
    return run_persistent_db_doctor(
        repair=repair,
        apply_schema=apply_schema,
        wait_for_connection=wait_for_connection,
        max_connect_attempts=max_connect_attempts,
        verbose_sanitized=verbose_sanitized,
    )


def mt5_capital_protection_status(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    blocked = _schema_missing_fast_fail("capital_protection")
    if blocked:
        return blocked
    return _status_write_free(
        run_capital_protection_governor(
            persist_events=False,
        )
    )


def mt5_strategy_tournament_status(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    blocked = _schema_missing_fast_fail("strategy_tournament")
    if blocked:
        return blocked
    return _status_write_free(
        run_strategy_tournament(
            load_rotation=False,
            persist_events=False,
        )
    )


def _schema_missing_fast_fail(endpoint: str, *, symbol: str = "") -> dict[str, Any]:
    freeze = persistent_intelligence_schema_freeze_status()
    if not freeze.get("writes_frozen"):
        return {}
    base = {
        "ok": True,
        **freeze,
        "status": f"mt5_{endpoint}_paused_by_db_schema_missing",
        "provider": "railway_postgres",
        "symbol": str(symbol or ""),
        "db_degraded": True,
        "schema_missing_write_freeze": True,
        "writes_frozen": True,
        "learning_state": "paused_by_db_schema_missing",
        "decision": "NO_TRADE",
        "reason": "persistent_intelligence_schema_missing",
        "recommended_action": "apply_schema_sql",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "status_endpoints_write_free": True,
        "secrets_printed": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
    if endpoint == "capital_protection":
        base.update(
            {
                "capital_state": "paused_by_db_schema_missing",
                "safe_to_trade": False,
                "circuit_breakers": [
                    {
                        "name": "persistent_intelligence_schema_missing",
                        "active": True,
                        "severity": "critical",
                        "reason": "persistent_intelligence_schema_missing",
                    }
                ],
            }
        )
    if endpoint == "strategy_tournament":
        base.update(
            {
                "recommended_action": "skip_tournament_until_schema_ready",
                "top_candidate": {},
                "ranked_candidates": [],
                "candidate_activated": False,
                "paper_forward_onboarding_started": False,
            }
        )
    return base


def _status_write_free(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload or {})
    result["status_endpoints_write_free"] = True
    result.setdefault("broker_touched", False)
    result.setdefault("order_executed", False)
    result.setdefault("order_policy", "journal_only_no_broker")
    return result


def _merge_open_shadow_payloads(
    runtime_payload: dict[str, Any],
    persistent_payload: dict[str, Any],
    *,
    symbol: str = "",
    limit: int = 100,
) -> dict[str, Any]:
    clean_symbol = _clean_symbol(symbol or runtime_payload.get("symbol") or "XAUUSD")
    safe_limit = max(1, min(int(limit or 100), 100))
    runtime_trades = [
        _paper_safe_trade(trade, source="runtime_memory")
        for trade in _rows(runtime_payload, "trades", "open_trades")
        if _is_symbol_match(trade, clean_symbol)
    ]
    persistent_trades = [
        _paper_safe_trade(trade, source="persistent_intelligence_fallback")
        for trade in _rows(persistent_payload, "open_trades", "trades")
        if _is_symbol_match(trade, clean_symbol)
    ]
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    duplicate_detected = False
    for trade in [*runtime_trades, *persistent_trades]:
        trade_id = str(trade.get("shadow_trade_id") or "")
        key = trade_id or f"{trade.get('symbol')}:{trade.get('timeframe')}:{trade.get('opened_at')}:{len(merged)}"
        if key in seen:
            duplicate_detected = True
            continue
        seen.add(key)
        merged.append(trade)
    runtime_count = len(runtime_trades)
    persistent_count = len(persistent_trades)
    if runtime_count and persistent_count:
        open_source = "merged"
    elif runtime_count:
        open_source = "runtime_memory"
    elif persistent_count:
        open_source = "persistent_intelligence_fallback"
    else:
        open_source = "none"
    result = dict(runtime_payload or {})
    result.update(
        {
            "ok": bool(runtime_payload.get("ok", True)) or bool(persistent_payload.get("ok")),
            "status": "mt5_shadow_trades_open_ready",
            "symbol": clean_symbol,
            "open_count": len(merged),
            "trades": merged[:safe_limit],
            "open_source": open_source,
            "rehydration_needed": bool(persistent_count and not runtime_count),
            "persistent_open_count": persistent_count,
            "runtime_open_count": runtime_count,
            "merged_open_count": len(merged),
            "duplicate_detected": duplicate_detected or (runtime_count + persistent_count > len(merged)),
            "persistent_intelligence_open": {
                "ok": bool(persistent_payload.get("ok")),
                "db_degraded": bool(persistent_payload.get("db_degraded")),
                "reason": persistent_payload.get("reason") or "",
                "provider": persistent_payload.get("provider") or "",
            },
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
    )
    return result


def _merge_runtime_closed_history(
    payload: dict[str, Any],
    *,
    symbol: str = "",
    timeframe: str = "",
    limit: int = 20,
) -> dict[str, Any]:
    clean_symbol = _clean_symbol(symbol or payload.get("symbol") or "XAUUSD")
    clean_timeframe = _clean_timeframe(timeframe or payload.get("timeframe") or "")
    safe_limit = max(1, min(int(limit or payload.get("limit") or 20), 100))
    runtime_rows = _runtime_closed_shadow_trades(clean_symbol, clean_timeframe)
    persistent_rows = [
        _paper_safe_trade(row, source="persistent_intelligence")
        for row in _rows(payload, "trades", "closed_trades")
        if _is_symbol_match(row, clean_symbol) and (not clean_timeframe or _clean_timeframe(row.get("timeframe")) == clean_timeframe)
    ]
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in [*runtime_rows, *persistent_rows]:
        trade_id = str(row.get("shadow_trade_id") or "")
        key = trade_id or f"{row.get('symbol')}:{row.get('timeframe')}:{row.get('closed_at')}:{len(merged)}"
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)
    open_trades = [row for row in merged if str(row.get("status") or "").casefold() == "open" and not row.get("closed_at")]
    closed_trades = [row for row in merged if str(row.get("status") or "").casefold() == "closed" or bool(row.get("closed_at"))]
    result = dict(payload or {})
    result.update(
        {
            "trades": merged[:safe_limit],
            "open_trades": open_trades[:safe_limit],
            "closed_trades": closed_trades[:safe_limit],
            "open_count": len(open_trades),
            "closed_count": len(closed_trades),
            "runtime_closed_event_count": len(runtime_rows),
            "persistent_closed_event_count": len([row for row in persistent_rows if str(row.get("status") or "").casefold() == "closed" or row.get("closed_at")]),
            "history_sources": {
                "runtime_closed_event": len(runtime_rows),
                "persistent_intelligence": len(persistent_rows),
            },
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
    )
    return result


def _runtime_closed_shadow_trades(symbol: str, timeframe: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for alias in {symbol, f"{symbol}.B" if symbol == "XAUUSD" else symbol}:
        snapshot = get_snapshot(alias, timeframe) or get_snapshot(alias) or {}
        for row in snapshot.get("recent_closed_shadow_trades") if isinstance(snapshot.get("recent_closed_shadow_trades"), list) else []:
            if not isinstance(row, dict):
                continue
            if not _is_symbol_match(row, symbol):
                continue
            if timeframe and _clean_timeframe(row.get("timeframe")) != timeframe:
                continue
            safe = _paper_safe_trade(row, source="runtime_closed_event")
            key = str(safe.get("shadow_trade_id") or f"{safe.get('symbol')}:{safe.get('timeframe')}:{safe.get('closed_at')}")
            if key in seen:
                continue
            seen.add(key)
            rows.append(safe)
    return rows


def _rows(payload: dict[str, Any], *keys: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in keys:
        value = payload.get(key) if isinstance(payload, dict) else None
        if isinstance(value, list):
            rows.extend([dict(row) for row in value if isinstance(row, dict)])
    return rows


def _paper_safe_trade(row: dict[str, Any], *, source: str) -> dict[str, Any]:
    trade = normalize_shadow_trade_sample_validity(dict(row or {}))
    trade.setdefault("symbol", _clean_symbol(trade.get("symbol")))
    trade.setdefault("timeframe", _clean_timeframe(trade.get("timeframe")))
    trade["record_source"] = source
    trade["source"] = source if source in {"runtime_closed_event", "persistent_intelligence"} else trade.get("source") or source
    trade["broker_touched"] = False
    trade["order_executed"] = False
    trade["order_policy"] = "journal_only_no_broker"
    return trade


def _is_symbol_match(row: dict[str, Any], symbol: str) -> bool:
    if not symbol:
        return True
    return _clean_symbol(row.get("symbol") or row.get("broker_symbol")) == _clean_symbol(symbol)


def _clean_symbol(value: object) -> str:
    return str(value or "").upper().strip().replace(".B", "")


def _clean_timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def mt5_ui_summary(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).ui_summary(symbol=symbol, timeframe=timeframe)


def mt5_status(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).status()


def mt5_journal_recent(*, memory: MemoryStore | None = None, limit: int = 25, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).journal_recent(limit=limit, symbol=symbol)


def mt5_performance(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).performance(symbol=symbol, timeframe=timeframe)


def mt5_performance_auto(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).performance_auto(symbol=symbol, timeframe=timeframe)


def mt5_forward_test(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).forward_test(symbol=symbol, timeframe=timeframe)


def mt5_outcomes_recent(*, memory: MemoryStore | None = None, symbol: str = "", limit: int = 25) -> dict[str, Any]:
    return build_router(memory).outcomes_recent(symbol=symbol, limit=limit)


def mt5_no_trade_report(*, memory: MemoryStore | None = None, symbol: str = "", limit: int = 50) -> dict[str, Any]:
    return build_router(memory).no_trade_report(symbol=symbol, limit=limit)


def mt5_shadow_trades(*, memory: MemoryStore | None = None, symbol: str = "", limit: int = 100) -> dict[str, Any]:
    return build_router(memory).shadow_trades(symbol=symbol, limit=limit)


def mt5_shadow_trades_open(*, memory: MemoryStore | None = None, symbol: str = "", limit: int = 100) -> dict[str, Any]:
    runtime_payload = build_router(memory).shadow_trades_open(symbol=symbol, limit=limit)
    persistent_payload = persistent_intelligence_open_shadow_trades(limit=limit)
    return _merge_open_shadow_payloads(runtime_payload, persistent_payload, symbol=symbol, limit=limit)


def mt5_shadow_trades_history(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "", limit: int = 20) -> dict[str, Any]:
    del memory
    payload = persistent_intelligence_shadow_trade_history(symbol=symbol, timeframe=timeframe, limit=limit)
    return _merge_runtime_closed_history(payload, symbol=symbol, timeframe=timeframe, limit=limit)


def mt5_shadow_trades_runtime_open_backfill(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    del memory
    body = payload if isinstance(payload, dict) else {}
    snapshot = body.get("snapshot") if isinstance(body.get("snapshot"), dict) else body
    return run_xau_m15_runtime_open_shadow_backfill(
        snapshot=snapshot if isinstance(snapshot, dict) else {},
        confirm_paper_only_backfill=bool(body.get("confirm_paper_only_backfill")),
    )


def mt5_shadow_trades_close_expired(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).shadow_trades_close_expired(payload)


def mt5_shadow_trade_close(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).shadow_trade_close(payload)


def mt5_debug_storage(*, memory: MemoryStore | None = None, symbol: str = "", limit: int = 20) -> dict[str, Any]:
    return build_router(memory).debug_storage(symbol=symbol, limit=limit)


def mt5_runtime_snapshot_inventory(
    *,
    memory: MemoryStore | None = None,
    symbol: str = "XAUUSD",
    broker_symbol: str = "XAUUSD.b",
    timeframe: str = "M15",
) -> dict[str, Any]:
    return runtime_snapshot_inventory(lookup_symbols=[symbol, broker_symbol], lookup_timeframe=timeframe)


def mt5_xau_m15_paper_observation_readiness(
    *,
    memory: MemoryStore | None = None,
    store: Any | None = None,
) -> dict[str, Any]:
    return run_xau_m15_paper_observation_readiness(store=store)


def mt5_paper_observation_readiness(
    *,
    memory: MemoryStore | None = None,
    symbol: str = "XAUUSD",
    broker_symbol: str = "XAUUSD.b",
    timeframe: str = "M15",
) -> dict[str, Any]:
    del memory
    from services.mt5.mt5_xau_m15_paper_observation_batch_runner import run_multi_asset_paper_observation_readiness

    return run_multi_asset_paper_observation_readiness(symbol=symbol, broker_symbol=broker_symbol, timeframe=timeframe)


def mt5_xau_m15_paper_observation_cycle(
    *,
    memory: MemoryStore | None = None,
    store: Any | None = None,
) -> dict[str, Any]:
    return run_xau_m15_paper_observation_cycle(store=store, paper_shadow_once=False)


def mt5_xau_m15_paper_observation_shadow_once(
    payload: dict[str, Any] | None = None,
    *,
    memory: MemoryStore | None = None,
    store: Any | None = None,
) -> dict[str, Any]:
    return run_xau_m15_paper_observation_shadow_once(payload=payload, store=store)


def mt5_xau_m15_paper_shadow_monitor(
    payload: dict[str, Any] | None = None,
    *,
    memory: MemoryStore | None = None,
    store: Any | None = None,
    apply_paper_close: bool = False,
) -> dict[str, Any]:
    body = payload or {}
    requested_apply = bool(apply_paper_close or body.get("apply_paper_close") is True)
    return run_xau_m15_paper_shadow_monitor(
        apply_paper_close=requested_apply,
        store=store,
        exit_policy=str(body.get("exit_policy") or "default"),
        time_stop_bars=int(_maybe_float(body.get("time_stop_bars")) or 1),
        max_hold_minutes=_maybe_float(body.get("max_hold_minutes")),
        min_r_to_arm_trailing=float(_maybe_float(body.get("min_r_to_arm_trailing")) or 0.25),
        giveback_r=float(_maybe_float(body.get("giveback_r")) or 0.15),
        fast_loss_cut_r=float(_maybe_float(body.get("fast_loss_cut_r")) or -0.25),
    )


def mt5_instrument(*, memory: MemoryStore | None = None, symbol: str = "", payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return build_router(memory).instrument(symbol=symbol, payload=payload)


def mt5_auto_forward_status(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).auto_forward_status(symbol=symbol)


def _maybe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def mt5_account_sync(payload: dict[str, Any] | None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).account_sync(payload)


def mt5_signal(payload: dict[str, Any] | None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).signal(payload)


def mt5_tick(payload: dict[str, Any] | None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).tick(payload)


def mt5_bars(payload: dict[str, Any] | None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).bars(payload)


def mt5_decision(symbol: str, *, memory: MemoryStore | None = None, timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).decision(symbol, timeframe=timeframe)


def mt5_order_request(payload: dict[str, Any] | None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).order_request(payload)


def mt5_order_result(payload: dict[str, Any] | None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).order_result(payload)


def mt5_manual_tests_reset(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    body = payload or {}
    return build_router(memory).reset_manual_tests(symbol=str(body.get("symbol") or body.get("ticker") or ""))


def mt5_metrics_exclude_old_proxy(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    body = payload or {}
    return build_router(memory).exclude_old_proxy_metrics(symbol=str(body.get("symbol") or body.get("ticker") or ""))


def mt5_replay_run(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).replay_run(payload)


def mt5_replay_results(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).replay_results(symbol=symbol)


def mt5_replay_status(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).replay_status(symbol=symbol)


def mt5_replay_reset(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).replay_reset(payload)


def mt5_backtest_run(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).backtest_run(payload)


def mt5_backtest_optimize(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).backtest_optimize(payload)


def mt5_backtest_latest(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).backtest_latest(symbol=symbol)


def mt5_forward_replay_run(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).forward_replay_run(payload)


def mt5_learning_run(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    blocked = _schema_missing_fast_fail("learning_run")
    if blocked:
        return blocked
    return build_router(memory).learning_run(payload)


def mt5_memory_summary(*, memory: MemoryStore | None = None, symbol: str = "", limit: int = 50) -> dict[str, Any]:
    return build_router(memory).memory_summary(symbol=symbol, limit=limit)


def mt5_learning_status(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    blocked = _schema_missing_fast_fail("learning_status", symbol=symbol)
    if blocked:
        return _status_write_free(_legacy_learning_status(blocked, symbol=symbol))
    return _status_write_free(_legacy_learning_status(build_router(memory).learning_status(symbol=symbol), symbol=symbol))


def mt5_autonomous_learning_status(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return _status_write_free(run_autonomous_learning_status(symbol=symbol or "BTCUSD", timeframe=timeframe))


def _legacy_learning_status(result: dict[str, Any], *, symbol: str = "") -> dict[str, Any]:
    payload = dict(result)
    payload["legacy_learning_status"] = True
    payload["autonomous_learning_status_endpoint"] = (
        f"/api/genesis/mt5/autonomous-learning/status?symbol={symbol or '{symbol}'}&timeframe={{timeframe}}"
    )
    return payload


def mt5_adaptive_state(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).adaptive_state(symbol=symbol, timeframe=timeframe)


def mt5_strategy_profiles(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).strategy_profiles(symbol=symbol)


def mt5_adaptive_recommendations(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).adaptive_recommendations(symbol=symbol, timeframe=timeframe)


def mt5_paper_defense(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).paper_defense_status(symbol=symbol)


def mt5_promoted_profile(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).promoted_profile(symbol=symbol, timeframe=timeframe)


def mt5_forward_profile_state(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).forward_profile_state(symbol=symbol, timeframe=timeframe)
