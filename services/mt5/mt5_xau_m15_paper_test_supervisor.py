from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from services.mt5.mt5_xau_m15_paper_observation_batch_runner import (
    DEFAULT_RESULTS_FILE,
    DEFAULT_STATE_FILE,
    HttpPaperObservationClient,
    LocalPaperObservationClient,
    compute_xau_m15_paper_batch_stats,
    _readiness_uses_db_snapshot,
    _readiness_with_db_snapshot,
    run_xau_m15_paper_observation_batch_runner,
)
from services.mt5.mt5_persistent_intelligence_store import suppress_noncritical_risk_event_writes
from services.mt5.mt5_xau_m15_paper_observation_readiness import BROKER_SYMBOL, CANDIDATE_PROFILE, SYMBOL, TIMEFRAME


SUPERVISOR_VERSION = "2026-07-03.xau_m15_paper_test_supervisor.v3"


def run_xau_m15_paper_test_supervisor(
    *,
    client: Any | None = None,
    base_url: str = "",
    symbol: str = SYMBOL,
    broker_symbol: str = BROKER_SYMBOL,
    timeframe: str = TIMEFRAME,
    allowed_symbols: list[str] | tuple[str, ...] | None = None,
    asset_configs: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
    target_trades: int = 3,
    max_cycles: int = 120,
    interval_seconds: float = 30.0,
    dry_run: bool = True,
    paper_only_confirmed: bool = False,
    once: bool = False,
    exit_policy: str = "fast_observation",
    time_stop_bars: int = 1,
    max_hold_minutes: float | None = None,
    min_r_to_arm_trailing: float = 0.15,
    giveback_r: float = 0.10,
    fast_loss_cut_r: float = -0.25,
    strict_paper_probe: bool = False,
    explain_gates: bool = False,
    wait_for_signal: bool = False,
    max_wait_minutes: float | None = None,
    preflight_only: bool = False,
    state_file: str | Path | None = DEFAULT_STATE_FILE,
    results_file: str | Path | None = DEFAULT_RESULTS_FILE,
    timeout_seconds: float = 10.0,
    sleep_fn: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    clean_symbol = str(symbol or SYMBOL).upper().strip()
    clean_timeframe = str(timeframe or TIMEFRAME).upper().strip()
    clean_broker_symbol = str(broker_symbol or clean_symbol)
    no_write_preflight = bool(preflight_only or dry_run)
    active_client = client or (
        HttpPaperObservationClient(
            base_url,
            timeout_seconds=timeout_seconds,
            symbol=clean_symbol,
            broker_symbol=clean_broker_symbol,
            timeframe=clean_timeframe,
            allowed_symbols=allowed_symbols,
            asset_configs=asset_configs,
            dry_run_no_persist=no_write_preflight,
            preflight_only=preflight_only,
        )
        if base_url
        else LocalPaperObservationClient(
            symbol=clean_symbol,
            broker_symbol=clean_broker_symbol,
            timeframe=clean_timeframe,
            allowed_symbols=allowed_symbols,
            asset_configs=asset_configs,
            dry_run_no_persist=no_write_preflight,
            preflight_only=preflight_only,
        )
    )
    if no_write_preflight:
        with suppress_noncritical_risk_event_writes("preflight_dry_run"):
            preflight = _preflight(active_client)
    else:
        preflight = _preflight(active_client)
    preflight["dry_run_no_persist"] = no_write_preflight
    preflight["suppressed_noncritical_risk_events"] = _preflight_suppressed_risk_events(preflight)
    preflight["dry_run_risk_events"] = list(preflight["suppressed_noncritical_risk_events"])
    if preflight_only:
        return _preflight_only_result(preflight=preflight, started=started, symbol=clean_symbol, broker_symbol=clean_broker_symbol, timeframe=clean_timeframe)
    if _unsafe([preflight.get("db_state", {}), preflight.get("open_payload", {}), preflight.get("history", {}), preflight.get("readiness", {})]):
        return _supervisor_result(
            "stopped_by_broker_safety_violation",
            preflight=preflight,
            stop_reason="broker_or_order_flag_detected",
            started=started,
            symbol=clean_symbol,
            broker_symbol=clean_broker_symbol,
            timeframe=clean_timeframe,
        )
    db = preflight["db_state"]
    queue_depth = int(_num(db.get("queue_depth")) or 0)
    queue_drain: dict[str, Any] = {}
    if queue_depth > 0:
        queue_drain = _safe_call(getattr(active_client, "queue_drain", None))
        db = _safe_call(active_client.persistent_status)
        preflight["db_state_after_drain"] = db
        preflight["queue_drain"] = queue_drain
        if int(_num(db.get("queue_depth")) or 0) > 0:
            return _supervisor_result(
                "stopped_by_db",
                preflight=preflight,
                stop_reason="queue_depth_remains_after_drain",
                queue_drain=queue_drain,
                started=started,
                symbol=clean_symbol,
                broker_symbol=clean_broker_symbol,
                timeframe=clean_timeframe,
            )
    db_block = _db_block_reason(db)
    if db_block:
        return _supervisor_result(
            "stopped_by_db",
            preflight=preflight,
            stop_reason=db_block,
            queue_drain=queue_drain,
            started=started,
            symbol=clean_symbol,
            broker_symbol=clean_broker_symbol,
            timeframe=clean_timeframe,
        )
    if int(_num(preflight.get("open_payload", {}).get("merged_open_count") or preflight.get("open_payload", {}).get("open_count")) or 0) > 1:
        return _supervisor_result(
            "stopped_by_duplicate_shadow",
            preflight=preflight,
            stop_reason="multiple_open_shadows",
            queue_drain=queue_drain,
            started=started,
            symbol=clean_symbol,
            broker_symbol=clean_broker_symbol,
            timeframe=clean_timeframe,
        )

    batch = run_xau_m15_paper_observation_batch_runner(
        client=active_client,
        target_trades=target_trades,
        max_cycles=max_cycles,
        interval_seconds=interval_seconds,
        dry_run=dry_run,
        paper_only_confirmed=paper_only_confirmed,
        once=once or (dry_run and not wait_for_signal),
        exit_policy=exit_policy,
        time_stop_bars=time_stop_bars,
        max_hold_minutes=max_hold_minutes,
        min_r_to_arm_trailing=min_r_to_arm_trailing,
        giveback_r=giveback_r,
        fast_loss_cut_r=fast_loss_cut_r,
        strict_paper_probe=strict_paper_probe,
        explain_gates=explain_gates,
        wait_for_signal=wait_for_signal,
        max_runtime_minutes=max_wait_minutes,
        state_file=state_file,
        results_file=results_file,
        sleep_fn=sleep_fn,
        timeout_seconds=timeout_seconds,
        symbol=clean_symbol,
        broker_symbol=clean_broker_symbol,
        timeframe=clean_timeframe,
        allowed_symbols=allowed_symbols,
        asset_configs=asset_configs,
    )
    state = _supervisor_state_from_batch(batch)
    return _supervisor_result(
        state,
        preflight=preflight,
        queue_drain=queue_drain,
        batch=batch,
        stop_reason=batch.get("stop_reason") or "",
        started=started,
        symbol=clean_symbol,
        broker_symbol=clean_broker_symbol,
        timeframe=clean_timeframe,
    )


def repair_orphan_state(
    *,
    state_file: str | Path,
    results_file: str | Path,
    confirm_paper_only_repair: bool = False,
    shadow_trade_id: str = "",
) -> dict[str, Any]:
    state_path = Path(state_file)
    results_path = Path(results_file)
    state = _load_json(state_path)
    results = _load_json(results_path)
    current_id = str(shadow_trade_id or state.get("current_open_shadow_id") or state.get("pending_reconciliation_shadow_id") or "").strip()
    if not confirm_paper_only_repair:
        return {
            "ok": False,
            "status": "xau_m15_orphan_repair_confirmation_required",
            "repair_applied": False,
            "shadow_trade_id": current_id,
            "reason": "confirm_paper_only_repair_required",
            **_safety(),
        }
    anomalies = state.setdefault("anomalies", [])
    if not isinstance(anomalies, list):
        anomalies = []
        state["anomalies"] = anomalies
    anomalies.append(
        {
            "at": _now(),
            "shadow_trade_id": current_id,
            "status": "orphaned_unmeasured",
            "reason": "paper_shadow_missing_open_and_close_record",
        }
    )
    state["current_open_shadow_id"] = ""
    state["pending_reconciliation_shadow_id"] = ""
    state["updated_at"] = _now()
    trades = results.get("trades") if isinstance(results.get("trades"), list) else []
    stats = compute_xau_m15_paper_batch_stats([dict(row) for row in trades if isinstance(row, dict)], state=state)
    results.update(
        {
            "schema_version": "2026-06-18.xau_m15_supervisor_results.v1",
            "run_id": results.get("run_id") or f"xau-m15-supervisor-{uuid4().hex[:8]}",
            "started_at": results.get("started_at") or _now(),
            "updated_at": _now(),
            "target_trades": results.get("target_trades") or 0,
            "trades": trades,
            "trades_opened": int(_num(state.get("trades_opened")) or 0),
            "trades_closed": len(trades),
            "anomalies": anomalies,
            "stats": stats,
            **_safety(),
        }
    )
    _write_json(state_path, state)
    _write_json(results_path, results)
    return {
        "ok": True,
        "status": "xau_m15_orphan_state_repaired",
        "repair_applied": True,
        "shadow_trade_id": current_id,
        "trades_closed_changed": False,
        "pnl_invented": False,
        "winrate_changed": False,
        "next_action": "restart_batch_with_clean_state",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        **_safety(),
    }


def _preflight(client: Any) -> dict[str, Any]:
    db_state = _safe_call(client.persistent_status)
    return {
        "supervisor_state": "preflight_ready",
        "db_state": db_state,
        "open_payload": _safe_call(client.open_shadow_trades),
        "history": _safe_call(client.shadow_trade_history),
        "readiness": _safe_call(_readiness_with_db_snapshot, client=client, db_state=db_state),
        "db_preflight_status_cache_hit": _readiness_uses_db_snapshot(client),
        **_safety(),
    }


def _preflight_suppressed_risk_events(preflight: dict[str, Any]) -> list[dict[str, Any]]:
    readiness = preflight.get("readiness") if isinstance(preflight.get("readiness"), dict) else {}
    events = readiness.get("suppressed_noncritical_risk_events")
    if isinstance(events, list):
        return [dict(row) for row in events if isinstance(row, dict)]
    capital = readiness.get("capital_protection") if isinstance(readiness.get("capital_protection"), dict) else {}
    events = capital.get("suppressed_noncritical_risk_events")
    if isinstance(events, list):
        return [dict(row) for row in events if isinstance(row, dict)]
    return []


def _preflight_only_result(
    *,
    preflight: dict[str, Any],
    started: float,
    symbol: str = SYMBOL,
    broker_symbol: str = BROKER_SYMBOL,
    timeframe: str = TIMEFRAME,
) -> dict[str, Any]:
    db = preflight.get("db_state") if isinstance(preflight.get("db_state"), dict) else {}
    readiness = preflight.get("readiness") if isinstance(preflight.get("readiness"), dict) else {}
    open_payload = preflight.get("open_payload") if isinstance(preflight.get("open_payload"), dict) else {}
    history = preflight.get("history") if isinstance(preflight.get("history"), dict) else {}
    open_count = int(_num(open_payload.get("open_count")) or 0)
    merged_open_count = int(_num(open_payload.get("merged_open_count")) or open_count)
    closed_count = int(_num(history.get("closed_count")) or len(history.get("closed_trades") if isinstance(history.get("closed_trades"), list) else []))
    blockers = _preflight_blockers(db=db, readiness=readiness, open_payload=open_payload, preflight=preflight)
    decision = _preflight_decision(blockers)
    next_safe_action = _preflight_next_safe_action(decision)
    safety_violation = _unsafe([db, readiness, open_payload, history])
    clean_symbol = str(symbol or SYMBOL).upper()
    clean_timeframe = str(timeframe or TIMEFRAME).upper()
    readiness_endpoint = (
        "GET /api/genesis/mt5/xau-m15/paper-observation/readiness"
        if clean_symbol == SYMBOL and clean_timeframe == TIMEFRAME
        else "local generic multi-asset readiness"
    )
    return {
        "ok": True,
        "status": "xau_m15_paper_test_supervisor_preflight_only_ready",
        "supervisor_version": SUPERVISOR_VERSION,
        "supervisor_state": "preflight_only",
        "preflight_only": True,
        "symbol": clean_symbol,
        "broker_symbol": broker_symbol,
        "timeframe": clean_timeframe,
        "candidate_profile": CANDIDATE_PROFILE if clean_symbol == SYMBOL and clean_timeframe == TIMEFRAME else f"multi_asset_paper_test|symbol={clean_symbol}|timeframe={clean_timeframe}",
        "allowed_endpoints": [
            "GET /api/genesis/mt5/persistent-intelligence/status",
            readiness_endpoint,
            f"GET /api/genesis/mt5/shadow-trades/open?symbol={clean_symbol}",
            f"GET /api/genesis/mt5/shadow-trades/history?symbol={clean_symbol}&timeframe={clean_timeframe}&limit=50",
        ],
        "db_available": bool(db.get("db_available")),
        "db_degraded": bool(db.get("db_degraded")),
        "tables_ready": bool(db.get("tables_ready")),
        "queue_depth": int(_num(db.get("queue_depth")) or 0),
        "bars_count": int(_num(readiness.get("bars_count")) or 0),
        "market_active": bool(readiness.get("market_active")),
        "market_active_reason": readiness.get("market_active_reason") or "",
        "readiness_state": readiness.get("readiness_state") or "",
        "entry_allowed_for_paper_test": bool(readiness.get("entry_allowed_for_paper_test")),
        "runtime_context_recent": bool(readiness.get("runtime_context_recent")),
        "capital_state": readiness.get("capital_state") or "",
        "capital_allows_observation": bool(readiness.get("capital_allows_observation")),
        "risk_state": readiness.get("risk_state") or "",
        "risk_allows_observation": bool(readiness.get("risk_allows_observation")),
        "dry_run_no_persist": bool(preflight.get("dry_run_no_persist")),
        "suppressed_noncritical_risk_events": list(preflight.get("suppressed_noncritical_risk_events") or []),
        "dry_run_risk_events": list(preflight.get("dry_run_risk_events") or []),
        "open_count": open_count,
        "merged_open_count": merged_open_count,
        "max_open_positions_total": int(_num(readiness.get("max_open_positions_total")) or 1),
        "closed_count": closed_count,
        "decision": decision,
        "blockers": blockers,
        "next_safe_action": next_safe_action,
        "next_action": next_safe_action,
        "preflight": preflight,
        "post_called": False,
        "monitor_called": False,
        "paper_shadow_created": False,
        "paper_close_applied": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        "safety_violation": bool(safety_violation),
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }


def _supervisor_result(
    state: str,
    *,
    preflight: dict[str, Any],
    started: float,
    stop_reason: str = "",
    queue_drain: dict[str, Any] | None = None,
    batch: dict[str, Any] | None = None,
    symbol: str = SYMBOL,
    broker_symbol: str = BROKER_SYMBOL,
    timeframe: str = TIMEFRAME,
) -> dict[str, Any]:
    batch_payload = batch or {}
    safety_violation = _unsafe([
        preflight.get("db_state", {}) if isinstance(preflight.get("db_state"), dict) else {},
        preflight.get("open_payload", {}) if isinstance(preflight.get("open_payload"), dict) else {},
        preflight.get("history", {}) if isinstance(preflight.get("history"), dict) else {},
        preflight.get("readiness", {}) if isinstance(preflight.get("readiness"), dict) else {},
        batch_payload,
        queue_drain or {},
    ])
    return {
        "ok": True,
        "status": "xau_m15_paper_test_supervisor_ready",
        "supervisor_version": SUPERVISOR_VERSION,
        "supervisor_state": state,
        "symbol": batch_payload.get("symbol") or symbol,
        "broker_symbol": batch_payload.get("broker_symbol") or broker_symbol,
        "timeframe": batch_payload.get("timeframe") or timeframe,
        "candidate_profile": batch_payload.get("candidate_profile") or (CANDIDATE_PROFILE if symbol == SYMBOL and timeframe == TIMEFRAME else f"multi_asset_paper_test|symbol={symbol}|timeframe={timeframe}"),
        "stop_reason": stop_reason,
        "current_phase": batch_payload.get("current_phase") or "",
        "readiness_state": batch_payload.get("readiness_state") or "",
        "bars_count": int(_num(batch_payload.get("bars_count")) or 0),
        "market_active": bool(batch_payload.get("market_active")),
        "market_active_reason": batch_payload.get("market_active_reason") or "",
        "db_available": bool(batch_payload.get("db_available")),
        "db_degraded": bool(batch_payload.get("db_degraded")),
        "tables_ready": bool(batch_payload.get("tables_ready")),
        "queue_depth": int(_num(batch_payload.get("queue_depth")) or 0),
        "max_open_positions_total": int(_num(batch_payload.get("max_open_positions_total")) or 1),
        "gate_summary": batch_payload.get("gate_summary") or {},
        "next_action": batch_payload.get("next_action") or "",
        "preflight": preflight,
        "queue_drain": queue_drain or {},
        "batch": batch_payload,
        "cycles_completed": int(_num(batch_payload.get("cycles_completed")) or 0),
        "session_id": batch_payload.get("session_id") or "",
        "session_started_at": batch_payload.get("session_started_at") or "",
        "target_scope": batch_payload.get("target_scope") or "session",
        "session_trades_opened": int(_num(batch_payload.get("session_trades_opened")) or 0),
        "session_trades_closed": int(_num(batch_payload.get("session_trades_closed")) or 0),
        "valid_trades_closed": int(_num(batch_payload.get("batch_stats", {}).get("valid_trades_closed") if isinstance(batch_payload.get("batch_stats"), dict) else batch_payload.get("valid_trades_closed")) or 0),
        "invalid_samples": int(_num(batch_payload.get("batch_stats", {}).get("invalid_samples") if isinstance(batch_payload.get("batch_stats"), dict) else batch_payload.get("invalid_samples")) or 0),
        "historical_closed_count": int(_num(batch_payload.get("historical_closed_count")) or 0),
        "current_shadow_id": batch_payload.get("current_shadow_id") or "",
        "current_shadow_source": batch_payload.get("current_shadow_source") or "",
        "open_count": int(_num(batch_payload.get("open_count")) or 0),
        "win_rate": _num(batch_payload.get("win_rate")) or 0.0,
        "expectancy": _num(batch_payload.get("expectancy")) or 0.0,
        "profit_factor": _num(batch_payload.get("profit_factor")) or 0.0,
        "avg_r": _num(batch_payload.get("batch_stats", {}).get("avg_r") if isinstance(batch_payload.get("batch_stats"), dict) else batch_payload.get("avg_r")) or 0.0,
        "last_closed_trade": batch_payload.get("last_closed_trade") if isinstance(batch_payload.get("last_closed_trade"), dict) else {},
        "failed_gate_names": batch_payload.get("failed_gate_names") or [],
        "failed_gate_reasons": batch_payload.get("failed_gate_reasons") or {},
        "risk_governor_reason": batch_payload.get("risk_governor_reason") or "",
        "recent_edge_negative": bool(batch_payload.get("recent_edge_negative")),
        "entry_allowed_for_paper_test": bool(batch_payload.get("entry_allowed_for_paper_test")),
        "entry_block_type": batch_payload.get("entry_block_type") or "",
        "paper_shadow_created": bool(batch_payload.get("paper_shadow_created")),
        "paper_close_applied": bool(batch_payload.get("paper_close_applied")),
        "open_persistence_failed": bool(batch_payload.get("open_persistence_failed")),
        "open_write_retained_critical": bool(batch_payload.get("open_write_retained_critical")),
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        "safety_violation": bool(safety_violation),
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }


def _supervisor_state_from_batch(batch: dict[str, Any]) -> str:
    runner_state = str(batch.get("runner_state") or "")
    mapping = {
        "stopped_by_target_trades": "stopped_by_target_reached",
        "stopped_by_safety": "stopped_by_broker_safety_violation",
        "readiness_blocked": "stopped_by_runtime",
    }
    return mapping.get(runner_state, runner_state or "preflight_ready")


def _db_block_reason(db: dict[str, Any]) -> str:
    if db.get("db_available") is not True:
        return "db_unavailable"
    if db.get("db_degraded") is not False:
        return "db_degraded"
    if db.get("tables_ready") is not True:
        return "tables_not_ready"
    queue_depth = _db_counter(db, "queue_depth")
    if queue_depth is None:
        return "queue_depth_missing"
    if queue_depth > 0:
        return "queue_depth_high"
    queued_writes = _db_counter(db, "queued_writes")
    if queued_writes is None:
        return "queued_writes_missing"
    if queued_writes > 0:
        return "queued_writes_pending"
    db_readiness_reason = str(db.get("db_readiness_blocking_reason") or "").strip()
    if db_readiness_reason:
        return db_readiness_reason
    failed_writes_total = _db_counter(db, "failed_writes_total")
    if failed_writes_total is None:
        failed_writes_total = _db_counter(db, "failed_writes")
    if failed_writes_total is None:
        return "failed_writes_missing"
    if db.get("failed_write_semantics_known") is not True:
        return "failed_write_semantics_unknown"
    failed_writes_active = _db_counter(db, "failed_writes_active")
    failed_writes_unresolved = _db_counter(db, "failed_writes_unresolved")
    failed_writes_critical = _db_counter(db, "failed_writes_critical")
    if failed_writes_active is None or failed_writes_unresolved is None or failed_writes_critical is None:
        return "failed_write_semantics_unknown"
    if failed_writes_critical > 0:
        return "failed_writes_critical"
    if failed_writes_unresolved > 0:
        return "failed_writes_unresolved"
    if failed_writes_active > 0:
        return "failed_writes_active"
    return ""


def _db_counter(db: dict[str, Any], key: str) -> int | None:
    if key not in db:
        return None
    value = _num(db.get(key))
    if value is None:
        return None
    return int(value)


def _preflight_blockers(*, db: dict[str, Any], readiness: dict[str, Any], open_payload: dict[str, Any], preflight: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if _unsafe([preflight.get("db_state", {}), preflight.get("open_payload", {}), preflight.get("history", {}), preflight.get("readiness", {})]):
        blockers.append("broker_or_order_flag_detected")
    db_block = _db_block_reason(db)
    if db_block:
        blockers.append(db_block)
    merged_open_count = int(_num(open_payload.get("merged_open_count")) or _num(open_payload.get("open_count")) or 0)
    if merged_open_count > 1:
        blockers.append("multiple_open_shadows")
    elif merged_open_count == 1:
        blockers.append("open_shadow_already_exists")
    if not readiness or not bool(readiness.get("ok", True)):
        blockers.append("readiness_unavailable")
    failed_gates = readiness.get("failed_gate_names") or readiness.get("failed_gates") or []
    if isinstance(failed_gates, list):
        blockers.extend(str(name) for name in failed_gates if name)
    for key in ("runtime_context_recent", "capital_allows_observation", "risk_allows_observation"):
        if key in readiness and not bool(readiness.get(key)):
            blockers.append(key)
    if str(readiness.get("readiness_state") or "") not in {"", "ready_for_one_cycle_paper_observation"} and not failed_gates:
        blockers.append(str(readiness.get("recommendation") or "readiness_blocked"))
    return _dedupe(blockers)


def _preflight_decision(blockers: list[str]) -> str:
    if "broker_or_order_flag_detected" in blockers:
        return "blocked_by_safety_flags"
    if any(
        blocker in blockers
        for blocker in (
            "db_unavailable",
            "db_degraded",
            "tables_not_ready",
            "queue_depth_missing",
            "queue_depth_high",
            "queued_writes_missing",
            "queued_writes_pending",
            "failed_writes_missing",
            "failed_write_semantics_unknown",
            "failed_writes_active",
            "failed_writes_unresolved",
            "failed_writes_critical",
        )
    ):
        return "blocked_by_db"
    if "multiple_open_shadows" in blockers:
        return "blocked_by_duplicate_shadow"
    if "open_shadow_already_exists" in blockers:
        return "blocked_by_existing_open_shadow"
    if blockers:
        return "blocked_by_readiness"
    return "preflight_ready"


def _preflight_next_safe_action(decision: str) -> str:
    mapping = {
        "blocked_by_safety_flags": "stop_and_investigate_safety_flags",
        "blocked_by_db": "fix_persistent_intelligence_before_dry_run",
        "blocked_by_duplicate_shadow": "inspect_lifecycle_with_allowed_gets_before_monitor_or_open",
        "blocked_by_existing_open_shadow": "inspect_existing_shadow_before_any_open_or_close",
        "blocked_by_readiness": "resolve_readiness_blockers_before_monitor_or_paper_open",
        "preflight_ready": "review_preflight_then_authorize_next_dry_run_scope",
    }
    return mapping.get(decision, "review_preflight_before_next_action")


def _safe_call(fn: Any, **kwargs: Any) -> dict[str, Any]:
    if fn is None:
        return {"ok": False, "reason": "missing_callable", **_safety()}
    try:
        result = fn(**kwargs)
    except Exception as exc:  # pragma: no cover
        return {"ok": False, "reason": type(exc).__name__, **_safety()}
    return dict(result or {"ok": False, "reason": "empty_payload", **_safety()})


def _unsafe(payloads: list[dict[str, Any]]) -> bool:
    for payload in payloads:
        if bool(payload.get("safety_violation")):
            return True
        if bool(payload.get("broker_touched")) or bool(payload.get("order_executed")):
            return True
        if bool(payload.get("candidate_activated")) or bool(payload.get("paper_forward_onboarding_started")):
            return True
        if bool(payload.get("applies_to_real_trading")):
            return True
        if str(payload.get("order_policy") or "journal_only_no_broker") != "journal_only_no_broker":
            return True
    return False


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True, default=str), encoding="utf-8")


def _num(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
