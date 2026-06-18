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
    run_xau_m15_paper_observation_batch_runner,
)
from services.mt5.mt5_xau_m15_paper_observation_readiness import BROKER_SYMBOL, CANDIDATE_PROFILE, SYMBOL, TIMEFRAME


SUPERVISOR_VERSION = "2026-06-18.xau_m15_paper_test_supervisor.v1"


def run_xau_m15_paper_test_supervisor(
    *,
    client: Any | None = None,
    base_url: str = "",
    target_trades: int = 3,
    max_cycles: int = 120,
    interval_seconds: float = 30.0,
    dry_run: bool = True,
    paper_only_confirmed: bool = False,
    once: bool = False,
    exit_policy: str = "fast_observation",
    time_stop_bars: int = 2,
    max_hold_minutes: float | None = None,
    min_r_to_arm_trailing: float = 0.15,
    giveback_r: float = 0.10,
    state_file: str | Path | None = DEFAULT_STATE_FILE,
    results_file: str | Path | None = DEFAULT_RESULTS_FILE,
    timeout_seconds: float = 10.0,
    sleep_fn: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    active_client = client or (HttpPaperObservationClient(base_url, timeout_seconds=timeout_seconds) if base_url else LocalPaperObservationClient())
    preflight = _preflight(active_client)
    if _unsafe([preflight.get("db_state", {}), preflight.get("open_payload", {}), preflight.get("history", {})]):
        return _supervisor_result(
            "stopped_by_broker_safety_violation",
            preflight=preflight,
            stop_reason="broker_or_order_flag_detected",
            started=started,
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
            )
    db_block = _db_block_reason(db)
    if db_block:
        return _supervisor_result(
            "stopped_by_db",
            preflight=preflight,
            stop_reason=db_block,
            queue_drain=queue_drain,
            started=started,
        )
    if int(_num(preflight.get("open_payload", {}).get("merged_open_count") or preflight.get("open_payload", {}).get("open_count")) or 0) > 1:
        return _supervisor_result(
            "stopped_by_duplicate_shadow",
            preflight=preflight,
            stop_reason="multiple_open_shadows",
            queue_drain=queue_drain,
            started=started,
        )

    batch = run_xau_m15_paper_observation_batch_runner(
        client=active_client,
        target_trades=target_trades,
        max_cycles=max_cycles,
        interval_seconds=interval_seconds,
        dry_run=dry_run,
        paper_only_confirmed=paper_only_confirmed,
        once=once or dry_run,
        exit_policy=exit_policy,
        time_stop_bars=time_stop_bars,
        max_hold_minutes=max_hold_minutes,
        min_r_to_arm_trailing=min_r_to_arm_trailing,
        giveback_r=giveback_r,
        state_file=state_file,
        results_file=results_file,
        sleep_fn=sleep_fn,
        timeout_seconds=timeout_seconds,
    )
    state = _supervisor_state_from_batch(batch)
    return _supervisor_result(
        state,
        preflight=preflight,
        queue_drain=queue_drain,
        batch=batch,
        stop_reason=batch.get("stop_reason") or "",
        started=started,
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
    return {
        "supervisor_state": "preflight_ready",
        "db_state": _safe_call(client.persistent_status),
        "open_payload": _safe_call(client.open_shadow_trades),
        "history": _safe_call(client.shadow_trade_history),
        "readiness": _safe_call(client.readiness),
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
) -> dict[str, Any]:
    batch_payload = batch or {}
    return {
        "ok": True,
        "status": "xau_m15_paper_test_supervisor_ready",
        "supervisor_version": SUPERVISOR_VERSION,
        "supervisor_state": state,
        "symbol": SYMBOL,
        "broker_symbol": BROKER_SYMBOL,
        "timeframe": TIMEFRAME,
        "candidate_profile": CANDIDATE_PROFILE,
        "stop_reason": stop_reason,
        "preflight": preflight,
        "queue_drain": queue_drain or {},
        "batch": batch_payload,
        "cycles_completed": int(_num(batch_payload.get("cycles_completed")) or 0),
        "paper_shadow_created": bool(batch_payload.get("paper_shadow_created")),
        "paper_close_applied": bool(batch_payload.get("paper_close_applied")),
        "open_persistence_failed": bool(batch_payload.get("open_persistence_failed")),
        "open_write_retained_critical": bool(batch_payload.get("open_write_retained_critical")),
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
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
    if not bool(db.get("db_available")):
        return "db_unavailable"
    if bool(db.get("db_degraded")):
        return "db_degraded"
    if not bool(db.get("tables_ready")):
        return "tables_not_ready"
    if int(_num(db.get("queue_depth")) or 0) > 0:
        return "queue_depth_high"
    return ""


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
        if bool(payload.get("broker_touched")) or bool(payload.get("order_executed")):
            return True
        if str(payload.get("order_policy") or "journal_only_no_broker") != "journal_only_no_broker":
            return True
    return False


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
