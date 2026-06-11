from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from services.mt5.mt5_autonomous_learning_orchestrator import run_autonomous_learning_orchestrator
from services.mt5.mt5_autonomous_learning_status import run_autonomous_learning_status


SUPERVISOR_VERSION = "2026-06-11.mt5_controlled_learning_loop_supervisor.v1"
DEFAULT_LOCK_PATH = Path(os.environ.get("TEMP") or ".") / "genesis_mt5_controlled_learning_loop.lock"
DEFAULT_MAX_QUEUE_DEPTH = 25
DEFAULT_MAX_OPEN_SHADOW_TRADES = 3
DEFAULT_MAX_CYCLES = 24

StatusRunner = Callable[..., dict[str, Any]]
CycleRunner = Callable[..., dict[str, Any]]


def run_controlled_learning_loop_supervisor(
    *,
    symbol: str = "BTCUSD",
    timeframe: str = "",
    cycles: int = 0,
    interval_seconds: int = 300,
    lock_path: str | Path | None = None,
    max_queue_depth: int = DEFAULT_MAX_QUEUE_DEPTH,
    max_open_shadow_trades: int = DEFAULT_MAX_OPEN_SHADOW_TRADES,
    dry_run_cycles: bool = False,
    status_runner: StatusRunner | None = None,
    orchestrator_runner: CycleRunner | None = None,
    sleep_between_cycles: bool = True,
) -> dict[str, Any]:
    requested_cycles = max(0, int(cycles or 0))
    safe_interval = max(0, int(interval_seconds or 0))
    status_fn = status_runner or run_autonomous_learning_status
    cycle_fn = orchestrator_runner or run_autonomous_learning_orchestrator
    status = _safe_status(status_fn, symbol=symbol, timeframe=timeframe)
    gates = evaluate_learning_gates(
        status,
        max_queue_depth=max_queue_depth,
        max_open_shadow_trades=max_open_shadow_trades,
    )
    stop_reason = _first_stop_reason(gates)

    if requested_cycles == 0:
        return _result(
            supervisor_state="dry_run_blocked" if stop_reason else "dry_run_ready",
            cycles_requested=0,
            cycles_completed=0,
            stop_reason=stop_reason,
            status=status,
            gates=gates,
            cycles=[],
            lock_path=lock_path,
            loop_started=False,
        )

    if requested_cycles > DEFAULT_MAX_CYCLES:
        return _result(
            supervisor_state="stopped",
            cycles_requested=requested_cycles,
            cycles_completed=0,
            stop_reason="cycles_above_supervisor_cap",
            status=status,
            gates=gates,
            cycles=[],
            lock_path=lock_path,
            loop_started=False,
        )

    path = Path(lock_path) if lock_path is not None else DEFAULT_LOCK_PATH
    if path.exists():
        return _result(
            supervisor_state="lock_active",
            cycles_requested=requested_cycles,
            cycles_completed=0,
            stop_reason="lock_active",
            status=status,
            gates=gates,
            cycles=[],
            lock_path=path,
            loop_started=False,
            ok=False,
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"started_at={_now()} cycles={requested_cycles}\n", encoding="utf-8")
    completed = 0
    cycle_summaries: list[dict[str, Any]] = []
    final_status = status
    final_gates = gates
    final_stop = stop_reason
    try:
        for index in range(requested_cycles):
            final_status = _safe_status(status_fn, symbol=symbol, timeframe=timeframe)
            final_gates = evaluate_learning_gates(
                final_status,
                max_queue_depth=max_queue_depth,
                max_open_shadow_trades=max_open_shadow_trades,
            )
            final_stop = _first_stop_reason(final_gates)
            if final_stop:
                break
            cycle = _safe_cycle(
                cycle_fn,
                symbol=symbol,
                timeframe=timeframe,
                dry_run=dry_run_cycles,
            )
            completed += 1
            cycle_summaries.append(_compact_cycle(cycle))
            final_stop = _post_cycle_stop_reason(cycle)
            if final_stop:
                break
            if sleep_between_cycles and safe_interval > 0 and index < requested_cycles - 1:
                time.sleep(safe_interval)
    finally:
        try:
            path.unlink(missing_ok=True)
        except TypeError:  # pragma: no cover - py<3.8 guard
            if path.exists():
                path.unlink()

    state = "completed" if completed == requested_cycles and not final_stop else "stopped"
    return _result(
        supervisor_state=state,
        cycles_requested=requested_cycles,
        cycles_completed=completed,
        stop_reason=final_stop,
        status=final_status,
        gates=final_gates,
        cycles=cycle_summaries,
        lock_path=path,
        loop_started=True,
    )


def evaluate_learning_gates(
    status: dict[str, Any],
    *,
    max_queue_depth: int = DEFAULT_MAX_QUEUE_DEPTH,
    max_open_shadow_trades: int = DEFAULT_MAX_OPEN_SHADOW_TRADES,
) -> list[dict[str, Any]]:
    db_state = _dict(status.get("db_state"))
    capital = _dict(status.get("capital_protection"))
    adaptive = _dict(status.get("adaptive_governor"))
    risk = _dict(status.get("risk_governor"))
    hygiene = _dict(status.get("shadow_hygiene"))
    breakers = status.get("circuit_breakers") if isinstance(status.get("circuit_breakers"), list) else []

    db_ready = bool(status.get("db_available") and status.get("tables_ready") and not status.get("db_degraded"))
    db_stop = "db_schema_missing" if not status.get("tables_ready") and db_state.get("missing_tables") else "db_degraded"
    queue_depth = int(_number(db_state.get("queue_depth")))
    open_shadow_trades = int(
        _number(
            hygiene.get("open_shadow_trades")
            if hygiene.get("open_shadow_trades") is not None
            else hygiene.get("open_shadow_count")
        )
    )
    capital_state = str(status.get("capital_state") or capital.get("capital_state") or "")
    adaptive_state = str(status.get("adaptive_state") or adaptive.get("global_state") or "")
    risk_present = bool(risk)
    risk_allowed = bool(risk.get("allowed", risk.get("risk_governor_allowed", True))) if risk_present else False
    runtime_missing = _critical_missing_runtime_context(breakers) or (db_ready and not risk_present)

    return [
        _gate("persistent_db_healthy", db_ready, db_stop, _db_detail(status)),
        _gate(
            "persistent_queue_depth_low",
            queue_depth <= int(max_queue_depth) and not bool(db_state.get("backoff_active")),
            "persistent_queue_depth_high",
            f"queue_depth={queue_depth} max_queue_depth={int(max_queue_depth)}",
        ),
        _gate(
            "capital_protection_allows_learning",
            capital_state != "kill_switch" and bool(capital.get("safe_to_trade", True)),
            "capital_protection_block",
            f"capital_state={capital_state or 'unknown'}",
        ),
        _gate(
            "adaptive_governor_allows_learning",
            adaptive_state != "kill_switch",
            "adaptive_governor_block",
            f"adaptive_state={adaptive_state or 'unknown'}",
        ),
        _gate(
            "risk_governor_allows_learning",
            risk_allowed,
            "risk_governor_block",
            f"risk_reason={risk.get('reason') or risk.get('risk_governor_reason') or 'missing'}",
        ),
        _gate(
            "open_shadow_trades_within_limit",
            open_shadow_trades <= int(max_open_shadow_trades) and bool(status.get("safe_to_open_new_shadow", True)),
            "max_open_shadow_trades",
            f"open_shadow_trades={open_shadow_trades} max_open_shadow_trades={int(max_open_shadow_trades)}",
        ),
        _gate(
            "runtime_context_complete",
            not runtime_missing,
            "missing_runtime_context_critical",
            "critical runtime context is missing" if runtime_missing else "",
        ),
    ]


def _safe_status(status_fn: StatusRunner, *, symbol: str, timeframe: str) -> dict[str, Any]:
    try:
        return status_fn(symbol=symbol, timeframe=timeframe)
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {
            "ok": False,
            "status": "controlled_learning_status_unavailable",
            "db_available": False,
            "db_degraded": True,
            "tables_ready": False,
            "db_state": {"provider": "unavailable", "db_available": False, "db_degraded": True, "tables_ready": False, "reason": type(exc).__name__},
            "capital_state": "",
            "adaptive_state": "",
            "safe_to_learn": False,
            "safe_to_open_new_shadow": False,
            "paper_rotation_recommendation": "status_unavailable_no_rotation",
            "paper_rotation_applied": False,
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
            **_safety(),
        }


def _safe_cycle(cycle_fn: CycleRunner, *, symbol: str, timeframe: str, dry_run: bool) -> dict[str, Any]:
    try:
        return cycle_fn(
            symbol=symbol,
            timeframe=timeframe,
            dry_run=dry_run,
            apply_paper_rotation=False,
            load_persistent=True,
            load_shadow_snapshot=True,
            load_rotation=False,
            run_trade_learning=not dry_run,
            persist_events=not dry_run,
        )
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {
            "ok": False,
            "learning_state": "cycle_error",
            "recommended_next_action": "NO_TRADE",
            "paper_rotation_recommendation": "cycle_error_no_rotation",
            "paper_rotation_applied": False,
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
            "reason": type(exc).__name__,
            **_safety(),
        }


def _result(
    *,
    supervisor_state: str,
    cycles_requested: int,
    cycles_completed: int,
    stop_reason: str,
    status: dict[str, Any],
    gates: list[dict[str, Any]],
    cycles: list[dict[str, Any]],
    lock_path: str | Path | None,
    loop_started: bool,
    ok: bool = True,
) -> dict[str, Any]:
    db_state = _dict(status.get("db_state"))
    return {
        "ok": ok,
        "status": f"controlled_learning_loop_supervisor_{supervisor_state}",
        "supervisor_version": SUPERVISOR_VERSION,
        "supervisor_state": supervisor_state,
        "decision": "NO_TRADE",
        "reason": f"supervisor:{stop_reason or supervisor_state}",
        "cycles_requested": int(cycles_requested),
        "cycles_completed": int(cycles_completed),
        "stop_reason": stop_reason,
        "loop_started": bool(loop_started),
        "lock_path": str(lock_path or DEFAULT_LOCK_PATH),
        "gates": gates,
        "db_state": db_state,
        "capital_state": status.get("capital_state") or "",
        "adaptive_state": status.get("adaptive_state") or "",
        "tournament_top_candidate": status.get("tournament_top_candidate") if isinstance(status.get("tournament_top_candidate"), dict) else None,
        "paper_rotation_recommendation": status.get("paper_rotation_recommendation") or "",
        "paper_rotation_applied": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "cycles": cycles,
        "apply_paper_rotation": False,
        "applies_to_real_trading": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        **_safety(),
    }


def _post_cycle_stop_reason(cycle: dict[str, Any]) -> str:
    if cycle.get("broker_touched") or cycle.get("order_executed") or cycle.get("order_policy") != "journal_only_no_broker":
        return "safety_violation"
    if cycle.get("paper_rotation_applied"):
        return "paper_rotation_applied_unexpected"
    if str(cycle.get("learning_state") or "").startswith("paused_by_"):
        return str(cycle.get("learning_state") or "cycle_paused")
    return ""


def _compact_cycle(cycle: dict[str, Any]) -> dict[str, Any]:
    return {
        "learning_state": cycle.get("learning_state") or "",
        "db_state": cycle.get("db_state") if isinstance(cycle.get("db_state"), dict) else {},
        "capital_state": cycle.get("capital_state") or "",
        "adaptive_state": cycle.get("adaptive_state") or "",
        "tournament_top_candidate": cycle.get("tournament_top_candidate") if isinstance(cycle.get("tournament_top_candidate"), dict) else None,
        "paper_rotation_recommendation": cycle.get("paper_rotation_recommendation") or "",
        "paper_rotation_applied": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "recommended_next_action": cycle.get("recommended_next_action") or "",
        **_safety(),
    }


def _first_stop_reason(gates: list[dict[str, Any]]) -> str:
    for gate in gates:
        if not gate.get("passed") and gate.get("stop"):
            return str(gate.get("stop_reason") or gate.get("name") or "gate_failed")
    return ""


def _critical_missing_runtime_context(breakers: list[Any]) -> bool:
    for breaker in breakers:
        if not isinstance(breaker, dict) or not breaker.get("active") or not breaker.get("critical"):
            continue
        text = f"{breaker.get('name') or ''} {breaker.get('reason') or ''}".casefold()
        if "missing" in text and "context" in text:
            return True
    return False


def _gate(name: str, passed: bool, stop_reason: str, detail: str) -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "stop": not bool(passed),
        "stop_reason": "" if passed else stop_reason,
        "detail": detail,
        **_safety(),
    }


def _db_detail(status: dict[str, Any]) -> str:
    db_state = _dict(status.get("db_state"))
    return (
        f"provider={status.get('provider') or db_state.get('provider') or ''} "
        f"db_available={bool(status.get('db_available'))} "
        f"tables_ready={bool(status.get('tables_ready'))} "
        f"db_degraded={bool(status.get('db_degraded'))}"
    )


def _dict(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _number(value: object) -> float:
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
