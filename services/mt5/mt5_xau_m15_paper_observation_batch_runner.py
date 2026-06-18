from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from services.mt5.mt5_bridge import mt5_shadow_trades_history, mt5_shadow_trades_open, mt5_xau_m15_paper_observation_readiness
from services.mt5.mt5_persistent_intelligence_store import persistent_intelligence_status
from services.mt5.mt5_xau_m15_paper_observation_readiness import (
    BROKER_SYMBOL,
    CANDIDATE_PROFILE,
    SYMBOL,
    TIMEFRAME,
    run_xau_m15_paper_observation_shadow_once,
)
from services.mt5.mt5_xau_m15_paper_shadow_monitor import run_xau_m15_paper_shadow_monitor


RUNNER_VERSION = "2026-06-16.xau_m15_paper_observation_batch_runner.v1"
STATE_SCHEMA_VERSION = "2026-06-16.xau_m15_paper_batch_state.v2"
DEFAULT_STATE_FILE = Path("data/research_outputs/xau_m15_paper_batch_state.json")
DEFAULT_RESULTS_FILE = Path("data/research_outputs/xau_m15_paper_batch_results.json")
ALLOWED_CLOSE_REASONS = {
    "take_profit_hit",
    "stop_loss_hit",
    "trailing_defensive_exit",
    "critical_safety_exit",
    "safety_exit",
    "paper_timebox_exit",
    "paper_stagnation_exit",
    "paper_fast_trailing_exit",
    "paper_fast_loss_cut",
}


class LocalPaperObservationClient:
    source = "local_process"

    def persistent_status(self) -> dict[str, Any]:
        return persistent_intelligence_status(write_test_event=False)

    def open_shadow_trades(self) -> dict[str, Any]:
        return mt5_shadow_trades_open(symbol=SYMBOL, limit=10)

    def shadow_trade_history(self) -> dict[str, Any]:
        return mt5_shadow_trades_history(symbol=SYMBOL, timeframe=TIMEFRAME, limit=20)

    def readiness(self) -> dict[str, Any]:
        return mt5_xau_m15_paper_observation_readiness()

    def monitor(
        self,
        *,
        apply_paper_close: bool = False,
        exit_policy: str = "default",
        time_stop_bars: int = 2,
        max_hold_minutes: float | None = None,
        min_r_to_arm_trailing: float = 0.15,
        giveback_r: float = 0.10,
    ) -> dict[str, Any]:
        return run_xau_m15_paper_shadow_monitor(
            apply_paper_close=apply_paper_close,
            exit_policy=exit_policy,
            time_stop_bars=time_stop_bars,
            max_hold_minutes=max_hold_minutes,
            min_r_to_arm_trailing=min_r_to_arm_trailing,
            giveback_r=giveback_r,
        )

    def open_shadow_once(self) -> dict[str, Any]:
        return run_xau_m15_paper_observation_shadow_once(
            payload={
                "confirm_paper_shadow_only": True,
                "symbol": SYMBOL,
                "timeframe": TIMEFRAME,
            }
        )

    def queue_drain(self) -> dict[str, Any]:
        from services.mt5.mt5_persistent_intelligence_store import persistent_intelligence_queue_drain

        return persistent_intelligence_queue_drain(max_items=50, drop_failed_noncritical=True)


class HttpPaperObservationClient:
    source = "remote_live_http_process"

    def __init__(self, base_url: str, *, timeout_seconds: float = 10.0) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.timeout_seconds = max(1.0, float(timeout_seconds or 10.0))

    def persistent_status(self) -> dict[str, Any]:
        return self._get("/api/genesis/mt5/persistent-intelligence/status")

    def open_shadow_trades(self) -> dict[str, Any]:
        return self._get(f"/api/genesis/mt5/shadow-trades/open?{urlencode({'symbol': SYMBOL})}")

    def shadow_trade_history(self) -> dict[str, Any]:
        return self._get(f"/api/genesis/mt5/shadow-trades/history?{urlencode({'symbol': SYMBOL, 'timeframe': TIMEFRAME, 'limit': 20})}")

    def readiness(self) -> dict[str, Any]:
        return self._get("/api/genesis/mt5/xau-m15/paper-observation/readiness")

    def monitor(
        self,
        *,
        apply_paper_close: bool = False,
        exit_policy: str = "default",
        time_stop_bars: int = 2,
        max_hold_minutes: float | None = None,
        min_r_to_arm_trailing: float = 0.15,
        giveback_r: float = 0.10,
    ) -> dict[str, Any]:
        policy = _exit_policy(exit_policy)
        if apply_paper_close or policy != "default":
            body = {
                "apply_paper_close": bool(apply_paper_close),
                "exit_policy": policy,
                "time_stop_bars": int(time_stop_bars or 0),
                "min_r_to_arm_trailing": float(min_r_to_arm_trailing or 0.0),
                "giveback_r": float(giveback_r or 0.0),
            }
            if max_hold_minutes is not None:
                body["max_hold_minutes"] = float(max_hold_minutes)
            return self._post("/api/genesis/mt5/xau-m15/paper-shadow/monitor", body)
        return self._get("/api/genesis/mt5/xau-m15/paper-shadow/monitor")

    def open_shadow_once(self) -> dict[str, Any]:
        return self._post(
            "/api/genesis/mt5/xau-m15/paper-observation/shadow-once",
            {
                "confirm_paper_shadow_only": True,
                "symbol": SYMBOL,
                "timeframe": TIMEFRAME,
            },
        )

    def queue_drain(self) -> dict[str, Any]:
        return self._post(
            "/api/genesis/mt5/persistent-intelligence/queue-drain",
            {
                "confirm_queue_drain": True,
                "max_items": 50,
            },
        )

    def _get(self, path: str) -> dict[str, Any]:
        request = Request(f"{self.base_url}{path}", headers={"Accept": "application/json"}, method="GET")
        return self._send(request)

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        request = Request(
            f"{self.base_url}{path}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8", "Accept": "application/json"},
            method="POST",
        )
        return self._send(request)

    def _send(self, request: Request) -> dict[str, Any]:
        with urlopen(request, timeout=self.timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if isinstance(payload, dict):
            payload.setdefault("client_source", self.source)
            return payload
        return {"ok": False, "reason": "invalid_http_payload", **_safety()}


def run_xau_m15_paper_observation_batch_runner(
    *,
    client: Any | None = None,
    base_url: str = "",
    target_trades: int = 20,
    max_cycles: int = 200,
    interval_seconds: float = 60.0,
    max_runtime_minutes: float | None = None,
    dry_run: bool = True,
    paper_only_confirmed: bool = False,
    once: bool = False,
    exit_policy: str = "default",
    time_stop_bars: int = 2,
    max_hold_minutes: float | None = None,
    min_r_to_arm_trailing: float = 0.15,
    giveback_r: float = 0.10,
    state_file: str | Path | None = DEFAULT_STATE_FILE,
    results_file: str | Path | None = DEFAULT_RESULTS_FILE,
    sleep_fn: Callable[[float], None] | None = None,
    timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    started = time.monotonic()
    active_client = client or (HttpPaperObservationClient(base_url, timeout_seconds=timeout_seconds) if base_url else LocalPaperObservationClient())
    state_path = Path(state_file) if state_file else None
    results_path = Path(results_file) if results_file else None
    state = _load_state(state_path)
    results = _load_results(results_path)
    trades = _trades(results)
    cycles_requested = 1 if once or dry_run else max(1, int(max_cycles or 1))
    cycles_requested = min(cycles_requested, max(1, int(max_cycles or 1)))
    cycle_outputs: list[dict[str, Any]] = []
    sleep = sleep_fn or time.sleep
    terminal = False

    for idx in range(cycles_requested):
        if max_runtime_minutes is not None and (time.monotonic() - started) / 60.0 >= float(max_runtime_minutes):
            cycle_outputs.append(_terminal_cycle("stopped_by_max_runtime", idx + 1, state, trades, "max_runtime_minutes_reached"))
            terminal = True
            break
        step = run_xau_m15_paper_observation_batch_step(
            client=active_client,
            state=state,
            trades=trades,
            cycle_number=idx + 1,
            target_trades=target_trades,
            dry_run=dry_run,
            paper_only_confirmed=paper_only_confirmed,
            exit_policy=exit_policy,
            time_stop_bars=time_stop_bars,
            max_hold_minutes=max_hold_minutes,
            min_r_to_arm_trailing=min_r_to_arm_trailing,
            giveback_r=giveback_r,
        )
        cycle_outputs.append(step)
        _merge_step_state(state, step)
        closed = _closed_trade_from_step(step)
        if closed:
            trades.append(closed)
        terminal = bool(step.get("terminal"))
        if dry_run or once or terminal:
            break
        if idx + 1 >= cycles_requested:
            break
        sleep(max(0.0, float(interval_seconds or 0.0)))

    if not cycle_outputs:
        cycle_outputs.append(_terminal_cycle("stopped_by_max_cycles", 0, state, trades, "no_cycles_requested"))

    final = cycle_outputs[-1]
    stats = compute_xau_m15_paper_batch_stats(trades, state=state, cycle_outputs=cycle_outputs)
    if not dry_run:
        _write_json(state_path, _public_state(state, stats, trades))
        _write_json(
            results_path,
            {
                "schema_version": STATE_SCHEMA_VERSION,
                "runner_version": RUNNER_VERSION,
                "trades": trades,
                "batch_stats": stats,
                "stats": stats,
                "updated_at": _now(),
                **_safety(),
            },
        )

    return {
        "ok": True,
        "status": "xau_m15_paper_observation_batch_runner_ready",
        "runner_version": RUNNER_VERSION,
        "mode": "dry_run" if dry_run else "paper_only_confirmed" if paper_only_confirmed else "blocked_missing_confirmation",
        "exit_policy": _exit_policy(exit_policy),
        "time_stop_bars": int(time_stop_bars or 0),
        "max_hold_minutes": max_hold_minutes,
        "min_r_to_arm_trailing": float(min_r_to_arm_trailing or 0.0),
        "giveback_r": float(giveback_r or 0.0),
        "client_source": getattr(active_client, "source", "injected"),
        "symbol": SYMBOL,
        "broker_symbol": BROKER_SYMBOL,
        "timeframe": TIMEFRAME,
        "candidate_profile": CANDIDATE_PROFILE,
        "runner_state": final.get("runner_state") or "batch_completed",
        "cycles_requested": cycles_requested,
        "cycles_completed": len(cycle_outputs),
        "target_trades": int(target_trades or 0),
        "stop_reason": final.get("stop_reason") or "",
        "cycle_results": cycle_outputs,
        "last_cycle": final,
        "batch_stats": stats,
        "paper_shadow_created": any(bool(cycle.get("paper_shadow_created")) for cycle in cycle_outputs),
        "paper_close_applied": any(bool(cycle.get("paper_close_applied")) for cycle in cycle_outputs),
        "open_persistence_failed": any(bool(cycle.get("open_persistence_failed")) for cycle in cycle_outputs),
        "open_write_retained_critical": any(bool(cycle.get("open_write_retained_critical")) for cycle in cycle_outputs),
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }


def run_xau_m15_paper_observation_batch_step(
    *,
    client: Any,
    state: dict[str, Any],
    trades: list[dict[str, Any]],
    cycle_number: int,
    target_trades: int,
    dry_run: bool,
    paper_only_confirmed: bool,
    exit_policy: str = "default",
    time_stop_bars: int = 2,
    max_hold_minutes: float | None = None,
    min_r_to_arm_trailing: float = 0.15,
    giveback_r: float = 0.10,
) -> dict[str, Any]:
    stats = compute_xau_m15_paper_batch_stats(trades, state=state, cycle_outputs=[])
    if int(stats.get("trades_closed") or 0) >= int(target_trades or 0) > 0:
        return _result("stopped_by_target_trades", cycle_number, stats, stop_reason="target_trades_reached", terminal=True)

    db = _safe_call(client.persistent_status)
    open_payload = _safe_call(client.open_shadow_trades)
    readiness = _safe_call(client.readiness)
    monitor = _safe_call(
        client.monitor,
        apply_paper_close=False,
        exit_policy=exit_policy,
        time_stop_bars=time_stop_bars,
        max_hold_minutes=max_hold_minutes,
        min_r_to_arm_trailing=min_r_to_arm_trailing,
        giveback_r=giveback_r,
    )
    payloads = [db, open_payload, readiness, monitor]
    if _unsafe_payload(payloads):
        return _result(
            "stopped_by_safety",
            cycle_number,
            stats,
            db_state=_public_db(db),
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            stop_reason="broker_or_order_flag_detected",
            terminal=True,
        )

    open_count = max(_open_count(open_payload), _open_count(monitor))
    current_shadow_id = _current_shadow_id(open_payload, monitor, state)
    if open_count > 1:
        return _result(
            "stopped_by_duplicate_shadow",
            cycle_number,
            stats,
            db_state=_public_db(db),
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            open_shadow_count=open_count,
            current_shadow_id=current_shadow_id,
            stop_reason="multiple_open_shadows",
            terminal=True,
        )

    pending_id = _pending_reconciliation_shadow_id(state, trades, stats)
    if pending_id:
        if open_count == 1 and current_shadow_id and current_shadow_id != pending_id:
            return _result(
                "stopped_by_duplicate_shadow",
                cycle_number,
                stats,
                db_state=_public_db(db),
                readiness=readiness,
                open_payload=open_payload,
                monitor=monitor,
                open_shadow_count=open_count,
                current_shadow_id=current_shadow_id,
                stop_reason="pending_reconciliation_shadow_mismatch",
                anomaly_type="pending_reconciliation_shadow_mismatch",
                orphan_shadow_trade_id=pending_id,
                terminal=True,
            )
        if open_count == 0:
            history = _safe_call(client.shadow_trade_history)
            if _unsafe_payload([history]):
                return _result(
                    "stopped_by_safety",
                    cycle_number,
                    stats,
                    db_state=_public_db(db),
                    readiness=readiness,
                    open_payload=open_payload,
                    monitor=monitor,
                    history=history,
                    stop_reason="history_safety_flag_detected",
                    terminal=True,
                )
            if not _history_available(history):
                return _result(
                    "stopped_by_history_unavailable",
                    cycle_number,
                    stats,
                    db_state=_public_db(db),
                    readiness=readiness,
                    open_payload=open_payload,
                    monitor=monitor,
                    history=history,
                    open_shadow_count=0,
                    current_shadow_id="",
                    stop_reason=str(history.get("reason") or "history_unavailable"),
                    anomaly="history_unavailable_for_pending_reconciliation",
                    anomaly_type="history_unavailable_for_pending_reconciliation",
                    orphan_shadow_trade_id=pending_id,
                    next_action="fix_history_before_next_open",
                    terminal=True,
                )
            closed = _find_closed_shadow_trade(history, pending_id)
            if closed:
                imported = _closed_trade_from_history(closed)
                stats_after = compute_xau_m15_paper_batch_stats([*trades, imported], state={**state, "current_open_shadow_id": ""}, cycle_outputs=[])
                return _result(
                    "reconciled_closed_shadow",
                    cycle_number,
                    stats_after,
                    db_state=_public_db(db),
                    readiness=readiness,
                    open_payload=open_payload,
                    monitor=monitor,
                    history=history,
                    open_shadow_count=0,
                    current_shadow_id="",
                    closed_trade=imported,
                    reconciled_shadow_trade_id=pending_id,
                    next_action="recheck_gates_for_next_shadow",
                )
            return _result(
                "stopped_by_orphaned_shadow_missing_close_record",
                cycle_number,
                stats,
                db_state=_public_db(db),
                readiness=readiness,
                open_payload=open_payload,
                monitor=monitor,
                history=history,
                open_shadow_count=0,
                current_shadow_id="",
                stop_reason="opened_shadow_missing_close_record",
                anomaly="opened_shadow_missing_close_record",
                anomaly_type="opened_shadow_missing_close_record",
                orphan_shadow_trade_id=pending_id,
                next_action="inspect_shadow_lifecycle_before_next_open",
                terminal=True,
            )

    db_block = _db_block_reason(db)
    if db_block:
        return _result(
            "stopped_by_db",
            cycle_number,
            stats,
            db_state=_public_db(db),
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            open_shadow_count=open_count,
            current_shadow_id=current_shadow_id,
            stop_reason=db_block,
            terminal=True,
        )

    if open_count == 1:
        return _handle_existing_shadow(
            client=client,
            cycle_number=cycle_number,
            stats=stats,
            db=db,
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            current_shadow_id=current_shadow_id,
            dry_run=dry_run,
            paper_only_confirmed=paper_only_confirmed,
            exit_policy=exit_policy,
            time_stop_bars=time_stop_bars,
            max_hold_minutes=max_hold_minutes,
            min_r_to_arm_trailing=min_r_to_arm_trailing,
            giveback_r=giveback_r,
        )

    orphan = _orphan_reason(state, open_payload, monitor)
    if orphan:
        return _result(
            "idle_no_shadow",
            cycle_number,
            stats,
            db_state=_public_db(db),
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            stop_reason=orphan,
            anomaly=orphan,
            next_action="verify_orphan_before_next_shadow",
        )

    readiness_block = _readiness_block_reason(readiness)
    if readiness_block:
        return _result(
            "readiness_blocked",
            cycle_number,
            stats,
            db_state=_public_db(db),
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            stop_reason=readiness_block,
            blocked_cycle=True,
            next_action="resolve_readiness_gates",
        )

    if dry_run or not paper_only_confirmed:
        return _result(
            "idle_no_shadow",
            cycle_number,
            stats,
            db_state=_public_db(db),
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            next_action="would_open_one_paper_shadow_after_confirmation",
            paper_shadow_created=False,
        )

    opened = _safe_call(client.open_shadow_once)
    if _unsafe_payload([opened]):
        return _result("stopped_by_safety", cycle_number, stats, stop_reason="open_shadow_safety_flag_detected", terminal=True, open_result=opened)
    if bool(opened.get("open_persistence_failed")):
        return _result(
            "stopped_by_open_persistence_failed",
            cycle_number,
            stats,
            db_state=_public_db(db),
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            open_result=opened,
            stop_reason="open_persistence_failed",
            next_action="drain_queue_or_backfill_runtime_open_shadow",
            terminal=True,
        )
    created = bool(opened.get("paper_shadow_created"))
    shadow_id = str(opened.get("shadow_trade_id") or "")
    if not created:
        return _result(
            "readiness_blocked",
            cycle_number,
            stats,
            db_state=_public_db(db),
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            open_result=opened,
            stop_reason=str(opened.get("reason") or "paper_shadow_open_rejected"),
            next_action="wait_and_recheck",
        )
    stats = {**stats, "trades_opened": int(stats.get("trades_opened") or 0) + 1, "last_shadow_trade_id": shadow_id, "current_open_shadow_id": shadow_id}
    return _result(
        "opening_shadow",
        cycle_number,
        stats,
        db_state=_public_db(db),
        readiness=readiness,
        open_payload=open_payload,
        monitor=monitor,
        open_result=opened,
        open_shadow_count=1,
        current_shadow_id=shadow_id,
        paper_shadow_created=True,
        next_action="monitor_open_shadow",
    )


def _handle_existing_shadow(
    *,
    client: Any,
    cycle_number: int,
    stats: dict[str, Any],
    db: dict[str, Any],
    readiness: dict[str, Any],
    open_payload: dict[str, Any],
    monitor: dict[str, Any],
    current_shadow_id: str,
    dry_run: bool,
    paper_only_confirmed: bool,
    exit_policy: str = "default",
    time_stop_bars: int = 2,
    max_hold_minutes: float | None = None,
    min_r_to_arm_trailing: float = 0.15,
    giveback_r: float = 0.10,
) -> dict[str, Any]:
    should_watch = bool(monitor.get("should_watch_only")) or str(monitor.get("safety_exit_category") or "") in {"entry_block_only", "caution_watch"}
    should_close = bool(monitor.get("should_close_paper"))
    exit_reason = str(monitor.get("exit_reason") or "")
    close_allowed = should_close and exit_reason in ALLOWED_CLOSE_REASONS
    if should_watch and not should_close:
        return _result(
            "watch_only",
            cycle_number,
            stats,
            db_state=_public_db(db),
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            open_shadow_count=1,
            current_shadow_id=current_shadow_id,
            watch_only_cycle=True,
            next_action="continue_monitoring_current_shadow",
        )
    if not close_allowed:
        return _result(
            "shadow_open_monitoring",
            cycle_number,
            stats,
            db_state=_public_db(db),
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            open_shadow_count=1,
            current_shadow_id=current_shadow_id,
            no_action_cycle=True,
            next_action="continue_monitoring_current_shadow",
        )
    if dry_run or not paper_only_confirmed:
        return _result(
            "close_pending",
            cycle_number,
            stats,
            db_state=_public_db(db),
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            open_shadow_count=1,
            current_shadow_id=current_shadow_id,
            next_action="would_close_paper_shadow_after_confirmation",
        )
    close_result = _safe_call(
        client.monitor,
        apply_paper_close=True,
        exit_policy=exit_policy,
        time_stop_bars=time_stop_bars,
        max_hold_minutes=max_hold_minutes,
        min_r_to_arm_trailing=min_r_to_arm_trailing,
        giveback_r=giveback_r,
    )
    if _unsafe_payload([close_result]):
        return _result("stopped_by_safety", cycle_number, stats, stop_reason="close_shadow_safety_flag_detected", terminal=True, monitor=close_result)
    applied = bool(close_result.get("paper_close_applied"))
    state = "close_applied" if applied else "close_pending"
    closed_trade = _closed_trade_from_monitor(close_result) if applied else {}
    stats_after = compute_xau_m15_paper_batch_stats([closed_trade] if closed_trade else [], state=stats, cycle_outputs=[])
    return _result(
        state,
        cycle_number,
        stats_after,
        db_state=_public_db(db),
        readiness=readiness,
        open_payload=open_payload,
        monitor=close_result,
        open_shadow_count=0 if applied else 1,
        current_shadow_id="" if applied else current_shadow_id,
        paper_close_applied=applied,
        closed_trade=closed_trade,
        next_action="recheck_gates_for_next_shadow" if applied else "retry_close_after_review",
    )


def compute_xau_m15_paper_batch_stats(
    trades: list[dict[str, Any]],
    *,
    state: dict[str, Any] | None = None,
    cycle_outputs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    closed = [dict(trade) for trade in trades if str(trade.get("status") or "closed") == "closed" or trade.get("exit_reason")]
    pnls = [float(_num(trade.get("pnl")) or 0.0) for trade in closed]
    rs = [float(_num(trade.get("r_multiple")) or 0.0) for trade in closed]
    wins = len([pnl for pnl in pnls if pnl > 0])
    losses = len([pnl for pnl in pnls if pnl < 0])
    breakeven = len(closed) - wins - losses
    gross_profit = round(sum(pnl for pnl in pnls if pnl > 0), 6)
    gross_loss = round(abs(sum(pnl for pnl in pnls if pnl < 0)), 6)
    cycles = cycle_outputs or []
    trades_opened = int(_num((state or {}).get("trades_opened")) or 0)
    trades_opened = max(trades_opened, len({str(trade.get("shadow_trade_id") or "") for trade in closed if trade.get("shadow_trade_id")}))
    return {
        "trades_opened": trades_opened,
        "trades_closed": len(closed),
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "win_rate": round((wins / len(closed)) * 100.0, 6) if closed else 0.0,
        "profit_factor": _profit_factor(gross_profit, gross_loss),
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "expectancy": round(sum(pnls) / len(closed), 6) if closed else 0.0,
        "avg_r": round(sum(rs) / len(rs), 6) if rs else 0.0,
        "median_r": _median(rs),
        "max_drawdown": _max_drawdown(pnls),
        "max_consecutive_losses": _max_consecutive_losses(pnls),
        "avg_duration_minutes": round(sum(float(_num(trade.get("age_minutes")) or 0.0) for trade in closed) / len(closed), 6) if closed else 0.0,
        "take_profit_count": len([trade for trade in closed if trade.get("exit_reason") == "take_profit_hit"]),
        "stop_loss_count": len([trade for trade in closed if trade.get("exit_reason") == "stop_loss_hit"]),
        "timebox_exit_count": len([trade for trade in closed if trade.get("exit_reason") == "paper_timebox_exit"]),
        "fast_trailing_exit_count": len([trade for trade in closed if trade.get("exit_reason") == "paper_fast_trailing_exit"]),
        "fast_loss_cut_count": len([trade for trade in closed if trade.get("exit_reason") == "paper_fast_loss_cut"]),
        "orphan_count": len([cycle for cycle in cycles if cycle.get("anomaly_type") == "opened_shadow_missing_close_record"]),
        "trailing_exit_count": len([trade for trade in closed if trade.get("exit_reason") == "trailing_defensive_exit"]),
        "critical_safety_exit_count": len([trade for trade in closed if trade.get("safety_exit_category") == "critical_safety_exit"]),
        "watch_only_cycles": int(_num((state or {}).get("watch_only_cycles")) or 0) + len([cycle for cycle in cycles if cycle.get("watch_only_cycle")]),
        "no_action_cycles": int(_num((state or {}).get("no_action_cycles")) or 0) + len([cycle for cycle in cycles if cycle.get("no_action_cycle")]),
        "blocked_cycles": int(_num((state or {}).get("blocked_cycles")) or 0) + len([cycle for cycle in cycles if cycle.get("blocked_cycle")]),
        "last_shadow_trade_id": _last_shadow_id(closed, state),
        "current_open_shadow_id": str((state or {}).get("current_open_shadow_id") or ""),
        "next_action": "continue_until_target" if len(closed) else "collect_first_trade",
        **_safety(),
    }


def _result(
    runner_state: str,
    cycle_number: int,
    batch_stats: dict[str, Any],
    *,
    db_state: dict[str, Any] | None = None,
    readiness: dict[str, Any] | None = None,
    open_payload: dict[str, Any] | None = None,
    monitor: dict[str, Any] | None = None,
    history: dict[str, Any] | None = None,
    open_result: dict[str, Any] | None = None,
    open_shadow_count: int | None = None,
    current_shadow_id: str = "",
    paper_shadow_created: bool = False,
    paper_close_applied: bool = False,
    closed_trade: dict[str, Any] | None = None,
    stop_reason: str = "",
    anomaly: str = "",
    anomaly_type: str = "",
    orphan_shadow_trade_id: str = "",
    reconciled_shadow_trade_id: str = "",
    next_action: str = "",
    terminal: bool = False,
    watch_only_cycle: bool = False,
    no_action_cycle: bool = False,
    blocked_cycle: bool = False,
) -> dict[str, Any]:
    mon = monitor or {}
    ready = readiness or {}
    return {
        "ok": True,
        "runner_state": runner_state,
        "cycle_number": int(cycle_number),
        "db_state": db_state or {},
        "readiness_state": ready.get("readiness_state") or "",
        "open_shadow_count": int(open_shadow_count if open_shadow_count is not None else _open_count(open_payload or {})),
        "current_shadow_id": current_shadow_id,
        "monitor_state": mon.get("monitor_state") or "",
        "exit_signal": bool(mon.get("exit_signal")),
        "exit_reason": mon.get("exit_reason") or "",
        "should_close_paper": bool(mon.get("should_close_paper")),
        "should_watch_only": bool(mon.get("should_watch_only")),
        "paper_shadow_created": bool(paper_shadow_created),
        "paper_close_applied": bool(paper_close_applied),
        "open_persistence_failed": bool((open_result or {}).get("open_persistence_failed")),
        "open_write_retained_critical": bool((open_result or {}).get("open_write_retained_critical")),
        "shadow_trade_id": current_shadow_id or str((open_result or {}).get("shadow_trade_id") or mon.get("shadow_trade_id") or ""),
        "readiness": ready,
        "open_shadow_payload": open_payload or {},
        "monitor": mon,
        "history": history or {},
        "open_result": open_result or {},
        "closed_trade": closed_trade or {},
        "batch_stats": batch_stats,
        "stop_reason": stop_reason,
        "anomaly": anomaly,
        "anomaly_type": anomaly_type,
        "orphan_shadow_trade_id": orphan_shadow_trade_id,
        "reconciled_shadow_trade_id": reconciled_shadow_trade_id,
        "terminal": bool(terminal),
        "watch_only_cycle": bool(watch_only_cycle),
        "no_action_cycle": bool(no_action_cycle),
        "blocked_cycle": bool(blocked_cycle),
        "next_action": next_action or "recheck",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _terminal_cycle(runner_state: str, cycle_number: int, state: dict[str, Any], trades: list[dict[str, Any]], reason: str) -> dict[str, Any]:
    return _result(runner_state, cycle_number, compute_xau_m15_paper_batch_stats(trades, state=state), stop_reason=reason, terminal=True)


def _safe_call(fn: Callable[..., dict[str, Any]], **kwargs: Any) -> dict[str, Any]:
    try:
        result = fn(**kwargs)
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {"ok": False, "reason": type(exc).__name__, **_safety()}
    return dict(result or {"ok": False, "reason": "empty_payload", **_safety()})


def _db_block_reason(db: dict[str, Any]) -> str:
    if not bool(db.get("db_available")):
        return "db_unavailable"
    if bool(db.get("db_degraded")):
        return "db_degraded"
    if not bool(db.get("tables_ready")):
        return "tables_not_ready"
    if int(_num(db.get("queue_depth")) or 0) > 0:
        return "queue_depth_high"
    if int(_num(db.get("queued_writes")) or 0) > 0:
        return "queued_writes_pending"
    return ""


def _readiness_block_reason(readiness: dict[str, Any]) -> str:
    if str(readiness.get("readiness_state") or "") != "ready_for_one_cycle_paper_observation":
        return str(readiness.get("recommendation") or "readiness_blocked")
    if not bool(readiness.get("candidate_found")):
        return "candidate_missing"
    if str(readiness.get("candidate_status") or "") != "paper_observation_review":
        return "candidate_not_in_review"
    for key in ("runtime_context_available", "runtime_context_recent", "tick_available", "capital_allows_observation", "risk_allows_observation", "adaptive_allows_observation"):
        if not bool(readiness.get(key)):
            return key
    if int(_num(readiness.get("bars_count")) or 0) < 100:
        return "m15_bars_count_below_100"
    return ""


def _orphan_reason(state: dict[str, Any], open_payload: dict[str, Any], monitor: dict[str, Any]) -> str:
    current = str(state.get("current_open_shadow_id") or "")
    if not current:
        return ""
    if _open_count(open_payload) == 0 and _open_count(monitor) == 0:
        return "orphaned_or_runtime_lost"
    return ""


def _pending_reconciliation_shadow_id(state: dict[str, Any], trades: list[dict[str, Any]], stats: dict[str, Any]) -> str:
    current = str(state.get("current_open_shadow_id") or state.get("pending_reconciliation_shadow_id") or "").strip()
    if current:
        return current
    opened = int(_num(state.get("trades_opened")) or _num(stats.get("trades_opened")) or 0)
    closed = len({str(trade.get("shadow_trade_id") or "") for trade in trades if trade.get("shadow_trade_id") and (trade.get("exit_reason") or str(trade.get("status") or "").casefold() == "closed")})
    if opened > closed:
        return str(state.get("last_shadow_trade_id") or "").strip()
    return ""


def _find_closed_shadow_trade(history: dict[str, Any], shadow_trade_id: str) -> dict[str, Any]:
    rows: list[Any] = []
    for key in ("closed_trades", "trades", "items"):
        value = history.get(key)
        if isinstance(value, list):
            rows.extend(value)
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("shadow_trade_id") or "") != str(shadow_trade_id or ""):
            continue
        if str(row.get("status") or "").casefold() == "closed" or bool(row.get("closed_at")):
            return dict(row)
    return {}


def _history_available(history: dict[str, Any]) -> bool:
    if not bool(history.get("ok")):
        return False
    if "history_available" in history and not bool(history.get("history_available")):
        return False
    if "closed_trades" not in history and "trades" not in history and "items" not in history:
        return False
    return True


def _closed_trade_from_history(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "shadow_trade_id": row.get("shadow_trade_id") or "",
        "symbol": row.get("symbol") or SYMBOL,
        "broker_symbol": row.get("broker_symbol") or BROKER_SYMBOL,
        "timeframe": row.get("timeframe") or TIMEFRAME,
        "strategy_profile": row.get("strategy_profile") or row.get("profile") or CANDIDATE_PROFILE,
        "side": row.get("side") or "",
        "entry_price": _num(row.get("entry_price")) or 0.0,
        "exit_price": _num(row.get("exit_price")) or 0.0,
        "last_price": _num(row.get("exit_price")) or 0.0,
        "stop_loss": _num(row.get("stop_loss")) or 0.0,
        "take_profit": _num(row.get("take_profit")) or 0.0,
        "pnl": _num(row.get("pnl")) or 0.0,
        "pnl_pct": _num(row.get("pnl_pct")) or 0.0,
        "r_multiple": _num(row.get("r_multiple")) or 0.0,
        "opened_at": row.get("opened_at") or "",
        "closed_at": row.get("closed_at") or _now(),
        "age_minutes": _num(row.get("age_minutes")) or 0.0,
        "bars_since_entry": int(_num(row.get("bars_since_entry")) or 0),
        "exit_reason": row.get("exit_reason") or "persisted_close",
        "safety_exit_category": row.get("safety_exit_category") or "",
        "safety_exit_reason_detail": row.get("safety_exit_reason_detail") or "",
        "close_decision_reason": row.get("close_decision_reason") or "imported_from_shadow_trade_history",
        "status": "closed",
        **_safety(),
    }


def _unsafe_payload(payloads: list[dict[str, Any]]) -> bool:
    for payload in payloads:
        if bool(payload.get("broker_touched")) or bool(payload.get("order_executed")):
            return True
        if str(payload.get("order_policy") or "journal_only_no_broker") != "journal_only_no_broker":
            return True
    return False


def _open_count(payload: dict[str, Any]) -> int:
    if "open_count" in payload:
        return int(_num(payload.get("open_count")) or 0)
    if "open_shadow_count" in payload:
        return int(_num(payload.get("open_shadow_count")) or 0)
    return 0


def _current_shadow_id(open_payload: dict[str, Any], monitor: dict[str, Any], state: dict[str, Any]) -> str:
    if monitor.get("shadow_trade_id"):
        return str(monitor.get("shadow_trade_id") or "")
    trades = open_payload.get("trades") if isinstance(open_payload.get("trades"), list) else []
    for trade in trades:
        if isinstance(trade, dict) and trade.get("shadow_trade_id"):
            return str(trade.get("shadow_trade_id") or "")
    return str(state.get("current_open_shadow_id") or "")


def _closed_trade_from_step(step: dict[str, Any]) -> dict[str, Any]:
    trade = step.get("closed_trade") if isinstance(step.get("closed_trade"), dict) else {}
    return dict(trade) if trade else {}


def _closed_trade_from_monitor(monitor: dict[str, Any]) -> dict[str, Any]:
    shadow_id = str(monitor.get("shadow_trade_id") or "")
    if not shadow_id:
        return {}
    return {
        "shadow_trade_id": shadow_id,
        "symbol": SYMBOL,
        "broker_symbol": BROKER_SYMBOL,
        "timeframe": TIMEFRAME,
        "strategy_profile": CANDIDATE_PROFILE,
        "side": monitor.get("side") or "",
        "entry_price": _num(monitor.get("entry_price")) or 0.0,
        "exit_price": _num(monitor.get("current_price")) or 0.0,
        "last_price": _num(monitor.get("current_price")) or 0.0,
        "stop_loss": _num(monitor.get("stop_loss")) or 0.0,
        "take_profit": _num(monitor.get("take_profit")) or 0.0,
        "pnl": _num(monitor.get("unrealized_pnl")) or 0.0,
        "pnl_pct": _num(monitor.get("unrealized_pnl_pct")) or 0.0,
        "r_multiple": _num(monitor.get("r_multiple")) or 0.0,
        "opened_at": monitor.get("opened_at") or "",
        "closed_at": _now(),
        "age_minutes": _num(monitor.get("age_minutes")) or 0.0,
        "bars_since_entry": int(_num(monitor.get("bars_since_entry")) or 0),
        "exit_reason": monitor.get("exit_reason") or "",
        "safety_exit_category": monitor.get("safety_exit_category") or "",
        "safety_exit_reason_detail": monitor.get("safety_exit_reason_detail") or "",
        "close_decision_reason": monitor.get("close_decision_reason") or "",
        "status": "closed",
        **_safety(),
    }


def _merge_step_state(state: dict[str, Any], step: dict[str, Any]) -> None:
    state["schema_version"] = STATE_SCHEMA_VERSION
    state["runner_version"] = RUNNER_VERSION
    state["updated_at"] = _now()
    state["cycles_completed"] = int(_num(state.get("cycles_completed")) or 0) + 1
    if step.get("paper_shadow_created"):
        state["trades_opened"] = int(_num(state.get("trades_opened")) or 0) + 1
        state["current_open_shadow_id"] = step.get("shadow_trade_id") or ""
        state["last_shadow_trade_id"] = step.get("shadow_trade_id") or ""
    if step.get("paper_close_applied"):
        state["current_open_shadow_id"] = ""
        state["pending_reconciliation_shadow_id"] = ""
        if step.get("shadow_trade_id"):
            state["last_shadow_trade_id"] = step.get("shadow_trade_id")
    if step.get("runner_state") == "reconciled_closed_shadow":
        state["current_open_shadow_id"] = ""
        state["pending_reconciliation_shadow_id"] = ""
        if step.get("reconciled_shadow_trade_id"):
            state["last_shadow_trade_id"] = step.get("reconciled_shadow_trade_id")
    if step.get("runner_state") == "stopped_by_orphaned_shadow_missing_close_record":
        state["pending_reconciliation_shadow_id"] = step.get("orphan_shadow_trade_id") or state.get("current_open_shadow_id") or ""
    for key in ("watch_only_cycles", "no_action_cycles", "blocked_cycles"):
        if step.get(key[:-1] if key.endswith("s") else key):
            state[key] = int(_num(state.get(key)) or 0) + 1
    if step.get("anomaly"):
        anomalies = state.setdefault("anomalies", [])
        if isinstance(anomalies, list):
            anomalies.append({"at": _now(), "reason": step.get("anomaly"), "shadow_trade_id": state.get("current_open_shadow_id") or ""})


def _public_state(state: dict[str, Any], stats: dict[str, Any], trades: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        **state,
        "schema_version": STATE_SCHEMA_VERSION,
        "pending_reconciliation_shadow_id": state.get("pending_reconciliation_shadow_id") or "",
        "anomalies": state.get("anomalies") if isinstance(state.get("anomalies"), list) else [],
        "trades": trades or [],
        "batch_stats": stats,
        "stats": stats,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        **_safety(),
    }


def _load_state(path: Path | None) -> dict[str, Any]:
    payload = _load_json(path)
    return dict(payload) if isinstance(payload, dict) else {}


def _load_results(path: Path | None) -> dict[str, Any]:
    payload = _load_json(path)
    return dict(payload) if isinstance(payload, dict) else {"trades": []}


def _load_json(path: Path | None) -> Any:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path | None, payload: dict[str, Any]) -> None:
    if not path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True, default=str), encoding="utf-8")


def _trades(results: dict[str, Any]) -> list[dict[str, Any]]:
    rows = results.get("trades") if isinstance(results.get("trades"), list) else []
    return [dict(row) for row in rows if isinstance(row, dict)]


def _public_db(db: dict[str, Any]) -> dict[str, Any]:
    return {
        "provider": db.get("provider") or "",
        "db_available": bool(db.get("db_available")),
        "db_degraded": bool(db.get("db_degraded")),
        "tables_ready": bool(db.get("tables_ready")),
        "queue_depth": int(_num(db.get("queue_depth")) or 0),
        "queued_writes": int(_num(db.get("queued_writes")) or 0),
        "failed_writes": int(_num(db.get("failed_writes")) or 0),
        "recommendation": db.get("recommendation") or "",
        **_safety(),
    }


def _profit_factor(gross_profit: float, gross_loss: float) -> float:
    if gross_loss > 0:
        return round(gross_profit / gross_loss, 6)
    return round(gross_profit, 6) if gross_profit > 0 else 0.0


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return round(ordered[mid], 6)
    return round((ordered[mid - 1] + ordered[mid]) / 2.0, 6)


def _max_drawdown(pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return round(abs(max_dd), 6)


def _max_consecutive_losses(pnls: list[float]) -> int:
    best = 0
    current = 0
    for pnl in pnls:
        if pnl < 0:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def _last_shadow_id(closed: list[dict[str, Any]], state: dict[str, Any] | None) -> str:
    if closed:
        return str(closed[-1].get("shadow_trade_id") or "")
    return str((state or {}).get("last_shadow_trade_id") or "")


def _num(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _exit_policy(value: object) -> str:
    clean = str(value or "default").casefold().strip()
    return "fast_observation" if clean == "fast_observation" else "default"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
