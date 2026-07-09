from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

from services.mt5.mt5_bridge import mt5_shadow_trades_history, mt5_shadow_trades_open, mt5_xau_m15_paper_observation_readiness
from services.mt5.mt5_capital_protection_governor import run_capital_protection_governor
from services.mt5.mt5_frozen_sample_guard import evaluate_frozen_sample
from services.mt5.mt5_market_active_guard import evaluate_market_active
from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore, persistent_intelligence_status
from services.mt5.mt5_risk_governor import assess_runtime_risk
from services.mt5.mt5_runtime_context_diagnostics import run_runtime_context_diagnostics
from services.mt5.mt5_runtime_snapshot import append_closed_shadow_trade, get_snapshot, update_open_shadow_trade
from services.mt5.mt5_xau_m15_paper_observation_readiness import (
    BROKER_SYMBOL,
    CANDIDATE_PROFILE,
    SYMBOL,
    TIMEFRAME,
    run_xau_m15_paper_observation_shadow_once,
)
from services.mt5.mt5_xau_m15_paper_shadow_monitor import run_xau_m15_paper_shadow_monitor


RUNNER_VERSION = "2026-07-03.xau_m15_paper_observation_batch_runner.v3"
STATE_SCHEMA_VERSION = "2026-07-02.xau_m15_paper_batch_state.v3"
DEFAULT_STATE_FILE = Path("data/research_outputs/xau_m15_paper_batch_state.json")
DEFAULT_RESULTS_FILE = Path("data/research_outputs/xau_m15_paper_batch_results.json")
DEFAULT_MARKET_GUARD = {
    "min_bars": 50,
    "max_bar_age_seconds": 5400,
    "max_tick_age_seconds": 5400,
    "require_tick": True,
    "movement_lookback_bars": 10,
    "freeze_lookback_bars": 5,
    "min_price_move_pct": 0.000001,
    "min_spread_move_multiple": 0.1,
    "min_absolute_move": 1e-12,
    "min_recent_range_pct": 0.0,
    "min_atr_pct": 0.0,
    "max_spread": None,
    "use_volume_freeze_check": True,
}
DEFAULT_MULTI_ASSET_MIN_BARS = 100
DEFAULT_MAX_OPEN_POSITIONS = 1
DEFAULT_MAX_OPEN_POSITIONS_TOTAL = 1
UNSAFE_POLICY_BLOCK_REASONS = {
    "asset_config_invalid_order_policy",
    "asset_config_missing_allow_broker_orders",
    "asset_config_allows_broker_orders",
    "asset_config_missing_allow_candidate_activation",
    "asset_config_allows_candidate_activation",
    "asset_config_missing_allow_paper_forward",
    "asset_config_allows_paper_forward",
    "asset_config_broker_touched_true",
    "asset_config_order_executed_true",
}
SAFETY_VIOLATION_POLICY_REASONS = {
    "asset_config_invalid_order_policy",
    "asset_config_allows_broker_orders",
    "asset_config_allows_candidate_activation",
    "asset_config_allows_paper_forward",
    "asset_config_broker_touched_true",
    "asset_config_order_executed_true",
}
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

    def __init__(
        self,
        *,
        symbol: str = SYMBOL,
        broker_symbol: str = BROKER_SYMBOL,
        timeframe: str = TIMEFRAME,
        allowed_symbols: list[str] | tuple[str, ...] | None = None,
        asset_configs: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
        store: Any | None = None,
        db_state: dict[str, Any] | None = None,
    ) -> None:
        self.symbol = _clean_symbol(symbol or SYMBOL)
        self.broker_symbol = str(broker_symbol or self.symbol)
        self.timeframe = _clean_timeframe(timeframe or TIMEFRAME)
        self.asset_configs = _asset_configs_from_inputs(asset_configs=asset_configs, allowed_symbols=allowed_symbols, default_timeframe=self.timeframe)
        self.asset_config = _asset_config_for(self.asset_configs, self.symbol, self.timeframe)
        self.store = store
        self.db_state = dict(db_state or {}) if db_state is not None else None

    def persistent_status(self) -> dict[str, Any]:
        if self.db_state is not None:
            return dict(self.db_state)
        return persistent_intelligence_status(write_test_event=False)

    def open_shadow_trades(self) -> dict[str, Any]:
        return mt5_shadow_trades_open(symbol=self.symbol, limit=10)

    def shadow_trade_history(self) -> dict[str, Any]:
        return mt5_shadow_trades_history(symbol=self.symbol, timeframe=self.timeframe, limit=50)

    def readiness(self) -> dict[str, Any]:
        config_error = _asset_config_error(self.asset_config)
        if config_error:
            return _asset_config_readiness_blocked(self.symbol, self.broker_symbol, self.timeframe, config_error, self.asset_config)
        if self.symbol != SYMBOL or self.timeframe != TIMEFRAME:
            return _generic_multi_asset_readiness(self.symbol, self.broker_symbol, self.timeframe, db_state=self.db_state, asset_config=self.asset_config)
        return _apply_asset_config_to_readiness(mt5_xau_m15_paper_observation_readiness(), self.asset_config)

    def monitor(
        self,
        *,
        apply_paper_close: bool = False,
        exit_policy: str = "default",
        time_stop_bars: int = 1,
        max_hold_minutes: float | None = None,
        min_r_to_arm_trailing: float = 0.15,
        giveback_r: float = 0.10,
        fast_loss_cut_r: float = -0.25,
    ) -> dict[str, Any]:
        config_error = _asset_config_error(self.asset_config)
        if config_error:
            return _generic_monitor_result(
                self.symbol,
                self.broker_symbol,
                self.timeframe,
                monitor_state="blocked_by_asset_config",
                open_shadow_count=0,
                exit_signal=False,
                exit_reason=config_error,
                paper_close_applied=False,
                shadow_status_after="unknown",
                activity={"market_active": False, "market_inactive_or_frozen": True, "no_price_movement": True},
            )
        if self.symbol != SYMBOL or self.timeframe != TIMEFRAME:
            return _generic_multi_asset_monitor(
                self.symbol,
                self.broker_symbol,
                self.timeframe,
                apply_paper_close=apply_paper_close,
                exit_policy=exit_policy,
                time_stop_bars=time_stop_bars,
                max_hold_minutes=max_hold_minutes,
                min_r_to_arm_trailing=min_r_to_arm_trailing,
                giveback_r=giveback_r,
                fast_loss_cut_r=fast_loss_cut_r,
                store=self.store,
                asset_config=self.asset_config,
            )
        return run_xau_m15_paper_shadow_monitor(
            apply_paper_close=apply_paper_close,
            exit_policy=exit_policy,
            time_stop_bars=time_stop_bars,
            max_hold_minutes=max_hold_minutes,
            min_r_to_arm_trailing=min_r_to_arm_trailing,
            giveback_r=giveback_r,
            fast_loss_cut_r=fast_loss_cut_r,
            )

    def open_shadow_once(self, *, strict_paper_probe: bool = False) -> dict[str, Any]:
        config_error = _asset_config_error(self.asset_config)
        if config_error:
            return _generic_shadow_once_rejected(self.symbol, self.broker_symbol, self.timeframe, reason=config_error, readiness=_asset_config_readiness_blocked(self.symbol, self.broker_symbol, self.timeframe, config_error, self.asset_config))
        if self.symbol != SYMBOL or self.timeframe != TIMEFRAME:
            return _generic_multi_asset_shadow_once(
                self.symbol,
                self.broker_symbol,
                self.timeframe,
                asset_config=self.asset_config,
                strict_paper_probe=strict_paper_probe,
                store=self.store,
                db_state=self.db_state,
            )
        return run_xau_m15_paper_observation_shadow_once(
            payload={
                "confirm_paper_shadow_only": True,
                "symbol": self.symbol,
                "timeframe": self.timeframe,
                "strict_paper_probe": bool(strict_paper_probe),
            }
        )

    def queue_drain(self) -> dict[str, Any]:
        from services.mt5.mt5_persistent_intelligence_store import persistent_intelligence_queue_drain

        return persistent_intelligence_queue_drain(max_items=50, drop_failed_noncritical=True)


class HttpPaperObservationClient:
    source = "remote_live_http_process"

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 10.0,
        symbol: str = SYMBOL,
        broker_symbol: str = BROKER_SYMBOL,
        timeframe: str = TIMEFRAME,
        allowed_symbols: list[str] | tuple[str, ...] | None = None,
        asset_configs: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
    ) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.timeout_seconds = max(1.0, float(timeout_seconds or 10.0))
        self.symbol = _clean_symbol(symbol or SYMBOL)
        self.broker_symbol = str(broker_symbol or self.symbol)
        self.timeframe = _clean_timeframe(timeframe or TIMEFRAME)
        self.asset_configs = _asset_configs_from_inputs(asset_configs=asset_configs, allowed_symbols=allowed_symbols, default_timeframe=self.timeframe)
        self.asset_config = _asset_config_for(self.asset_configs, self.symbol, self.timeframe)

    def persistent_status(self) -> dict[str, Any]:
        return self._get("/api/genesis/mt5/persistent-intelligence/status")

    def open_shadow_trades(self) -> dict[str, Any]:
        return self._get(f"/api/genesis/mt5/shadow-trades/open?{urlencode({'symbol': self.symbol})}")

    def shadow_trade_history(self) -> dict[str, Any]:
        return self._get(f"/api/genesis/mt5/shadow-trades/history?{urlencode({'symbol': self.symbol, 'timeframe': self.timeframe, 'limit': 50})}")

    def readiness(self) -> dict[str, Any]:
        config_error = _asset_config_error(self.asset_config)
        if config_error:
            return _asset_config_readiness_blocked(self.symbol, self.broker_symbol, self.timeframe, config_error, self.asset_config)
        if self.symbol != SYMBOL or self.timeframe != TIMEFRAME:
            query = urlencode({"symbol": self.symbol, "broker_symbol": self.broker_symbol, "timeframe": self.timeframe})
            try:
                return _apply_asset_config_to_readiness(
                    self._get(f"/api/genesis/mt5/paper-observation/readiness?{query}"),
                    self.asset_config,
                )
            except Exception:
                return _generic_http_readiness_unavailable(self.symbol, self.broker_symbol, self.timeframe)
        return _apply_asset_config_to_readiness(self._get("/api/genesis/mt5/xau-m15/paper-observation/readiness"), self.asset_config)

    def monitor(
        self,
        *,
        apply_paper_close: bool = False,
        exit_policy: str = "default",
        time_stop_bars: int = 1,
        max_hold_minutes: float | None = None,
        min_r_to_arm_trailing: float = 0.15,
        giveback_r: float = 0.10,
        fast_loss_cut_r: float = -0.25,
    ) -> dict[str, Any]:
        config_error = _asset_config_error(self.asset_config)
        if config_error:
            return _generic_monitor_result(
                self.symbol,
                self.broker_symbol,
                self.timeframe,
                monitor_state="blocked_by_asset_config",
                open_shadow_count=0,
                exit_signal=False,
                exit_reason=config_error,
                paper_close_applied=False,
                shadow_status_after="unknown",
                activity={"market_active": False, "market_inactive_or_frozen": True, "no_price_movement": True},
            )
        if self.symbol != SYMBOL or self.timeframe != TIMEFRAME:
            return _http_monitor_asset_mismatch(self.symbol, self.broker_symbol, self.timeframe)
        policy = _exit_policy(exit_policy)
        if apply_paper_close or policy != "default":
            body = {
                "apply_paper_close": bool(apply_paper_close),
                "exit_policy": policy,
                "time_stop_bars": int(time_stop_bars or 0),
                "min_r_to_arm_trailing": float(min_r_to_arm_trailing or 0.0),
                "giveback_r": float(giveback_r or 0.0),
                "fast_loss_cut_r": float(fast_loss_cut_r or -0.25),
            }
            if max_hold_minutes is not None:
                body["max_hold_minutes"] = float(max_hold_minutes)
            return self._post("/api/genesis/mt5/xau-m15/paper-shadow/monitor", body)
        return self._get("/api/genesis/mt5/xau-m15/paper-shadow/monitor")

    def open_shadow_once(self, *, strict_paper_probe: bool = False) -> dict[str, Any]:
        config_error = _asset_config_error(self.asset_config)
        if config_error:
            return _generic_shadow_once_rejected(self.symbol, self.broker_symbol, self.timeframe, reason=config_error, readiness=_asset_config_readiness_blocked(self.symbol, self.broker_symbol, self.timeframe, config_error, self.asset_config))
        if self.symbol != SYMBOL or self.timeframe != TIMEFRAME:
            return _generic_shadow_once_rejected(self.symbol, self.broker_symbol, self.timeframe, reason="generic_http_shadow_once_endpoint_not_enabled")
        return self._post(
            "/api/genesis/mt5/xau-m15/paper-observation/shadow-once",
            {
                "confirm_paper_shadow_only": True,
                "symbol": self.symbol,
                "timeframe": self.timeframe,
                "strict_paper_probe": bool(strict_paper_probe),
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
    symbol: str = SYMBOL,
    broker_symbol: str = BROKER_SYMBOL,
    timeframe: str = TIMEFRAME,
    target_trades: int = 20,
    max_cycles: int = 200,
    interval_seconds: float = 60.0,
    max_runtime_minutes: float | None = None,
    dry_run: bool = True,
    paper_only_confirmed: bool = False,
    once: bool = False,
    exit_policy: str = "default",
    time_stop_bars: int = 1,
    max_hold_minutes: float | None = None,
    min_r_to_arm_trailing: float = 0.15,
    giveback_r: float = 0.10,
    fast_loss_cut_r: float = -0.25,
    strict_paper_probe: bool = False,
    explain_gates: bool = False,
    wait_for_signal: bool = False,
    state_file: str | Path | None = DEFAULT_STATE_FILE,
    results_file: str | Path | None = DEFAULT_RESULTS_FILE,
    sleep_fn: Callable[[float], None] | None = None,
    timeout_seconds: float = 10.0,
    allowed_symbols: list[str] | tuple[str, ...] | None = None,
    asset_configs: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    clean_symbol = _clean_symbol(symbol or SYMBOL)
    clean_timeframe = _clean_timeframe(timeframe or TIMEFRAME)
    clean_broker_symbol = str(broker_symbol or clean_symbol)
    clean_asset_configs = _asset_configs_from_inputs(asset_configs=asset_configs, allowed_symbols=allowed_symbols, default_timeframe=clean_timeframe)
    active_client = client or (
        HttpPaperObservationClient(
            base_url,
            timeout_seconds=timeout_seconds,
            symbol=clean_symbol,
            broker_symbol=clean_broker_symbol,
            timeframe=clean_timeframe,
            asset_configs=clean_asset_configs,
        )
        if base_url
        else LocalPaperObservationClient(
            symbol=clean_symbol,
            broker_symbol=clean_broker_symbol,
            timeframe=clean_timeframe,
            asset_configs=clean_asset_configs,
        )
    )
    state_path = Path(state_file) if state_file else None
    results_path = Path(results_file) if results_file else None
    state = _load_state(state_path)
    results = _load_results(results_path)
    _ensure_session(state, results, symbol=clean_symbol, broker_symbol=clean_broker_symbol, timeframe=clean_timeframe)
    trades = _trades(results)
    cycles_requested = 1 if once or (dry_run and not wait_for_signal) else max(1, int(max_cycles or 1))
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
            fast_loss_cut_r=fast_loss_cut_r,
            strict_paper_probe=strict_paper_probe,
            explain_gates=explain_gates,
            symbol=clean_symbol,
            broker_symbol=clean_broker_symbol,
            timeframe=clean_timeframe,
        )
        cycle_outputs.append(step)
        _merge_step_state(state, step)
        closed = _closed_trade_from_step(step)
        if closed:
            closed.setdefault("session_id", state.get("session_id") or "")
            trades.append(closed)
        terminal = bool(step.get("terminal"))
        if (dry_run and not wait_for_signal) or once or terminal:
            break
        if idx + 1 >= cycles_requested:
            break
        sleep(max(0.0, float(interval_seconds or 0.0)))

    if not cycle_outputs:
        cycle_outputs.append(_terminal_cycle("stopped_by_max_cycles", 0, state, trades, "no_cycles_requested"))

    final = cycle_outputs[-1]
    stats = compute_xau_m15_paper_batch_stats(trades, state=state, cycle_outputs=cycle_outputs, symbol=clean_symbol, timeframe=clean_timeframe)
    if not dry_run:
        _write_json(state_path, _public_state(state, stats, trades))
        _write_json(
            results_path,
            {
                "schema_version": STATE_SCHEMA_VERSION,
                "runner_version": RUNNER_VERSION,
                "session_id": state.get("session_id") or "",
                "session_started_at": state.get("session_started_at") or "",
                "target_scope": "session",
                "session_trades_opened": stats.get("session_trades_opened", 0),
                "session_trades_closed": stats.get("session_trades_closed", 0),
                "trades": trades,
                "session_trades": _session_closed_trades(_closed_rows(trades), state),
                "batch_stats": stats,
                "stats": stats,
                "updated_at": _now(),
                **_safety(),
            },
        )

    final_readiness = final.get("readiness") if isinstance(final.get("readiness"), dict) else {}
    final_db = final.get("db_state") if isinstance(final.get("db_state"), dict) else {}
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
        "fast_loss_cut_r": float(fast_loss_cut_r or -0.25),
        "strict_paper_probe": bool(strict_paper_probe),
        "explain_gates": bool(explain_gates),
        "wait_for_signal": bool(wait_for_signal),
        "client_source": getattr(active_client, "source", "injected"),
        "asset": clean_symbol,
        "symbol": clean_symbol,
        "broker_symbol": clean_broker_symbol,
        "timeframe": clean_timeframe,
        "bars_count": int(_num(final_readiness.get("bars_count")) or 0),
        "market_active": bool(final_readiness.get("market_active")),
        "market_active_reason": final_readiness.get("market_active_reason") or "",
        "db_available": bool(final_db.get("db_available")),
        "db_degraded": bool(final_db.get("db_degraded")),
        "tables_ready": bool(final_db.get("tables_ready")),
        "queue_depth": int(_num(final_db.get("queue_depth")) or 0),
        "max_open_positions_total": int(_num(final_readiness.get("max_open_positions_total")) or DEFAULT_MAX_OPEN_POSITIONS_TOTAL),
        "candidate_profile": CANDIDATE_PROFILE if clean_symbol == SYMBOL and clean_timeframe == TIMEFRAME else f"multi_asset_paper_test|symbol={clean_symbol}|timeframe={clean_timeframe}",
        "session_id": state.get("session_id") or "",
        "session_started_at": state.get("session_started_at") or "",
        "target_scope": "session",
        "session_trades_opened": stats.get("session_trades_opened", 0),
        "session_trades_closed": stats.get("session_trades_closed", 0),
        "historical_closed_count": stats.get("historical_closed_count", 0),
        "current_shadow_id": stats.get("current_shadow_id") or "",
        "current_shadow_source": final.get("current_shadow_source") or "",
        "runner_state": final.get("runner_state") or "batch_completed",
        "cycles_requested": cycles_requested,
        "cycles_completed": len(cycle_outputs),
        "target_trades": int(target_trades or 0),
        "stop_reason": final.get("stop_reason") or "",
        "current_phase": final.get("current_phase") or "",
        "readiness_state": final.get("readiness_state") or "",
        "gate_summary": final.get("gate_summary") or {},
        "next_action": final.get("next_action") or "",
        "open_count": final.get("open_shadow_count") or 0,
        "win_rate": stats.get("win_rate", 0.0),
        "expectancy": stats.get("expectancy", 0.0),
        "profit_factor": stats.get("profit_factor", 0.0),
        "last_closed_trade": final.get("closed_trade") or {},
        "failed_gate_names": final.get("failed_gate_names") or [],
        "failed_gate_reasons": final.get("failed_gate_reasons") or {},
        "risk_governor_reason": final.get("risk_governor_reason") or "",
        "recent_edge_negative": bool(final.get("recent_edge_negative")),
        "entry_allowed_for_paper_test": bool(final.get("entry_allowed_for_paper_test")),
        "entry_block_type": final.get("entry_block_type") or "",
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
        "safety_violation": any(bool(cycle.get("safety_violation")) for cycle in cycle_outputs),
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
    time_stop_bars: int = 1,
    max_hold_minutes: float | None = None,
    min_r_to_arm_trailing: float = 0.15,
    giveback_r: float = 0.10,
    fast_loss_cut_r: float = -0.25,
    strict_paper_probe: bool = False,
    explain_gates: bool = False,
    symbol: str = SYMBOL,
    broker_symbol: str = BROKER_SYMBOL,
    timeframe: str = TIMEFRAME,
) -> dict[str, Any]:
    clean_symbol = _clean_symbol(symbol or SYMBOL)
    clean_timeframe = _clean_timeframe(timeframe or TIMEFRAME)
    clean_broker_symbol = str(broker_symbol or clean_symbol)
    stats = compute_xau_m15_paper_batch_stats(trades, state=state, cycle_outputs=[], symbol=clean_symbol, timeframe=clean_timeframe)

    db = _safe_call(client.persistent_status)
    open_payload = _safe_call(client.open_shadow_trades)
    readiness = _normalize_readiness_diagnostics(_safe_call(client.readiness))
    monitor = _safe_call(
        client.monitor,
        apply_paper_close=False,
        exit_policy=exit_policy,
        time_stop_bars=time_stop_bars,
        max_hold_minutes=max_hold_minutes,
        min_r_to_arm_trailing=min_r_to_arm_trailing,
        giveback_r=giveback_r,
        fast_loss_cut_r=fast_loss_cut_r,
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
    current_shadow_source = _current_shadow_source(open_payload, monitor)
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
            current_shadow_source=current_shadow_source,
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
                current_shadow_source=current_shadow_source,
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
            current_shadow_source=current_shadow_source,
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
            current_shadow_source=current_shadow_source,
            dry_run=dry_run,
            paper_only_confirmed=paper_only_confirmed,
            exit_policy=exit_policy,
            time_stop_bars=time_stop_bars,
            max_hold_minutes=max_hold_minutes,
            min_r_to_arm_trailing=min_r_to_arm_trailing,
            giveback_r=giveback_r,
            fast_loss_cut_r=fast_loss_cut_r,
            symbol=clean_symbol,
            broker_symbol=clean_broker_symbol,
            timeframe=clean_timeframe,
        )

    if int(stats.get("session_trades_closed") or stats.get("trades_closed") or 0) >= int(target_trades or 0) > 0:
        return _result(
            "stopped_by_target_trades",
            cycle_number,
            stats,
            db_state=_public_db(db),
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            open_shadow_count=0,
            current_shadow_id="",
            current_shadow_source=current_shadow_source,
            stop_reason="target_trades_reached",
            closed_trade=_last_valid_session_closed_trade(trades, state, clean_symbol, clean_timeframe),
            terminal=True,
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
            current_shadow_source=current_shadow_source,
            anomaly=orphan,
            next_action="verify_orphan_before_next_shadow",
        )

    adaptive_wait = _adaptive_paper_wait_reason(readiness, strict_paper_probe=bool(strict_paper_probe))
    if adaptive_wait:
        return _result(
            "waiting_for_high_quality_paper_signal",
            cycle_number,
            stats,
            db_state=_public_db(db),
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            stop_reason=adaptive_wait,
            blocked_cycle=True,
            next_action="wait_for_high_quality_paper_signal",
        )

    readiness_block = _readiness_block_reason(readiness, strict_paper_probe=bool(strict_paper_probe))
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
            next_action="explain_gates" if explain_gates else "resolve_readiness_gates",
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
            current_shadow_source=current_shadow_source,
        )

    opened = _safe_call(client.open_shadow_once, strict_paper_probe=bool(strict_paper_probe))
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
            current_shadow_source=current_shadow_source,
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
            current_shadow_source=current_shadow_source,
            next_action="wait_and_recheck",
        )
    open_contract_error = _open_response_contract_error(opened)
    if open_contract_error:
        return _result(
            "stopped_by_invalid_open_response",
            cycle_number,
            stats,
            db_state=_public_db(db),
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            open_result=opened,
            stop_reason=open_contract_error,
            current_shadow_id="",
            current_shadow_source="",
            paper_shadow_created=False,
            open_shadow_count=0,
            next_action="recheck_open_shadow_contract",
            terminal=True,
        )
    stats = {
        **stats,
        "trades_opened": int(stats.get("trades_opened") or 0) + 1,
        "session_trades_opened": int(stats.get("session_trades_opened") or 0) + 1,
        "last_shadow_trade_id": shadow_id,
        "current_open_shadow_id": shadow_id,
        "current_shadow_id": shadow_id,
    }
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
        current_shadow_source="opened_this_cycle",
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
    current_shadow_source: str = "",
    dry_run: bool,
    paper_only_confirmed: bool,
    exit_policy: str = "default",
    time_stop_bars: int = 1,
    max_hold_minutes: float | None = None,
    min_r_to_arm_trailing: float = 0.15,
    giveback_r: float = 0.10,
    fast_loss_cut_r: float = -0.25,
    symbol: str = SYMBOL,
    broker_symbol: str = BROKER_SYMBOL,
    timeframe: str = TIMEFRAME,
) -> dict[str, Any]:
    clean_symbol = _clean_symbol(symbol or SYMBOL)
    clean_broker_symbol = str(broker_symbol or clean_symbol)
    clean_timeframe = _clean_timeframe(timeframe or TIMEFRAME)
    should_watch = bool(monitor.get("should_watch_only")) or str(monitor.get("safety_exit_category") or "") in {"entry_block_only", "caution_watch"}
    should_close = bool(monitor.get("should_close_paper"))
    exit_reason = str(monitor.get("exit_reason") or "")
    close_allowed = should_close and exit_reason in ALLOWED_CLOSE_REASONS
    if str(monitor.get("monitor_state") or "") == "blocked_by_asset_config":
        return _result(
            "readiness_blocked",
            cycle_number,
            stats,
            db_state=_public_db(db),
            readiness=readiness,
            open_payload=open_payload,
            monitor=monitor,
            open_shadow_count=1,
            current_shadow_id=current_shadow_id,
            current_shadow_source=current_shadow_source,
            stop_reason=exit_reason or "asset_config_block",
            blocked_cycle=True,
            next_action="provide_explicit_asset_config_before_monitor_or_open",
        )
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
            current_shadow_source=current_shadow_source,
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
            current_shadow_source=current_shadow_source,
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
            current_shadow_source=current_shadow_source,
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
        fast_loss_cut_r=fast_loss_cut_r,
    )
    if _unsafe_payload([close_result]):
        return _result("stopped_by_safety", cycle_number, stats, stop_reason="close_shadow_safety_flag_detected", terminal=True, monitor=close_result)
    applied = bool(close_result.get("paper_close_applied"))
    state = "close_applied" if applied else "close_pending"
    closed_trade = _closed_trade_from_monitor(close_result, symbol=clean_symbol, broker_symbol=clean_broker_symbol, timeframe=clean_timeframe) if applied else {}
    if closed_trade:
        closed_trade.setdefault("session_id", stats.get("session_id") or "")
    stats_state = {
        **stats,
        "session_shadow_trade_ids": list(_session_trade_ids(stats)) + ([closed_trade.get("shadow_trade_id")] if closed_trade else []),
        "current_open_shadow_id": "" if applied else current_shadow_id,
    }
    stats_after = compute_xau_m15_paper_batch_stats([closed_trade] if closed_trade else [], state=stats_state, cycle_outputs=[], symbol=clean_symbol, timeframe=clean_timeframe)
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
        current_shadow_source=current_shadow_source,
        paper_close_applied=applied,
        closed_trade=closed_trade,
        next_action="recheck_gates_for_next_shadow" if applied else "retry_close_after_review",
    )


def compute_xau_m15_paper_batch_stats(
    trades: list[dict[str, Any]],
    *,
    state: dict[str, Any] | None = None,
    cycle_outputs: list[dict[str, Any]] | None = None,
    symbol: str = "",
    timeframe: str = "",
) -> dict[str, Any]:
    state_payload = state or {}
    session_id = str(state_payload.get("session_id") or "")
    session_started_at = str(state_payload.get("session_started_at") or "")
    clean_symbol = _clean_symbol(symbol or state_payload.get("symbol") or SYMBOL)
    clean_timeframe = _clean_timeframe(timeframe or state_payload.get("timeframe") or TIMEFRAME)
    all_closed = [
        dict(trade)
        for trade in trades
        if (str(trade.get("status") or "closed") == "closed" or trade.get("exit_reason"))
        and _trade_matches_symbol_timeframe(trade, clean_symbol, clean_timeframe)
    ]
    session_closed_all = _session_closed_trades(all_closed, state_payload)
    invalid_closed = [trade for trade in session_closed_all if not _valid_winrate_sample(trade)]
    closed = [trade for trade in session_closed_all if _valid_winrate_sample(trade)]
    pnls = [float(_num(trade.get("pnl")) or 0.0) for trade in closed]
    rs = [float(_num(trade.get("r_multiple")) or 0.0) for trade in closed]
    wins = len([pnl for pnl in pnls if pnl > 0])
    losses = len([pnl for pnl in pnls if pnl < 0])
    breakeven = len(closed) - wins - losses
    gross_profit = round(sum(pnl for pnl in pnls if pnl > 0), 6)
    gross_loss = round(abs(sum(pnl for pnl in pnls if pnl < 0)), 6)
    cycles = cycle_outputs or []
    trades_opened = int(_num(state_payload.get("trades_opened")) or 0)
    trades_opened = max(trades_opened, len({str(trade.get("shadow_trade_id") or "") for trade in closed if trade.get("shadow_trade_id")}))
    session_trade_ids = _session_trade_ids(state_payload)
    session_trades_opened = max(int(_num(state_payload.get("session_trades_opened")) or 0), len(session_trade_ids), len({str(trade.get("shadow_trade_id") or "") for trade in closed if trade.get("shadow_trade_id")}))
    side_stats = _side_stats(closed)
    exit_reason_counts = _exit_reason_counts(closed)
    return {
        "session_id": session_id,
        "session_started_at": session_started_at,
        "symbol": clean_symbol,
        "timeframe": clean_timeframe,
        "target_scope": "session",
        "trades_opened": trades_opened,
        "trades_closed": len(closed),
        "valid_trades_closed": len(closed),
        "invalid_samples": len(invalid_closed),
        "invalid_sample_count": len(invalid_closed),
        "invalid_winrate_sample_ids": [str(trade.get("shadow_trade_id") or "") for trade in invalid_closed if trade.get("shadow_trade_id")],
        "session_trades_opened": session_trades_opened,
        "session_trades_closed": len(closed),
        "raw_session_trades_closed": len(session_closed_all),
        "historical_closed_count": max(0, len(all_closed) - len(session_closed_all)),
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
        "exit_reason_counts": exit_reason_counts,
        "side_stats": side_stats,
        "take_profit_count": len([trade for trade in closed if trade.get("exit_reason") == "take_profit_hit"]),
        "stop_loss_count": len([trade for trade in closed if trade.get("exit_reason") == "stop_loss_hit"]),
        "timebox_exit_count": len([trade for trade in closed if trade.get("exit_reason") == "paper_timebox_exit"]),
        "fast_trailing_exit_count": len([trade for trade in closed if trade.get("exit_reason") == "paper_fast_trailing_exit"]),
        "fast_loss_cut_count": len([trade for trade in closed if trade.get("exit_reason") == "paper_fast_loss_cut"]),
        "orphan_count": len([cycle for cycle in cycles if cycle.get("anomaly_type") == "opened_shadow_missing_close_record"]),
        "trailing_exit_count": len([trade for trade in closed if trade.get("exit_reason") == "trailing_defensive_exit"]),
        "critical_safety_exit_count": len([trade for trade in closed if trade.get("safety_exit_category") == "critical_safety_exit"]),
        "watch_only_cycles": int(_num(state_payload.get("watch_only_cycles")) or 0) + len([cycle for cycle in cycles if cycle.get("watch_only_cycle")]),
        "no_action_cycles": int(_num(state_payload.get("no_action_cycles")) or 0) + len([cycle for cycle in cycles if cycle.get("no_action_cycle")]),
        "blocked_cycles": int(_num(state_payload.get("blocked_cycles")) or 0) + len([cycle for cycle in cycles if cycle.get("blocked_cycle")]),
        "last_shadow_trade_id": _last_shadow_id(closed, state),
        "current_open_shadow_id": str(state_payload.get("current_open_shadow_id") or ""),
        "current_shadow_id": str(state_payload.get("current_open_shadow_id") or ""),
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
    current_shadow_source: str = "",
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
    stats = batch_stats or {}
    current_phase = _current_phase(runner_state, ready)
    gate_summary = _gate_summary_from_readiness(ready)
    safety_violation = _unsafe_payload([db_state or {}, ready, open_payload or {}, mon, history or {}, open_result or {}, closed_trade or {}])
    return {
        "ok": True,
        "runner_state": runner_state,
        "current_phase": current_phase,
        "cycle_number": int(cycle_number),
        "symbol": stats.get("symbol") or ready.get("symbol") or "",
        "broker_symbol": ready.get("broker_symbol") or "",
        "timeframe": stats.get("timeframe") or ready.get("timeframe") or "",
        "db_state": db_state or {},
        "readiness_state": ready.get("readiness_state") or "",
        "bars_count": int(_num(ready.get("bars_count")) or 0),
        "market_active": bool(ready.get("market_active")),
        "market_active_reason": ready.get("market_active_reason") or "",
        "db_available": bool((db_state or {}).get("db_available")),
        "db_degraded": bool((db_state or {}).get("db_degraded")),
        "tables_ready": bool((db_state or {}).get("tables_ready")),
        "queue_depth": int(_num((db_state or {}).get("queue_depth")) or 0),
        "max_open_positions_total": int(_num(ready.get("max_open_positions_total")) or DEFAULT_MAX_OPEN_POSITIONS_TOTAL),
        "gate_summary": gate_summary,
        "failed_gate_names": ready.get("failed_gate_names") or ready.get("failed_gates") or [],
        "failed_gate_reasons": ready.get("failed_gate_reasons") or {},
        "risk_governor_reason": ready.get("risk_governor_reason") or "",
        "recent_edge_negative": bool(ready.get("recent_edge_negative")),
        "entry_allowed_for_paper_test": bool(ready.get("entry_allowed_for_paper_test")),
        "entry_block_type": ready.get("entry_block_type") or "",
        "strict_paper_probe": ready.get("strict_paper_probe") if isinstance(ready.get("strict_paper_probe"), dict) else {},
        "open_shadow_count": int(open_shadow_count if open_shadow_count is not None else _open_count(open_payload or {})),
        "open_count": int(open_shadow_count if open_shadow_count is not None else _open_count(open_payload or {})),
        "current_shadow_id": current_shadow_id,
        "current_shadow_source": current_shadow_source,
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
        "batch_stats": stats,
        "win_rate": stats.get("win_rate", 0.0),
        "expectancy": stats.get("expectancy", 0.0),
        "profit_factor": stats.get("profit_factor", 0.0),
        "last_closed_trade": closed_trade or {},
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
        "safety_violation": bool(safety_violation),
        **_safety(),
    }


def _terminal_cycle(runner_state: str, cycle_number: int, state: dict[str, Any], trades: list[dict[str, Any]], reason: str) -> dict[str, Any]:
    return _result(runner_state, cycle_number, compute_xau_m15_paper_batch_stats(trades, state=state), stop_reason=reason, terminal=True)


def _current_phase(runner_state: str, readiness: dict[str, Any]) -> str:
    strict = readiness.get("strict_paper_probe") if isinstance(readiness.get("strict_paper_probe"), dict) else {}
    if bool(readiness.get("recent_edge_negative")):
        if runner_state == "waiting_for_high_quality_paper_signal":
            return "adaptive_paper_cooldown"
        if bool(strict.get("strict_paper_probe_passed")):
            return "strict_paper_probe"
        return "adaptive_paper_cooldown"
    if runner_state in {"opening_shadow", "shadow_open_monitoring", "watch_only", "close_pending", "close_applied"}:
        return "paper_observation_lifecycle"
    if str(readiness.get("readiness_state") or "") == "ready_for_one_cycle_paper_observation":
        return "ready_for_paper_observation"
    return "readiness_gate_review"


def _gate_summary_from_readiness(readiness: dict[str, Any]) -> dict[str, Any]:
    summary = readiness.get("gate_summary") if isinstance(readiness.get("gate_summary"), dict) else {}
    if summary:
        return dict(summary)
    risk_reason = _readiness_risk_reason(readiness)
    return {
        "failed_gate_names": readiness.get("failed_gate_names") or readiness.get("failed_gates") or [],
        "risk_governor_reason": risk_reason,
        "recent_edge_negative": _is_recent_edge_negative(risk_reason),
    }


def _normalize_readiness_diagnostics(readiness: dict[str, Any]) -> dict[str, Any]:
    ready = dict(readiness or {})
    failed = [str(name) for name in (ready.get("failed_gate_names") or ready.get("failed_gates") or [])]
    if failed and "failed_gate_names" not in ready:
        ready["failed_gate_names"] = failed
    gates = ready.get("gates") if isinstance(ready.get("gates"), dict) else {}
    reasons = ready.get("failed_gate_reasons") if isinstance(ready.get("failed_gate_reasons"), dict) else {}
    if not reasons and gates:
        reasons = {}
        for name in failed:
            gate = gates.get(name) if isinstance(gates.get(name), dict) else {}
            reasons[name] = {"actual": gate.get("actual"), "required": gate.get("required")}
        ready["failed_gate_reasons"] = reasons
    risk_reason = _readiness_risk_reason(ready)
    recent_edge = _is_recent_edge_negative(risk_reason)
    if risk_reason and not ready.get("risk_governor_reason"):
        ready["risk_governor_reason"] = risk_reason
    if recent_edge:
        ready["recent_edge_negative"] = True
        ready.setdefault("entry_block_type", "adaptive_paper_cooldown")
        if not ready.get("entry_allowed_for_paper_test"):
            ready["entry_allowed_for_paper_test"] = False
        if str(ready.get("recommendation") or "") in {"", "resolve_observation_safety_gates", "readiness_blocked"}:
            ready["recommendation"] = "adaptive_paper_cooldown_wait_for_high_quality_paper_signal"
        strict = ready.get("strict_paper_probe") if isinstance(ready.get("strict_paper_probe"), dict) else {}
        if not strict:
            failed_strict = ["signal_direction"]
            if int(_num(ready.get("bars_count")) or 0) < 100:
                failed_strict.append("volatility_ok")
            if not bool(ready.get("runtime_context_recent")):
                failed_strict.append("runtime_context_recent")
            if not bool(ready.get("spread_available")):
                failed_strict.append("spread_ok")
            if int(_num(ready.get("open_shadow_count")) or 0) > 0:
                failed_strict.append("no_duplicate_shadow")
            ready["strict_paper_probe"] = {
                "mode": "strict_paper_probe",
                "strict_paper_probe_passed": False,
                "failed_strict_gate_names": _dedupe(failed_strict),
                "signal_direction": "",
                "runner_state_if_blocked": "waiting_for_high_quality_paper_signal",
                "legacy_readiness_normalized": True,
                "paper_only": True,
                "applies_to_real_trading": False,
                **_safety(),
            }
    readiness_contract_error = _readiness_contract_error(ready)
    if readiness_contract_error and str(ready.get("readiness_state") or "") == "ready_for_one_cycle_paper_observation":
        ready["readiness_state_original"] = "ready_for_one_cycle_paper_observation"
        ready["readiness_state"] = "blocked"
        ready["readiness_contract_warning"] = readiness_contract_error
        if str(ready.get("recommendation") or "") in {"", "ready_for_one_cycle_paper_observation", "resolve_observation_safety_gates", "readiness_blocked"}:
            ready["recommendation"] = readiness_contract_error
        failed_names = [str(name) for name in (ready.get("failed_gate_names") or [])]
        if readiness_contract_error not in failed_names:
            failed_names.append(readiness_contract_error)
        ready["failed_gate_names"] = _dedupe(failed_names)
        reasons = ready.get("failed_gate_reasons") if isinstance(ready.get("failed_gate_reasons"), dict) else {}
        reasons[readiness_contract_error] = {"actual": False, "required": True}
        ready["failed_gate_reasons"] = reasons
    ready["gate_summary"] = _gate_summary_from_readiness(ready)
    return ready


def _readiness_risk_reason(readiness: dict[str, Any]) -> str:
    direct = str(readiness.get("risk_governor_reason") or "").strip()
    if direct:
        return direct
    gates = readiness.get("gates") if isinstance(readiness.get("gates"), dict) else {}
    risk_gate = gates.get("risk_allows_observation") if isinstance(gates.get("risk_allows_observation"), dict) else {}
    actual = str(risk_gate.get("actual") or "").strip()
    if actual:
        return actual
    reasons = readiness.get("failed_gate_reasons") if isinstance(readiness.get("failed_gate_reasons"), dict) else {}
    risk_reason = reasons.get("risk_allows_observation") if isinstance(reasons.get("risk_allows_observation"), dict) else {}
    actual = str(risk_reason.get("actual") or "").strip()
    if actual:
        return actual
    return ""


def _is_recent_edge_negative(reason: object) -> bool:
    return "recent_edge_negative" in str(reason or "").casefold()


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _safe_call(fn: Callable[..., dict[str, Any]], **kwargs: Any) -> dict[str, Any]:
    try:
        result = fn(**kwargs)
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {"ok": False, "reason": type(exc).__name__, **_safety()}
    return dict(result or {"ok": False, "reason": "empty_payload", **_safety()})


def _open_response_contract_error(opened: dict[str, Any]) -> str:
    if not bool(opened.get("paper_shadow_created")):
        return ""
    if not str(opened.get("shadow_trade_id") or "").strip():
        return "open_response_missing_shadow_trade_id"
    after_count = _open_response_after_count(opened)
    if after_count is None:
        return "open_response_missing_open_count_after"
    if after_count < 1:
        return "open_response_created_without_open_shadow"
    return ""


def _open_response_after_count(opened: dict[str, Any]) -> int | None:
    for key in ("open_shadow_count_after", "merged_open_count", "open_count", "open_shadow_count"):
        if key in opened:
            value = _num(opened.get(key))
            if value is None:
                return None
            return int(value)
    return None


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
    if failed_writes_total > 0:
        dropped_total = _db_counter(db, "dropped_noncritical_writes_total")
        if dropped_total is None:
            dropped_total = _db_counter(db, "dropped_noncritical_writes")
        if (
            dropped_total != failed_writes_total
            or str(db.get("last_db_error_category") or "")
            or str(db.get("last_db_error_at") or "")
            or db.get("queue_drain_succeeded") is not True
        ):
            return "failed_write_semantics_unknown"
    return ""


def _db_counter(db: dict[str, Any], key: str) -> int | None:
    if key not in db:
        return None
    value = _num(db.get(key))
    if value is None:
        return None
    return int(value)


def _readiness_contract_error(readiness: dict[str, Any]) -> str:
    if not bool(readiness.get("candidate_found")):
        return "candidate_missing"
    if str(readiness.get("candidate_status") or "") != "paper_observation_review":
        return "candidate_not_in_review"
    for key in ("runtime_context_available", "runtime_context_recent", "tick_available", "capital_allows_observation", "risk_allows_observation", "adaptive_allows_observation"):
        if not bool(readiness.get(key)):
            return key
    min_bars = _readiness_min_bars(readiness)
    if int(_num(readiness.get("bars_count")) or 0) < min_bars:
        return f"bars_count_below_{min_bars}"
    return ""


def _adaptive_paper_wait_reason(readiness: dict[str, Any], *, strict_paper_probe: bool) -> str:
    if not bool(readiness.get("recent_edge_negative")):
        return ""
    if not strict_paper_probe:
        return "adaptive_paper_cooldown_requires_strict_paper_probe"
    strict = readiness.get("strict_paper_probe") if isinstance(readiness.get("strict_paper_probe"), dict) else {}
    if bool(strict.get("strict_paper_probe_passed")):
        return ""
    failed = ",".join(str(name) for name in strict.get("failed_strict_gate_names") or []) or "strict_paper_probe"
    return f"adaptive_paper_cooldown_wait_for_high_quality_paper_signal:{failed}"


def _readiness_block_reason(readiness: dict[str, Any], *, strict_paper_probe: bool = False) -> str:
    strict = readiness.get("strict_paper_probe") if isinstance(readiness.get("strict_paper_probe"), dict) else {}
    if strict_paper_probe and bool(readiness.get("recent_edge_negative")) and bool(strict.get("strict_paper_probe_passed")):
        return ""
    if str(readiness.get("readiness_state") or "") != "ready_for_one_cycle_paper_observation":
        return str(readiness.get("recommendation") or "readiness_blocked")
    if not bool(readiness.get("candidate_found")):
        return "candidate_missing"
    if str(readiness.get("candidate_status") or "") != "paper_observation_review":
        return "candidate_not_in_review"
    for key in ("runtime_context_available", "runtime_context_recent", "tick_available", "capital_allows_observation", "risk_allows_observation", "adaptive_allows_observation"):
        if key == "risk_allows_observation" and strict_paper_probe and bool(readiness.get("recent_edge_negative")):
            continue
        if not bool(readiness.get(key)):
            return key
    min_bars = _readiness_min_bars(readiness)
    if int(_num(readiness.get("bars_count")) or 0) < min_bars:
        return f"bars_count_below_{min_bars}"
    return ""


def _readiness_min_bars(readiness: dict[str, Any]) -> int:
    activity = readiness.get("market_activity") if isinstance(readiness.get("market_activity"), dict) else {}
    configured = int(_num(activity.get("min_bars")) or 0)
    if configured > 0:
        return configured
    if str(readiness.get("status") or "").startswith("multi_asset_"):
        return 50
    return 100


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
        "valid_winrate_sample": row.get("valid_winrate_sample") if "valid_winrate_sample" in row else None,
        "invalid_winrate_sample": bool(row.get("invalid_winrate_sample")),
        "invalid_sample_reason": row.get("invalid_sample_reason") or "",
        "sample_valid": row.get("sample_valid") if "sample_valid" in row else None,
        "invalid_reason": row.get("invalid_reason") or row.get("invalid_sample_reason") or "",
        "metric_exclusion_reason": row.get("metric_exclusion_reason") or "",
        "market_active": row.get("market_active"),
        "market_active_at_entry": row.get("market_active_at_entry"),
        "market_active_at_exit": row.get("market_active_at_exit"),
        "market_inactive_or_frozen": bool(row.get("market_inactive_or_frozen")),
        "frozen_market_detected": bool(row.get("frozen_market_detected")),
        "no_price_movement": bool(row.get("no_price_movement")),
        "price_movement_observed": bool(row.get("price_movement_observed")),
        "price_source": row.get("price_source") or "",
        "status": "closed",
        **_safety(),
    }


def _unsafe_payload(payloads: list[dict[str, Any]]) -> bool:
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


def _open_count(payload: dict[str, Any]) -> int:
    if "merged_open_count" in payload:
        return int(_num(payload.get("merged_open_count")) or 0)
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


def _current_shadow_source(open_payload: dict[str, Any], monitor: dict[str, Any]) -> str:
    return str(monitor.get("shadow_source") or open_payload.get("open_source") or "")


def _closed_trade_from_step(step: dict[str, Any]) -> dict[str, Any]:
    trade = step.get("closed_trade") if isinstance(step.get("closed_trade"), dict) else {}
    return dict(trade) if trade else {}


def _closed_trade_from_monitor(
    monitor: dict[str, Any],
    *,
    symbol: str = SYMBOL,
    broker_symbol: str = BROKER_SYMBOL,
    timeframe: str = TIMEFRAME,
) -> dict[str, Any]:
    shadow_id = str(monitor.get("shadow_trade_id") or "")
    if not shadow_id:
        return {}
    trade = {
        "shadow_trade_id": shadow_id,
        "symbol": _clean_symbol(symbol),
        "broker_symbol": broker_symbol or _clean_symbol(symbol),
        "timeframe": _clean_timeframe(timeframe),
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
        "market_active": monitor.get("market_active"),
        "market_inactive_or_frozen": bool(monitor.get("market_inactive_or_frozen")),
        "no_price_movement": bool(monitor.get("no_price_movement")),
        "price_source": monitor.get("price_source") or monitor.get("current_price_source") or "",
        "status": "closed",
        **_safety(),
    }
    trade.update(_winrate_sample_flags(trade))
    return trade


def _merge_step_state(state: dict[str, Any], step: dict[str, Any]) -> None:
    state["schema_version"] = STATE_SCHEMA_VERSION
    state["runner_version"] = RUNNER_VERSION
    state.setdefault("session_id", f"xau-m15-session-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}")
    state.setdefault("session_started_at", _now())
    state["target_scope"] = "session"
    state["updated_at"] = _now()
    state["cycles_completed"] = int(_num(state.get("cycles_completed")) or 0) + 1
    if step.get("paper_shadow_created"):
        state["trades_opened"] = int(_num(state.get("trades_opened")) or 0) + 1
        state["session_trades_opened"] = int(_num(state.get("session_trades_opened")) or 0) + 1
        state["current_open_shadow_id"] = step.get("shadow_trade_id") or ""
        state["last_shadow_trade_id"] = step.get("shadow_trade_id") or ""
        _append_session_shadow_id(state, step.get("shadow_trade_id"))
    if step.get("paper_close_applied"):
        state["current_open_shadow_id"] = ""
        state["pending_reconciliation_shadow_id"] = ""
        state["session_trades_closed"] = int(_num(state.get("session_trades_closed")) or 0) + 1
        if step.get("shadow_trade_id"):
            state["last_shadow_trade_id"] = step.get("shadow_trade_id")
            _append_session_shadow_id(state, step.get("shadow_trade_id"))
    if step.get("runner_state") == "reconciled_closed_shadow":
        state["current_open_shadow_id"] = ""
        state["pending_reconciliation_shadow_id"] = ""
        state["session_trades_closed"] = int(_num(state.get("session_trades_closed")) or 0) + 1
        if step.get("reconciled_shadow_trade_id"):
            state["last_shadow_trade_id"] = step.get("reconciled_shadow_trade_id")
            _append_session_shadow_id(state, step.get("reconciled_shadow_trade_id"))
    if step.get("runner_state") == "stopped_by_orphaned_shadow_missing_close_record":
        state["pending_reconciliation_shadow_id"] = step.get("orphan_shadow_trade_id") or state.get("current_open_shadow_id") or ""
    for key in ("watch_only_cycles", "no_action_cycles", "blocked_cycles"):
        if step.get(key[:-1] if key.endswith("s") else key):
            state[key] = int(_num(state.get(key)) or 0) + 1
    if step.get("anomaly"):
        anomalies = state.setdefault("anomalies", [])
        if isinstance(anomalies, list):
            anomalies.append({"at": _now(), "reason": step.get("anomaly"), "shadow_trade_id": state.get("current_open_shadow_id") or ""})


def _append_session_shadow_id(state: dict[str, Any], shadow_trade_id: object) -> None:
    shadow_id = str(shadow_trade_id or "").strip()
    if not shadow_id:
        return
    ids = state.get("session_shadow_trade_ids")
    if not isinstance(ids, list):
        ids = []
        state["session_shadow_trade_ids"] = ids
    if shadow_id not in {str(item) for item in ids}:
        ids.append(shadow_id)


def _public_state(state: dict[str, Any], stats: dict[str, Any], trades: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        **state,
        "schema_version": STATE_SCHEMA_VERSION,
        "pending_reconciliation_shadow_id": state.get("pending_reconciliation_shadow_id") or "",
        "anomalies": state.get("anomalies") if isinstance(state.get("anomalies"), list) else [],
        "trades": trades or [],
        "session_trades": _session_closed_trades(_closed_rows(trades or []), state),
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


def _closed_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if isinstance(row, dict) and (str(row.get("status") or "closed") == "closed" or row.get("exit_reason"))]


def _ensure_session(
    state: dict[str, Any],
    results: dict[str, Any],
    *,
    symbol: str = SYMBOL,
    broker_symbol: str = BROKER_SYMBOL,
    timeframe: str = TIMEFRAME,
) -> None:
    clean_symbol = _clean_symbol(symbol or state.get("symbol") or results.get("symbol") or SYMBOL)
    clean_timeframe = _clean_timeframe(timeframe or state.get("timeframe") or results.get("timeframe") or TIMEFRAME)
    session_id = str(state.get("session_id") or results.get("session_id") or "").strip()
    if not session_id:
        session_id = f"{clean_symbol.lower()}-{clean_timeframe.lower()}-session-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"
    started_at = str(state.get("session_started_at") or results.get("session_started_at") or "").strip() or _now()
    state["session_id"] = session_id
    state["session_started_at"] = started_at
    state["symbol"] = clean_symbol
    state["broker_symbol"] = broker_symbol or clean_symbol
    state["timeframe"] = clean_timeframe
    state["target_scope"] = "session"
    ids = state.get("session_shadow_trade_ids")
    state["session_shadow_trade_ids"] = [str(item) for item in ids if item] if isinstance(ids, list) else []


def _session_closed_trades(closed: list[dict[str, Any]], state: dict[str, Any]) -> list[dict[str, Any]]:
    session_id = str(state.get("session_id") or "")
    if not session_id:
        return closed
    ids = _session_trade_ids(state)
    selected: list[dict[str, Any]] = []
    for trade in closed:
        shadow_id = str(trade.get("shadow_trade_id") or "")
        if str(trade.get("session_id") or "") == session_id or (shadow_id and shadow_id in ids):
            selected.append(dict(trade))
    return selected


def _session_trade_ids(state: dict[str, Any]) -> set[str]:
    raw = state.get("session_shadow_trade_ids")
    ids = {str(item) for item in raw if item} if isinstance(raw, list) else set()
    for key in ("current_open_shadow_id", "pending_reconciliation_shadow_id", "last_shadow_trade_id"):
        value = str(state.get(key) or "").strip()
        if value:
            ids.add(value)
    return ids


def _trade_matches_symbol_timeframe(trade: dict[str, Any], symbol: str, timeframe: str) -> bool:
    trade_symbol = _clean_symbol(trade.get("symbol") or symbol)
    trade_timeframe = _clean_timeframe(trade.get("timeframe") or timeframe)
    return trade_symbol == _clean_symbol(symbol) and trade_timeframe == _clean_timeframe(timeframe)


def _valid_winrate_sample(trade: dict[str, Any]) -> bool:
    return bool(_winrate_sample_flags(trade)["valid_winrate_sample"])


def _winrate_sample_flags(trade: dict[str, Any]) -> dict[str, Any]:
    frozen = evaluate_frozen_sample(trade)
    if frozen.get("frozen_sample"):
        return {
            "sample_valid": False,
            "valid_winrate_sample": False,
            "invalid_winrate_sample": True,
            "invalid_sample_reason": frozen["invalid_reason"],
            "invalid_reason": frozen["invalid_reason"],
            "metric_exclusion_reason": frozen["metric_exclusion_reason"],
            "use_for_winrate": False,
            "use_for_optimization": False,
            "use_for_calibration": False,
            "strategy_promotion_eligible": False,
            "candidate_promotion_eligible": False,
        }
    if trade.get("valid_winrate_sample") is True:
        return {
            "sample_valid": True,
            "valid_winrate_sample": True,
            "invalid_winrate_sample": False,
            "invalid_sample_reason": "",
            "invalid_reason": "",
            "metric_exclusion_reason": "",
            "use_for_winrate": True,
            "use_for_optimization": True,
            "use_for_calibration": True,
            "strategy_promotion_eligible": True,
            "candidate_promotion_eligible": True,
        }
    if trade.get("invalid_winrate_sample") is True:
        reason = str(trade.get("invalid_sample_reason") or trade.get("invalid_reason") or "invalid_winrate_sample")
        return {
            "sample_valid": False,
            "valid_winrate_sample": False,
            "invalid_winrate_sample": True,
            "invalid_sample_reason": reason,
            "invalid_reason": reason,
            "metric_exclusion_reason": str(trade.get("metric_exclusion_reason") or "excluded_from_winrate_invalid_sample"),
            "use_for_winrate": False,
            "use_for_optimization": False,
            "use_for_calibration": False,
            "strategy_promotion_eligible": False,
            "candidate_promotion_eligible": False,
        }
    if trade.get("sample_valid") is False:
        reason = str(trade.get("invalid_reason") or trade.get("invalid_sample_reason") or "invalid_winrate_sample")
        return {
            "sample_valid": False,
            "valid_winrate_sample": False,
            "invalid_winrate_sample": True,
            "invalid_sample_reason": reason,
            "invalid_reason": reason,
            "metric_exclusion_reason": str(trade.get("metric_exclusion_reason") or "excluded_from_winrate_invalid_sample"),
            "use_for_winrate": False,
            "use_for_optimization": False,
            "use_for_calibration": False,
            "strategy_promotion_eligible": False,
            "candidate_promotion_eligible": False,
        }
    market_inactive = bool(trade.get("market_inactive_or_frozen") or trade.get("no_price_movement"))
    entry = _num(trade.get("entry_price"))
    exit_price = _num(trade.get("exit_price") or trade.get("last_price"))
    pnl = _num(trade.get("pnl")) or 0.0
    r_multiple = _num(trade.get("r_multiple")) or 0.0
    same_price = entry is not None and exit_price is not None and abs(float(entry) - float(exit_price)) <= 1e-12
    if market_inactive:
        reason = "market_inactive_or_frozen" if trade.get("market_inactive_or_frozen") else "no_price_movement"
        return {
            "sample_valid": False,
            "valid_winrate_sample": False,
            "invalid_winrate_sample": True,
            "invalid_sample_reason": reason,
            "invalid_reason": reason,
            "metric_exclusion_reason": "excluded_from_winrate_frozen_market" if reason == "market_inactive_or_frozen" else "excluded_from_winrate_no_price_movement",
            "use_for_winrate": False,
            "use_for_optimization": False,
            "use_for_calibration": False,
            "strategy_promotion_eligible": False,
            "candidate_promotion_eligible": False,
        }
    if same_price and abs(pnl) <= 1e-12 and abs(r_multiple) <= 1e-12:
        if bool(trade.get("market_active")) and str(trade.get("price_source") or "").strip():
            return {
                "sample_valid": True,
                "valid_winrate_sample": True,
                "invalid_winrate_sample": False,
                "invalid_sample_reason": "",
                "invalid_reason": "",
                "metric_exclusion_reason": "",
                "use_for_winrate": True,
                "use_for_optimization": True,
                "use_for_calibration": True,
                "strategy_promotion_eligible": True,
                "candidate_promotion_eligible": True,
            }
        return {
            "sample_valid": False,
            "valid_winrate_sample": False,
            "invalid_winrate_sample": True,
            "invalid_sample_reason": "no_price_movement",
            "invalid_reason": "no_price_movement",
            "metric_exclusion_reason": "excluded_from_winrate_no_price_movement",
            "use_for_winrate": False,
            "use_for_optimization": False,
            "use_for_calibration": False,
            "strategy_promotion_eligible": False,
            "candidate_promotion_eligible": False,
        }
    return {
        "sample_valid": True,
        "valid_winrate_sample": True,
        "invalid_winrate_sample": False,
        "invalid_sample_reason": "",
        "invalid_reason": "",
        "metric_exclusion_reason": "",
        "use_for_winrate": True,
        "use_for_optimization": True,
        "use_for_calibration": True,
        "strategy_promotion_eligible": True,
        "candidate_promotion_eligible": True,
    }


def _side_stats(closed: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "buy": _side_stat(closed, "buy"),
        "sell": _side_stat(closed, "sell"),
        "buy_count": len([trade for trade in closed if str(trade.get("side") or "").casefold() == "buy"]),
        "sell_count": len([trade for trade in closed if str(trade.get("side") or "").casefold() == "sell"]),
        "buy_win_rate": _side_stat(closed, "buy")["win_rate"],
        "sell_win_rate": _side_stat(closed, "sell")["win_rate"],
        "buy_expectancy": _side_stat(closed, "buy")["expectancy"],
        "sell_expectancy": _side_stat(closed, "sell")["expectancy"],
    }


def _side_stat(closed: list[dict[str, Any]], side: str) -> dict[str, Any]:
    rows = [trade for trade in closed if str(trade.get("side") or "").casefold() == side]
    pnls = [float(_num(trade.get("pnl")) or 0.0) for trade in rows]
    wins = len([pnl for pnl in pnls if pnl > 0])
    return {
        "count": len(rows),
        "win_rate": round((wins / len(rows)) * 100.0, 6) if rows else 0.0,
        "expectancy": round(sum(pnls) / len(rows), 6) if rows else 0.0,
    }


def _exit_reason_counts(closed: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for trade in closed:
        reason = str(trade.get("exit_reason") or "unknown")
        counts[reason] = counts.get(reason, 0) + 1
    return counts


def _public_db(db: dict[str, Any]) -> dict[str, Any]:
    return {
        "provider": db.get("provider") or "",
        "db_available": bool(db.get("db_available")),
        "db_degraded": bool(db.get("db_degraded")),
        "tables_ready": bool(db.get("tables_ready")),
        "queue_depth": int(_num(db.get("queue_depth")) or 0),
        "queued_writes": int(_num(db.get("queued_writes")) or 0),
        "failed_writes": int(_num(db.get("failed_writes")) or 0),
        "failed_writes_total": int(_num(db.get("failed_writes_total")) or _num(db.get("failed_writes")) or 0),
        "failed_writes_active": int(_num(db.get("failed_writes_active")) or 0),
        "failed_writes_unresolved": int(_num(db.get("failed_writes_unresolved")) or 0),
        "failed_writes_critical": int(_num(db.get("failed_writes_critical")) or 0),
        "failed_write_semantics_known": bool(db.get("failed_write_semantics_known")),
        "dropped_noncritical_writes_total": int(_num(db.get("dropped_noncritical_writes_total")) or _num(db.get("dropped_noncritical_writes")) or 0),
        "db_readiness_blocking_reason": db.get("db_readiness_blocking_reason") or "",
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


def _last_valid_session_closed_trade(trades: list[dict[str, Any]], state: dict[str, Any], symbol: str, timeframe: str) -> dict[str, Any]:
    closed = [
        dict(trade)
        for trade in _session_closed_trades(_closed_rows(trades), state)
        if _trade_matches_symbol_timeframe(trade, symbol, timeframe) and _valid_winrate_sample(trade)
    ]
    return dict(closed[-1]) if closed else {}


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


def _allowed_symbol_set(allowed_symbols: list[str] | tuple[str, ...] | None = None) -> set[str]:
    raw = list(allowed_symbols or [])
    return {_clean_symbol(item) for item in raw if _clean_symbol(item)}


def _asset_configs_from_inputs(
    *,
    asset_configs: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
    allowed_symbols: list[str] | tuple[str, ...] | None = None,
    default_timeframe: str = TIMEFRAME,
) -> list[dict[str, Any]]:
    if asset_configs is not None:
        return [dict(item) for item in asset_configs if isinstance(item, dict)]
    raw = list(allowed_symbols or [])
    if not raw:
        env_value = os.getenv("GENESIS_PAPER_TEST_ASSET_CONFIGS") or ""
        if env_value:
            try:
                decoded = json.loads(env_value)
                if isinstance(decoded, dict):
                    decoded = [decoded]
                if isinstance(decoded, list):
                    return [dict(item) for item in decoded if isinstance(item, dict)]
            except (TypeError, ValueError):
                return []
        return []
    return [_default_asset_config(_clean_symbol(symbol), timeframe=default_timeframe) for symbol in raw if _clean_symbol(symbol)]


def _default_asset_config(symbol: str, *, timeframe: str = TIMEFRAME) -> dict[str, Any]:
    clean_symbol = _clean_symbol(symbol)
    clean_timeframe = _clean_timeframe(timeframe)
    guard = dict(DEFAULT_MARKET_GUARD)
    guard["min_bars"] = DEFAULT_MULTI_ASSET_MIN_BARS
    return {
        "symbol": clean_symbol,
        "broker_symbol": clean_symbol if clean_symbol != SYMBOL else BROKER_SYMBOL,
        "timeframe": clean_timeframe,
        "enabled": True,
        "order_policy": "journal_only_no_broker",
        "allow_broker_orders": False,
        "allow_candidate_activation": False,
        "allow_paper_forward": False,
        "max_open_positions": DEFAULT_MAX_OPEN_POSITIONS,
        "max_open_positions_total": DEFAULT_MAX_OPEN_POSITIONS_TOTAL,
        "min_bars": DEFAULT_MULTI_ASSET_MIN_BARS,
        "market_guard": guard,
        "journal_metadata": {
            "source": "explicit_allow_symbol_cli",
            "strategy_profile": f"multi_asset_paper_test|symbol={clean_symbol}|timeframe={clean_timeframe}",
            "paper_only": True,
        },
    }


def _asset_config_for(configs: list[dict[str, Any]], symbol: str, timeframe: str) -> dict[str, Any]:
    clean_symbol = _clean_symbol(symbol)
    clean_timeframe = _clean_timeframe(timeframe)
    for config in configs:
        if _clean_symbol(config.get("symbol")) == clean_symbol and _clean_timeframe(config.get("timeframe")) == clean_timeframe:
            return dict(config)
    if configs:
        return {
            "_lookup_failed": True,
            "symbol": clean_symbol,
            "timeframe": clean_timeframe,
            "known_assets": [
                {"symbol": _clean_symbol(item.get("symbol")), "timeframe": _clean_timeframe(item.get("timeframe"))}
                for item in configs
                if isinstance(item, dict)
            ],
        }
    return {}


def _asset_config_error(config: dict[str, Any]) -> str:
    if not config:
        return "missing_explicit_asset_config_allowlist"
    if bool(config.get("_lookup_failed")):
        return "asset_not_in_explicit_asset_config_allowlist"
    if config.get("enabled") is not True:
        return "asset_config_not_enabled"
    if str(config.get("order_policy") or "") != "journal_only_no_broker":
        return "asset_config_invalid_order_policy"
    if "allow_broker_orders" not in config:
        return "asset_config_missing_allow_broker_orders"
    if bool(config.get("allow_broker_orders")):
        return "asset_config_allows_broker_orders"
    if "allow_candidate_activation" not in config:
        return "asset_config_missing_allow_candidate_activation"
    if bool(config.get("allow_candidate_activation")):
        return "asset_config_allows_candidate_activation"
    if "allow_paper_forward" not in config:
        return "asset_config_missing_allow_paper_forward"
    if bool(config.get("allow_paper_forward")):
        return "asset_config_allows_paper_forward"
    if int(_num(config.get("max_open_positions")) or 0) != DEFAULT_MAX_OPEN_POSITIONS:
        return "asset_config_invalid_max_open_positions"
    if int(_num(config.get("max_open_positions_total")) or 0) != DEFAULT_MAX_OPEN_POSITIONS_TOTAL:
        return "asset_config_invalid_max_open_positions_total"
    if bool(config.get("broker_touched")):
        return "asset_config_broker_touched_true"
    if bool(config.get("order_executed")):
        return "asset_config_order_executed_true"
    guard = config.get("market_guard") if isinstance(config.get("market_guard"), dict) else {}
    if not guard:
        return "asset_config_missing_market_guard"
    min_bars = int(_num(config.get("min_bars") or guard.get("min_bars")) or 0)
    if min_bars <= 0:
        return "asset_config_missing_min_bars"
    if not isinstance(config.get("journal_metadata"), dict):
        return "asset_config_missing_journal_metadata"
    return ""


def _asset_market_guard(config: dict[str, Any]) -> dict[str, Any]:
    guard = dict(DEFAULT_MARKET_GUARD)
    if isinstance(config.get("market_guard"), dict):
        guard.update(config.get("market_guard") or {})
    if config.get("min_bars") not in (None, ""):
        guard["min_bars"] = int(_num(config.get("min_bars")) or guard["min_bars"])
    return guard


def _asset_max_open_positions(config: dict[str, Any]) -> int:
    return int(_num(config.get("max_open_positions")) or DEFAULT_MAX_OPEN_POSITIONS)


def _asset_max_open_positions_total(config: dict[str, Any]) -> int:
    return int(_num(config.get("max_open_positions_total")) or DEFAULT_MAX_OPEN_POSITIONS_TOTAL)


def _asset_journal_metadata(config: dict[str, Any]) -> dict[str, Any]:
    metadata = config.get("journal_metadata") if isinstance(config.get("journal_metadata"), dict) else {}
    return dict(metadata or {})


def _asset_config_readiness_blocked(symbol: str, broker_symbol: str, timeframe: str, reason: str, config: dict[str, Any]) -> dict[str, Any]:
    clean_symbol = _clean_symbol(symbol)
    clean_timeframe = _clean_timeframe(timeframe)
    unsafe_policy = reason in UNSAFE_POLICY_BLOCK_REASONS
    return {
        "ok": True,
        "status": "asset_config_readiness_blocked",
        "symbol": clean_symbol,
        "broker_symbol": broker_symbol or clean_symbol,
        "timeframe": clean_timeframe,
        "candidate_found": True,
        "candidate_status": "paper_observation_review",
        "candidate_profile": f"multi_asset_paper_test|symbol={clean_symbol}|timeframe={clean_timeframe}",
        "asset_config": _public_asset_config(config),
        "readiness_state": "blocked_unsafe_order_policy" if unsafe_policy else "blocked",
        "recommendation": reason,
        "failed_gates": [reason],
        "failed_gate_names": [reason],
        "failed_gate_reasons": {reason: {"actual": _public_asset_config(config), "required": "explicit enabled paper-only asset config"}},
        "entry_allowed_for_paper_test": False,
        "entry_block_type": "unsafe_order_policy" if unsafe_policy else "asset_config_block",
        "runtime_context_available": False,
        "runtime_context_recent": False,
        "tick_available": False,
        "bars_count": 0,
        "open_count": 0,
        "open_shadow_count": 0,
        "max_open_positions": DEFAULT_MAX_OPEN_POSITIONS,
        "max_open_positions_total": DEFAULT_MAX_OPEN_POSITIONS_TOTAL,
        "capital_state": "not_evaluated",
        "capital_reason": reason,
        "capital_allows_observation": False,
        "adaptive_allows_observation": True,
        "risk_allows_observation": False,
        "risk_governor_reason": reason,
        "safety_violation": reason in SAFETY_VIOLATION_POLICY_REASONS,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _public_asset_config(config: dict[str, Any]) -> dict[str, Any]:
    if not config:
        return {}
    return {
        "symbol": _clean_symbol(config.get("symbol")),
        "broker_symbol": config.get("broker_symbol") or "",
        "timeframe": _clean_timeframe(config.get("timeframe")),
        "enabled": bool(config.get("enabled")),
        "order_policy": config.get("order_policy") or "",
        "allow_broker_orders": bool(config.get("allow_broker_orders")),
        "allow_candidate_activation": bool(config.get("allow_candidate_activation")),
        "allow_paper_forward": bool(config.get("allow_paper_forward")),
        "max_open_positions": int(_num(config.get("max_open_positions")) or 0),
        "max_open_positions_total": int(_num(config.get("max_open_positions_total")) or 0),
        "min_bars": int(_num(config.get("min_bars") or (config.get("market_guard") or {}).get("min_bars")) or 0) if isinstance(config.get("market_guard") or {}, dict) else int(_num(config.get("min_bars")) or 0),
        "journal_metadata": _asset_journal_metadata(config),
    }


def _apply_asset_config_to_readiness(readiness: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    reason = _asset_config_error(config)
    if reason:
        return _asset_config_readiness_blocked(readiness.get("symbol") or config.get("symbol") or SYMBOL, readiness.get("broker_symbol") or config.get("broker_symbol") or BROKER_SYMBOL, readiness.get("timeframe") or config.get("timeframe") or TIMEFRAME, reason, config)
    guard = _asset_market_guard(config)
    min_bars = int(_num(guard.get("min_bars")) or 0)
    bars_count = int(_num(readiness.get("bars_count")) or 0)
    failed = list(readiness.get("failed_gate_names") or readiness.get("failed_gates") or [])
    if min_bars > 0 and bars_count < min_bars and "asset_config_min_bars" not in failed:
        failed.append("asset_config_min_bars")
    blocked = bool(failed) or str(readiness.get("readiness_state") or "") != "ready_for_one_cycle_paper_observation"
    patched = {
        **readiness,
        "asset_config": _public_asset_config(config),
        "market_guard": guard,
        "journal_metadata": _asset_journal_metadata(config),
        "max_open_positions": _asset_max_open_positions(config),
        "max_open_positions_total": _asset_max_open_positions_total(config),
        "failed_gate_names": failed,
        "failed_gates": failed,
        "entry_allowed_for_paper_test": bool(readiness.get("entry_allowed_for_paper_test")) and not blocked,
    }
    if "asset_config_min_bars" in failed:
        patched["readiness_state"] = "blocked"
        patched["recommendation"] = "asset_config_min_bars"
        patched["entry_block_type"] = "asset_config_block"
    return patched


def _generic_capital_protection_state(db: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    open_trade = snapshot.get("open_shadow_trade") if isinstance(snapshot.get("open_shadow_trade"), dict) else {}
    open_trades = [dict(open_trade)] if open_trade else []
    try:
        result = run_capital_protection_governor(
            open_trades=open_trades,
            closed_trades=[],
            persistent_status=db,
            runtime_snapshot=snapshot,
            load_shadow_snapshot=False,
            load_persistent=False,
            persist_events=False,
        )
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {
            "ok": False,
            "status": "capital_protection_governor_unavailable",
            "reason": type(exc).__name__,
            "capital_state": "unknown",
            "safe_to_trade": False,
            **_safety(),
        }
    if not isinstance(result, dict):
        return {
            "ok": False,
            "status": "capital_protection_governor_invalid_payload",
            "reason": "non_dict_capital_protection_result",
            "capital_state": "unknown",
            "safe_to_trade": False,
            **_safety(),
        }
    return dict(result)


def _generic_capital_allows_observation(capital: dict[str, Any]) -> bool:
    if not isinstance(capital, dict) or capital.get("ok") is False:
        return False
    state = str(capital.get("capital_state") or "").casefold()
    return state == "normal" and bool(capital.get("safe_to_trade"))


def _generic_capital_block_reason(capital: dict[str, Any]) -> str:
    if not isinstance(capital, dict):
        return "capital_protection_unavailable"
    if capital.get("ok") is False:
        return str(capital.get("reason") or capital.get("status") or "capital_protection_unavailable")
    state = str(capital.get("capital_state") or "").casefold()
    if state != "normal":
        return f"capital_state_{state or 'missing'}"
    if not bool(capital.get("safe_to_trade")):
        return str(capital.get("reason") or "capital_protection_not_safe_to_trade")
    return ""


def _generic_multi_asset_shadow_once(
    symbol: str,
    broker_symbol: str,
    timeframe: str,
    *,
    asset_config: dict[str, Any] | None = None,
    strict_paper_probe: bool = False,
    store: Any | None = None,
    db_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    clean_symbol = _clean_symbol(symbol)
    clean_timeframe = _clean_timeframe(timeframe)
    clean_broker_symbol = str(broker_symbol or clean_symbol)
    config = dict(asset_config or {})
    config_error = _asset_config_error(config)
    if config_error:
        return _generic_shadow_once_rejected(clean_symbol, clean_broker_symbol, clean_timeframe, reason=config_error, readiness=_asset_config_readiness_blocked(clean_symbol, clean_broker_symbol, clean_timeframe, config_error, config))
    if clean_timeframe != "M15":
        return _generic_shadow_once_rejected(clean_symbol, clean_broker_symbol, clean_timeframe, reason="timeframe_not_allowed_for_multi_asset_paper_test")

    readiness = _generic_multi_asset_readiness(clean_symbol, clean_broker_symbol, clean_timeframe, db_state=db_state, asset_config=config)
    activity = readiness.get("market_activity") if isinstance(readiness.get("market_activity"), dict) else {}
    if str(readiness.get("readiness_state") or "") != "ready_for_one_cycle_paper_observation":
        reason = str(readiness.get("recommendation") or "readiness_blocked")
        return _generic_shadow_once_rejected(clean_symbol, clean_broker_symbol, clean_timeframe, reason=f"blocked_by_readiness_gate:{reason}", readiness=readiness)
    if not bool(activity.get("market_active")):
        return _generic_shadow_once_rejected(clean_symbol, clean_broker_symbol, clean_timeframe, reason=str(activity.get("reason") or "market_inactive_or_frozen"), readiness=readiness)

    snapshot = _snapshot_for_symbol(clean_symbol, clean_broker_symbol, clean_timeframe)
    signal = _generic_multi_asset_signal(readiness, snapshot)
    if not signal.get("can_open"):
        return _generic_shadow_once_rejected(clean_symbol, clean_broker_symbol, clean_timeframe, reason="no_trade_signal", readiness=readiness, signal=signal)

    trade = _build_generic_multi_asset_shadow_trade(readiness, signal=signal, asset_config=config)
    persist_result = _persist_generic_shadow_trade(store, trade)
    if not bool(persist_result.get("ok")):
        retained = bool(persist_result.get("queued") or persist_result.get("critical_persistence_failed") or persist_result.get("schema_missing_write_freeze"))
        return {
            "ok": False,
            "status": "multi_asset_paper_shadow_once_open_persistence_failed",
            "mode": "paper_shadow_once",
            "decision": "NO_TRADE",
            "reason": "open_persistence_failed",
            "symbol": clean_symbol,
            "broker_symbol": clean_broker_symbol,
            "timeframe": clean_timeframe,
            "candidate_profile": trade.get("strategy_profile") or "",
            "readiness": readiness,
            "paper_shadow_created": False,
            "shadow_trade_id": trade.get("shadow_trade_id") or "",
            "shadow_trade": trade,
            "persistent_shadow_write": persist_result,
            "persistent_shadow_write_ok": False,
            "open_persistence_failed": True,
            "open_write_retained_critical": retained,
            "open_shadow_count_after": 0,
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
            "applies_to_real_trading": False,
            **_safety(),
        }

    update_open_shadow_trade(clean_symbol, trade, timeframe=clean_timeframe)
    return {
        "ok": True,
        "status": "multi_asset_paper_shadow_once_created",
        "mode": "paper_shadow_once",
        "decision": "NO_TRADE",
        "reason": "multi_asset_paper_observation_shadow_once_created",
        "symbol": clean_symbol,
        "broker_symbol": clean_broker_symbol,
        "timeframe": clean_timeframe,
        "candidate_profile": trade.get("strategy_profile") or "",
        "paper_shadow_created": True,
        "shadow_trade_id": trade.get("shadow_trade_id") or "",
        "side": trade.get("side") or "",
        "signal_direction": trade.get("signal_direction") or "",
        "entry_reason": trade.get("entry_reason") or "",
        "shadow_trade": trade,
        "readiness": readiness,
        "market_activity": activity,
        "persistent_shadow_write": persist_result,
        "persistent_shadow_write_ok": bool(persist_result.get("ok")),
        "open_shadow_count_after": 1,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _generic_multi_asset_monitor(
    symbol: str,
    broker_symbol: str,
    timeframe: str,
    *,
    apply_paper_close: bool = False,
    exit_policy: str = "default",
    time_stop_bars: int = 1,
    max_hold_minutes: float | None = None,
    min_r_to_arm_trailing: float = 0.15,
    giveback_r: float = 0.10,
    fast_loss_cut_r: float = -0.25,
    store: Any | None = None,
    asset_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    clean_symbol = _clean_symbol(symbol)
    clean_timeframe = _clean_timeframe(timeframe)
    clean_broker_symbol = str(broker_symbol or clean_symbol)
    snapshot = _snapshot_for_symbol(clean_symbol, clean_broker_symbol, clean_timeframe)
    open_trade = snapshot.get("open_shadow_trade") if isinstance(snapshot.get("open_shadow_trade"), dict) else {}
    if not open_trade:
        return _generic_monitor_result(
            clean_symbol,
            clean_broker_symbol,
            clean_timeframe,
            monitor_state="no_action",
            open_shadow_count=0,
            exit_signal=False,
            exit_reason="no_open_shadow",
            paper_close_applied=False,
            shadow_status_after="none",
        )

    metrics = _generic_trade_metrics(open_trade, snapshot)
    config = dict(asset_config or {})
    guard = _asset_market_guard(config) if not _asset_config_error(config) else dict(DEFAULT_MARKET_GUARD)
    activity = _market_activity_from_snapshot(snapshot, guard=guard)
    exit_signal, exit_reason = _generic_exit_decision(
        metrics,
        exit_policy=exit_policy,
        time_stop_bars=time_stop_bars,
        max_hold_minutes=max_hold_minutes,
        min_r_to_arm_trailing=min_r_to_arm_trailing,
        giveback_r=giveback_r,
        fast_loss_cut_r=fast_loss_cut_r,
    )
    updated_trade = {
        **open_trade,
        "last_price": metrics["current_price"],
        "unrealized_pnl": metrics["unrealized_pnl"],
        "unrealized_pnl_pct": metrics["unrealized_pnl_pct"],
        "r_multiple": metrics["r_multiple"],
        "bars_since_entry": metrics["bars_since_entry"],
        "updated_at": _now(),
        **_safety(),
    }
    paper_close_applied = False
    persist_result: dict[str, Any] = {"ok": True, "skipped": True}
    if apply_paper_close and exit_signal:
        closed = _generic_closed_trade(updated_trade, metrics, exit_reason, activity)
        persist_result = _persist_generic_shadow_trade(store, closed)
        if not bool(persist_result.get("ok")):
            return {
                **_generic_monitor_result(
                    clean_symbol,
                    clean_broker_symbol,
                    clean_timeframe,
                    monitor_state="close_blocked_by_persistence",
                    open_shadow_count=1,
                    exit_signal=True,
                    exit_reason=exit_reason,
                    paper_close_applied=False,
                    shadow_status_after="open",
                    trade=updated_trade,
                    metrics=metrics,
                    activity=activity,
                ),
                "persist_result": persist_result,
                "close_persistence_failed": True,
                "next_action": "drain_queue_and_retry_close",
            }
        update_open_shadow_trade(clean_symbol, None, timeframe=clean_timeframe)
        append_closed_shadow_trade(clean_symbol, closed, timeframe=clean_timeframe)
        paper_close_applied = True
        updated_trade = closed
    elif not exit_signal:
        update_open_shadow_trade(clean_symbol, updated_trade, timeframe=clean_timeframe)

    return {
        **_generic_monitor_result(
            clean_symbol,
            clean_broker_symbol,
            clean_timeframe,
            monitor_state="exit_applied" if paper_close_applied else "exit_pending" if exit_signal else "open_monitoring",
            open_shadow_count=0 if paper_close_applied else 1,
            exit_signal=bool(exit_signal),
            exit_reason=exit_reason,
            paper_close_applied=paper_close_applied,
            shadow_status_after="closed" if paper_close_applied else "open",
            trade=updated_trade,
            metrics=metrics,
            activity=activity,
        ),
        "persist_result": persist_result,
        "close_persistence_failed": False,
    }


def _generic_multi_asset_readiness(
    symbol: str,
    broker_symbol: str,
    timeframe: str,
    *,
    db_state: dict[str, Any] | None = None,
    asset_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    clean_symbol = _clean_symbol(symbol)
    clean_timeframe = _clean_timeframe(timeframe)
    config = dict(asset_config or {})
    config_error = _asset_config_error(config)
    if config_error:
        return _asset_config_readiness_blocked(clean_symbol, broker_symbol, clean_timeframe, config_error, config)
    guard = _asset_market_guard(config)
    min_bars = int(_num(guard.get("min_bars")) or 0)
    snapshot = _snapshot_for_symbol(clean_symbol, broker_symbol, clean_timeframe)
    runtime = run_runtime_context_diagnostics(symbol=clean_symbol, timeframe=clean_timeframe, snapshot=snapshot, generic_snapshot={})
    db = dict(db_state) if db_state is not None else persistent_intelligence_status(write_test_event=False)
    tick = snapshot.get("last_tick") if isinstance(snapshot.get("last_tick"), dict) else {}
    capital = _generic_capital_protection_state(db, snapshot)
    capital_allowed = _generic_capital_allows_observation(capital)
    capital_reason = _generic_capital_block_reason(capital)
    try:
        risk = assess_runtime_risk(clean_symbol, timeframe=clean_timeframe, tick=tick)
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        risk = {"allowed": False, "reason": type(exc).__name__, "risk_state": "unknown", **_safety()}
    activity = _market_activity_from_snapshot(snapshot, guard=guard)
    failed: list[str] = []
    db_block = _db_block_reason(db)
    if db_block:
        failed.append(db_block)
    if not bool(runtime.get("runtime_snapshot_available")):
        failed.append("runtime_context_available")
    if not bool(runtime.get("runtime_snapshot_recent")):
        failed.append("runtime_context_recent")
    if int(_num(runtime.get("bars_count")) or 0) < min_bars:
        failed.append(f"bars_count_below_{min_bars}")
    if not bool(runtime.get("latest_tick")):
        failed.append("latest_tick_available")
    market_reason = str(activity.get("reason") or "")
    if not bool(activity.get("market_active")) and market_reason and market_reason not in failed:
        failed.append(market_reason)
    if not capital_allowed:
        failed.append("capital_allows_observation")
    if not bool(risk.get("allowed", risk.get("risk_governor_allowed", False))):
        failed.append("risk_allows_observation")
    ready = not failed
    readiness_state = "ready_for_one_cycle_paper_observation" if ready else "blocked_db_not_clean" if db_block else "blocked_capital_protection" if not capital_allowed else "blocked_market_inactive" if market_reason else "blocked"
    recommendation = "ready_for_one_cycle_paper_observation" if ready else db_block or capital_reason or market_reason or "resolve_multi_asset_observation_gates"
    failed_gate_reasons: dict[str, dict[str, Any]] = {}
    if not capital_allowed:
        failed_gate_reasons["capital_allows_observation"] = {
            "actual": capital_reason or capital.get("capital_state") or "unknown",
            "required": "capital_state=normal,safe_to_trade=true",
        }
    return {
        "ok": True,
        "status": "multi_asset_paper_observation_readiness_ready",
        "readiness_version": "2026-07-04.multi_asset_paper_observation_readiness.v1",
        "candidate_found": True,
        "candidate_status": "paper_observation_review",
        "candidate_profile": f"multi_asset_paper_test|symbol={clean_symbol}|timeframe={clean_timeframe}",
        "asset_config": _public_asset_config(config),
        "market_guard": guard,
        "journal_metadata": _asset_journal_metadata(config),
        "max_open_positions": _asset_max_open_positions(config),
        "max_open_positions_total": _asset_max_open_positions_total(config),
        "symbol": clean_symbol,
        "broker_symbol": broker_symbol or clean_symbol,
        "timeframe": clean_timeframe,
        "db_state": db,
        "runtime_context_available": bool(runtime.get("runtime_snapshot_available")),
        "runtime_context_recent": bool(runtime.get("runtime_snapshot_recent")),
        "runtime_snapshot_context": runtime.get("runtime_snapshot_context") or "",
        "bars_count": int(_num(runtime.get("bars_count")) or 0),
        "tick_available": bool(runtime.get("latest_tick")),
        "latest_tick_at": runtime.get("last_tick_at") or "",
        "latest_bars_at": runtime.get("bars_last_at") or "",
        "market_active": bool(activity["market_active"]),
        "market_active_reason": market_reason,
        "price_moved_recently": bool(activity["price_moved_recently"]),
        "spread_valid": bool(activity["spread_valid"]),
        "quote_valid": bool(activity.get("quote_valid", activity.get("current_price_valid"))),
        "current_price": activity["current_price"],
        "current_price_valid": bool(activity["current_price_valid"]),
        "market_activity": activity,
        "capital_state": capital.get("capital_state") or "",
        "capital_reason": capital_reason,
        "capital_protection": capital,
        "capital_allows_observation": capital_allowed,
        "adaptive_allows_observation": True,
        "risk_allows_observation": bool(risk.get("allowed", risk.get("risk_governor_allowed", False))),
        "risk_governor_reason": risk.get("reason") or risk.get("risk_governor_reason") or "",
        "risk_state": risk.get("risk_state") or "",
        "open_shadow_count": 0,
        "open_count": 0,
        "paper_shadow_created": False,
        "readiness_state": readiness_state,
        "recommendation": recommendation,
        "failed_gates": failed,
        "failed_gate_names": failed,
        "failed_gate_reasons": failed_gate_reasons,
        "entry_allowed_for_paper_test": ready,
        "entry_block_type": "none" if ready else "db_not_clean" if db_block else "safety_gate_block",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def run_multi_asset_paper_observation_readiness(
    *,
    symbol: str,
    broker_symbol: str = "",
    timeframe: str = TIMEFRAME,
    db_state: dict[str, Any] | None = None,
    asset_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    clean_symbol = _clean_symbol(symbol)
    clean_timeframe = _clean_timeframe(timeframe)
    config = dict(asset_config) if isinstance(asset_config, dict) else _default_asset_config(clean_symbol, timeframe=clean_timeframe)
    if broker_symbol and not asset_config:
        config["broker_symbol"] = broker_symbol
    return _generic_multi_asset_readiness(
        clean_symbol,
        broker_symbol or str(config.get("broker_symbol") or clean_symbol),
        clean_timeframe,
        db_state=db_state,
        asset_config=config,
    )


def _generic_http_readiness_unavailable(symbol: str, broker_symbol: str, timeframe: str) -> dict[str, Any]:
    clean_symbol = _clean_symbol(symbol)
    clean_timeframe = _clean_timeframe(timeframe)
    return {
        "ok": True,
        "status": "multi_asset_http_readiness_not_enabled",
        "symbol": clean_symbol,
        "broker_symbol": broker_symbol or clean_symbol,
        "timeframe": clean_timeframe,
        "candidate_found": True,
        "candidate_status": "paper_observation_review",
        "readiness_state": "blocked",
        "recommendation": "use_dedicated_crypto_m15_supervisor_or_add_live_http_endpoint",
        "failed_gates": ["generic_http_readiness_endpoint_missing"],
        "failed_gate_names": ["generic_http_readiness_endpoint_missing"],
        "entry_allowed_for_paper_test": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _generic_allowlist_readiness_blocked(symbol: str, broker_symbol: str, timeframe: str, allowed_symbols: set[str]) -> dict[str, Any]:
    clean_symbol = _clean_symbol(symbol)
    clean_timeframe = _clean_timeframe(timeframe)
    return {
        "ok": True,
        "status": "multi_asset_paper_observation_readiness_ready",
        "symbol": clean_symbol,
        "broker_symbol": broker_symbol or clean_symbol,
        "timeframe": clean_timeframe,
        "candidate_found": True,
        "candidate_status": "paper_observation_review",
        "candidate_profile": f"multi_asset_paper_test|symbol={clean_symbol}|timeframe={clean_timeframe}",
        "readiness_state": "blocked",
        "recommendation": "symbol_not_in_paper_test_allowlist",
        "failed_gates": ["symbol_not_in_paper_test_allowlist"],
        "failed_gate_names": ["symbol_not_in_paper_test_allowlist"],
        "failed_gate_reasons": {"symbol_not_in_paper_test_allowlist": {"actual": clean_symbol, "required": sorted(allowed_symbols)}},
        "entry_allowed_for_paper_test": False,
        "entry_block_type": "paper_test_allowlist_block",
        "runtime_context_available": False,
        "runtime_context_recent": False,
        "tick_available": False,
        "bars_count": 0,
        "capital_state": "not_evaluated",
        "capital_reason": "symbol_not_in_paper_test_allowlist",
        "capital_allows_observation": False,
        "adaptive_allows_observation": True,
        "risk_allows_observation": False,
        "risk_governor_reason": "symbol_not_in_paper_test_allowlist",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _generic_shadow_once_rejected(
    symbol: str,
    broker_symbol: str,
    timeframe: str,
    *,
    reason: str,
    readiness: dict[str, Any] | None = None,
    signal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    clean_symbol = _clean_symbol(symbol)
    clean_timeframe = _clean_timeframe(timeframe)
    return {
        "ok": False,
        "status": "multi_asset_paper_shadow_once_rejected",
        "decision": "NO_TRADE",
        "reason": reason,
        "symbol": clean_symbol,
        "broker_symbol": broker_symbol or clean_symbol,
        "timeframe": clean_timeframe,
        "paper_shadow_created": False,
        "shadow_trade_id": "",
        "open_count": 0,
        "open_shadow_count": 0,
        "open_shadow_count_after": 0,
        "readiness": readiness or {},
        "signal": signal or {},
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        "safety_violation": bool((readiness or {}).get("safety_violation") or (signal or {}).get("safety_violation")),
        **_safety(),
    }


def _generic_multi_asset_signal(readiness: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    bars = snapshot.get("ohlc_recent") if isinstance(snapshot.get("ohlc_recent"), list) else []
    if not bars and isinstance(snapshot.get("bars"), list):
        bars = snapshot.get("bars") or []
    if not bars and isinstance(snapshot.get("recent_bars"), list):
        bars = snapshot.get("recent_bars") or []
    if not bars and isinstance(snapshot.get("bars"), list):
        bars = snapshot.get("bars") or []
    if not bars and isinstance(snapshot.get("recent_bars"), list):
        bars = snapshot.get("recent_bars") or []
    closes: list[float] = []
    for row in bars[-5:]:
        if isinstance(row, dict):
            value = _num(row.get("close") or row.get("c"))
            if value is not None:
                closes.append(float(value))
    if len(closes) < 3:
        return {"can_open": False, "signal_direction": "", "signal_source": "", "invalidation_reason": "insufficient_direction_context", **_safety()}
    spread = _num((readiness.get("market_activity") or {}).get("spread") if isinstance(readiness.get("market_activity"), dict) else 0.0) or 0.0
    delta = closes[-1] - closes[-3]
    min_move = max(abs(closes[-3]) * 0.000001, spread * 0.1, 1e-12)
    if delta >= min_move:
        return {
            "can_open": True,
            "signal_direction": "buy",
            "signal_source": "multi_asset_recent_momentum",
            "entry_reason": "multi_asset_recent_momentum_up",
            "invalidation_reason": "",
            **_safety(),
        }
    if delta <= -min_move:
        return {
            "can_open": True,
            "signal_direction": "sell",
            "signal_source": "multi_asset_recent_momentum",
            "entry_reason": "multi_asset_recent_momentum_down",
            "invalidation_reason": "",
            **_safety(),
        }
    return {"can_open": False, "signal_direction": "", "signal_source": "", "invalidation_reason": "no_clear_direction", **_safety()}


def _build_generic_multi_asset_shadow_trade(readiness: dict[str, Any], *, signal: dict[str, Any], asset_config: dict[str, Any] | None = None) -> dict[str, Any]:
    now = _now()
    clean_symbol = _clean_symbol(readiness.get("symbol"))
    clean_timeframe = _clean_timeframe(readiness.get("timeframe"))
    clean_broker_symbol = str(readiness.get("broker_symbol") or clean_symbol)
    config = dict(asset_config or {})
    metadata = _asset_journal_metadata(config)
    profile = str(metadata.get("strategy_profile") or f"multi_asset_paper_test|symbol={clean_symbol}|timeframe={clean_timeframe}")
    source = str(metadata.get("source") or "multi_asset_paper_observation_shadow_once")
    activity = readiness.get("market_activity") if isinstance(readiness.get("market_activity"), dict) else {}
    entry = _num(activity.get("current_price") or readiness.get("current_price")) or 0.0
    side = "sell" if str(signal.get("signal_direction") or "").casefold() == "sell" else "buy"
    spread = _num(activity.get("spread")) or 0.0
    stop_distance = max(abs(entry) * 0.0025, spread * 4.0 if spread else 0.0, 1e-8)
    stop = round(entry + stop_distance, 8) if side == "sell" else round(entry - stop_distance, 8)
    target = round(entry - stop_distance * 1.2, 8) if side == "sell" else round(entry + stop_distance * 1.2, 8)
    shadow_id = f"{clean_symbol.lower()}-{clean_timeframe.lower()}-paper-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"
    return {
        "shadow_trade_id": shadow_id,
        "symbol": clean_symbol,
        "broker_symbol": clean_broker_symbol,
        "timeframe": clean_timeframe,
        "candidate_profile": profile,
        "strategy_profile": profile,
        "profile": profile,
        "side": side,
        "action": side.upper(),
        "signal_direction": side,
        "entry_reason": signal.get("entry_reason") or "multi_asset_paper_observation",
        "invalidation_reason": signal.get("invalidation_reason") or "",
        "entry_price": entry,
        "entry": entry,
        "stop_loss": stop,
        "take_profit": target,
        "initial_risk": abs(entry - stop) if entry and stop else 0.0,
        "status": "open",
        "lifecycle_status": "open",
        "source": source,
        "journal_metadata": metadata,
        "asset_config": _public_asset_config(config),
        "opened_at": now,
        "updated_at": now,
        "last_price": entry,
        "unrealized_pnl": 0.0,
        "unrealized_pnl_pct": 0.0,
        "r_multiple": 0.0,
        "bars_since_entry": 0,
        "paper_observation": True,
        "paper_forward_candidate": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        "market_active": bool(activity.get("market_active")),
        "market_active_at_entry": bool(activity.get("market_active")),
        "market_active_at_exit": False,
        "market_inactive_or_frozen": bool(activity.get("market_inactive_or_frozen")),
        "frozen_market_detected": bool(activity.get("market_inactive_or_frozen") or activity.get("frozen_ohlc") or str(activity.get("reason") or "") == "frozen_ohlc"),
        "no_price_movement": bool(activity.get("no_price_movement")),
        "price_movement_observed": bool(activity.get("price_moved_recently")),
        "price_source": "runtime_tick_bar_context",
        "reason": "explicit_confirmed_multi_asset_paper_observation_shadow_once",
        **_safety(),
    }


def _generic_monitor_result(
    symbol: str,
    broker_symbol: str,
    timeframe: str,
    *,
    monitor_state: str,
    open_shadow_count: int,
    exit_signal: bool,
    exit_reason: str,
    paper_close_applied: bool,
    shadow_status_after: str,
    trade: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
    activity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(trade or {})
    m = metrics or {}
    a = activity or {}
    return {
        "ok": True,
        "status": "multi_asset_paper_shadow_monitor_ready",
        "monitor_state": monitor_state,
        "symbol": _clean_symbol(symbol),
        "broker_symbol": broker_symbol or _clean_symbol(symbol),
        "timeframe": _clean_timeframe(timeframe),
        "candidate_profile": payload.get("strategy_profile") or f"multi_asset_paper_test|symbol={_clean_symbol(symbol)}|timeframe={_clean_timeframe(timeframe)}",
        "open_shadow_count": int(open_shadow_count),
        "shadow_trade_id": payload.get("shadow_trade_id") or "",
        "side": m.get("side") or payload.get("side") or "",
        "entry_price": m.get("entry_price", payload.get("entry_price") or 0.0),
        "current_price": m.get("current_price", payload.get("last_price") or 0.0),
        "stop_loss": m.get("stop_loss", payload.get("stop_loss") or 0.0),
        "take_profit": m.get("take_profit", payload.get("take_profit") or 0.0),
        "unrealized_pnl": m.get("unrealized_pnl", payload.get("unrealized_pnl") or 0.0),
        "unrealized_pnl_pct": m.get("unrealized_pnl_pct", payload.get("unrealized_pnl_pct") or 0.0),
        "r_multiple": m.get("r_multiple", payload.get("r_multiple") or 0.0),
        "age_minutes": m.get("age_minutes", payload.get("age_minutes") or 0.0),
        "bars_since_entry": int(_num(m.get("bars_since_entry", payload.get("bars_since_entry") or 0)) or 0),
        "exit_signal": bool(exit_signal),
        "exit_reason": exit_reason,
        "should_close_paper": bool(exit_signal),
        "should_watch_only": False,
        "paper_close_applied": bool(paper_close_applied),
        "shadow_status_after": shadow_status_after,
        "market_active": bool(a.get("market_active") if a else payload.get("market_active")),
        "market_inactive_or_frozen": bool(a.get("market_inactive_or_frozen") if a else payload.get("market_inactive_or_frozen")),
        "no_price_movement": bool(a.get("no_price_movement") if a else payload.get("no_price_movement")),
        "price_source": "runtime_tick_bar_context" if m.get("current_price") else payload.get("price_source") or "",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _http_monitor_asset_mismatch(symbol: str, broker_symbol: str, timeframe: str) -> dict[str, Any]:
    clean_symbol = _clean_symbol(symbol)
    clean_timeframe = _clean_timeframe(timeframe)
    return {
        "ok": True,
        "status": "multi_asset_http_monitor_blocked",
        "monitor_state": "blocked_monitor_asset_mismatch",
        "readiness_state": "blocked_monitor_asset_mismatch",
        "recommendation": "use_asset_specific_monitor_endpoint",
        "symbol": clean_symbol,
        "broker_symbol": broker_symbol or clean_symbol,
        "timeframe": clean_timeframe,
        "candidate_profile": f"multi_asset_paper_test|symbol={clean_symbol}|timeframe={clean_timeframe}",
        "open_shadow_count": 0,
        "open_count": 0,
        "paper_shadow_created": False,
        "shadow_trade_id": "",
        "exit_signal": False,
        "exit_reason": "blocked_monitor_asset_mismatch",
        "should_close_paper": False,
        "should_watch_only": False,
        "paper_close_applied": False,
        "shadow_status_after": "unknown",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _generic_trade_metrics(open_trade: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    tick = snapshot.get("last_tick") if isinstance(snapshot.get("last_tick"), dict) else {}
    current = _num(tick.get("last") or tick.get("bid") or tick.get("ask") or snapshot.get("last_price") or open_trade.get("last_price")) or 0.0
    entry = _num(open_trade.get("entry_price") or open_trade.get("entry")) or 0.0
    side = "sell" if str(open_trade.get("side") or "").casefold() == "sell" else "buy"
    pnl = (entry - current) if side == "sell" else (current - entry)
    risk = abs(entry - (_num(open_trade.get("stop_loss")) or entry)) or _num(open_trade.get("initial_risk")) or 0.0
    bars_since_entry = int(_num(open_trade.get("bars_since_entry")) or 0) + 1
    return {
        "side": side,
        "entry_price": round(entry, 8),
        "current_price": round(current, 8),
        "stop_loss": _num(open_trade.get("stop_loss")) or 0.0,
        "take_profit": _num(open_trade.get("take_profit")) or 0.0,
        "unrealized_pnl": round(pnl, 8),
        "unrealized_pnl_pct": round((pnl / entry) * 100.0, 8) if entry else 0.0,
        "r_multiple": round(pnl / risk, 8) if risk else 0.0,
        "age_minutes": _age_minutes(open_trade.get("opened_at")),
        "bars_since_entry": bars_since_entry,
    }


def _generic_exit_decision(
    metrics: dict[str, Any],
    *,
    exit_policy: str,
    time_stop_bars: int,
    max_hold_minutes: float | None,
    min_r_to_arm_trailing: float,
    giveback_r: float,
    fast_loss_cut_r: float,
) -> tuple[bool, str]:
    side = str(metrics.get("side") or "")
    current = _num(metrics.get("current_price")) or 0.0
    stop = _num(metrics.get("stop_loss")) or 0.0
    target = _num(metrics.get("take_profit")) or 0.0
    r_value = _num(metrics.get("r_multiple")) or 0.0
    if side == "buy" and stop and current <= stop:
        return True, "stop_loss_hit"
    if side == "sell" and stop and current >= stop:
        return True, "stop_loss_hit"
    if side == "buy" and target and current >= target:
        return True, "take_profit_hit"
    if side == "sell" and target and current <= target:
        return True, "take_profit_hit"
    if _exit_policy(exit_policy) == "fast_observation":
        if r_value <= float(fast_loss_cut_r):
            return True, "paper_fast_loss_cut"
        if int(_num(metrics.get("bars_since_entry")) or 0) >= max(1, int(time_stop_bars or 1)):
            return True, "paper_timebox_exit"
        if r_value >= float(min_r_to_arm_trailing) and r_value <= float(min_r_to_arm_trailing) - float(giveback_r):
            return True, "paper_fast_trailing_exit"
    if max_hold_minutes is not None and (_num(metrics.get("age_minutes")) or 0.0) >= float(max_hold_minutes):
        return True, "paper_stagnation_exit"
    return False, ""


def _generic_closed_trade(open_trade: dict[str, Any], metrics: dict[str, Any], exit_reason: str, activity: dict[str, Any]) -> dict[str, Any]:
    closed = {
        **open_trade,
        "exit_price": metrics["current_price"],
        "last_price": metrics["current_price"],
        "pnl": metrics["unrealized_pnl"],
        "pnl_pct": metrics["unrealized_pnl_pct"],
        "r_multiple": metrics["r_multiple"],
        "bars_since_entry": metrics["bars_since_entry"],
        "status": "closed",
        "lifecycle_status": "closed",
        "closed_at": _now(),
        "exit_reason": exit_reason,
        "market_active": bool(activity.get("market_active")),
        "market_active_at_entry": bool(open_trade.get("market_active_at_entry", open_trade.get("market_active"))),
        "market_active_at_exit": bool(activity.get("market_active")),
        "market_inactive_or_frozen": bool(activity.get("market_inactive_or_frozen")),
        "frozen_market_detected": bool(activity.get("market_inactive_or_frozen") or activity.get("frozen_ohlc") or str(activity.get("reason") or "") == "frozen_ohlc"),
        "no_price_movement": bool(activity.get("no_price_movement")),
        "price_movement_observed": bool(activity.get("price_moved_recently")),
        "price_source": "runtime_tick_bar_context",
        **_safety(),
    }
    closed.update(_winrate_sample_flags(closed))
    return closed


def _persist_generic_shadow_trade(store: Any | None, trade: dict[str, Any]) -> dict[str, Any]:
    active_store = store or MT5PersistentIntelligenceStore()
    if not hasattr(active_store, "record_shadow_trade"):
        return {"ok": True, "skipped": True, "reason": "store_has_no_record_shadow_trade", **_safety()}
    try:
        result = active_store.record_shadow_trade(trade, critical=True)
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {"ok": False, "reason": type(exc).__name__, **_safety()}
    return dict(result or {"ok": False, "reason": "empty_record_shadow_trade_result", **_safety()})


def _age_minutes(opened_at: object) -> float:
    text = str(opened_at or "").strip()
    if not text:
        return 0.0
    try:
        opened = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if opened.tzinfo is None:
            opened = opened.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - opened).total_seconds() / 60.0)
    except ValueError:
        return 0.0


def _snapshot_for_symbol(symbol: str, broker_symbol: str, timeframe: str) -> dict[str, Any]:
    for alias in _dedupe([symbol, broker_symbol, str(symbol).upper(), str(broker_symbol).upper()]):
        snapshot = get_snapshot(alias, timeframe) or {}
        if snapshot:
            return dict(snapshot)
    return {}


def _market_activity_from_snapshot(snapshot: dict[str, Any], *, min_bars: int = 50, guard: dict[str, Any] | None = None) -> dict[str, Any]:
    guard_payload = dict(guard or {})
    guard_payload.setdefault("min_bars", int(_num(guard_payload.get("min_bars")) or min_bars))
    return evaluate_market_active(snapshot, guard_payload)


def _db_ok(db: dict[str, Any]) -> bool:
    return _db_block_reason(db) == ""


def _clean_symbol(value: object) -> str:
    text = str(value or "").upper().strip()
    if text.endswith(".B"):
        text = text[:-2]
    return text or SYMBOL


def _clean_timeframe(value: object) -> str:
    return str(value or "").upper().strip() or TIMEFRAME
