from __future__ import annotations

import logging
import time
from typing import Any
from uuid import uuid4

from services.genesis.memory_store import MemoryStore
from services.mt5.mt5_backtester import (
    BacktestSettings,
    _decision_from_history,
    _elapsed_ms,
    _force_close,
    _load_bars,
    _metrics,
    _now,
    _open_trade,
    _reason_counts,
    _recent_loss_cluster,
    _settings,
    _safety,
    _timeframe_minutes,
    _timed_out,
    _top_reasons,
    _update_trade,
)
from services.mt5.mt5_config import MT5RuntimeConfig, get_mt5_config


_LOG = logging.getLogger("genesis.mt5.forward_replay")


class MT5ForwardReplay:
    """Accelerated paper-forward replay. It never mutates live MT5 runtime state."""

    def __init__(self, *, memory: MemoryStore | None = None, config: MT5RuntimeConfig | None = None) -> None:
        self.memory = memory
        self.config = config or get_mt5_config()

    def run(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        started = time.monotonic()
        body = dict(payload or {})
        _LOG.info("forward_replay_parse_start symbol=%s timeframe=%s", body.get("symbol") or "BTCUSD", body.get("timeframe") or "M30")
        profile = str(body.get("profile") or "quality_loose").strip().casefold() or "quality_loose"
        settings_body = {
            **body,
            "profile": profile,
            "filter_profile": str(body.get("filter_profile") or profile).strip().casefold() or profile,
            "save_results": False,
        }
        settings = _settings(settings_body, self.config)
        checkpoints = _parse_checkpoints(body.get("checkpoints"))
        warnings: list[str] = []
        errors: list[str] = []
        try:
            bars, load_warnings = _load_bars(body, settings)
            warnings.extend(load_warnings)
            bars = bars[: settings.max_bars]
            _LOG.info(
                "forward_replay_parse_done symbol=%s timeframe=%s bars_loaded=%s max_bars=%s",
                settings.symbol,
                settings.timeframe,
                len(bars),
                settings.max_bars,
            )
            if not bars:
                return _empty_forward_replay(settings, checkpoints, started, warnings + ["historical_data_not_available"])

            _LOG.info("forward_replay_run_start symbol=%s timeframe=%s bars_loaded=%s profile=%s", settings.symbol, settings.timeframe, len(bars), profile)
            trades, no_trade_count, blocked, replay_state = _simulate_forward_replay(settings, bars, started)
            _LOG.info(
                "forward_replay_run_done symbol=%s timeframe=%s trades=%s no_trade_count=%s state=%s",
                settings.symbol,
                settings.timeframe,
                len(trades),
                no_trade_count,
                replay_state.get("status") or "",
            )
            if replay_state.get("status") == "forward_replay_timeout_or_loop_guard":
                return _guard_result(settings, checkpoints, trades, no_trade_count, blocked, started, warnings, replay_state)
            summary = _metrics(trades, initial_balance=settings.initial_balance)
            degraded, degradation_reason = _early_guardrail(summary)
            if replay_state.get("degraded"):
                degraded = True
                degradation_reason = str(replay_state.get("degradation_reason") or degradation_reason)
            status = "observation_only" if degraded else "paper_forward_candidate"
            result = {
                "ok": True,
                "api_status": "mt5_forward_replay_completed",
                "status": status,
                "symbol": settings.symbol,
                "normalized_symbol": settings.normalized_symbol,
                "timeframe": settings.timeframe,
                "profile": profile,
                "mode": "paper_forward_candidate",
                "bars_loaded": len(bars),
                "max_bars": settings.max_bars,
                "total_trades": summary["total_trades"],
                "closed": summary["closed"],
                "open": summary["open"],
                "wins": summary["wins"],
                "losses": summary["losses"],
                "win_rate": summary["win_rate"],
                "profit_factor": summary["profit_factor"],
                "expectancy": summary["expectancy"],
                "net_pnl": summary["net_pnl"],
                "max_drawdown": summary["max_drawdown"],
                "avg_win": summary["avg_win"],
                "avg_loss": summary["avg_loss"],
                "rr_avg": summary["rr_avg"],
                "degraded": degraded,
                "degradation_reason": degradation_reason,
                "early_guardrail_active": True,
                "early_guardrail_min_trades": 10,
                "early_guardrail_pf_min": 0.8,
                "early_guardrail_expectancy_min": 0.0,
                "early_guardrail_win_rate_min": 35.0,
                "checkpoints": _checkpoint_payloads(checkpoints, trades, settings.initial_balance),
                "recent_trades": summary["recent_trades"],
                "exit_reason_counts": summary["exit_reason_counts"],
                "buy_win_rate": summary["buy_win_rate"],
                "sell_win_rate": summary["sell_win_rate"],
                "buy_pf": summary["buy_pf"],
                "sell_pf": summary["sell_pf"],
                "side_stats": summary["side_stats"],
                "no_trade_count": no_trade_count,
                "blocked_reasons": _top_reasons(blocked),
                "blocked_reason_counts": _reason_counts(blocked),
                "persist": bool(body.get("persist") is True),
                "saved": False,
                "live_runtime_mutated": False,
                "promoted_profile_mutated": False,
                "shadow_trades_mutated": False,
                "warnings": warnings,
                **_safety(),
                "duration_ms": _elapsed_ms(started),
                "created_at": _now(),
            }
            if body.get("persist") is True:
                result = self._persist(settings.symbol, result)
            return result
        except Exception as exc:
            errors.append(str(exc)[:500])
            _LOG.warning("forward_replay_error symbol=%s timeframe=%s error=%s", settings.symbol, settings.timeframe, str(exc)[:240])
            return {
                "ok": False,
                "api_status": "mt5_forward_replay_error",
                "status": "forward_replay_error",
                "symbol": settings.symbol,
                "normalized_symbol": settings.normalized_symbol,
                "timeframe": settings.timeframe,
                "profile": profile,
                "errors": errors,
                "warnings": warnings,
                **_safety(),
                "duration_ms": _elapsed_ms(started),
            }

    def _persist(self, symbol: str, result: dict[str, Any]) -> dict[str, Any]:
        if self.memory is None:
            return {**result, "saved": False, "warnings": [*list(result.get("warnings") or []), "persist_requested_but_memory_unavailable"]}
        try:
            event = self.memory.save_mt5_event("mt5_forward_replay_runs", symbol, result, "mt5_forward_replay", "medium")
            return {**result, "saved": True, "event": event}
        except Exception as exc:
            return {**result, "saved": False, "warnings": [*list(result.get("warnings") or []), f"persist_failed:{str(exc)[:160]}"]}


def _simulate_forward_replay(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    started: float,
) -> tuple[list[dict[str, Any]], int, list[str], dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    blocked: list[str] = []
    no_trade_count = 0
    open_trade: dict[str, Any] | None = None
    cooldown_until = -1
    replay_id = f"forward-replay-{settings.symbol}-{uuid4().hex[:8]}"
    state: dict[str, Any] = {"degraded": False, "degradation_reason": "", "stopped_at_closed_trades": 0}
    iterations = 0
    max_iterations = len(bars) + 5

    for index in range(1, len(bars)):
        iterations += 1
        if iterations > max_iterations:
            state = {
                "status": "forward_replay_timeout_or_loop_guard",
                "reason": "iteration_limit_exceeded",
                "iterations": iterations,
                "max_iterations": max_iterations,
                "degraded": False,
                "degradation_reason": "",
            }
            blocked.append("iteration_limit_exceeded")
            break
        if _timed_out(started, settings.timeout_seconds):
            blocked.append("timeout_guard")
            state = {
                "status": "forward_replay_timeout_or_loop_guard",
                "reason": "timeout_guard",
                "iterations": iterations,
                "max_iterations": max_iterations,
                "degraded": False,
                "degradation_reason": "",
            }
            break

        bar = bars[index]
        if open_trade:
            open_trade, closed = _update_trade(settings, open_trade, bar, index)
            if closed:
                trades.append({**closed, "source": "mt5_forward_replay", "forward_replay": True, "replay_id": replay_id})
                open_trade = None
                degraded, reason = _early_guardrail(_metrics(trades, initial_balance=settings.initial_balance))
                if degraded:
                    state = {"degraded": True, "degradation_reason": reason, "stopped_at_closed_trades": len(trades)}
                    break

        if index >= len(bars) - 1 or open_trade:
            continue
        if index < cooldown_until:
            no_trade_count += 1
            blocked.append("cooldown_active")
            continue

        history = bars[max(0, index - 80) : index]
        decision = _decision_from_history(history, settings)
        if not decision["actionable"]:
            no_trade_count += 1
            blocked.append(str(decision.get("reason") or "no_edge"))
            continue

        opened = _open_trade(settings, decision, bar, index, f"{replay_id}-{index}")
        if opened is None:
            no_trade_count += 1
            blocked.append("missing_risk_parameters")
            continue

        open_trade = {
            **opened,
            "source": "mt5_forward_replay",
            "forward_replay": True,
            "replay_id": replay_id,
            "profile": settings.filter_profile,
            "strategy_profile": settings.filter_profile,
            "auto_forward": True,
            "paper_forward_candidate": True,
            "manual_test": False,
            "excluded_from_main_metrics": False,
            **_safety(),
        }
        if _recent_loss_cluster(trades):
            cooldown_until = index + max(1, int(20 / _timeframe_minutes(settings.timeframe)))

    if open_trade and not state.get("degraded"):
        closed = _force_close(settings, open_trade, bars[-1], len(bars) - 1, "time_stop")
        trades.append({**closed, "source": "mt5_forward_replay", "forward_replay": True, "replay_id": replay_id})
        degraded, reason = _early_guardrail(_metrics(trades, initial_balance=settings.initial_balance))
        if degraded:
            state = {"degraded": True, "degradation_reason": reason, "stopped_at_closed_trades": len(trades)}

    return trades, no_trade_count, blocked, state


def _guard_result(
    settings: BacktestSettings,
    checkpoints: list[int],
    trades: list[dict[str, Any]],
    no_trade_count: int,
    blocked: list[str],
    started: float,
    warnings: list[str],
    replay_state: dict[str, Any],
) -> dict[str, Any]:
    summary = _metrics(trades, initial_balance=settings.initial_balance)
    return {
        "ok": False,
        "api_status": "mt5_forward_replay_loop_guard",
        "status": "forward_replay_timeout_or_loop_guard",
        "symbol": settings.symbol,
        "normalized_symbol": settings.normalized_symbol,
        "timeframe": settings.timeframe,
        "profile": settings.filter_profile,
        "bars_loaded": int(replay_state.get("max_iterations") or 0) - 5 if replay_state.get("max_iterations") else 0,
        "iterations": int(replay_state.get("iterations") or 0),
        "max_iterations": int(replay_state.get("max_iterations") or 0),
        "guard_reason": replay_state.get("reason") or "timeout_or_loop_guard",
        "degraded": False,
        "degradation_reason": "",
        "checkpoints": _checkpoint_payloads(checkpoints, trades, settings.initial_balance),
        "no_trade_count": no_trade_count,
        "blocked_reasons": _top_reasons(blocked),
        "blocked_reason_counts": _reason_counts(blocked),
        "warnings": [*warnings, "forward replay stopped by timeout/loop guard"],
        **summary,
        **_safety(),
        "duration_ms": _elapsed_ms(started),
        "created_at": _now(),
    }


def _early_guardrail(summary: dict[str, Any]) -> tuple[bool, str]:
    closed = int(summary.get("closed") or summary.get("closed_trades") or 0)
    if closed < 10:
        return False, ""
    pf = float(summary.get("profit_factor") or 0.0)
    expectancy = float(summary.get("expectancy") or 0.0)
    win_rate = float(summary.get("win_rate") or 0.0)
    if pf < 0.8 or expectancy < 0.0 or win_rate < 35.0:
        return True, "early_forward_underperformance"
    return False, ""


def _checkpoint_payloads(checkpoints: list[int], trades: list[dict[str, Any]], initial_balance: float) -> list[dict[str, Any]]:
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    payloads: list[dict[str, Any]] = []
    for checkpoint in checkpoints:
        subset = closed[: min(checkpoint, len(closed))]
        metrics = _metrics(subset, initial_balance=initial_balance)
        degraded, reason = _early_guardrail(metrics)
        payloads.append(
            {
                "checkpoint": checkpoint,
                "reached": len(closed) >= checkpoint,
                "closed": metrics["closed"],
                "wins": metrics["wins"],
                "losses": metrics["losses"],
                "win_rate": metrics["win_rate"],
                "profit_factor": metrics["profit_factor"],
                "expectancy": metrics["expectancy"],
                "max_drawdown": metrics["max_drawdown"],
                "degraded": degraded,
                "degradation_reason": reason,
            }
        )
    return payloads


def _parse_checkpoints(raw: Any) -> list[int]:
    if isinstance(raw, str):
        values = [part.strip() for part in raw.split(",")]
    elif isinstance(raw, list):
        values = raw
    else:
        values = [10, 25, 50, 100]
    checkpoints: list[int] = []
    for value in values:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0 and parsed not in checkpoints:
            checkpoints.append(parsed)
    return checkpoints or [10, 25, 50, 100]


def _empty_forward_replay(
    settings: BacktestSettings,
    checkpoints: list[int],
    started: float,
    warnings: list[str],
) -> dict[str, Any]:
    summary = _metrics([], initial_balance=settings.initial_balance)
    return {
        "ok": True,
        "api_status": "mt5_forward_replay_no_data",
        "status": "paper_forward_candidate",
        "symbol": settings.symbol,
        "normalized_symbol": settings.normalized_symbol,
        "timeframe": settings.timeframe,
        "profile": settings.filter_profile,
        "bars_loaded": 0,
        "degraded": False,
        "degradation_reason": "",
        "checkpoints": _checkpoint_payloads(checkpoints, [], settings.initial_balance),
        "no_trade_count": 0,
        "blocked_reasons": [],
        "warnings": warnings,
        **summary,
        **_safety(),
        "duration_ms": _elapsed_ms(started),
        "created_at": _now(),
    }
