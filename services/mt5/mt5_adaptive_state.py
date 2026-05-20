from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from services.genesis.memory_store import MemoryStore
from services.mt5.mt5_db_circuit_breaker import is_db_degraded, record_db_error, record_db_success
from services.mt5.mt5_runtime_snapshot import get_snapshot, update_adaptive_state
from services.mt5.mt5_shadow_trading import get_recent_mt5_shadow_trades_fast, is_main_metric_trade


CLOSED_STATUSES = {"win", "loss", "breakeven"}
SAFETY_FLAGS = {
    "broker_touched": False,
    "order_executed": False,
    "order_policy": "journal_only_no_broker",
}


class MT5AdaptiveStateEngine:
    """Computes journal-only adaptive state from closed MT5 trade memories."""

    def __init__(self, *, memory: MemoryStore | None = None) -> None:
        self.memory = memory or MemoryStore()

    def compute(self, *, symbol: str = "", timeframe: str = "", limit: int = 2000) -> dict[str, Any]:
        started = time.monotonic()
        clean_symbol = _symbol(symbol)
        clean_timeframe = str(timeframe or "").upper().strip()
        safe_limit = _clamp_int(limit, 2000, 1, 2000)
        if is_db_degraded():
            return _state_from_snapshot(clean_symbol, clean_timeframe, reason="db_degraded_snapshot")
        try:
            trades = [
                trade
                for trade in _latest_trade_memories(self.memory, clean_symbol, limit=safe_limit)
                if trade.get("status") in CLOSED_STATUSES
                and (not clean_timeframe or str(trade.get("timeframe") or "").upper() == clean_timeframe)
            ]
            record_db_success(_elapsed_ms(started))
        except Exception as exc:
            record_db_error(exc, duration_ms=_elapsed_ms(started))
            return _state_from_snapshot(clean_symbol, clean_timeframe, reason="adaptive_state_db_error", error=str(exc))
        data_source_used = "trade_memory"
        if not trades:
            try:
                trades = [
                    trade
                    for trade in get_recent_mt5_shadow_trades_fast(self.memory, clean_symbol, limit=min(safe_limit, 100), timeframe=clean_timeframe)
                    if trade.get("status") in CLOSED_STATUSES
                    and is_main_metric_trade(trade, query_symbol=clean_symbol)
                    and not bool(trade.get("broker_touched"))
                    and not bool(trade.get("order_executed"))
                ]
            except Exception as exc:
                record_db_error(exc, duration_ms=_elapsed_ms(started))
                return _state_from_snapshot(clean_symbol, clean_timeframe, reason="adaptive_state_shadow_db_error", error=str(exc))
            data_source_used = "shadow_trades_fast_path" if trades else "no_data"
        ordered = sorted(trades, key=lambda trade: str(trade.get("closed_at") or trade.get("updated_at") or ""))
        closed = len(ordered)
        win_streak, loss_streak = _current_streaks(ordered)
        last_10 = ordered[-10:]
        last_20 = ordered[-20:]
        rolling_win_rate = _win_rate(last_20)
        rolling_profit_factor = _profit_factor(last_20)
        rolling_expectancy = _expectancy(last_20)
        rolling_drawdown = _max_drawdown(last_20)
        regime_health = _regime_health(last_20)
        time_stop_cluster = _time_stop_cluster(last_10)
        loss_cluster = _loss_cluster(last_10)
        negative_edge = closed >= 15 and rolling_profit_factor < 1.0 and rolling_expectancy < 0
        bot_state = _bot_state(
            closed=closed,
            loss_streak=loss_streak,
            win_streak=win_streak,
            rolling_profit_factor=rolling_profit_factor,
            rolling_win_rate=rolling_win_rate,
            rolling_drawdown=rolling_drawdown,
            negative_edge=negative_edge,
            time_stop_cluster=time_stop_cluster,
            loss_cluster=loss_cluster,
        )
        recommendation_summary = _recommendation_summary(bot_state, closed, rolling_profit_factor, rolling_expectancy, time_stop_cluster, loss_cluster)
        payload = {
            "ok": True,
            "status": "mt5_adaptive_state_ready",
            "symbol": clean_symbol,
            "timeframe": clean_timeframe,
            "bot_state": bot_state,
            "closed_trades": closed,
            "current_win_streak": win_streak,
            "current_loss_streak": loss_streak,
            "last_10_win_rate": _win_rate(last_10),
            "last_20_win_rate": _win_rate(last_20),
            "rolling_win_rate": rolling_win_rate,
            "rolling_profit_factor": rolling_profit_factor,
            "rolling_expectancy": rolling_expectancy,
            "rolling_drawdown": rolling_drawdown,
            "regime_health": {
                **regime_health,
                "negative_edge": negative_edge,
                "caution": bot_state == "caution",
                "pause_new_entries": bot_state == "pause_new_entries",
                "time_stop_cluster": time_stop_cluster,
                "loss_cluster": loss_cluster,
            },
            "negative_edge": negative_edge,
            "time_stop_cluster": time_stop_cluster,
            "loss_cluster": loss_cluster,
            "recommendation_summary": recommendation_summary,
            "data_source_used": data_source_used,
            "updated_at": _now(),
            "duration_ms": _elapsed_ms(started),
            **SAFETY_FLAGS,
        }
        update_adaptive_state(clean_symbol, payload)
        return payload


def _latest_trade_memories(memory: MemoryStore, symbol: str, *, limit: int = 2000) -> list[dict[str, Any]]:
    rows = memory.get_mt5_events("mt5_trade_memory", symbol or None, limit=_clamp_int(limit, 2000, 1, 2000))
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        trade_id = str(payload.get("trade_id") or payload.get("shadow_trade_id") or row.get("created_at") or "")
        if trade_id and trade_id not in latest:
            latest[trade_id] = payload
    return list(latest.values())


def _current_streaks(trades: list[dict[str, Any]]) -> tuple[int, int]:
    win_streak = 0
    loss_streak = 0
    for trade in reversed(trades):
        status = str(trade.get("status") or "").casefold()
        if status == "win" and loss_streak == 0:
            win_streak += 1
            continue
        if status == "loss" and win_streak == 0:
            loss_streak += 1
            continue
        break
    return win_streak, loss_streak


def _win_rate(trades: list[dict[str, Any]]) -> float:
    closed = [trade for trade in trades if trade.get("status") in CLOSED_STATUSES]
    if not closed:
        return 0.0
    wins = sum(1 for trade in closed if trade.get("status") == "win")
    return round((wins / len(closed)) * 100, 2)


def _profit_factor(trades: list[dict[str, Any]]) -> float:
    gross_win = sum(max(_pnl_value(trade), 0.0) for trade in trades if trade.get("status") in CLOSED_STATUSES)
    gross_loss = abs(sum(min(_pnl_value(trade), 0.0) for trade in trades if trade.get("status") in CLOSED_STATUSES))
    if gross_win <= 0 and gross_loss <= 0:
        return 0.0
    if gross_loss <= 0:
        return round(gross_win, 4)
    return round(gross_win / gross_loss, 4)


def _expectancy(trades: list[dict[str, Any]]) -> float:
    closed = [trade for trade in trades if trade.get("status") in CLOSED_STATUSES]
    if not closed:
        return 0.0
    return round(sum(_pnl_value(trade) for trade in closed) / len(closed), 4)


def _max_drawdown(trades: list[dict[str, Any]]) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    ordered = sorted(trades, key=lambda trade: str(trade.get("closed_at") or trade.get("updated_at") or ""))
    for trade in ordered:
        if trade.get("status") not in CLOSED_STATUSES:
            continue
        equity += _pnl_value(trade)
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return round(drawdown, 4)


def _regime_health(trades: list[dict[str, Any]]) -> dict[str, Any]:
    regimes: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        regime = str(trade.get("regime") or trade.get("market_regime_label") or "unknown").strip() or "unknown"
        regimes.setdefault(regime, []).append(trade)
    return {
        regime: {
            "trades": len(items),
            "win_rate": _win_rate(items),
            "profit_factor": _profit_factor(items),
            "expectancy": _expectancy(items),
        }
        for regime, items in regimes.items()
    }


def _bot_state(
    *,
    closed: int,
    loss_streak: int,
    win_streak: int,
    rolling_profit_factor: float,
    rolling_win_rate: float,
    rolling_drawdown: float,
    negative_edge: bool = False,
    time_stop_cluster: bool = False,
    loss_cluster: bool = False,
) -> str:
    if loss_cluster:
        return "pause_new_entries"
    if rolling_drawdown >= 3.0 and closed >= 5:
        return "pause_new_entries"
    if loss_streak >= 3:
        return "drawdown_defense"
    if negative_edge or time_stop_cluster:
        return "caution"
    if closed >= 20 and rolling_profit_factor < 1.0:
        return "caution"
    if closed >= 30 and rolling_profit_factor > 1.5 and rolling_win_rate > 60:
        return "hot_streak" if win_streak >= 3 else "normal"
    if closed >= 10 and rolling_profit_factor < 1.1:
        return "recovery_mode"
    return "normal"


def _recommendation_summary(bot_state: str, closed: int, profit_factor: float, expectancy: float, time_stop_cluster: bool = False, loss_cluster: bool = False) -> str:
    if loss_cluster:
        return "Loss cluster detectado: pausar nuevas entradas paper temporalmente."
    if time_stop_cluster:
        return "Time stop cluster detectado: evitar rango/chop y exigir momentum claro."
    if closed < 30:
        return "Muestra insuficiente: seguir en paper hasta al menos 30 trades cerrados."
    if bot_state in {"drawdown_defense", "pause_new_entries"}:
        return "Bajar agresividad paper y exigir mayor confirmacion antes de nuevas entradas."
    if profit_factor > 1.3 and expectancy > 0:
        return "Perfil candidato para seguir validando; no subir riesgo real sin aprobacion."
    return "Mantener observacion y no cambiar configuracion automaticamente."


def _pnl_value(trade: dict[str, Any]) -> float:
    value = _number(trade.get("r_multiple"))
    if value is not None:
        return value
    value = _number(trade.get("pnl"))
    if value is not None:
        return value
    return _number(trade.get("pnl_pct")) or 0.0


def _time_stop_cluster(trades: list[dict[str, Any]]) -> bool:
    closed = [trade for trade in trades if trade.get("status") in CLOSED_STATUSES]
    if len(closed) < 10:
        return False
    time_stops = sum(1 for trade in closed if str(trade.get("exit_reason") or "") == "time_stop")
    return time_stops > len(closed) / 2


def _loss_cluster(trades: list[dict[str, Any]]) -> bool:
    closed = [trade for trade in trades if trade.get("status") in CLOSED_STATUSES][-5:]
    if len(closed) < 5:
        return False
    return sum(1 for trade in closed if trade.get("status") == "loss") >= 3


def _number(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp_int(value: object, default: int, minimum: int, maximum: int) -> int:
    try:
        number = int(value) if value is not None and value != "" else default
    except (TypeError, ValueError):
        number = default
    return max(minimum, min(maximum, number))


def _symbol(value: object) -> str:
    return str(value or "").upper().strip()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _elapsed_ms(started: float) -> int:
    return int(round((time.monotonic() - started) * 1000))


def _state_from_snapshot(symbol: str, timeframe: str, *, reason: str, error: str = "") -> dict[str, Any]:
    snapshot = get_snapshot(symbol) or {}
    state = snapshot.get("latest_adaptive_state") if isinstance(snapshot.get("latest_adaptive_state"), dict) else {}
    if state:
        payload = dict(state)
        payload["data_source_used"] = reason
        payload["warning"] = "using cached runtime snapshot"
        if error:
            payload["error"] = error[:240]
        payload["broker_touched"] = False
        payload["order_executed"] = False
        payload["order_policy"] = "journal_only_no_broker"
        return payload
    summary = snapshot.get("latest_performance_summary") if isinstance(snapshot.get("latest_performance_summary"), dict) else {}
    if summary:
        closed = int(summary.get("closed") or 0)
        return {
            "ok": True,
            "status": "mt5_adaptive_state_ready",
            "symbol": symbol,
            "timeframe": timeframe,
            "bot_state": "normal" if closed < 20 or (_number(summary.get("profit_factor")) or 0.0) >= 1.0 else "caution",
            "closed_trades": closed,
            "current_win_streak": 0,
            "current_loss_streak": 0,
            "last_10_win_rate": _number(summary.get("win_rate")) or 0.0,
            "last_20_win_rate": _number(summary.get("win_rate")) or 0.0,
            "rolling_win_rate": _number(summary.get("win_rate")) or 0.0,
            "rolling_profit_factor": _number(summary.get("profit_factor")) or 0.0,
            "rolling_expectancy": _number(summary.get("expectancy")) or 0.0,
            "rolling_drawdown": _number(summary.get("max_drawdown") or summary.get("drawdown")) or 0.0,
            "regime_health": {},
            "recommendation_summary": "Fast path usa performance snapshot; learning pesado aislado.",
            "data_source_used": reason,
            "error": error[:240] if error else "",
            "updated_at": _now(),
            **SAFETY_FLAGS,
        }
    return {
        "ok": True,
        "status": "no_snapshot_yet",
        "symbol": symbol,
        "timeframe": timeframe,
        "bot_state": "normal",
        "closed_trades": 0,
        "current_win_streak": 0,
        "current_loss_streak": 0,
        "last_10_win_rate": 0.0,
        "last_20_win_rate": 0.0,
        "rolling_win_rate": 0.0,
        "rolling_profit_factor": 0.0,
        "rolling_expectancy": 0.0,
        "rolling_drawdown": 0.0,
        "regime_health": {},
        "recommendation_summary": "Sin snapshot MT5 todavia; hot path protegido.",
        "data_source_used": reason,
        "error": error[:240] if error else "",
        "updated_at": _now(),
        **SAFETY_FLAGS,
    }
