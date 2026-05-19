from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from services.genesis.memory_store import MemoryStore
from services.mt5.instrument_resolver import normalize_mt5_symbol
from services.mt5.mt5_adaptive_state import MT5AdaptiveStateEngine
from services.mt5.mt5_performance import MT5Performance
from services.mt5.mt5_shadow_trading import MT5ShadowTrading, is_main_metric_trade


SAFETY_FLAGS = {
    "broker_touched": False,
    "order_executed": False,
    "order_policy": "journal_only_no_broker",
}
RECOMMENDATION_READ_LIMIT = 100


class MT5AdaptiveRecommendationEngine:
    """Creates human-approved adaptive recommendations from paper metrics."""

    def __init__(self, *, memory: MemoryStore | None = None) -> None:
        self.memory = memory or MemoryStore()

    def recommend(
        self,
        *,
        symbol: str = "",
        timeframe: str = "",
        state: dict[str, Any] | None = None,
        profile_stats: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        started = time.monotonic()
        clean_symbol = _symbol(symbol)
        clean_timeframe = str(timeframe or "").upper().strip()
        state_payload, data_source_used = _select_state_payload(self.memory, clean_symbol, clean_timeframe, state)
        stats = profile_stats or _latest_profile_stats(self.memory, clean_symbol)
        closed = int(state_payload.get("closed_trades") or 0)
        pf = _number(state_payload.get("rolling_profit_factor")) or 0.0
        expectancy = _number(state_payload.get("rolling_expectancy")) or 0.0
        win_rate = _number(state_payload.get("rolling_win_rate")) or 0.0
        avg_win, avg_loss = _avg_win_loss(clean_symbol, self.memory)
        recommendations: list[dict[str, Any]] = []

        if str(state_payload.get("bot_state") or "").casefold() == "caution" or pf < 1.0 or expectancy < 0:
            recommendations.append(
                _recommendation(
                    clean_symbol,
                    "strategy_filter",
                    "Estado caution: mantener paper, filtrar nuevas entradas y exigir mayor confirmacion.",
                    f"Estado {state_payload.get('bot_state') or 'normal'} con PF {pf} y expectancy {expectancy}; no sugerir trading real.",
                    "medium" if closed >= 30 else "low",
                )
            )
        if closed < 30:
            recommendations.append(
                _recommendation(
                    clean_symbol,
                    "sample_warning",
                    "Mantener paper exploration, no usar todavia para decidir rentabilidad.",
                    f"Muestra insuficiente: {closed} trades cerrados; Genesis exige minimo 30.",
                    "low",
                )
            )
        if closed >= 30 and pf > 1.3 and expectancy > 0:
            recommendations.append(
                _recommendation(
                    clean_symbol,
                    "strategy_filter",
                    "Seguir validando el perfil hasta 50 trades antes de promoverlo.",
                    f"PF {pf} y expectancy {expectancy} positivos con {closed} trades.",
                    "medium",
                )
            )
        if closed >= 50 and pf > 1.3 and expectancy > 0:
            recommendations.append(
                _recommendation(
                    clean_symbol,
                    "strategy_filter",
                    "Marcar el perfil como candidate_profile en memoria paper.",
                    "La muestra supera 50 trades y mantiene PF/expectancy positivos.",
                    "medium",
                )
            )
        if avg_win > 0 and abs(avg_loss) > avg_win * 2:
            recommendations.append(
                _recommendation(
                    clean_symbol,
                    "trailing_adjustment",
                    "Revisar time_stop/trailing para cortar perdidas antes.",
                    f"Perdida promedio {avg_loss} es mas de 2x la ganancia promedio {avg_win}.",
                    "medium" if closed >= 30 else "low",
                )
            )
        if int(state_payload.get("current_loss_streak") or 0) >= 3:
            recommendations.append(
                _recommendation(
                    clean_symbol,
                    "risk_adjustment",
                    "Activar defensa paper: bajar agresividad y exigir mejor score.",
                    "Racha de 3 perdidas cerradas detectada.",
                    "medium",
                )
            )
        if not recommendations:
            recommendations.append(
                _recommendation(
                    clean_symbol,
                    "risk_adjustment",
                    "Mantener configuracion paper actual y seguir midiendo.",
                    f"Estado {state_payload.get('bot_state') or 'normal'}, PF {pf}, win rate {win_rate}%.",
                    "low" if closed < 30 else "medium",
                )
            )

        return {
            "ok": True,
            "status": "mt5_adaptive_recommendations_ready",
            "symbol": clean_symbol,
            "timeframe": clean_timeframe,
            "closed_trades": closed,
            "bot_state": state_payload.get("bot_state") or "normal",
            "profile_stats_count": len(stats),
            "data_source_used": data_source_used,
            "rolling_win_rate": win_rate,
            "rolling_profit_factor": pf,
            "rolling_expectancy": expectancy,
            "rolling_drawdown": _number(state_payload.get("rolling_drawdown")) or 0.0,
            "current_win_streak": int(state_payload.get("current_win_streak") or 0),
            "current_loss_streak": int(state_payload.get("current_loss_streak") or 0),
            "recommendations": recommendations,
            "count": len(recommendations),
            "duration_ms": _elapsed_ms(started),
            "updated_at": _now(),
            **SAFETY_FLAGS,
        }


def _recommendation(symbol: str, recommendation_type: str, recommendation: str, reason: str, confidence: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "recommendation_type": recommendation_type,
        "recommendation": recommendation,
        "reason": reason,
        "confidence": confidence,
        "requires_approval": True,
        "applied": False,
        **SAFETY_FLAGS,
    }


def _latest_profile_stats(memory: MemoryStore, symbol: str) -> list[dict[str, Any]]:
    rows = memory.get_mt5_events("mt5_strategy_profile_stats", symbol or None, limit=RECOMMENDATION_READ_LIMIT)
    stats = []
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        if _matches_symbol(payload, symbol):
            stats.append(payload)
    return stats


def _avg_win_loss(symbol: str, memory: MemoryStore) -> tuple[float, float]:
    rows = memory.get_mt5_events("mt5_trade_memory", symbol or None, limit=RECOMMENDATION_READ_LIMIT)
    wins: list[float] = []
    losses: list[float] = []
    for row in rows:
        trade = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        if not _is_valid_closed_trade(trade, symbol):
            continue
        value = _number(trade.get("r_multiple"))
        if value is None:
            value = _number(trade.get("pnl")) or 0.0
        if value > 0:
            wins.append(value)
        elif value < 0:
            losses.append(value)
    avg_win = round(sum(wins) / len(wins), 4) if wins else 0.0
    avg_loss = round(sum(losses) / len(losses), 4) if losses else 0.0
    return avg_win, avg_loss


def _select_state_payload(
    memory: MemoryStore,
    symbol: str,
    timeframe: str,
    state: dict[str, Any] | None,
) -> tuple[dict[str, Any], str]:
    if state and int((state or {}).get("closed_trades") or 0) > 0:
        return state, "adaptive_state"
    persisted = _latest_adaptive_state(memory, symbol, timeframe)
    if int((persisted or {}).get("closed_trades") or 0) > 0:
        return persisted, "adaptive_state"
    fallback = _performance_state_fallback(memory, symbol, timeframe)
    if int((fallback or {}).get("closed_trades") or 0) > 0:
        return fallback, "performance_fallback"
    computed = state or MT5AdaptiveStateEngine(memory=memory).compute(symbol=symbol, timeframe=timeframe, limit=RECOMMENDATION_READ_LIMIT)
    if int((computed or {}).get("closed_trades") or 0) > 0:
        return computed, "adaptive_state_limited"
    return computed or _empty_state(symbol, timeframe), "no_data"


def _latest_adaptive_state(memory: MemoryStore, symbol: str, timeframe: str) -> dict[str, Any]:
    rows = memory.get_mt5_events("mt5_adaptive_state", symbol or None, limit=20)
    clean_timeframe = str(timeframe or "").upper().strip()
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        if not _matches_symbol(payload, symbol):
            continue
        if clean_timeframe and str(payload.get("timeframe") or "").upper() not in {"", clean_timeframe}:
            continue
        return payload
    return {}


def _performance_state_fallback(memory: MemoryStore, symbol: str, timeframe: str) -> dict[str, Any]:
    report = MT5Performance(memory=memory).report(symbol=symbol, timeframe=timeframe, limit=RECOMMENDATION_READ_LIMIT)
    summary = report.get("summary_forward_auto") if isinstance(report.get("summary_forward_auto"), dict) else {}
    if not summary or int(summary.get("closed") or 0) <= 0:
        summary = report.get("summary_auto") if isinstance(report.get("summary_auto"), dict) else {}
    closed_trades = _closed_shadow_trades(memory, symbol, timeframe)
    win_streak, loss_streak = _current_streaks(closed_trades)
    closed = int(summary.get("closed") or len(closed_trades))
    return {
        "ok": True,
        "status": "mt5_adaptive_state_from_performance",
        "symbol": _symbol(symbol),
        "timeframe": str(timeframe or "").upper().strip(),
        "bot_state": "drawdown_defense" if loss_streak >= 3 else "normal",
        "closed_trades": closed,
        "current_win_streak": win_streak,
        "current_loss_streak": loss_streak,
        "rolling_win_rate": _number(summary.get("win_rate")) or 0.0,
        "rolling_profit_factor": _number(summary.get("profit_factor")) or 0.0,
        "rolling_expectancy": _number(summary.get("expectancy")) or 0.0,
        "rolling_drawdown": _number(summary.get("max_drawdown") or summary.get("drawdown")) or 0.0,
        **SAFETY_FLAGS,
    }


def _closed_shadow_trades(memory: MemoryStore, symbol: str, timeframe: str) -> list[dict[str, Any]]:
    clean_timeframe = str(timeframe or "").upper().strip()
    trades = [
        trade
        for trade in MT5ShadowTrading(memory=memory).trades(symbol, limit=RECOMMENDATION_READ_LIMIT)
        if _is_valid_closed_trade(trade, symbol)
        and is_main_metric_trade(trade, query_symbol=symbol)
        and (not clean_timeframe or str(trade.get("timeframe") or "").upper() == clean_timeframe)
    ]
    return sorted(trades, key=lambda trade: str(trade.get("closed_at") or trade.get("updated_at") or ""))


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


def _profile_stats_from_memory(memory: MemoryStore, symbol: str) -> list[dict[str, Any]]:
    rows = memory.get_mt5_events("mt5_trade_memory", symbol or None, limit=RECOMMENDATION_READ_LIMIT)
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        trade = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        if not _is_valid_closed_trade(trade, symbol):
            continue
        key = "|".join(
            (
                _symbol(trade.get("symbol") or symbol),
                str(trade.get("timeframe") or "UNKNOWN").upper(),
                str(trade.get("strategy_profile") or "unknown"),
                str(trade.get("regime") or "unknown"),
            )
        )
        groups.setdefault(key, []).append(trade)
    return [
        {
            "profile_stat_id": key,
            "symbol": items[0].get("symbol") or _symbol(symbol),
            "timeframe": str(items[0].get("timeframe") or "").upper(),
            "strategy_profile": items[0].get("strategy_profile") or "unknown",
            "regime": items[0].get("regime") or "unknown",
            "trades": len(items),
            "wins": sum(1 for item in items if item.get("status") == "win"),
            "losses": sum(1 for item in items if item.get("status") == "loss"),
            **SAFETY_FLAGS,
        }
        for key, items in groups.items()
    ]


def _is_valid_closed_trade(trade: dict[str, Any], symbol: str) -> bool:
    if not _matches_symbol(trade, symbol):
        return False
    if normalize_mt5_symbol(trade.get("normalized_symbol") or trade.get("symbol")) != normalize_mt5_symbol(symbol):
        return False
    if normalize_mt5_symbol(symbol) == "BTCUSD":
        if str(trade.get("instrument_type") or "").casefold() != "crypto_spot":
            return False
        if not bool(trade.get("is_spot_crypto")):
            return False
    if str(trade.get("status") or "").casefold() not in {"win", "loss", "breakeven"}:
        return False
    lifecycle = str(trade.get("lifecycle_status") or "closed").casefold()
    if lifecycle not in {"", "closed"}:
        return False
    if bool(trade.get("broker_touched")) or bool(trade.get("order_executed")):
        return False
    if bool(trade.get("excluded_from_main_metrics")):
        return False
    return True


def _matches_symbol(payload: dict[str, Any], symbol: str) -> bool:
    if not symbol:
        return True
    wanted = normalize_mt5_symbol(symbol)
    candidates = {
        normalize_mt5_symbol(payload.get("symbol")),
        normalize_mt5_symbol(payload.get("normalized_symbol")),
        normalize_mt5_symbol(payload.get("original_symbol")),
    }
    return wanted in candidates


def _number(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _symbol(value: object) -> str:
    return str(value or "").upper().strip()


def _empty_state(symbol: str, timeframe: str) -> dict[str, Any]:
    return {
        "symbol": _symbol(symbol),
        "timeframe": str(timeframe or "").upper().strip(),
        "bot_state": "normal",
        "closed_trades": 0,
        "current_win_streak": 0,
        "current_loss_streak": 0,
        "rolling_win_rate": 0.0,
        "rolling_profit_factor": 0.0,
        "rolling_expectancy": 0.0,
        "rolling_drawdown": 0.0,
        **SAFETY_FLAGS,
    }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _elapsed_ms(started: float) -> int:
    return int(round((time.monotonic() - started) * 1000))
