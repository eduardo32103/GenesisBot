from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.genesis.memory_store import MemoryStore
from services.mt5.mt5_adaptive_state import MT5AdaptiveStateEngine


SAFETY_FLAGS = {
    "broker_touched": False,
    "order_executed": False,
    "order_policy": "journal_only_no_broker",
}


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
        clean_symbol = _symbol(symbol)
        clean_timeframe = str(timeframe or "").upper().strip()
        state_payload = state or MT5AdaptiveStateEngine(memory=self.memory).compute(symbol=clean_symbol, timeframe=clean_timeframe)
        stats = profile_stats or _latest_profile_stats(self.memory, clean_symbol)
        closed = int(state_payload.get("closed_trades") or 0)
        pf = _number(state_payload.get("rolling_profit_factor")) or 0.0
        expectancy = _number(state_payload.get("rolling_expectancy")) or 0.0
        win_rate = _number(state_payload.get("rolling_win_rate")) or 0.0
        avg_win, avg_loss = _avg_win_loss(clean_symbol, self.memory)
        recommendations: list[dict[str, Any]] = []

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
            "recommendations": recommendations,
            "count": len(recommendations),
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
    rows = memory.get_mt5_events("mt5_strategy_profile_stats", symbol or None, limit=500)
    return [row.get("payload") for row in rows if isinstance(row.get("payload"), dict)]


def _avg_win_loss(symbol: str, memory: MemoryStore) -> tuple[float, float]:
    rows = memory.get_mt5_events("mt5_trade_memory", symbol or None, limit=2000)
    wins: list[float] = []
    losses: list[float] = []
    for row in rows:
        trade = row.get("payload") if isinstance(row.get("payload"), dict) else {}
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


def _number(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _symbol(value: object) -> str:
    return str(value or "").upper().strip()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
