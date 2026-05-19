from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.genesis.memory_store import MemoryStore
from services.mt5.mt5_adaptive_state import MT5AdaptiveStateEngine
from services.mt5.mt5_journal import MT5Journal
from services.mt5.mt5_performance import MT5Performance


SAFETY_FLAGS = {
    "broker_touched": False,
    "order_executed": False,
    "order_policy": "journal_only_no_broker",
}


class MT5PaperDefense:
    """Paper-only caution filter for new MT5 shadow entries."""

    def __init__(self, *, memory: MemoryStore | None = None) -> None:
        self.memory = memory or MemoryStore()
        self.journal = MT5Journal(memory=self.memory)

    def state(self, *, symbol: str = "") -> dict[str, Any]:
        clean_symbol = _symbol(symbol)
        adaptive_state = _latest_adaptive_state(self.memory, clean_symbol)
        if not adaptive_state or int(adaptive_state.get("closed_trades") or 0) <= 0:
            adaptive_state = MT5AdaptiveStateEngine(memory=self.memory).compute(symbol=clean_symbol)
        if int(adaptive_state.get("closed_trades") or 0) <= 0:
            adaptive_state = _state_from_performance(self.memory, clean_symbol)
        reasons = _caution_reasons(adaptive_state)
        recovery = _recovery_ready(adaptive_state)
        hold = _recovery_hold_active(self.memory, clean_symbol, adaptive_state) if recovery else {"hold_active": False, "reason": ""}
        caution_active = bool(reasons) or bool(hold.get("hold_active"))
        filters = _filters_applied(caution_active)
        return {
            "ok": True,
            "status": "mt5_paper_defense_ready",
            "symbol": clean_symbol,
            "caution_mode_active": caution_active,
            "paper_defense_active": caution_active,
            "reason": reasons[0] if reasons else hold.get("reason") or ("recovery_conditions_met" if recovery else "normal"),
            "reasons": reasons,
            "filters_applied": filters,
            "blocked_count": _count_events(self.memory, clean_symbol, "paper_caution_block"),
            "allowed_count": _count_events(self.memory, clean_symbol, "paper_caution_allow"),
            "rolling_profit_factor": _number(adaptive_state.get("rolling_profit_factor")) or 0.0,
            "rolling_expectancy": _number(adaptive_state.get("rolling_expectancy")) or 0.0,
            "rolling_drawdown": _number(adaptive_state.get("rolling_drawdown")) or 0.0,
            "last_10_win_rate": _number(adaptive_state.get("last_10_win_rate") or adaptive_state.get("rolling_win_rate")) or 0.0,
            "current_loss_streak": int(_number(adaptive_state.get("current_loss_streak")) or 0),
            "closed_trades": int(_number(adaptive_state.get("closed_trades")) or 0),
            "bot_state": adaptive_state.get("bot_state") or "normal",
            "updated_at": _now(),
            **SAFETY_FLAGS,
        }

    def evaluate_new_entry(
        self,
        *,
        symbol: str,
        tick: dict[str, Any],
        market_scores: dict[str, Any],
        decision: dict[str, Any],
        max_spread_points: float,
    ) -> dict[str, Any]:
        defense = self.state(symbol=symbol)
        if not defense["caution_mode_active"]:
            return {
                "allowed": True,
                "caution_mode_active": False,
                "reason": "paper_defense_inactive",
                "block_reasons": [],
                "filters_applied": [],
                **SAFETY_FLAGS,
            }
        score = _number(market_scores.get("score") or decision.get("score")) or 0.0
        trend_score = _number(market_scores.get("trend_score") or decision.get("trend_score")) or 0.0
        momentum_score = _number(market_scores.get("momentum_score") or decision.get("momentum_score")) or 0.0
        volatility_score = _number(market_scores.get("volatility_score") or decision.get("volatility_score")) or 0.0
        regime = str(market_scores.get("regime") or decision.get("regime") or decision.get("market_regime") or "").casefold()
        spread = _number(tick.get("spread_points") or tick.get("spread")) or 0.0
        avg_spread = _average_recent_spread(self.memory, symbol)
        block_reasons: list[str] = []
        effective_min_score = 70.0
        if score < effective_min_score:
            block_reasons.append("score_too_low")
        if trend_score < 55:
            block_reasons.append("trend_not_confirmed")
        if momentum_score < 55:
            block_reasons.append("momentum_not_confirmed")
        if regime == "not_confirmed" and score < 65:
            block_reasons.append("regime_not_confirmed")
        if max_spread_points > 0 and spread > max_spread_points:
            block_reasons.append("spread_too_high")
        if avg_spread > 0 and spread > avg_spread:
            block_reasons.append("spread_above_recent_average")
        if volatility_score >= 75 and (trend_score < 55 or momentum_score < 55):
            block_reasons.append("high_volatility_without_confirmation")
        allowed = not block_reasons
        event_type = "paper_caution_allow" if allowed else "paper_caution_block"
        primary_reason = "paper_caution_filters_passed" if allowed else _primary_reason(defense["reasons"], block_reasons)
        payload = {
            "event_type": event_type,
            "symbol": _symbol(symbol),
            "reason": primary_reason,
            "block_reasons": block_reasons,
            "caution_reasons": defense["reasons"],
            "score": score,
            "effective_min_score": effective_min_score,
            "trend_score": trend_score,
            "momentum_score": momentum_score,
            "volatility_score": volatility_score,
            "regime": regime,
            "spread": spread,
            "average_recent_spread": avg_spread,
            "closed_trades_at_event": defense.get("closed_trades", 0),
            "timestamp": _now(),
            **SAFETY_FLAGS,
        }
        self.journal.save("mt5_paper_defense_events", symbol, payload, confidence="media")
        return {
            "allowed": allowed,
            "caution_mode_active": True,
            "reason": primary_reason,
            "block_reasons": block_reasons,
            "filters_applied": defense["filters_applied"],
            "effective_min_score": effective_min_score,
            "score": score,
            "trend_score": trend_score,
            "momentum_score": momentum_score,
            "volatility_score": volatility_score,
            "spread": spread,
            "average_recent_spread": avg_spread,
            **SAFETY_FLAGS,
        }


def _latest_adaptive_state(memory: MemoryStore, symbol: str) -> dict[str, Any]:
    rows = memory.get_mt5_events("mt5_adaptive_state", symbol or None, limit=20)
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        if payload:
            return payload
    return {}


def _state_from_performance(memory: MemoryStore, symbol: str) -> dict[str, Any]:
    report = MT5Performance(memory=memory).report(symbol=symbol)
    summary = report.get("summary_forward_auto") if isinstance(report.get("summary_forward_auto"), dict) else {}
    if not summary:
        summary = report.get("summary_auto") if isinstance(report.get("summary_auto"), dict) else {}
    return {
        "symbol": _symbol(symbol),
        "bot_state": "caution" if (_number(summary.get("profit_factor")) or 0.0) < 1.0 and int(summary.get("closed") or 0) >= 10 else "normal",
        "closed_trades": int(summary.get("closed") or 0),
        "last_10_win_rate": _number(summary.get("win_rate")) or 0.0,
        "rolling_profit_factor": _number(summary.get("profit_factor")) or 0.0,
        "rolling_expectancy": _number(summary.get("expectancy")) or 0.0,
        "rolling_drawdown": _number(summary.get("max_drawdown") or summary.get("drawdown")) or 0.0,
        "current_loss_streak": 0,
    }


def _caution_reasons(state: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    closed = int(_number(state.get("closed_trades")) or 0)
    if str(state.get("bot_state") or "").casefold() == "caution" and closed >= 10:
        reasons.append("bot_state_caution")
    if (_number(state.get("rolling_profit_factor")) or 0.0) < 1.0 and closed >= 10:
        reasons.append("rolling_pf_below_1")
    if (_number(state.get("rolling_expectancy")) or 0.0) < 0 and closed >= 10:
        reasons.append("negative_expectancy")
    if (_number(state.get("rolling_drawdown")) or 0.0) > 0.35 and closed >= 10:
        reasons.append("high_drawdown")
    last_10 = _number(state.get("last_10_win_rate") or state.get("rolling_win_rate")) or 0.0
    if last_10 <= 50 and closed >= 10:
        reasons.append("low_recent_win_rate")
    return _dedupe(reasons)


def _recovery_ready(state: dict[str, Any]) -> bool:
    return (
        (_number(state.get("rolling_profit_factor")) or 0.0) > 1.25
        and (_number(state.get("rolling_expectancy")) or 0.0) > 0
        and (_number(state.get("last_10_win_rate") or state.get("rolling_win_rate")) or 0.0) >= 60
        and int(_number(state.get("current_loss_streak")) or 0) == 0
    )


def _recovery_hold_active(memory: MemoryStore, symbol: str, state: dict[str, Any]) -> dict[str, Any]:
    rows = memory.get_mt5_events("mt5_paper_defense_events", symbol or None, limit=20)
    block_payloads = [
        row.get("payload")
        for row in rows
        if isinstance(row.get("payload"), dict) and row.get("payload", {}).get("event_type") == "paper_caution_block"
    ]
    if not block_payloads:
        return {"hold_active": False, "reason": ""}
    last_block = block_payloads[0]
    closed_then = int(_number(last_block.get("closed_trades_at_event")) or 0)
    closed_now = int(_number(state.get("closed_trades")) or 0)
    hours = _hours_since(str(last_block.get("timestamp") or ""))
    if closed_now - closed_then >= 10 or hours >= 6:
        return {"hold_active": False, "reason": ""}
    return {"hold_active": True, "reason": "recovery_hold_period_active"}


def _filters_applied(active: bool) -> list[str]:
    if not active:
        return []
    return [
        "min_score_plus_15",
        "trend_score_gte_55",
        "momentum_score_gte_55",
        "block_not_confirmed_regime_under_65",
        "block_spread_above_recent_average_or_max",
        "block_high_volatility_without_confirmation",
        "max_open_trades_1",
    ]


def _average_recent_spread(memory: MemoryStore, symbol: str) -> float:
    rows = memory.get_mt5_events("mt5_ticks", symbol or None, limit=25)
    spreads: list[float] = []
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        spread = _number(payload.get("spread_points") or payload.get("spread"))
        if spread is not None and spread > 0:
            spreads.append(spread)
    return round(sum(spreads) / len(spreads), 8) if spreads else 0.0


def _count_events(memory: MemoryStore, symbol: str, event_type: str) -> int:
    rows = memory.get_mt5_events("mt5_paper_defense_events", symbol or None, limit=1000)
    return sum(1 for row in rows if isinstance(row.get("payload"), dict) and row["payload"].get("event_type") == event_type)


def _primary_reason(caution_reasons: list[str], block_reasons: list[str]) -> str:
    for reason in caution_reasons:
        if reason in {"rolling_pf_below_1", "negative_expectancy", "high_drawdown", "low_recent_win_rate"}:
            return reason
    return block_reasons[0] if block_reasons else (caution_reasons[0] if caution_reasons else "paper_caution_block")


def _hours_since(value: str) -> float:
    if not value:
        return 999.0
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return 999.0
    return max((datetime.now(timezone.utc) - parsed).total_seconds(), 0.0) / 3600.0


def _dedupe(values: list[str]) -> list[str]:
    output: list[str] = []
    for value in values:
        if value and value not in output:
            output.append(value)
    return output


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
