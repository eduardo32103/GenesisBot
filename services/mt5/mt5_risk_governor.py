from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RiskGovernorLimits:
    max_daily_loss_pct: float = 1.0
    max_weekly_loss_pct: float = 3.0
    max_monthly_loss_pct: float = 6.0
    max_total_drawdown_pct: float = 5.0
    max_consecutive_losses: int = 4
    defensive_consecutive_losses: int = 3
    caution_consecutive_losses: int = 2
    max_open_trades: int = 1
    max_symbol_exposure: float = 1.0
    max_correlation_exposure: float = 1.0
    max_spread_points: float = 60.0
    max_slippage_points: float = 10.0
    max_trade_duration_minutes: float = 240.0
    forward_pf_threshold: float = 1.0
    min_forward_trades: int = 20
    min_recent_expectancy: float = 0.0
    drawdown_acceleration_threshold_pct: float = 2.0
    max_hedge_fraction: float = 0.5


class MT5RiskGovernor:
    """Paper-only risk governor. It blocks bad signals; it never executes broker actions."""

    def __init__(self, limits: RiskGovernorLimits | None = None) -> None:
        self.limits = limits or RiskGovernorLimits()

    def assess(
        self,
        *,
        signal: dict[str, Any] | None = None,
        account_state: dict[str, Any] | None = None,
        performance: dict[str, Any] | None = None,
        market: dict[str, Any] | None = None,
        open_trades: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        signal = signal or {}
        account_state = account_state or {}
        performance = performance or {}
        market = market or {}
        open_trades = [trade for trade in (open_trades or []) if isinstance(trade, dict)]

        state, multiplier = self._state_from_losses(account_state, performance)
        checks = [
            (self._pct(account_state, "daily_loss_pct") >= self.limits.max_daily_loss_pct, "daily_loss_limit_reached", "lockdown", 0.0),
            (self._pct(account_state, "weekly_loss_pct") >= self.limits.max_weekly_loss_pct, "weekly_loss_limit_reached", "lockdown", 0.0),
            (self._pct(account_state, "monthly_loss_pct") >= self.limits.max_monthly_loss_pct, "monthly_loss_limit_reached", "lockdown", 0.0),
            (self._pct(account_state, "total_drawdown_pct", "drawdown_pct") >= self.limits.max_total_drawdown_pct, "drawdown_limit_reached", "lockdown", 0.0),
            (self._int(performance, "consecutive_losses", "current_loss_streak") >= self.limits.max_consecutive_losses, "consecutive_loss_lockdown", "lockdown", 0.0),
            (len(open_trades) >= self.limits.max_open_trades, "max_open_trades_reached", "caution", 0.0),
            (self._float(account_state, "symbol_exposure") > self.limits.max_symbol_exposure, "symbol_exposure_limit", "defensive", 0.0),
            (self._float(account_state, "correlation_exposure") > self.limits.max_correlation_exposure, "correlation_exposure_limit", "defensive", 0.0),
            (self._float(market, "spread_points", "spread") > self.limits.max_spread_points, "spread_too_high", "caution", 0.0),
            (self._float(market, "slippage_points", "slippage") > self.limits.max_slippage_points, "slippage_too_high", "caution", 0.0),
            (bool(market.get("high_volatility_event") or market.get("news_spike")), "high_volatility_event", "defensive", 0.0),
            (str(market.get("regime") or market.get("market_regime") or "").casefold() in {"", "unclear", "not_confirmed", "unknown"}, "market_regime_unclear", "caution", min(multiplier, 0.25)),
            (self._recent_edge_negative(performance), "recent_edge_negative", "defensive", 0.0),
            (self._forward_pf_bad(performance), "forward_pf_below_threshold", "defensive", 0.0),
            (self._expectancy_bad(performance), "expectancy_negative", "defensive", 0.0),
            (self._drawdown_accelerating(performance), "drawdown_accelerating", "defensive", 0.0),
            (self._martingale_detected(signal, performance), "martingale_or_loss_scaling_blocked", "lockdown", 0.0),
        ]
        for failed, reason, risk_state, suggested in checks:
            if failed:
                return self._blocked(reason, risk_state, suggested)
        return {
            "allowed": True,
            "reason": "risk_governor_pass",
            "risk_state": state,
            "suggested_lot_multiplier": multiplier,
            "hedge_needed": False,
            "hedge_score": 0.0,
            "hedge_reason": "",
            **_safety(),
        }

    def assess_hedge(
        self,
        *,
        open_trade: dict[str, Any] | None = None,
        hedge_signal: dict[str, Any] | None = None,
        account_state: dict[str, Any] | None = None,
        market: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        open_trade = open_trade or {}
        hedge_signal = hedge_signal or {}
        account_state = account_state or {}
        market = market or {}
        exposure = abs(self._float(open_trade, "exposure", "risk_pct", default=0.0))
        requested = abs(self._float(hedge_signal, "hedge_fraction", "size_fraction", default=0.0))
        max_fraction = min(self.limits.max_hedge_fraction, 0.5)
        if not open_trade:
            return self._hedge(False, "no_open_trade_to_hedge", 0.0, 0.0)
        if self._pct(account_state, "daily_loss_pct") >= self.limits.max_daily_loss_pct or self._pct(account_state, "weekly_loss_pct") >= self.limits.max_weekly_loss_pct:
            return self._hedge(False, "loss_limits_block_hedge", 0.0, 0.0)
        if requested <= 0 or requested > max_fraction:
            return self._hedge(False, "hedge_size_limit", 0.0, 0.0)
        if requested * exposure > exposure * max_fraction:
            return self._hedge(False, "hedge_would_increase_exposure_too_much", 0.0, 0.0)
        if not bool(hedge_signal.get("contrary_regime_confirmed")):
            return self._hedge(False, "hedge_regime_not_confirmed", 0.0, 0.0)
        if not bool(market.get("volatility_elevated") or hedge_signal.get("breakdown_confirmed") or hedge_signal.get("breakout_failed")):
            return self._hedge(False, "hedge_volatility_or_invalidation_missing", 0.0, 0.0)
        if self._float(hedge_signal, "expected_drawdown_change", default=0.0) > 0:
            return self._hedge(False, "hedge_increases_expected_drawdown", 0.0, 0.0)
        if self._float(hedge_signal, "stop_loss", default=0.0) <= 0 or self._float(hedge_signal, "max_life_minutes", default=0.0) <= 0:
            return self._hedge(False, "hedge_missing_stop_or_time_limit", 0.0, 0.0)
        score = min(100.0, 50.0 + requested * 100.0)
        return self._hedge(True, "hedge_reduces_net_risk", requested, score)

    def _state_from_losses(self, account_state: dict[str, Any], performance: dict[str, Any]) -> tuple[str, float]:
        losses = self._int(performance, "consecutive_losses", "current_loss_streak")
        drawdown = self._pct(account_state, "total_drawdown_pct", "drawdown_pct")
        if losses >= self.limits.max_consecutive_losses or drawdown >= self.limits.max_total_drawdown_pct:
            return "lockdown", 0.0
        if losses >= self.limits.defensive_consecutive_losses:
            return "defensive", 0.0
        if losses >= self.limits.caution_consecutive_losses:
            return "caution", 0.25
        return "normal", 1.0

    def _recent_edge_negative(self, performance: dict[str, Any]) -> bool:
        return bool(performance.get("recent_edge_negative")) or (
            self._int(performance, "recent_closed", "closed") >= 10
            and self._float(performance, "recent_profit_factor", "profit_factor") < 1.0
            and self._float(performance, "recent_expectancy", "expectancy") <= self.limits.min_recent_expectancy
        )

    def _forward_pf_bad(self, performance: dict[str, Any]) -> bool:
        closed = self._int(performance, "forward_closed", "closed")
        return closed >= self.limits.min_forward_trades and self._float(performance, "forward_profit_factor", "profit_factor") < self.limits.forward_pf_threshold

    def _expectancy_bad(self, performance: dict[str, Any]) -> bool:
        closed = self._int(performance, "forward_closed", "closed")
        return closed >= self.limits.min_forward_trades and self._float(performance, "forward_expectancy", "expectancy") <= 0

    def _drawdown_accelerating(self, performance: dict[str, Any]) -> bool:
        return bool(performance.get("drawdown_accelerating")) or self._pct(performance, "drawdown_acceleration_pct") >= self.limits.drawdown_acceleration_threshold_pct

    def _martingale_detected(self, signal: dict[str, Any], performance: dict[str, Any]) -> bool:
        loss_streak = self._int(performance, "consecutive_losses", "current_loss_streak")
        multiplier = self._float(signal, "lot_multiplier", "risk_multiplier", default=1.0)
        return loss_streak > 0 and multiplier > 1.0

    def _blocked(self, reason: str, risk_state: str, multiplier: float) -> dict[str, Any]:
        return {
            "allowed": False,
            "reason": reason,
            "risk_state": risk_state,
            "suggested_lot_multiplier": multiplier,
            "hedge_needed": False,
            "hedge_score": 0.0,
            "hedge_reason": "",
            **_safety(),
        }

    def _hedge(self, allowed: bool, reason: str, fraction: float, score: float) -> dict[str, Any]:
        return {
            "allowed": allowed,
            "hedge_needed": bool(allowed),
            "hedge_score": round(score, 4),
            "hedge_reason": reason,
            "max_hedge_fraction": self.limits.max_hedge_fraction,
            "suggested_hedge_fraction": round(fraction, 4),
            "risk_state": "defensive" if allowed else "caution",
            "suggested_lot_multiplier": 0.0,
            "reason": reason,
            **_safety(),
        }

    def _int(self, data: dict[str, Any], *keys: str) -> int:
        return int(self._float(data, *keys, default=0.0))

    def _pct(self, data: dict[str, Any], *keys: str) -> float:
        return self._float(data, *keys, default=0.0)

    def _float(self, data: dict[str, Any], *keys: str, default: float = 0.0) -> float:
        for key in keys:
            parsed = _number(data.get(key))
            if parsed is not None:
                return float(parsed)
        return default


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
