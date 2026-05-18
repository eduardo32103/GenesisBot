from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from services.mt5.mt5_order_model import MT5OrderIntent
from services.mt5.mt5_symbol_mapper import MT5SymbolMapper


@dataclass(frozen=True)
class MT5BridgeConfig:
    enabled: bool = False
    demo_only: bool = True
    live_trading_enabled: bool = False
    order_execution_enabled: bool = False
    kill_switch: bool = True
    max_daily_loss_pct: float = 2.0
    max_position_risk_pct: float = 0.5
    max_open_trades: int = 1
    max_spread_points: float = 50.0
    min_rr: float = 1.2
    paper_exploration_enabled: bool = False

    @classmethod
    def from_env(cls) -> "MT5BridgeConfig":
        return cls(
            enabled=_bool_env("MT5_ENABLED", False),
            demo_only=_bool_env("MT5_DEMO_ONLY", True),
            live_trading_enabled=_bool_env("MT5_LIVE_TRADING_ENABLED", False),
            order_execution_enabled=_bool_env("MT5_ORDER_EXECUTION_ENABLED", False),
            kill_switch=_bool_env("MT5_KILL_SWITCH", True),
            max_daily_loss_pct=_float_env("MT5_MAX_DAILY_LOSS_PCT", 2.0),
            max_position_risk_pct=_float_env("MT5_MAX_POSITION_RISK_PCT", 0.5),
            max_open_trades=int(_float_env("MT5_MAX_OPEN_TRADES", 1)),
            max_spread_points=_float_env("MT5_MAX_SPREAD", _float_env("MT5_MAX_SPREAD_POINTS", 50.0)),
            min_rr=_float_env("MT5_MIN_RR", 1.2),
            paper_exploration_enabled=_bool_env("MT5_PAPER_EXPLORATION_ENABLED", False),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "MT5_ENABLED": self.enabled,
            "MT5_DEMO_ONLY": self.demo_only,
            "MT5_LIVE_TRADING_ENABLED": self.live_trading_enabled,
            "MT5_ORDER_EXECUTION_ENABLED": self.order_execution_enabled,
            "MT5_KILL_SWITCH": self.kill_switch,
            "MT5_MAX_DAILY_LOSS_PCT": self.max_daily_loss_pct,
            "MT5_MAX_POSITION_RISK_PCT": self.max_position_risk_pct,
            "MT5_MAX_OPEN_TRADES": self.max_open_trades,
            "MT5_MAX_SPREAD_POINTS": self.max_spread_points,
            "MT5_MIN_RR": self.min_rr,
            "MT5_PAPER_EXPLORATION_ENABLED": self.paper_exploration_enabled,
        }


class MT5RiskGuard:
    def __init__(self, *, config: MT5BridgeConfig | None = None, symbol_mapper: MT5SymbolMapper | None = None) -> None:
        self.config = config or MT5BridgeConfig.from_env()
        self.symbol_mapper = symbol_mapper or MT5SymbolMapper()

    def evaluate_order(self, intent: MT5OrderIntent, *, account_state: dict[str, Any] | None = None) -> dict[str, Any]:
        account = account_state or {}
        symbol_info = self.symbol_mapper.map_symbol(intent.symbol)
        reasons: list[str] = []

        if self.config.kill_switch:
            reasons.append("kill_switch_active")
        if not self.config.enabled:
            reasons.append("mt5_disabled")
        if not symbol_info["ok"]:
            reasons.append(symbol_info["reason"])
        if self.config.demo_only and account and not bool(account.get("is_demo")):
            reasons.append("demo_only_account_required")
        if not self.config.live_trading_enabled:
            reasons.append("live_trading_disabled")
        if not self.config.order_execution_enabled:
            reasons.append("order_execution_disabled")
        if intent.action in {"BUY", "SELL", "HEDGE", "REDUCE"} and intent.stop_loss is None:
            reasons.append("stop_loss_required")
        if intent.risk_pct > self.config.max_position_risk_pct:
            reasons.append("risk_pct_above_limit")
        daily_loss = intent.daily_loss_pct or _to_float(account.get("daily_loss_pct")) or 0.0
        if daily_loss >= self.config.max_daily_loss_pct:
            reasons.append("max_daily_loss_reached")
        open_trades = intent.open_trades or int(_to_float(account.get("open_trades")) or 0)
        if open_trades >= self.config.max_open_trades:
            reasons.append("max_open_trades_reached")
        if intent.spread_points is not None and intent.spread_points > self.config.max_spread_points:
            reasons.append("spread_too_high")
        if intent.no_trade_score >= 70:
            reasons.append("no_trade_score_block")
        if intent.hedge_score >= 80 and intent.action in {"BUY", "SELL"}:
            reasons.append("hedge_score_hard_block")
        if intent.confidence not in {"medium", "high"} and intent.action in {"BUY", "SELL"}:
            reasons.append("confidence_too_low")
        rr = _risk_reward(intent)
        if intent.action in {"BUY", "SELL"} and rr is not None and rr < self.config.min_rr:
            reasons.append("risk_reward_too_low")

        executable_demo = (
            not reasons
            and self.config.enabled
            and self.config.demo_only
            and self.config.order_execution_enabled
            and not self.config.live_trading_enabled
            and not self.config.kill_switch
        )
        return {
            "ok": not reasons,
            "allowed": not reasons,
            "demo_order_allowed": executable_demo,
            "blocked": bool(reasons),
            "reasons": reasons,
            "primary_reason": reasons[0] if reasons else "passed",
            "warnings": list(symbol_info.get("warnings") or []),
            "risk_reward": rr,
            "symbol": symbol_info,
            "order_policy": "journal_only_no_broker" if reasons or not executable_demo else "demo_only",
            "broker_touched": False,
            "order_executed": False,
            "config": self.config.to_payload(),
        }


def _risk_reward(intent: MT5OrderIntent) -> float | None:
    if intent.entry is None or intent.stop_loss is None or intent.take_profit is None:
        return None
    risk = abs(intent.entry - intent.stop_loss)
    reward = abs(intent.take_profit - intent.entry)
    if risk <= 0:
        return None
    return round(reward / risk, 4)


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return str(raw).strip().casefold() in {"1", "true", "yes", "on", "si"}


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
