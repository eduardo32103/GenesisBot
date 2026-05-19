from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


_TRUE_VALUES = {"1", "true", "yes", "on", "si", "sí", "enabled"}


@dataclass(frozen=True)
class MT5RuntimeConfig:
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
    shadow_time_stop_hours: float = 12.0
    shadow_time_stop_bars: int = 12
    shadow_breakeven_r: float = 0.40
    shadow_trail_start_r: float = 0.70
    shadow_trail_distance_r: float = 0.30
    shadow_signal_flip_close: bool = True

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
            "MT5_SHADOW_TIME_STOP_HOURS": self.shadow_time_stop_hours,
            "MT5_SHADOW_TIME_STOP_BARS": self.shadow_time_stop_bars,
            "MT5_SHADOW_BREAKEVEN_R": self.shadow_breakeven_r,
            "MT5_SHADOW_TRAIL_START_R": self.shadow_trail_start_r,
            "MT5_SHADOW_TRAIL_DISTANCE_R": self.shadow_trail_distance_r,
            "MT5_SHADOW_SIGNAL_FLIP_CLOSE": self.shadow_signal_flip_close,
        }


def get_mt5_config() -> MT5RuntimeConfig:
    return MT5RuntimeConfig(
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
        shadow_time_stop_hours=_float_env("MT5_SHADOW_TIME_STOP_HOURS", 12.0),
        shadow_time_stop_bars=int(_float_env("MT5_SHADOW_TIME_STOP_BARS", 12)),
        shadow_breakeven_r=_float_env("MT5_SHADOW_BREAKEVEN_R", 0.40),
        shadow_trail_start_r=_float_env("MT5_SHADOW_TRAIL_START_R", 0.70),
        shadow_trail_distance_r=_float_env("MT5_SHADOW_TRAIL_DISTANCE_R", 0.30),
        shadow_signal_flip_close=_bool_env("MT5_SHADOW_SIGNAL_FLIP_CLOSE", True),
    )


def is_paper_exploration_enabled() -> bool:
    return get_mt5_config().paper_exploration_enabled


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return str(raw).strip().casefold() in _TRUE_VALUES


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
