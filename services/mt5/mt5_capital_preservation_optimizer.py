from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any

from services.mt5.mt5_backtester import (
    BacktestSettings,
    _close,
    _decision_from_history,
    _force_close,
    _load_bars,
    _metrics,
    _number,
    _open_trade,
    _reason_counts,
    _safety,
    _settings,
    _timeframe_minutes,
    _timed_out,
)
from services.mt5.mt5_config import MT5RuntimeConfig, get_mt5_config
CAPITAL_PRESERVATION_PROFILES = [
    "breakout_pullback_v1",
    "breakout_pullback_v2_safe",
    "breakout_pullback_v3_balanced",
    "trend_continuation_v1",
    "trend_continuation_v2_low_drawdown",
    "trend_continuation_v3_balanced",
    "mean_reversion_v1_safe",
    "volatility_squeeze_v1",
    "session_filtered_v1",
    "liquidity_sweep_reversal_v1",
    "liquidity_sweep_v2_confirmed",
    "ema_rsi_confirmed_v1",
    "atr_trailing_v1",
    "anti_chop_v2_safe",
    "anti_chop_v3_low_drawdown",
    "anti_chop_v4_balanced",
    "quality_v3_conservative",
    "quality_v4_strict_rr",
    "trend_v2_drawdown_guard",
    "trend_v3_pullback_confirmed",
    "momentum_v2_filtered",
    "momentum_v3_no_chase",
    "rsi_reversal_v2_confirmed",
    "capital_preservation_v1",
    "capital_preservation_v2",
    "capital_preservation_v3_balanced",
    "low_drawdown_v1",
    "low_drawdown_v2",
    "low_drawdown_v3_more_trades",
]

CAPITAL_PRESERVATION_TIMEFRAMES = ["M15", "M30", "H1"]

_PROFILE_PARAMS: dict[str, dict[str, Any]] = {
    "breakout_pullback_v1": {
        "strategy_family": "breakout_pullback",
        "min_trend_score": 52.0,
        "min_momentum_score": 50.0,
        "max_rsi_for_buy": 74.0,
        "min_rsi_for_sell": 26.0,
        "score_cap_when_weak": 58.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 62.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 30.0,
        "ema_distance_max_atr": 2.2,
        "extended_candle_atr": 1.8,
        "atr_stop_multiplier": 1.2,
    },
    "breakout_pullback_v2_safe": {
        "strategy_family": "breakout_pullback",
        "min_trend_score": 58.0,
        "min_momentum_score": 55.0,
        "max_rsi_for_buy": 70.0,
        "min_rsi_for_sell": 30.0,
        "score_cap_when_weak": 54.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 68.0,
        "max_spread_points": 20.0,
        "min_volatility_score": 34.0,
        "ema_distance_max_atr": 1.8,
        "extended_candle_atr": 1.5,
        "atr_stop_multiplier": 1.0,
    },
    "breakout_pullback_v3_balanced": {
        "strategy_family": "breakout_pullback",
        "min_trend_score": 50.0,
        "min_momentum_score": 48.0,
        "max_rsi_for_buy": 76.0,
        "min_rsi_for_sell": 24.0,
        "score_cap_when_weak": 60.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 60.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 24.0,
        "ema_distance_max_atr": 2.8,
        "extended_candle_atr": 2.1,
        "atr_stop_multiplier": 1.15,
    },
    "trend_continuation_v1": {
        "strategy_family": "trend_continuation",
        "min_trend_score": 58.0,
        "min_momentum_score": 52.0,
        "max_rsi_for_buy": 76.0,
        "min_rsi_for_sell": 24.0,
        "score_cap_when_weak": 56.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 64.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 28.0,
        "ema_distance_max_atr": 2.4,
        "extended_candle_atr": 1.9,
        "atr_stop_multiplier": 1.1,
    },
    "trend_continuation_v2_low_drawdown": {
        "strategy_family": "trend_continuation",
        "min_trend_score": 64.0,
        "min_momentum_score": 56.0,
        "max_rsi_for_buy": 72.0,
        "min_rsi_for_sell": 28.0,
        "score_cap_when_weak": 52.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 70.0,
        "max_spread_points": 20.0,
        "min_volatility_score": 32.0,
        "ema_distance_max_atr": 1.8,
        "extended_candle_atr": 1.5,
        "atr_stop_multiplier": 0.95,
    },
    "trend_continuation_v3_balanced": {
        "strategy_family": "trend_continuation",
        "min_trend_score": 54.0,
        "min_momentum_score": 48.0,
        "max_rsi_for_buy": 78.0,
        "min_rsi_for_sell": 22.0,
        "score_cap_when_weak": 58.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 61.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 24.0,
        "ema_distance_max_atr": 3.0,
        "extended_candle_atr": 2.2,
        "atr_stop_multiplier": 1.05,
    },
    "mean_reversion_v1_safe": {
        "strategy_family": "mean_reversion",
        "min_trend_score": 35.0,
        "min_momentum_score": 35.0,
        "max_rsi_for_buy": 42.0,
        "min_rsi_for_sell": 58.0,
        "score_cap_when_weak": 62.0,
        "allow_reversal": True,
        "avoid_chop": False,
        "min_score": 58.0,
        "max_spread_points": 20.0,
        "min_volatility_score": 24.0,
        "allowed_regime": "chop",
        "ema_distance_max_atr": 2.8,
        "extended_candle_atr": 2.0,
        "atr_stop_multiplier": 0.9,
    },
    "volatility_squeeze_v1": {
        "strategy_family": "volatility_squeeze",
        "min_trend_score": 45.0,
        "min_momentum_score": 55.0,
        "max_rsi_for_buy": 75.0,
        "min_rsi_for_sell": 25.0,
        "score_cap_when_weak": 58.0,
        "allow_reversal": False,
        "avoid_chop": False,
        "min_score": 62.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 18.0,
        "ema_distance_max_atr": 2.5,
        "extended_candle_atr": 1.7,
        "atr_stop_multiplier": 1.1,
    },
    "session_filtered_v1": {
        "strategy_family": "trend_continuation",
        "session_filter": True,
        "session_hours": [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
        "min_trend_score": 56.0,
        "min_momentum_score": 54.0,
        "max_rsi_for_buy": 74.0,
        "min_rsi_for_sell": 26.0,
        "score_cap_when_weak": 55.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 65.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 30.0,
        "ema_distance_max_atr": 2.0,
        "extended_candle_atr": 1.7,
        "atr_stop_multiplier": 1.0,
    },
    "liquidity_sweep_reversal_v1": {
        "strategy_family": "liquidity_sweep_reversal",
        "min_trend_score": 35.0,
        "min_momentum_score": 35.0,
        "max_rsi_for_buy": 46.0,
        "min_rsi_for_sell": 54.0,
        "score_cap_when_weak": 62.0,
        "allow_reversal": True,
        "avoid_chop": False,
        "min_score": 60.0,
        "max_spread_points": 20.0,
        "min_volatility_score": 26.0,
        "ema_distance_max_atr": 3.0,
        "extended_candle_atr": 2.2,
        "atr_stop_multiplier": 0.9,
    },
    "liquidity_sweep_v2_confirmed": {
        "strategy_family": "liquidity_sweep_reversal",
        "min_trend_score": 30.0,
        "min_momentum_score": 30.0,
        "max_rsi_for_buy": 48.0,
        "min_rsi_for_sell": 52.0,
        "score_cap_when_weak": 64.0,
        "allow_reversal": True,
        "avoid_chop": False,
        "min_score": 58.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 22.0,
        "ema_distance_max_atr": 3.4,
        "extended_candle_atr": 2.4,
        "atr_stop_multiplier": 0.95,
    },
    "ema_rsi_confirmed_v1": {
        "strategy_family": "ema_rsi_confirmed",
        "min_trend_score": 55.0,
        "min_momentum_score": 50.0,
        "max_rsi_for_buy": 68.0,
        "min_rsi_for_sell": 32.0,
        "score_cap_when_weak": 54.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 64.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 28.0,
        "ema_distance_max_atr": 1.6,
        "extended_candle_atr": 1.5,
        "atr_stop_multiplier": 1.0,
    },
    "atr_trailing_v1": {
        "strategy_family": "trend_continuation",
        "atr_trailing": True,
        "partial_exit": True,
        "min_trend_score": 58.0,
        "min_momentum_score": 55.0,
        "max_rsi_for_buy": 74.0,
        "min_rsi_for_sell": 26.0,
        "score_cap_when_weak": 54.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 66.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 30.0,
        "ema_distance_max_atr": 2.0,
        "extended_candle_atr": 1.7,
        "atr_stop_multiplier": 1.1,
    },
    "anti_chop_v2_safe": {
        "min_trend_score": 55.0,
        "min_momentum_score": 55.0,
        "max_rsi_for_buy": 74.0,
        "min_rsi_for_sell": 26.0,
        "score_cap_when_weak": 56.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 66.0,
        "max_spread_points": 30.0,
        "min_volatility_score": 35.0,
    },
    "anti_chop_v3_low_drawdown": {
        "min_trend_score": 58.0,
        "min_momentum_score": 58.0,
        "max_rsi_for_buy": 72.0,
        "min_rsi_for_sell": 28.0,
        "score_cap_when_weak": 54.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 68.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 40.0,
    },
    "anti_chop_v4_balanced": {
        "strategy_family": "trend_continuation",
        "min_trend_score": 52.0,
        "min_momentum_score": 50.0,
        "max_rsi_for_buy": 76.0,
        "min_rsi_for_sell": 24.0,
        "score_cap_when_weak": 58.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 62.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 28.0,
        "ema_distance_max_atr": 2.8,
        "extended_candle_atr": 2.0,
        "atr_stop_multiplier": 1.0,
    },
    "quality_v3_conservative": {
        "min_trend_score": 58.0,
        "min_momentum_score": 58.0,
        "max_rsi_for_buy": 72.0,
        "min_rsi_for_sell": 28.0,
        "score_cap_when_weak": 54.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 68.0,
        "max_spread_points": 30.0,
        "min_volatility_score": 35.0,
    },
    "quality_v4_strict_rr": {
        "min_trend_score": 62.0,
        "min_momentum_score": 60.0,
        "max_rsi_for_buy": 70.0,
        "min_rsi_for_sell": 30.0,
        "score_cap_when_weak": 52.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 70.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 38.0,
    },
    "trend_v2_drawdown_guard": {
        "min_trend_score": 65.0,
        "min_momentum_score": 45.0,
        "max_rsi_for_buy": 76.0,
        "min_rsi_for_sell": 24.0,
        "score_cap_when_weak": 55.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 66.0,
        "max_spread_points": 30.0,
        "min_volatility_score": 34.0,
    },
    "trend_v3_pullback_confirmed": {
        "min_trend_score": 66.0,
        "min_momentum_score": 50.0,
        "max_rsi_for_buy": 73.0,
        "min_rsi_for_sell": 27.0,
        "score_cap_when_weak": 53.0,
        "allow_reversal": True,
        "avoid_chop": True,
        "min_score": 68.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 35.0,
    },
    "momentum_v2_filtered": {
        "min_trend_score": 48.0,
        "min_momentum_score": 65.0,
        "max_rsi_for_buy": 74.0,
        "min_rsi_for_sell": 26.0,
        "score_cap_when_weak": 55.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 66.0,
        "max_spread_points": 30.0,
        "min_volatility_score": 36.0,
    },
    "momentum_v3_no_chase": {
        "min_trend_score": 52.0,
        "min_momentum_score": 68.0,
        "max_rsi_for_buy": 70.0,
        "min_rsi_for_sell": 30.0,
        "score_cap_when_weak": 52.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 70.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 38.0,
    },
    "rsi_reversal_v2_confirmed": {
        "min_trend_score": 55.0,
        "min_momentum_score": 55.0,
        "max_rsi_for_buy": 68.0,
        "min_rsi_for_sell": 32.0,
        "score_cap_when_weak": 52.0,
        "allow_reversal": True,
        "avoid_chop": True,
        "min_score": 67.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 36.0,
    },
    "capital_preservation_v1": {
        "min_trend_score": 62.0,
        "min_momentum_score": 62.0,
        "max_rsi_for_buy": 68.0,
        "min_rsi_for_sell": 32.0,
        "score_cap_when_weak": 50.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 72.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 38.0,
    },
    "capital_preservation_v2": {
        "min_trend_score": 65.0,
        "min_momentum_score": 65.0,
        "max_rsi_for_buy": 66.0,
        "min_rsi_for_sell": 34.0,
        "score_cap_when_weak": 48.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 74.0,
        "max_spread_points": 20.0,
        "min_volatility_score": 40.0,
    },
    "capital_preservation_v3_balanced": {
        "strategy_family": "ema_rsi_confirmed",
        "min_trend_score": 56.0,
        "min_momentum_score": 54.0,
        "max_rsi_for_buy": 72.0,
        "min_rsi_for_sell": 28.0,
        "score_cap_when_weak": 56.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 64.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 28.0,
        "ema_distance_max_atr": 2.2,
        "extended_candle_atr": 1.9,
        "atr_stop_multiplier": 1.0,
    },
    "low_drawdown_v1": {
        "min_trend_score": 60.0,
        "min_momentum_score": 60.0,
        "max_rsi_for_buy": 70.0,
        "min_rsi_for_sell": 30.0,
        "score_cap_when_weak": 50.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 70.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 36.0,
    },
    "low_drawdown_v2": {
        "min_trend_score": 64.0,
        "min_momentum_score": 62.0,
        "max_rsi_for_buy": 68.0,
        "min_rsi_for_sell": 32.0,
        "score_cap_when_weak": 48.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 73.0,
        "max_spread_points": 20.0,
        "min_volatility_score": 40.0,
    },
    "low_drawdown_v3_more_trades": {
        "strategy_family": "ema_rsi_confirmed",
        "min_trend_score": 55.0,
        "min_momentum_score": 52.0,
        "max_rsi_for_buy": 73.0,
        "min_rsi_for_sell": 27.0,
        "score_cap_when_weak": 56.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 62.0,
        "max_spread_points": 25.0,
        "min_volatility_score": 26.0,
        "ema_distance_max_atr": 2.4,
        "extended_candle_atr": 1.9,
        "atr_stop_multiplier": 0.95,
    },
}

_DEFAULT_RR_VALUES = [0.8, 1.0, 1.2, 1.5]
_DEFAULT_TIME_STOP_BARS = [1, 2, 3, 4, 6]
_DEFAULT_SCORE_MIN = [55, 60, 65, 70, 75]
_DEFAULT_SPREAD_MAX = [20, 25, 30]


@dataclass(frozen=True)
class CapitalSearchConfig:
    profile: str
    risk_reward: float
    time_stop_bars: int
    score_min: float
    spread_max: float
    volatility_filter: bool
    anti_chop_filter: bool
    cooldown_after_loss_bars: int
    block_after_consecutive_losses: int
    trailing_stop: bool
    max_adverse_excursion_filter: bool
    no_trade_if_recent_edge_negative: bool
    no_trade_if_drawdown_accelerating: bool
    session_filter: bool = False
    partial_exit: bool = False
    atr_trailing: bool = False
    adaptive_time_stop: bool = False
    risk_pct: float = 0.1
    trailing_start_r: float = 0.6
    trailing_distance_r: float = 0.35

    def key(self) -> str:
        return (
            f"{self.profile}|rr={self.risk_reward}|ts={self.time_stop_bars}|score={self.score_min}|"
            f"spread={self.spread_max}|vol={int(self.volatility_filter)}|chop={int(self.anti_chop_filter)}|"
            f"cd={self.cooldown_after_loss_bars}|loss={self.block_after_consecutive_losses}|trail={int(self.trailing_stop)}|"
            f"mae={int(self.max_adverse_excursion_filter)}|edge={int(self.no_trade_if_recent_edge_negative)}|"
            f"dd={int(self.no_trade_if_drawdown_accelerating)}|session={int(self.session_filter)}|"
            f"partial={int(self.partial_exit)}|atrtrail={int(self.atr_trailing)}|adaptive={int(self.adaptive_time_stop)}"
        )


class MT5CapitalPreservationOptimizer:
    """Cold-path strategy search focused on survival first.

    This class reads local CSV bars and writes only optional report artifacts. It
    never mutates promoted profiles, forward state, runtime snapshots, shadow
    trades, broker settings, or order execution.
    """

    def __init__(self, *, config: MT5RuntimeConfig | None = None) -> None:
        self.config = config or get_mt5_config()

    def run(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        started = time.monotonic()
        body = dict(payload or {})
        symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
        csv_dir = Path(str(body.get("csv_dir") or Path("data") / "backtests"))
        timeframes = _requested_list(body.get("timeframes"), CAPITAL_PRESERVATION_TIMEFRAMES)
        profiles = [profile for profile in _requested_list(body.get("profiles"), CAPITAL_PRESERVATION_PROFILES) if profile in _PROFILE_PARAMS]
        max_bars = int(_number(body.get("max_bars")) or 5000)
        timeout_seconds = float(_number(body.get("timeout_seconds")) or 4.0)
        per_evaluation_timeout = float(_number(body.get("per_evaluation_timeout_seconds")) or timeout_seconds or 2.0)
        max_runtime_seconds = float(_number(body.get("max_runtime_seconds")) or 0.0)
        progress_every = max(1, int(_number(body.get("progress_every")) or 0) or 25)
        progress_callback = body.get("progress_callback") if callable(body.get("progress_callback")) else None
        incremental_callback = body.get("incremental_callback") if callable(body.get("incremental_callback")) else None
        existing_results = [row for row in body.get("existing_results", []) if isinstance(row, dict)] if isinstance(body.get("existing_results"), list) else []
        existing_keys = {_result_key(row) for row in existing_results}
        max_evaluations = max(1, int(_number(body.get("max_evaluations")) or 180))
        grid_budget = max(1, (max_evaluations + max(1, len(timeframes)) - 1) // max(1, len(timeframes)))
        grid = _build_search_grid(body, profiles, max_evaluations=grid_budget)
        rows: list[dict[str, Any]] = list(existing_results)
        errors: list[dict[str, Any]] = []
        total_evaluations = min(max_evaluations, len(grid) * max(1, len(timeframes)))
        completed = len(rows)
        skipped = 0
        stop_requested = False

        for timeframe in timeframes:
            if stop_requested:
                break
            csv_path = Path(str(body.get(f"csv_path_{timeframe.lower()}") or csv_dir / f"{symbol}_{timeframe}_5000.csv"))
            if not csv_path.exists():
                errors.append({"timeframe": timeframe, "path": str(csv_path), "error": "csv_not_found"})
                continue
            csv_text = csv_path.read_text(encoding="utf-8-sig")
            base_settings = _settings(
                {
                    **body,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "csv_text": csv_text,
                    "max_bars": max_bars,
                    "save_results": False,
                    "timeout_seconds": per_evaluation_timeout,
                    "compare_filters": False,
                },
                self.config,
            )
            base_settings = replace(base_settings, timeout_seconds=max(0.01, min(per_evaluation_timeout, 20.0)))
            bars, warnings = _load_bars({"csv_text": csv_text}, base_settings)
            bars = bars[: base_settings.max_bars]
            if not bars:
                errors.append({"timeframe": timeframe, "path": str(csv_path), "error": "no_bars_loaded", "warnings": warnings})
                continue
            for config in grid:
                if completed + skipped >= max_evaluations:
                    stop_requested = True
                    break
                current = completed + skipped + 1
                if max_runtime_seconds > 0 and (time.monotonic() - started) >= max_runtime_seconds:
                    errors.append({"timeframe": timeframe, "profile": config.profile, "error": "max_runtime_seconds_reached"})
                    stop_requested = True
                    break
                settings = _settings_for_capital_config(base_settings, config)
                key = _result_key({"timeframe": settings.timeframe, "profile": settings.filter_profile, "parameters": _config_payload(config)})
                if key in existing_keys:
                    skipped += 1
                    continue
                if progress_callback and (current == 1 or current % progress_every == 0):
                    _safe_progress(
                        progress_callback,
                        {
                            "current": current,
                            "total": total_evaluations,
                            "timeframe": settings.timeframe,
                            "profile": settings.filter_profile,
                            "parameters": _config_payload(config),
                            "elapsed_seconds": round(time.monotonic() - started, 2),
                            "best_score": _best_score(rows),
                        },
                    )
                row = self._evaluate(settings, bars, config, source_csv=str(csv_path))
                rows.append(row)
                existing_keys.add(_result_key(row))
                completed += 1
                if incremental_callback and (completed == 1 or completed % progress_every == 0):
                    _safe_incremental(
                        incremental_callback,
                        {
                            "rows": rows,
                            "current": current,
                            "total": total_evaluations,
                            "interrupted": False,
                            "errors": errors,
                        },
                    )

        rows.sort(key=lambda item: (item["recommendation"] != "paper_forward_candidate", -float(item["capital_preservation_score"])))
        candidates = [item for item in rows if item["recommendation"] == "paper_forward_candidate"]
        recommendation = "paper_forward_candidate" if candidates else ("observation_only" if rows else "reject")
        return {
            "ok": True,
            "status": "mt5_capital_preservation_optimizer_completed",
            "symbol": symbol,
            "timeframes": timeframes,
            "profiles": profiles,
            "evaluations_requested": total_evaluations,
            "evaluations_completed": completed,
            "evaluations_skipped": skipped,
            "max_runtime_reached": stop_requested,
            "interrupted": False,
            "results": rows,
            "candidates": candidates,
            "best_profile": candidates[0] if candidates else (rows[0] if rows else None),
            "recommendation": recommendation if candidates else "reject",
            "genesis_reading": _summary_reading(candidates, rows),
            "errors": errors,
            "live_runtime_mutated": False,
            "promoted_profile_mutated": False,
            "forward_state_mutated": False,
            "shadow_trades_mutated": False,
            "martingale_enabled": False,
            "grid_enabled": False,
            "averaging_down_enabled": False,
            "increase_size_after_loss_enabled": False,
            **_safety(),
            "duration_ms": int((time.monotonic() - started) * 1000),
        }

    def _evaluate(
        self,
        settings: BacktestSettings,
        bars: list[dict[str, Any]],
        config: CapitalSearchConfig,
        *,
        source_csv: str,
    ) -> dict[str, Any]:
        started = time.monotonic()
        trades, no_trade_count, blocked, sim_state = _simulate_capital_preservation(settings, bars, config, started)
        timed_out = "timeout_guard" in blocked or (time.monotonic() - started) >= settings.timeout_seconds
        full = _metrics(trades, initial_balance=settings.initial_balance)
        windows = _critical_windows(settings, bars, trades)
        split = _split_metrics_from_trades(settings, bars, trades)
        monte_carlo = (
            _timeout_monte_carlo()
            if timed_out
            else _monte_carlo_stress(trades, initial_balance=settings.initial_balance, max_drawdown_limit=5000.0)
        )
        gate = _capital_candidate_gate(full, windows, split, monte_carlo, settings, sim_state)
        if timed_out and "timeout" not in gate["reasons"]:
            gate = {"passed": False, "reasons": ["timeout", *gate["reasons"]]}
        score = _capital_preservation_score(full, windows, split, monte_carlo, gate)
        recommendation = "paper_forward_candidate" if gate["passed"] else ("reject" if timed_out else "observation_only" if int(full.get("closed") or 0) else "reject")
        return {
            "timeframe": settings.timeframe,
            "profile": settings.filter_profile,
            "source_csv": source_csv,
            "parameters": _config_payload(config),
            "risk_reward": config.risk_reward,
            "time_stop_bars": config.time_stop_bars,
            "score_min": config.score_min,
            "spread_max": config.spread_max,
            "closed": full["closed"],
            "wins": full["wins"],
            "losses": full["losses"],
            "win_rate": full["win_rate"],
            "profit_factor": full["profit_factor"],
            "expectancy": full["expectancy"],
            "net_pnl": full["net_pnl"],
            "max_drawdown": full["max_drawdown"],
            "avg_win": full["avg_win"],
            "avg_loss": full["avg_loss"],
            "buy_pf": full["buy_pf"],
            "sell_pf": full["sell_pf"],
            "buy_win_rate": full["buy_win_rate"],
            "sell_win_rate": full["sell_win_rate"],
            "exit_reason_counts": full["exit_reason_counts"],
            "side_stats": full["side_stats"],
            "regime_stats": full["regime_stats"],
            "hour_stats": full["hour_stats"],
            "no_trade_count": no_trade_count,
            "blocked_reason_counts": _reason_counts(blocked),
            "timed_out": timed_out,
            "reject_reason": "timeout" if timed_out else "",
            "windows": windows,
            "train_pf": split["train_summary"].get("profit_factor", 0.0),
            "test_pf": split["test_summary"].get("profit_factor", 0.0),
            "train_expectancy": split["train_summary"].get("expectancy", 0.0),
            "test_expectancy": split["test_summary"].get("expectancy", 0.0),
            "train_drawdown": split["train_summary"].get("max_drawdown", 0.0),
            "test_drawdown": split["test_summary"].get("max_drawdown", 0.0),
            "train_trades": split["train_summary"].get("closed", 0),
            "test_trades": split["test_summary"].get("closed", 0),
            "walk_forward_results": split.get("walk_forward_results") or [],
            "monte_carlo": monte_carlo,
            "degraded": bool(sim_state.get("risk_lockdown")),
            "degradation_reason": sim_state.get("degradation_reason") or "",
            "capital_preservation_score": score,
            "recommendation": recommendation,
            "candidate": gate["passed"],
            "pass_fail_reasons": gate["reasons"],
            "guardrails": _paper_forward_guardrails(max_drawdown=3000.0 if full["max_drawdown"] <= 3000 else 5000.0),
            "risk_governor_compatible": True,
            "risk_governor_blocks": sim_state.get("risk_governor_blocks", 0),
            "max_open_trades_observed": sim_state.get("max_open_trades_observed", 0),
            "martingale_enabled": False,
            "grid_enabled": False,
            "averaging_down_enabled": False,
            "increase_size_after_loss_enabled": False,
            "applies_to_paper_shadow": recommendation == "paper_forward_candidate",
            "applies_to_real_trading": False,
            "live_runtime_mutated": False,
            "promoted_profile_mutated": False,
            "forward_state_mutated": False,
            "shadow_trades_mutated": False,
            **_safety(),
        }


def _simulate_capital_preservation(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    config: CapitalSearchConfig,
    started: float,
) -> tuple[list[dict[str, Any]], int, list[str], dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    blocked: list[str] = []
    no_trade_count = 0
    open_trade: dict[str, Any] | None = None
    cooldown_until = -1
    max_open = 0
    risk_blocks = 0
    iterations = 0
    max_iterations = len(bars) + 5

    for index in range(1, len(bars)):
        iterations += 1
        if iterations > max_iterations:
            blocked.append("loop_guard")
            break
        if _timed_out(started, settings.timeout_seconds):
            blocked.append("timeout_guard")
            break
        bar = bars[index]
        if open_trade:
            open_trade, closed = _update_trade_capital(settings, open_trade, bar, index, config)
            max_open = max(max_open, 1)
            if closed:
                trades.append(closed)
                open_trade = None
                if closed.get("status") == "loss":
                    cooldown_until = max(cooldown_until, index + config.cooldown_after_loss_bars)
        if index >= len(bars) - 1:
            continue
        if open_trade:
            continue
        if index < cooldown_until:
            no_trade_count += 1
            blocked.append("cooldown_after_loss")
            continue
        if _loss_streak(trades) >= config.block_after_consecutive_losses:
            cooldown_until = index + max(config.cooldown_after_loss_bars, config.block_after_consecutive_losses)
            no_trade_count += 1
            blocked.append("block_after_consecutive_losses")
            continue
        if config.no_trade_if_recent_edge_negative and _recent_edge_negative(trades):
            no_trade_count += 1
            blocked.append("recent_edge_negative")
            continue
        if config.no_trade_if_drawdown_accelerating and _drawdown_accelerating(trades, settings.initial_balance):
            no_trade_count += 1
            blocked.append("drawdown_accelerating")
            continue
        if config.max_adverse_excursion_filter and _recent_mae_bad(trades):
            no_trade_count += 1
            blocked.append("max_adverse_excursion_filter")
            continue
        pre_risk_reason = _fast_risk_block(settings, trades, config, "trend")
        if pre_risk_reason:
            risk_blocks += 1
            no_trade_count += 1
            blocked.append(f"risk_governor_{pre_risk_reason}")
            continue
        history = bars[max(0, index - 80) : index]
        decision = _capital_decision_from_history(history, settings, config)
        if config.volatility_filter and float(_number(decision.get("volatility_score")) or 0.0) < float(settings.filter_params.get("min_volatility_score") or 35.0):
            no_trade_count += 1
            blocked.append("volatility_too_low")
            continue
        if not decision["actionable"]:
            no_trade_count += 1
            blocked.append(str(decision.get("reason") or "no_edge"))
            continue
        risk_reason = _fast_risk_block(settings, trades, config, str(decision.get("regime") or "trend"), has_open_trade=bool(open_trade))
        if risk_reason:
            risk_blocks += 1
            no_trade_count += 1
            blocked.append(f"risk_governor_{risk_reason}")
            continue
        open_trade = _open_trade(settings, decision, bar, index, f"capital-{config.profile}-{index}")
        if open_trade is None:
            no_trade_count += 1
            blocked.append("missing_risk_parameters")
            continue
        open_trade = _apply_capital_trade_risk(open_trade, decision, settings, config)
        open_trade = {
            **open_trade,
            "source": "mt5_capital_preservation_optimizer",
            "capital_preservation_search": True,
            "filter_profile": config.profile,
            "strategy_profile": config.profile,
            "risk_governor_allowed": True,
            "risk_governor_reason": "risk_governor_pass",
            "risk_state": "normal",
            "suggested_lot_multiplier": 1.0,
            "trailing_stop_active": False,
            "virtual_stop_loss": open_trade.get("stop_loss"),
            "martingale_enabled": False,
            "grid_enabled": False,
            "averaging_down_enabled": False,
            "increase_size_after_loss_enabled": False,
        }
        max_open = max(max_open, 1)
    if open_trade:
        trades.append(_force_close(settings, open_trade, bars[-1], len(bars) - 1, "time_stop"))
    return trades, no_trade_count, blocked, {
        "risk_governor_blocks": risk_blocks,
        "max_open_trades_observed": max_open,
        "iterations": iterations,
        "max_iterations": max_iterations,
        "risk_lockdown": False,
        "degradation_reason": "",
    }


def _capital_decision_from_history(history: list[dict[str, Any]], settings: BacktestSettings, config: CapitalSearchConfig) -> dict[str, Any]:
    params = settings.filter_params or {}
    family = str(params.get("strategy_family") or "legacy").strip().casefold()
    if family == "legacy":
        return _decision_from_history(history, settings)
    features = _market_features(history)
    if not features:
        return {"actionable": False, "reason": "insufficient_history"}
    common_block = _common_entry_block(features, settings, config, params)
    if common_block:
        return _blocked_decision(common_block, features)
    if family == "breakout_pullback":
        return _breakout_pullback_decision(features, settings, params)
    if family == "trend_continuation":
        return _trend_continuation_decision(features, settings, params)
    if family == "mean_reversion":
        return _mean_reversion_decision(features, settings, params)
    if family == "volatility_squeeze":
        return _volatility_squeeze_decision(features, settings, params)
    if family == "liquidity_sweep_reversal":
        return _liquidity_sweep_decision(features, settings, params)
    if family == "ema_rsi_confirmed":
        return _ema_rsi_decision(features, settings, params)
    return _decision_from_history(history, settings)


def _market_features(history: list[dict[str, Any]]) -> dict[str, Any]:
    if len(history) < 20:
        return {}
    closes = [float(row["close"]) for row in history if _number(row.get("close")) is not None]
    highs = [float(row["high"]) for row in history if _number(row.get("high")) is not None]
    lows = [float(row["low"]) for row in history if _number(row.get("low")) is not None]
    opens = [float(row["open"]) for row in history if _number(row.get("open")) is not None]
    if len(closes) < 20 or len(highs) < 20 or len(lows) < 20 or len(opens) < 20:
        return {}
    close = closes[-1]
    open_price = opens[-1]
    prev_close = closes[-2]
    ema20 = _ema(closes, min(20, len(closes)))
    ema50 = _ema(closes, min(50, len(closes)))
    rsi = _rsi(closes, min(14, max(2, len(closes) - 1)))
    atr = _atr(highs, lows, closes, min(14, len(closes) - 1))
    atr_pct = (atr / close) * 100 if close else 0.0
    momentum = close - closes[max(0, len(closes) - 4)]
    momentum_pct = (momentum / closes[max(0, len(closes) - 4)]) * 100 if closes[max(0, len(closes) - 4)] else 0.0
    trend_score = 50.0
    trend_score += 15.0 if close > ema20 else -15.0
    trend_score += 12.0 if ema20 > ema50 else -12.0
    trend_score += min(12.0, abs(ema20 - ema50) / close * 2200.0) * (1 if ema20 > ema50 else -1)
    trend_score = min(90.0, max(10.0, trend_score))
    momentum_score = min(90.0, max(10.0, 50.0 + momentum_pct * 35.0))
    recent_range = max(highs[-12:]) - min(lows[-12:])
    previous_range = max(highs[-28:-12] or highs[-12:]) - min(lows[-28:-12] or lows[-12:])
    volatility_score = min(90.0, max(10.0, atr_pct * 45.0))
    regime = "trend" if abs(ema20 - ema50) / close * 100 >= 0.15 and atr_pct >= 0.20 else "chop"
    recent_high = max(highs[-13:-1])
    recent_low = min(lows[-13:-1])
    prior_high = max(highs[-24:-8] or highs[-13:-1])
    prior_low = min(lows[-24:-8] or lows[-13:-1])
    high = highs[-1]
    low = lows[-1]
    body = abs(close - open_price)
    candle_range = max(high - low, 0.000001)
    distance20_atr = abs(close - ema20) / atr if atr else 0.0
    distance50_atr = abs(close - ema50) / atr if atr else 0.0
    hour = _hour_from_bar(history[-1])
    return {
        "close": close,
        "open": open_price,
        "high": high,
        "low": low,
        "prev_close": prev_close,
        "ema20": ema20,
        "ema50": ema50,
        "rsi": rsi,
        "atr": atr,
        "atr_pct": atr_pct,
        "momentum_pct": momentum_pct,
        "trend_score": trend_score,
        "momentum_score": momentum_score,
        "volatility_score": volatility_score,
        "regime": regime,
        "recent_high": recent_high,
        "recent_low": recent_low,
        "prior_high": prior_high,
        "prior_low": prior_low,
        "recent_range": recent_range,
        "previous_range": previous_range,
        "body_atr": body / atr if atr else 0.0,
        "body_ratio": body / candle_range if candle_range else 0.0,
        "distance20_atr": distance20_atr,
        "distance50_atr": distance50_atr,
        "hour": hour,
    }


def _common_entry_block(features: dict[str, Any], settings: BacktestSettings, config: CapitalSearchConfig, params: dict[str, Any]) -> str:
    if settings.spread_points > config.spread_max:
        return "spread_too_high"
    if config.session_filter or bool(params.get("session_filter")):
        allowed = params.get("session_hours") if isinstance(params.get("session_hours"), list) else [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        if features.get("hour") is not None and int(features["hour"]) not in {int(item) for item in allowed}:
            return "session_filter"
    min_vol = float(_number(params.get("min_volatility_score")) or 0.0)
    if config.volatility_filter and features["volatility_score"] < min_vol:
        return "volatility_too_low"
    allowed_regime = str(params.get("allowed_regime") or "").casefold()
    if allowed_regime and features["regime"] != allowed_regime:
        return "market_regime_filter"
    if config.anti_chop_filter and bool(params.get("avoid_chop")) and features["regime"] == "chop" and str(params.get("strategy_family")) not in {"mean_reversion", "liquidity_sweep_reversal", "volatility_squeeze"}:
        return "regime_chop"
    if features["body_atr"] > float(_number(params.get("extended_candle_atr")) or 1.8):
        return "extended_candle_no_chase"
    if features["distance20_atr"] > float(_number(params.get("ema_distance_max_atr")) or 2.4):
        return "ema_distance_too_far"
    return ""


def _breakout_pullback_decision(features: dict[str, Any], settings: BacktestSettings, params: dict[str, Any]) -> dict[str, Any]:
    min_score = float(_number(params.get("min_score")) or settings.min_score)
    score = _score(features, trend_weight=0.42, momentum_weight=0.34, volatility_weight=0.24)
    broke_up = features["recent_high"] > features["prior_high"]
    broke_down = features["recent_low"] < features["prior_low"]
    pullback_buy = features["low"] <= features["ema20"] + features["atr"] * 0.45 and features["close"] > features["ema20"] and features["close"] > features["prev_close"]
    pullback_sell = features["high"] >= features["ema20"] - features["atr"] * 0.45 and features["close"] < features["ema20"] and features["close"] < features["prev_close"]
    if broke_up and pullback_buy and features["trend_score"] >= params.get("min_trend_score", 55) and features["rsi"] <= params.get("max_rsi_for_buy", 74) and score >= min_score:
        return _action("buy", score, "breakout_pullback_confirmed", features)
    if broke_down and pullback_sell and (100 - features["trend_score"]) >= 35 and features["rsi"] >= params.get("min_rsi_for_sell", 26) and score >= min_score:
        return _action("sell", score, "breakout_pullback_confirmed", features)
    return _blocked_decision("pullback_not_confirmed", features, score)


def _trend_continuation_decision(features: dict[str, Any], settings: BacktestSettings, params: dict[str, Any]) -> dict[str, Any]:
    min_score = float(_number(params.get("min_score")) or settings.min_score)
    score = _score(features, trend_weight=0.48, momentum_weight=0.34, volatility_weight=0.18)
    buy = features["close"] > features["ema20"] > features["ema50"] and features["momentum_score"] >= params.get("min_momentum_score", 52) and features["rsi"] < params.get("max_rsi_for_buy", 76)
    sell = features["close"] < features["ema20"] < features["ema50"] and features["momentum_score"] <= 45 and features["rsi"] > params.get("min_rsi_for_sell", 24)
    if buy and features["trend_score"] >= params.get("min_trend_score", 58) and score >= min_score:
        return _action("buy", score, "trend_continuation_confirmed", features)
    if sell and (100 - features["trend_score"]) >= 38 and score >= min_score:
        return _action("sell", score, "trend_continuation_confirmed", features)
    return _blocked_decision("trend_continuation_not_confirmed", features, score)


def _mean_reversion_decision(features: dict[str, Any], settings: BacktestSettings, params: dict[str, Any]) -> dict[str, Any]:
    min_score = float(_number(params.get("min_score")) or settings.min_score)
    reversion_score = min(90.0, max(10.0, 45.0 + features["distance20_atr"] * 12.0 + (50.0 - abs(features["rsi"] - 50.0)) * 0.25))
    buy = features["regime"] == "chop" and features["rsi"] <= params.get("max_rsi_for_buy", 42) and features["close"] > features["prev_close"] and features["low"] < features["ema20"] - features["atr"] * 0.45
    sell = features["regime"] == "chop" and features["rsi"] >= params.get("min_rsi_for_sell", 58) and features["close"] < features["prev_close"] and features["high"] > features["ema20"] + features["atr"] * 0.45
    if buy and reversion_score >= min_score:
        return _action("buy", reversion_score, "mean_reversion_confirmed", features)
    if sell and reversion_score >= min_score:
        return _action("sell", reversion_score, "mean_reversion_confirmed", features)
    return _blocked_decision("mean_reversion_not_confirmed", features, reversion_score)


def _volatility_squeeze_decision(features: dict[str, Any], settings: BacktestSettings, params: dict[str, Any]) -> dict[str, Any]:
    min_score = float(_number(params.get("min_score")) or settings.min_score)
    compressed = features["previous_range"] > 0 and features["recent_range"] <= features["previous_range"] * 0.75
    score = _score(features, trend_weight=0.25, momentum_weight=0.48, volatility_weight=0.27)
    if compressed and features["close"] > features["recent_high"] and features["momentum_score"] >= 56 and score >= min_score and features["rsi"] < params.get("max_rsi_for_buy", 75):
        return _action("buy", score, "volatility_squeeze_breakout", features)
    if compressed and features["close"] < features["recent_low"] and features["momentum_score"] <= 44 and score >= min_score and features["rsi"] > params.get("min_rsi_for_sell", 25):
        return _action("sell", score, "volatility_squeeze_breakdown", features)
    return _blocked_decision("squeeze_breakout_not_confirmed", features, score)


def _liquidity_sweep_decision(features: dict[str, Any], settings: BacktestSettings, params: dict[str, Any]) -> dict[str, Any]:
    min_score = float(_number(params.get("min_score")) or settings.min_score)
    score = min(90.0, max(10.0, 55.0 + features["body_ratio"] * 20.0 + features["volatility_score"] * 0.15))
    swept_low = features["low"] < features["recent_low"] and features["close"] > features["recent_low"] and features["close"] > features["open"]
    swept_high = features["high"] > features["recent_high"] and features["close"] < features["recent_high"] and features["close"] < features["open"]
    if swept_low and features["rsi"] <= params.get("max_rsi_for_buy", 46) and score >= min_score:
        return _action("buy", score, "liquidity_sweep_reversal", features)
    if swept_high and features["rsi"] >= params.get("min_rsi_for_sell", 54) and score >= min_score:
        return _action("sell", score, "liquidity_sweep_reversal", features)
    return _blocked_decision("liquidity_sweep_not_confirmed", features, score)


def _ema_rsi_decision(features: dict[str, Any], settings: BacktestSettings, params: dict[str, Any]) -> dict[str, Any]:
    min_score = float(_number(params.get("min_score")) or settings.min_score)
    score = _score(features, trend_weight=0.40, momentum_weight=0.35, volatility_weight=0.25)
    buy = features["close"] > features["ema20"] > features["ema50"] and 45 <= features["rsi"] <= params.get("max_rsi_for_buy", 68) and features["momentum_score"] >= params.get("min_momentum_score", 50)
    sell = features["close"] < features["ema20"] < features["ema50"] and params.get("min_rsi_for_sell", 32) <= features["rsi"] <= 55 and features["momentum_score"] <= 48
    if buy and score >= min_score:
        return _action("buy", score, "ema_rsi_confirmed", features)
    if sell and score >= min_score:
        return _action("sell", score, "ema_rsi_confirmed", features)
    return _blocked_decision("ema_rsi_not_confirmed", features, score)


def _score(features: dict[str, Any], *, trend_weight: float, momentum_weight: float, volatility_weight: float) -> float:
    return round(
        features["trend_score"] * trend_weight
        + features["momentum_score"] * momentum_weight
        + features["volatility_score"] * volatility_weight,
        2,
    )


def _action(side: str, score: float, reason: str, features: dict[str, Any]) -> dict[str, Any]:
    return {
        "actionable": True,
        "side": side,
        "score": round(score, 2),
        "raw_score": round(score, 2),
        "trend_score": round(features["trend_score"], 2),
        "momentum_score": round(features["momentum_score"], 2),
        "volatility_score": round(features["volatility_score"], 2),
        "regime": features["regime"],
        "confidence": "medium" if score >= 65 else "low",
        "reason": reason,
        "rsi": round(features["rsi"], 2),
        "ema20": round(features["ema20"], 6),
        "ema50": round(features["ema50"], 6),
        "atr": round(features["atr"], 6),
        "atr_pct": round(features["atr_pct"], 6),
    }


def _blocked_decision(reason: str, features: dict[str, Any] | None = None, score: float | None = None) -> dict[str, Any]:
    features = features or {}
    return {
        "actionable": False,
        "reason": reason,
        "score": round(float(score if score is not None else features.get("trend_score") or 0.0), 2),
        "trend_score": round(float(features.get("trend_score") or 0.0), 2),
        "momentum_score": round(float(features.get("momentum_score") or 0.0), 2),
        "volatility_score": round(float(features.get("volatility_score") or 0.0), 2),
        "regime": features.get("regime") or "",
        "rsi": round(float(features.get("rsi") or 0.0), 2),
    }


def _update_trade_capital(
    settings: BacktestSettings,
    trade: dict[str, Any],
    bar: dict[str, Any],
    index: int,
    config: CapitalSearchConfig,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    high = float(_number(bar.get("high")) or _number(bar.get("close")) or 0.0)
    low = float(_number(bar.get("low")) or _number(bar.get("close")) or 0.0)
    close = float(_number(bar.get("close")) or 0.0)
    side = str(trade.get("side") or "").lower()
    entry = float(_number(trade.get("entry_price")) or _number(trade.get("entry")) or close)
    stop_loss = float(_number(trade.get("stop_loss")) or entry)
    take_profit = float(_number(trade.get("take_profit")) or entry)
    risk = abs(entry - stop_loss) or max(entry * 0.015, 0.000001)
    if side == "buy":
        mfe = high - entry
        mae = low - entry
        stop_hit = low <= stop_loss
        target_hit = high >= take_profit
    else:
        mfe = entry - low
        mae = entry - high
        stop_hit = high >= stop_loss
        target_hit = low <= take_profit
    updated = {
        **trade,
        "last_price": close,
        "max_favorable_excursion": round(max(float(_number(trade.get("max_favorable_excursion")) or 0.0), mfe), 6),
        "max_adverse_excursion": round(min(float(_number(trade.get("max_adverse_excursion")) or 0.0), mae), 6),
        "bars_open": max(0, index - int(trade.get("opened_index") or index)),
        "updated_at": str(bar.get("time") or ""),
        **_safety(),
    }
    if stop_hit and target_hit:
        return {}, _close_capital(settings, updated, stop_loss, "stop_loss", bar)
    if stop_hit:
        return {}, _close_capital(settings, updated, stop_loss, "stop_loss", bar)
    if target_hit:
        return {}, _close_capital(settings, updated, take_profit, "take_profit", bar)
    if config.partial_exit and not bool(updated.get("partial_exit_taken")):
        best = float(_number(updated.get("max_favorable_excursion")) or 0.0)
        if best >= risk * 0.85:
            updated["partial_exit_taken"] = True
            updated["partial_pnl"] = round(risk * 0.35, 6)
            updated["virtual_stop_loss"] = entry
    if config.trailing_stop or config.atr_trailing:
        best = float(_number(updated.get("max_favorable_excursion")) or 0.0)
        if best >= risk * config.trailing_start_r:
            atr = float(_number(updated.get("atr_at_entry")) or risk)
            trail_distance = atr * 1.15 if config.atr_trailing else risk * config.trailing_distance_r
            if side == "buy":
                virtual_stop = max(float(_number(updated.get("virtual_stop_loss")) or stop_loss), close - trail_distance, entry)
                updated["virtual_stop_loss"] = round(virtual_stop, 6)
                updated["trailing_stop_active"] = True
                if low <= virtual_stop:
                    return {}, _close_capital(settings, updated, virtual_stop, "trailing_stop", bar)
            else:
                virtual_stop = min(float(_number(updated.get("virtual_stop_loss")) or stop_loss), close + trail_distance, entry)
                updated["virtual_stop_loss"] = round(virtual_stop, 6)
                updated["trailing_stop_active"] = True
                if high >= virtual_stop:
                    return {}, _close_capital(settings, updated, virtual_stop, "trailing_stop", bar)
    time_stop_limit = settings.time_stop_bars
    if config.adaptive_time_stop and float(_number(updated.get("max_favorable_excursion")) or 0.0) >= risk * 0.35:
        time_stop_limit += 1
    if int(updated.get("bars_open") or 0) >= time_stop_limit:
        return {}, _close_capital(settings, updated, close, "time_stop", bar)
    return updated, None


def _close_capital(settings: BacktestSettings, trade: dict[str, Any], exit_price: float, reason: str, bar: dict[str, Any]) -> dict[str, Any]:
    closed = _close(settings, trade, exit_price, reason, bar)
    partial_pnl = float(_number(trade.get("partial_pnl")) or 0.0)
    if partial_pnl:
        pnl = float(_number(closed.get("pnl")) or 0.0) + partial_pnl
        entry = float(_number(closed.get("entry_price")) or _number(closed.get("entry")) or 0.0)
        risk = abs(float(_number(closed.get("initial_risk")) or 0.0)) or max(entry * 0.015, 0.000001)
        closed = {
            **closed,
            "pnl": round(pnl, 6),
            "pnl_pct": round((pnl / entry) * 100, 6) if entry else 0.0,
            "r_multiple": round(pnl / risk, 6) if risk else 0.0,
            "status": "win" if pnl > 0 else "loss" if pnl < 0 else "breakeven",
            "partial_exit_taken": True,
            "partial_pnl": round(partial_pnl, 6),
        }
    return closed


def _apply_capital_trade_risk(
    trade: dict[str, Any],
    decision: dict[str, Any],
    settings: BacktestSettings,
    config: CapitalSearchConfig,
) -> dict[str, Any]:
    atr = float(_number(decision.get("atr")) or 0.0)
    params = settings.filter_params or {}
    multiplier = float(_number(params.get("atr_stop_multiplier")) or 0.0)
    if atr <= 0 or multiplier <= 0:
        return trade
    side = str(trade.get("side") or "").lower()
    entry = float(_number(trade.get("entry_price")) or _number(trade.get("entry")) or 0.0)
    if entry <= 0:
        return trade
    stop_distance = atr * multiplier
    stop_distance = max(entry * 0.0035, min(stop_distance, entry * 0.018))
    stop = entry - stop_distance if side == "buy" else entry + stop_distance
    target = entry + stop_distance * settings.min_rr if side == "buy" else entry - stop_distance * settings.min_rr
    return {
        **trade,
        "stop_loss": round(stop, 6),
        "take_profit": round(target, 6),
        "initial_risk": round(stop_distance, 6),
        "risk_reward": settings.min_rr,
        "atr_at_entry": round(atr, 6),
        "virtual_stop_loss": round(stop, 6),
        "features_snapshot": {
            **(trade.get("features_snapshot") if isinstance(trade.get("features_snapshot"), dict) else {}),
            "atr": round(atr, 6),
            "atr_pct": decision.get("atr_pct"),
            "strategy_family": (settings.filter_params or {}).get("strategy_family") or "legacy",
        },
        "partial_exit_enabled": bool(config.partial_exit),
        "atr_trailing_enabled": bool(config.atr_trailing),
        "adaptive_time_stop": bool(config.adaptive_time_stop),
    }


def _settings_for_capital_config(settings: BacktestSettings, config: CapitalSearchConfig) -> BacktestSettings:
    params = dict(_PROFILE_PARAMS[config.profile])
    params["min_score"] = config.score_min
    params["max_spread_points"] = config.spread_max
    params["avoid_chop"] = bool(config.anti_chop_filter)
    params["session_filter"] = bool(config.session_filter or params.get("session_filter"))
    params["partial_exit"] = bool(config.partial_exit or params.get("partial_exit"))
    params["atr_trailing"] = bool(config.atr_trailing or params.get("atr_trailing"))
    params["adaptive_time_stop"] = bool(config.adaptive_time_stop)
    if not config.volatility_filter:
        params["min_volatility_score"] = 0.0
    return replace(
        settings,
        filter_profile=config.profile,
        filter_params=params,
        min_score=config.score_min,
        max_spread_points=config.spread_max,
        min_rr=float(config.risk_reward),
        time_stop_bars=max(1, int(config.time_stop_bars)),
        risk_pct=float(config.risk_pct),
    )


def _build_search_grid(body: dict[str, Any], profiles: list[str], *, max_evaluations: int) -> list[CapitalSearchConfig]:
    rr_values = _requested_numbers(body.get("risk_reward_values") or body.get("rr_values"), _DEFAULT_RR_VALUES)
    time_stop_values = [int(value) for value in _requested_numbers(body.get("time_stop_bars"), _DEFAULT_TIME_STOP_BARS)]
    score_values = _requested_numbers(body.get("score_min_values"), _DEFAULT_SCORE_MIN)
    spread_values = _requested_numbers(body.get("spread_max_values"), _DEFAULT_SPREAD_MAX)
    cooldown_values = [int(value) for value in _requested_numbers(body.get("cooldown_after_loss_values"), [1, 2, 3, 4])]
    block_values = [int(value) for value in _requested_numbers(body.get("block_after_consecutive_losses_values"), [2, 3])]
    risk_values = _requested_numbers(body.get("risk_pct_values"), [0.05, 0.1, 0.2])
    configs: list[CapitalSearchConfig] = []

    for profile in profiles:
        base = _PROFILE_PARAMS[profile]
        default_score = float(base.get("min_score") or 65.0)
        default_spread = float(base.get("max_spread_points") or 25.0)
        for rr in rr_values:
            for bars in time_stop_values:
                configs.append(_config(profile, rr, bars, default_score, default_spread, True, True, 2, 2, True, True, True, True, False, False, bool(base.get("atr_trailing")), True, risk_values[0]))
        for score in score_values:
            for spread in spread_values:
                configs.append(_config(profile, 1.2, 3, score, spread, True, True, 2, 2, True, True, True, True, False, False, bool(base.get("atr_trailing")), True, risk_values[0]))
        for cooldown in cooldown_values:
            for block in block_values:
                configs.append(_config(profile, 1.0, 2, default_score, default_spread, True, True, cooldown, block, True, True, True, True, False, False, bool(base.get("atr_trailing")), True, risk_values[0]))
        for trailing in [False, True]:
            for vol_filter in [False, True]:
                for anti_chop in [False, True]:
                    configs.append(_config(profile, 1.2, 3, default_score, default_spread, vol_filter, anti_chop, 2, 2, trailing, True, True, True, False, bool(base.get("partial_exit")), bool(base.get("atr_trailing")), True, risk_values[0]))
        for session in [False, True]:
            for partial in [False, True]:
                configs.append(_config(profile, 1.2, 3, default_score, default_spread, True, True, 2, 2, True, True, True, True, session or bool(base.get("session_filter")), partial or bool(base.get("partial_exit")), bool(base.get("atr_trailing")), True, risk_values[0]))
        for risk_pct in risk_values:
            configs.append(_config(profile, 1.2, 3, default_score, default_spread, True, True, 2, 2, True, True, True, True, False, bool(base.get("partial_exit")), bool(base.get("atr_trailing")), True, risk_pct))

    unique: dict[str, CapitalSearchConfig] = {}
    for item in configs:
        unique.setdefault(item.key(), item)
    ordered = list(unique.values())
    ordered.sort(key=lambda item: (item.spread_max, -item.score_min, item.risk_reward, item.time_stop_bars, item.profile))
    return ordered[: max(1, max_evaluations)]


def _config(
    profile: str,
    rr: float,
    bars: int,
    score: float,
    spread: float,
    vol_filter: bool,
    anti_chop: bool,
    cooldown: int,
    block: int,
    trailing: bool,
    mae_filter: bool,
    edge_filter: bool,
    dd_filter: bool,
    session_filter: bool,
    partial_exit: bool,
    atr_trailing: bool,
    adaptive_time_stop: bool,
    risk_pct: float,
) -> CapitalSearchConfig:
    return CapitalSearchConfig(
        profile=profile,
        risk_reward=float(rr),
        time_stop_bars=max(1, int(bars)),
        score_min=float(score),
        spread_max=float(spread),
        volatility_filter=bool(vol_filter),
        anti_chop_filter=bool(anti_chop),
        cooldown_after_loss_bars=max(1, int(cooldown)),
        block_after_consecutive_losses=max(2, int(block)),
        trailing_stop=bool(trailing),
        max_adverse_excursion_filter=bool(mae_filter),
        no_trade_if_recent_edge_negative=bool(edge_filter),
        no_trade_if_drawdown_accelerating=bool(dd_filter),
        session_filter=bool(session_filter),
        partial_exit=bool(partial_exit),
        atr_trailing=bool(atr_trailing),
        adaptive_time_stop=bool(adaptive_time_stop),
        risk_pct=float(risk_pct),
    )


def _critical_windows(settings: BacktestSettings, bars: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    midpoint = max(3, len(bars) // 2)
    windows = {
        "first_half": (0, midpoint),
        "second_half": (midpoint, len(bars)),
        "last_1000": (max(0, len(bars) - 1000), len(bars)),
        "last_2000": (max(0, len(bars) - 2000), len(bars)),
    }
    payload: dict[str, dict[str, Any]] = {}
    for name, (start, end) in windows.items():
        scoped = [
            trade
            for trade in trades
            if trade.get("lifecycle_status") == "closed"
            and start <= int(_number(trade.get("opened_index")) or 0) < end
        ]
        summary = _metrics(scoped, initial_balance=settings.initial_balance)
        payload[name] = {
            "closed": summary["closed"],
            "profit_factor": summary["profit_factor"],
            "expectancy": summary["expectancy"],
            "max_drawdown": summary["max_drawdown"],
            "win_rate": summary["win_rate"],
        }
    return payload


def _split_metrics_from_trades(settings: BacktestSettings, bars: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, Any]:
    train_count = max(3, min(len(bars) - 2, int(len(bars) * 0.6))) if len(bars) >= 6 else max(1, len(bars) // 2)
    train_trades = [
        trade
        for trade in trades
        if trade.get("lifecycle_status") == "closed"
        and int(_number(trade.get("opened_index")) or 0) < train_count
    ]
    test_trades = [
        trade
        for trade in trades
        if trade.get("lifecycle_status") == "closed"
        and int(_number(trade.get("opened_index")) or 0) >= train_count
    ]
    train_summary = _metrics(train_trades, initial_balance=settings.initial_balance)
    test_summary = _metrics(test_trades, initial_balance=settings.initial_balance)
    rolling = _rolling_windows_from_trades(settings, bars, trades)
    return {
        "train_summary": train_summary,
        "test_summary": test_summary,
        "train_no_trade_count": 0,
        "test_no_trade_count": 0,
        "train_blocked": [],
        "test_blocked": [],
        "walk_forward_results": rolling,
    }


def _rolling_windows_from_trades(settings: BacktestSettings, bars: list[dict[str, Any]], trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    train_window = min(500, max(50, len(bars) // 4))
    test_window = min(250, max(25, len(bars) // 8))
    if len(bars) < train_window + test_window:
        return []
    windows: list[dict[str, Any]] = []
    start = 0
    window_index = 1
    while start + train_window + test_window <= len(bars) and window_index <= 8:
        train_start = start
        train_end = start + train_window
        test_start = train_end
        test_end = train_end + test_window
        train_trades = [
            trade
            for trade in trades
            if trade.get("lifecycle_status") == "closed"
            and train_start <= int(_number(trade.get("opened_index")) or 0) < train_end
        ]
        test_trades = [
            trade
            for trade in trades
            if trade.get("lifecycle_status") == "closed"
            and test_start <= int(_number(trade.get("opened_index")) or 0) < test_end
        ]
        train_summary = _metrics(train_trades, initial_balance=settings.initial_balance)
        test_summary = _metrics(test_trades, initial_balance=settings.initial_balance)
        windows.append(
            {
                "window": window_index,
                "train_bars": train_window,
                "test_bars": test_window,
                "train_pf": train_summary["profit_factor"],
                "test_pf": test_summary["profit_factor"],
                "train_expectancy": train_summary["expectancy"],
                "test_expectancy": test_summary["expectancy"],
                "train_drawdown": train_summary["max_drawdown"],
                "test_drawdown": test_summary["max_drawdown"],
                "train_trades": train_summary["closed"],
                "test_trades": test_summary["closed"],
            }
        )
        start += test_window
        window_index += 1
    return windows


def _capital_candidate_gate(
    full: dict[str, Any],
    windows: dict[str, dict[str, Any]],
    split: dict[str, Any],
    monte_carlo: dict[str, Any],
    settings: BacktestSettings,
    sim_state: dict[str, Any],
) -> dict[str, Any]:
    reasons: list[str] = []
    min_closed = 50 if settings.timeframe.upper().startswith("H") else 75
    closed = int(full.get("closed") or 0)
    if closed < min_closed:
        reasons.append("sample_too_small")
    if float(full.get("profit_factor") or 0.0) < 1.20:
        reasons.append("pf_below_1_20")
    if float(full.get("expectancy") or 0.0) <= 0:
        reasons.append("expectancy_not_positive")
    if float(full.get("win_rate") or 0.0) < 45.0:
        reasons.append("win_rate_below_45")
    if float(full.get("max_drawdown") or 0.0) > 5000.0:
        reasons.append("drawdown_above_5000")
    for name, metrics in windows.items():
        if int(metrics.get("closed") or 0) >= 10 and float(metrics.get("profit_factor") or 0.0) < 1.0:
            reasons.append(f"{name}_pf_below_1")
        if int(metrics.get("closed") or 0) >= 10 and float(metrics.get("expectancy") or 0.0) < -0.05:
            reasons.append(f"{name}_expectancy_strong_negative")
    for window in split.get("walk_forward_results") or []:
        if int(window.get("test_trades") or 0) >= 10 and float(window.get("test_pf") or 0.0) < 1.0:
            reasons.append("walk_forward_test_pf_below_1")
            break
    test_summary = split.get("test_summary") if isinstance(split.get("test_summary"), dict) else {}
    if int(test_summary.get("closed") or 0) >= 10 and float(test_summary.get("profit_factor") or 0.0) < 1.0:
        reasons.append("test_pf_below_1")
    if int(full.get("side_stats", {}).get("buy", {}).get("trades", 0)) >= 10 and float(full.get("buy_pf") or 0.0) < 0.8:
        reasons.append("buy_side_weak")
    if int(full.get("side_stats", {}).get("sell", {}).get("trades", 0)) >= 10 and float(full.get("sell_pf") or 0.0) < 0.8:
        reasons.append("sell_side_weak")
    if _depends_on_single_trade(full):
        reasons.append("single_trade_dependency")
    if bool(sim_state.get("risk_lockdown")):
        reasons.append("risk_lockdown")
    if not monte_carlo.get("passed"):
        reasons.extend([f"monte_carlo_{reason}" for reason in monte_carlo.get("fail_reasons", [])])
    return {"passed": not reasons, "reasons": reasons or ["passes_capital_preservation_rules"]}


def _monte_carlo_stress(
    trades: list[dict[str, Any]],
    *,
    initial_balance: float,
    max_drawdown_limit: float,
    simulations: int = 1000,
) -> dict[str, Any]:
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    pnls = [float(_number(trade.get("pnl")) or 0.0) for trade in closed]
    if not pnls:
        return {
            "passed": False,
            "fail_reasons": ["no_closed_trades"],
            "simulations": simulations,
            "risk_of_ruin": 1.0,
            "max_drawdown_p95": 0.0,
            "profit_factor_stressed": 0.0,
            "expectancy_stressed": 0.0,
            "worst_loss_streak": 0,
        }
    rng = random.Random(20260521)
    drawdowns: list[float] = []
    ruin_count = 0
    for _ in range(simulations):
        sample = [rng.choice(pnls) for _ in pnls]
        drawdown = _max_drawdown_from_pnls(sample, initial_balance)
        drawdowns.append(drawdown)
        if drawdown > max_drawdown_limit:
            ruin_count += 1
    drawdowns.sort()
    p95_index = min(len(drawdowns) - 1, int(len(drawdowns) * 0.95))
    stressed = _stress_pnls(pnls)
    pf_stressed = _profit_factor(stressed)
    expectancy_stressed = sum(stressed) / len(stressed) if stressed else 0.0
    fail_reasons: list[str] = []
    risk_of_ruin = ruin_count / simulations
    if risk_of_ruin > 0.05:
        fail_reasons.append("risk_of_ruin_high")
    if drawdowns[p95_index] > max_drawdown_limit:
        fail_reasons.append("drawdown_p95_above_limit")
    if pf_stressed < 1.05:
        fail_reasons.append("stressed_pf_below_1_05")
    if expectancy_stressed < 0:
        fail_reasons.append("stressed_expectancy_negative")
    return {
        "passed": not fail_reasons,
        "fail_reasons": fail_reasons,
        "simulations": simulations,
        "risk_of_ruin": round(risk_of_ruin, 4),
        "max_drawdown_p95": round(drawdowns[p95_index], 6),
        "profit_factor_stressed": round(pf_stressed, 4),
        "expectancy_stressed": round(expectancy_stressed, 6),
        "worst_loss_streak": _worst_loss_streak(pnls),
        "removed_best_5": len(pnls) >= 5,
        "stress_rules": ["shuffle", "bootstrap", "double_spread_slippage_proxy", "reduce_tp_effective", "increase_losses_10pct", "remove_best_5"],
    }


def _timeout_monte_carlo() -> dict[str, Any]:
    return {
        "passed": False,
        "fail_reasons": ["timeout"],
        "simulations": 0,
        "risk_of_ruin": 1.0,
        "max_drawdown_p95": 0.0,
        "profit_factor_stressed": 0.0,
        "expectancy_stressed": 0.0,
        "worst_loss_streak": 0,
        "removed_best_5": False,
        "stress_rules": [],
    }


def _capital_preservation_score(
    full: dict[str, Any],
    windows: dict[str, dict[str, Any]],
    split: dict[str, Any],
    monte_carlo: dict[str, Any],
    gate: dict[str, Any],
) -> float:
    pf = float(full.get("profit_factor") or 0.0)
    expectancy = float(full.get("expectancy") or 0.0)
    win_rate = float(full.get("win_rate") or 0.0)
    closed = int(full.get("closed") or 0)
    drawdown = float(full.get("max_drawdown") or 0.0)
    test_summary = split.get("test_summary") if isinstance(split.get("test_summary"), dict) else {}
    test_pf = float(test_summary.get("profit_factor") or 0.0)
    score = 0.0
    score += max(0.0, min(pf, 2.5) - 1.0) * 130.0
    score += max(0.0, expectancy) * 650.0
    score += min(closed, 240) * 0.32
    score += max(0.0, win_rate - 45.0) * 0.8
    score += max(0.0, min(test_pf, 2.0) - 1.0) * 65.0
    score -= max(0.0, 1.2 - pf) * 180.0
    score -= drawdown / 55.0
    score -= max(0, 75 - closed) * 5.0
    score -= float(monte_carlo.get("risk_of_ruin") or 0.0) * 250.0
    score -= max(0.0, float(monte_carlo.get("max_drawdown_p95") or 0.0) - 3000.0) / 35.0
    for metrics in windows.values():
        if int(metrics.get("closed") or 0) >= 10:
            score -= max(0.0, 1.0 - float(metrics.get("profit_factor") or 0.0)) * 55.0
            score -= max(0.0, -float(metrics.get("expectancy") or 0.0)) * 320.0
    if not monte_carlo.get("passed"):
        score -= 200.0
    if not gate.get("passed"):
        score -= 25.0 * len(gate.get("reasons") or [])
    return round(score, 4)


def _depends_on_single_trade(full: dict[str, Any]) -> bool:
    best = full.get("best_trade") if isinstance(full.get("best_trade"), dict) else {}
    net = float(_number(full.get("net_pnl")) or 0.0)
    best_pnl = float(_number(best.get("pnl")) or 0.0)
    if net <= 0 or best_pnl <= 0:
        return False
    return best_pnl > max(net * 0.5, 1.0)


def _fast_risk_block(
    settings: BacktestSettings,
    trades: list[dict[str, Any]],
    config: CapitalSearchConfig,
    regime: str,
    *,
    has_open_trade: bool = False,
) -> str:
    if has_open_trade:
        return "max_open_trades_reached"
    if settings.spread_points > config.spread_max:
        return "spread_too_high"
    if _loss_streak(trades) >= max(4, config.block_after_consecutive_losses + 1):
        return "consecutive_loss_lockdown"
    if _current_drawdown_pct(trades, settings.initial_balance) >= 5.0:
        return "drawdown_limit_reached"
    closed = len([trade for trade in trades if trade.get("lifecycle_status") == "closed"])
    if closed >= 20 and _quick_profit_factor(trades) < 1.0:
        return "forward_pf_below_threshold"
    if closed >= 20 and _quick_expectancy(trades) <= 0:
        return "expectancy_negative"
    if config.no_trade_if_recent_edge_negative and _recent_edge_negative(trades):
        return "recent_edge_negative"
    if config.no_trade_if_drawdown_accelerating and _drawdown_accelerating(trades, settings.initial_balance):
        return "drawdown_accelerating"
    if str(regime or "").casefold() in {"", "unclear", "not_confirmed", "unknown"}:
        return "market_regime_unclear"
    return ""


def _ema(values: list[float], length: int) -> float:
    if not values:
        return 0.0
    length = max(1, length)
    alpha = 2 / (length + 1)
    ema = values[0]
    for value in values[1:]:
        ema = value * alpha + ema * (1 - alpha)
    return ema


def _rsi(values: list[float], length: int) -> float:
    if len(values) < 2:
        return 50.0
    changes = [values[index] - values[index - 1] for index in range(1, len(values))]
    window = changes[-max(1, length) :]
    gains = [change for change in window if change > 0]
    losses = [-change for change in window if change < 0]
    avg_gain = sum(gains) / max(len(window), 1)
    avg_loss = sum(losses) / max(len(window), 1)
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    return 100 - (100 / (1 + (avg_gain / avg_loss)))


def _atr(highs: list[float], lows: list[float], closes: list[float], length: int) -> float:
    if len(closes) < 2:
        return 0.0
    true_ranges: list[float] = []
    start = max(1, len(closes) - max(1, length))
    for index in range(start, len(closes)):
        high = highs[index]
        low = lows[index]
        prev_close = closes[index - 1]
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(true_ranges) / len(true_ranges) if true_ranges else 0.0


def _hour_from_bar(bar: dict[str, Any]) -> int | None:
    value = str(bar.get("time") or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).hour
    except Exception:
        try:
            return int(value.split()[1].split(":")[0])
        except Exception:
            return None


def _recent_edge_negative(trades: list[dict[str, Any]]) -> bool:
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    recent = closed[-10:]
    if len(recent) < 10:
        return False
    return _profit_factor([float(_number(trade.get("pnl")) or 0.0) for trade in recent]) < 1.0 and _quick_expectancy(recent) <= 0


def _drawdown_accelerating(trades: list[dict[str, Any]], initial_balance: float) -> bool:
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    if len(closed) < 12:
        return False
    older = closed[-12:-6]
    newer = closed[-6:]
    older_dd = _max_drawdown_from_pnls([float(_number(trade.get("pnl")) or 0.0) for trade in older], initial_balance)
    newer_dd = _max_drawdown_from_pnls([float(_number(trade.get("pnl")) or 0.0) for trade in newer], initial_balance)
    return newer_dd > older_dd * 1.25 and sum(float(_number(trade.get("pnl")) or 0.0) for trade in newer) < 0


def _recent_mae_bad(trades: list[dict[str, Any]]) -> bool:
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    recent = closed[-5:]
    if len(recent) < 5:
        return False
    bad = 0
    for trade in recent:
        risk = abs(float(_number(trade.get("initial_risk")) or 0.0)) or 1.0
        mae = abs(min(float(_number(trade.get("max_adverse_excursion")) or 0.0), 0.0))
        if mae / risk >= 0.85:
            bad += 1
    return bad >= 3


def _loss_streak(trades: list[dict[str, Any]]) -> int:
    streak = 0
    for trade in reversed([item for item in trades if item.get("lifecycle_status") == "closed"]):
        if trade.get("status") == "loss":
            streak += 1
        else:
            break
    return streak


def _quick_profit_factor(trades: list[dict[str, Any]]) -> float:
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    return _profit_factor([float(_number(trade.get("pnl")) or 0.0) for trade in closed])


def _quick_expectancy(trades: list[dict[str, Any]]) -> float:
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    values = [float(_number(trade.get("r_multiple")) or 0.0) for trade in closed]
    return sum(values) / len(values) if values else 0.0


def _current_drawdown_pct(trades: list[dict[str, Any]], initial_balance: float) -> float:
    equity = initial_balance
    peak = initial_balance
    for trade in [item for item in trades if item.get("lifecycle_status") == "closed"]:
        equity += float(_number(trade.get("pnl")) or 0.0)
        peak = max(peak, equity)
    if peak <= 0:
        return 0.0
    return max(0.0, ((peak - equity) / peak) * 100.0)


def _stress_pnls(pnls: list[float]) -> list[float]:
    sorted_pnls = sorted(pnls, reverse=True)
    trim_count = min(5, max(0, len(sorted_pnls) // 10))
    trimmed = sorted_pnls[trim_count:] if trim_count else list(pnls)
    return [value * 0.85 if value > 0 else value * 1.1 for value in trimmed]


def _max_drawdown_from_pnls(pnls: list[float], initial_balance: float) -> float:
    equity = initial_balance
    peak = initial_balance
    max_dd = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


def _profit_factor(values: list[float]) -> float:
    gross_win = sum(value for value in values if value > 0)
    gross_loss = abs(sum(value for value in values if value < 0))
    if gross_loss <= 0:
        return gross_win if gross_win > 0 else 0.0
    return gross_win / gross_loss


def _worst_loss_streak(pnls: list[float]) -> int:
    worst = 0
    current = 0
    for value in pnls:
        if value < 0:
            current += 1
            worst = max(worst, current)
        else:
            current = 0
    return worst


def _paper_forward_guardrails(*, max_drawdown: float) -> dict[str, Any]:
    return {
        "early_guardrail_min_trades": 10,
        "early_pf_min": 0.9,
        "early_expectancy_min": 0.0,
        "early_win_rate_min": 40.0,
        "main_guardrail_min_trades": 50,
        "main_pf_min": 1.15,
        "main_expectancy_min": 0.0,
        "max_forward_drawdown": max_drawdown,
        "degrade_to": "observation_only",
    }


def _summary_reading(candidates: list[dict[str, Any]], rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No hubo datos historicos para evaluar. Mantener observation_only."
    if not candidates:
        return "Ningun perfil paso preservacion de capital. Recomendacion: reject/observation_only; no real trading."
    best = candidates[0]
    return (
        f"Mejor candidato paper-forward: {best['timeframe']} {best['profile']} "
        f"PF {best['profit_factor']} DD {best['max_drawdown']} score {best['capital_preservation_score']}. "
        "No promover a real trading."
    )


def _config_payload(config: CapitalSearchConfig) -> dict[str, Any]:
    return {
        "risk_reward": config.risk_reward,
        "time_stop_bars": config.time_stop_bars,
        "score_min": config.score_min,
        "spread_max": config.spread_max,
        "volatility_filter": config.volatility_filter,
        "anti_chop_filter": config.anti_chop_filter,
        "cooldown_after_loss_bars": config.cooldown_after_loss_bars,
        "block_after_consecutive_losses": config.block_after_consecutive_losses,
        "trailing_stop": config.trailing_stop,
        "max_adverse_excursion_filter": config.max_adverse_excursion_filter,
        "no_trade_if_recent_edge_negative": config.no_trade_if_recent_edge_negative,
        "no_trade_if_drawdown_accelerating": config.no_trade_if_drawdown_accelerating,
        "session_filter": config.session_filter,
        "partial_exit": config.partial_exit,
        "atr_trailing": config.atr_trailing,
        "adaptive_time_stop": config.adaptive_time_stop,
        "risk_pct": config.risk_pct,
    }


def _requested_list(raw: Any, default: list[str]) -> list[str]:
    if isinstance(raw, str):
        values = [part.strip() for part in raw.split(",")]
    elif isinstance(raw, list):
        values = [str(part or "").strip() for part in raw]
    else:
        values = list(default)
    return [value for value in values if value]


def _requested_numbers(raw: Any, default: list[float]) -> list[float]:
    values = _requested_list(raw, [str(value) for value in default])
    parsed = [float(_number(value) or 0.0) for value in values]
    return [value for value in parsed if value > 0] or default


def _result_key(row: dict[str, Any]) -> str:
    params = row.get("parameters") if isinstance(row.get("parameters"), dict) else {}
    try:
        encoded = json.dumps(params, sort_keys=True, separators=(",", ":"))
    except Exception:
        encoded = str(params)
    return f"{row.get('timeframe','')}|{row.get('profile','')}|{encoded}"


def _best_score(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    return max(float(_number(row.get("capital_preservation_score")) or 0.0) for row in rows)


def _safe_progress(callback: Any, payload: dict[str, Any]) -> None:
    try:
        callback(payload)
    except Exception:
        return


def _safe_incremental(callback: Any, payload: dict[str, Any]) -> None:
    try:
        callback(payload)
    except Exception:
        return


def write_capital_preservation_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    import csv

    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "capital_preservation_optimizer_results.csv"
    json_path = root / "capital_preservation_optimizer_results.json"
    summary_path = root / "capital_preservation_optimizer_summary.md"
    columns = [
        "timeframe",
        "profile",
        "risk_reward",
        "time_stop_bars",
        "score_min",
        "spread_max",
        "closed",
        "wins",
        "losses",
        "win_rate",
        "profit_factor",
        "expectancy",
        "max_drawdown",
        "test_pf",
        "test_expectancy",
        "monte_carlo_risk_of_ruin",
        "monte_carlo_drawdown_p95",
        "stressed_profit_factor",
        "capital_preservation_score",
        "recommendation",
        "candidate",
        "pass_fail_reasons",
        "broker_touched",
        "order_executed",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in result.get("results") or []:
            mc = row.get("monte_carlo") if isinstance(row.get("monte_carlo"), dict) else {}
            writer.writerow(
                {
                    "timeframe": row.get("timeframe", ""),
                    "profile": row.get("profile", ""),
                    "risk_reward": row.get("risk_reward", ""),
                    "time_stop_bars": row.get("time_stop_bars", ""),
                    "score_min": row.get("score_min", ""),
                    "spread_max": row.get("spread_max", ""),
                    "closed": row.get("closed", 0),
                    "wins": row.get("wins", 0),
                    "losses": row.get("losses", 0),
                    "win_rate": row.get("win_rate", 0),
                    "profit_factor": row.get("profit_factor", 0),
                    "expectancy": row.get("expectancy", 0),
                    "max_drawdown": row.get("max_drawdown", 0),
                    "test_pf": row.get("test_pf", 0),
                    "test_expectancy": row.get("test_expectancy", 0),
                    "monte_carlo_risk_of_ruin": mc.get("risk_of_ruin", 0),
                    "monte_carlo_drawdown_p95": mc.get("max_drawdown_p95", 0),
                    "stressed_profit_factor": mc.get("profit_factor_stressed", 0),
                    "capital_preservation_score": row.get("capital_preservation_score", 0),
                    "recommendation": row.get("recommendation", ""),
                    "candidate": row.get("candidate", False),
                    "pass_fail_reasons": ";".join(str(reason) for reason in row.get("pass_fail_reasons", [])),
                    "broker_touched": row.get("broker_touched", False),
                    "order_executed": row.get("order_executed", False),
                }
            )
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    summary_path.write_text(capital_preservation_summary_markdown(result), encoding="utf-8")
    return csv_path, json_path, summary_path


def capital_preservation_summary_markdown(result: dict[str, Any]) -> str:
    rows = list(result.get("results") or [])
    candidates = list(result.get("candidates") or [])
    lines = [
        "# MT5 Capital Preservation Optimizer Summary",
        "",
        "Safety: `broker_touched=false`, `order_executed=false`, `order_policy=journal_only_no_broker`.",
        "",
        f"Recommendation: **{result.get('recommendation', 'reject')}**",
        "",
        "This report is paper-only. It never recommends real trading.",
        "",
    ]
    if candidates:
        lines.append("## Paper-Forward Candidates")
        for row in candidates[:5]:
            lines.append(
                f"- `{row['timeframe']} {row['profile']}` params `{row.get('parameters')}`: "
                f"PF `{row['profit_factor']}`, expectancy `{row['expectancy']}`, "
                f"DD `{row['max_drawdown']}`, stressed PF `{row.get('monte_carlo', {}).get('profit_factor_stressed')}`."
            )
    else:
        lines.extend(["## Candidates", "No profile passed capital-preservation gates. Recommendation: reject/observation_only."])
    lines.extend(["", "## Top Results"])
    for row in rows[:20]:
        reasons = ", ".join(str(reason) for reason in row.get("pass_fail_reasons", [])[:5])
        lines.append(
            f"- `{row.get('timeframe')} {row.get('profile')}` PF `{row.get('profit_factor')}`, "
            f"WR `{row.get('win_rate')}`, DD `{row.get('max_drawdown')}`, "
            f"score `{row.get('capital_preservation_score')}`, recommendation `{row.get('recommendation')}`. "
            f"Reasons: {reasons}."
        )
    lines.extend(
        [
            "",
            "## Risk Position",
            "- No real trading.",
            "- No martingale, no grid, no averaging losses, no size increase after losses.",
            "- MaxOpenTrades remains 1 inside the simulator.",
            "- If there is doubt, Genesis should choose `NO_TRADE`.",
        ]
    )
    return "\n".join(lines) + "\n"
