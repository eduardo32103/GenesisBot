from __future__ import annotations

from itertools import product
from typing import Any


class ParameterSearch:
    """Small bounded parameter search; intentionally avoids broad curve fitting."""

    def candidates_for(self, profile: dict[str, Any], *, asset_class: str = "") -> list[dict[str, Any]]:
        name = str(profile.get("name") or "")
        base = {
            "ema_fast": 20,
            "ema_mid": 50,
            "ema_slow": 200,
            "rsi_floor": 45,
            "rsi_ceiling": 70,
            "atr_stop": 2.0,
            "trailing_atr": 2.5,
            "min_relative_volume": 1.0,
            "adx_min": 18,
            "donchian_entry": 55,
            "donchian_exit": 20,
        }
        if name == "Defensive ETF Core" or asset_class == "Index ETF":
            grid = {
                "ema_fast": [20],
                "ema_mid": [50, 55],
                "ema_slow": [200],
                "atr_stop": [2.5, 3.0],
                "trailing_atr": [3.0],
                "min_relative_volume": [0.6, 0.8],
                "adx_min": [12, 15],
            }
        elif name in {"Crypto Momentum V4", "Crypto Momentum V3", "Crypto Momentum V2", "BTC Breakout Retest", "BTC Volatility Expansion"} or asset_class == "Crypto":
            grid = {
                "ema_fast": [10, 20, 21],
                "ema_mid": [50, 55],
                "ema_slow": [100, 200],
                "rsi_floor": [45, 50, 55],
                "rsi_ceiling": [60, 70],
                "atr_stop": [2.0, 2.5, 3.0, 3.5],
                "trailing_atr": [2.5, 3.0, 3.5, 4.0],
                "min_relative_volume": [1.0, 1.1, 1.2, 1.5],
                "adx_min": [15, 20, 25],
                "min_signal_score": [55, 60, 65, 70],
                "donchian_entry": [40, 55],
                "donchian_exit": [15, 20],
            }
        elif name == "Mean Reversion":
            grid = {
                "ema_fast": [10, 20],
                "ema_mid": [50],
                "ema_slow": [200],
                "rsi_floor": [35, 40],
                "rsi_ceiling": [55, 60],
                "atr_stop": [1.5, 2.0],
                "trailing_atr": [1.5, 2.0],
                "min_relative_volume": [0.8, 1.0],
                "adx_min": [10, 15],
            }
        else:
            grid = {
                "ema_fast": [10, 20, 21],
                "ema_mid": [50, 55],
                "ema_slow": [100, 200],
                "rsi_floor": [40, 45, 50],
                "rsi_ceiling": [65, 70],
                "atr_stop": [1.5, 2.0, 2.5],
                "trailing_atr": [2.0, 2.5, 3.0],
                "min_relative_volume": [1.0, 1.2],
                "adx_min": [15, 20, 25],
            }
        return _bounded_grid(base, grid, max_candidates=18)


def generate_parameter_candidates(profile: dict[str, Any], *, asset_class: str = "") -> list[dict[str, Any]]:
    return ParameterSearch().candidates_for(profile, asset_class=asset_class)


def _bounded_grid(base: dict[str, Any], grid: dict[str, list[Any]], *, max_candidates: int) -> list[dict[str, Any]]:
    keys = list(grid)
    candidates: list[dict[str, Any]] = []
    for values in product(*(grid[key] for key in keys)):
        params = dict(base)
        params.update(dict(zip(keys, values)))
        if params["ema_fast"] >= params["ema_mid"] or params["ema_mid"] >= params["ema_slow"]:
            continue
        candidates.append(params)
        if len(candidates) >= max_candidates:
            break
    return candidates or [base]
