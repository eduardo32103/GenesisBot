from __future__ import annotations

from typing import Any

from services.trading_intelligence.strategy_metrics import calculate_strategy_metrics, score_metrics


class BacktestEngine:
    """Small paper-research engine for comparing strategy families on backend bars."""

    def run(self, bars: list[dict[str, Any]] | None, profile: dict[str, Any], *, timeframe: str = "1H") -> dict[str, Any]:
        clean_bars = _normalize_bars(bars or [])
        profile_name = str(profile.get("name") or "")
        parameters = profile.get("parameters") if isinstance(profile.get("parameters"), dict) else {}
        if len(clean_bars) < 80:
            metrics = calculate_strategy_metrics([], benchmark_return=0)
            return {
                "profile": profile_name,
                "timeframe": timeframe,
                "parameters": parameters,
                "status": "insufficient_data",
                "metrics": metrics,
                "benchmark": {"return": 0.0},
                "trades": [],
                "notes": ["Sin suficientes barras backend para validar; usar recomendacion de perfil y paper trading."],
            }

        closes = [bar["close"] for bar in clean_bars]
        highs = [bar["high"] for bar in clean_bars]
        lows = [bar["low"] for bar in clean_bars]
        volumes = [bar["volume"] for bar in clean_bars]
        ema20 = _ema(closes, 20)
        ema50 = _ema(closes, 50)
        ema100 = _ema(closes, 100)
        ema200 = _ema(closes, 200)
        avg_volume = _sma(volumes, 20)
        atr = _atr(clean_bars, 14)
        trades = _simulate_profile(profile_name, clean_bars, ema20, ema50, ema100, ema200, avg_volume, atr, parameters)
        benchmark_return = (closes[-1] / closes[0] - 1.0) * 100.0 if closes[0] else 0.0
        equity_curve = _equity_curve_from_trades(trades)
        metrics = calculate_strategy_metrics(trades, equity_curve=equity_curve, benchmark_return=benchmark_return)
        walk_forward = _walk_forward(trades)
        return {
            "profile": profile_name,
            "timeframe": timeframe,
            "parameters": parameters,
            "status": metrics["status"],
            "metrics": {**metrics, "quality_score": score_metrics(metrics)},
            "benchmark": {"return": round(benchmark_return, 4), "type": "buy_and_hold"},
            "walk_forward": walk_forward,
            "trades": trades[-50:],
            "notes": _notes(profile_name, metrics, benchmark_return),
        }


def run_profile_backtest(bars: list[dict[str, Any]] | None, profile: dict[str, Any], *, timeframe: str = "1H") -> dict[str, Any]:
    return BacktestEngine().run(bars, profile, timeframe=timeframe)


def _simulate_profile(
    profile_name: str,
    bars: list[dict[str, float]],
    ema20: list[float | None],
    ema50: list[float | None],
    ema100: list[float | None],
    ema200: list[float | None],
    avg_volume: list[float | None],
    atr: list[float | None],
    parameters: dict[str, Any],
) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    position: dict[str, Any] | None = None
    lower_name = profile_name.casefold()
    min_relative_volume = float(parameters.get("min_relative_volume") or 1.0)
    crypto_profiles = {"Crypto Momentum", "Crypto Momentum V2", "Crypto Momentum V3", "Crypto Momentum V4", "BTC Breakout Retest", "BTC Volatility Expansion"}
    atr_stop = float(parameters.get("atr_stop") or (3.0 if profile_name in {"Defensive ETF Core", *crypto_profiles} else 2.0))
    donchian_entry = int(parameters.get("donchian_entry") or 55)
    donchian_exit = int(parameters.get("donchian_exit") or 20)
    max_hold = 260 if profile_name == "Defensive ETF Core" else 180 if profile_name in crypto_profiles else 100
    for idx in range(60, len(bars)):
        close = bars[idx]["close"]
        high = bars[idx]["high"]
        low = bars[idx]["low"]
        volume = bars[idx]["volume"]
        rel_vol = volume / avg_volume[idx] if avg_volume[idx] else 1.0
        trend_up = _gt(close, ema200[idx]) and _gt(ema50[idx], ema200[idx])
        trend_down = _lt(close, ema200[idx]) and _lt(ema50[idx], ema200[idx])
        ema50_slope_up = ema50[idx] is not None and ema50[idx - 10] is not None and ema50[idx] > ema50[idx - 10]
        near_ema50 = ema50[idx] is not None and atr[idx] is not None and abs(close - ema50[idx]) <= atr[idx] * 1.2
        near_ema100 = ema100[idx] is not None and atr[idx] is not None and abs(close - ema100[idx]) <= atr[idx] * 1.2
        breakout = idx > donchian_entry and close > max(bar["high"] for bar in bars[idx - donchian_entry : idx]) and rel_vol >= min_relative_volume
        breakdown = idx > donchian_entry and close < min(bar["low"] for bar in bars[idx - donchian_entry : idx]) and rel_vol >= min_relative_volume
        exit_channel_long = min(bar["low"] for bar in bars[max(0, idx - donchian_exit) : idx]) if idx > donchian_exit else close
        exit_channel_short = max(bar["high"] for bar in bars[max(0, idx - donchian_exit) : idx]) if idx > donchian_exit else close
        if position is None:
            enter = False
            side = "long"
            if profile_name == "Defensive ETF Core":
                enter = trend_up and ema50_slope_up and (near_ema50 or near_ema100 or _crosses_above(bars, ema50, idx)) and rel_vol >= min_relative_volume
            elif profile_name == "Trend Pullback":
                enter = trend_up and (near_ema50 or _crosses_above(bars, ema20, idx)) and rel_vol >= min(0.9, min_relative_volume)
            elif profile_name == "Breakout Volume":
                enter = breakout
            elif profile_name == "Trend Following":
                enter = trend_up and _gt(ema20[idx], ema50[idx]) and rel_vol >= min(0.8, min_relative_volume)
            elif profile_name in {"Crypto Momentum", "Crypto Momentum V2", "Crypto Momentum V3", "Crypto Momentum V4"}:
                enter = (trend_up or breakout) and rel_vol >= min(0.8, min_relative_volume)
            elif profile_name == "BTC Breakout Retest":
                recent_breakout = any(bars[p]["close"] > max(bar["high"] for bar in bars[max(0, p - donchian_entry) : p]) for p in range(max(60, idx - 8), idx) if p > donchian_entry)
                retest_reclaim = recent_breakout and low <= max(bar["high"] for bar in bars[idx - donchian_exit : idx]) and close > max(bar["high"] for bar in bars[idx - donchian_exit : idx])
                enter = trend_up and retest_reclaim and rel_vol >= min_relative_volume
            elif profile_name == "BTC Volatility Expansion":
                range_now = max(bar["high"] for bar in bars[idx - 20 : idx]) - min(bar["low"] for bar in bars[idx - 20 : idx])
                range_prior = max(bar["high"] for bar in bars[idx - 40 : idx - 20]) - min(bar["low"] for bar in bars[idx - 40 : idx - 20]) if idx > 100 else range_now
                enter = trend_up and breakout and range_now > range_prior * 1.05 and rel_vol >= min_relative_volume
            elif profile_name == "Commodity Regime":
                enter = (trend_up and rel_vol >= min(0.8, min_relative_volume)) or (trend_down and breakdown)
                side = "short" if trend_down and breakdown else "long"
            elif profile_name == "Gold Defensive":
                enter = trend_up and (near_ema50 or breakout)
            elif profile_name == "Mean Reversion":
                enter = idx > 40 and close <= min(bar["low"] for bar in bars[idx - 20 : idx]) * 1.01 and not trend_down
            if enter:
                current_atr = atr[idx] or max(close * 0.02, 0.01)
                position = {"entry": close, "entry_index": idx, "side": side, "stop": close - current_atr * atr_stop if side == "long" else close + current_atr * atr_stop}
            continue

        side = position["side"]
        held = idx - int(position["entry_index"])
        exit_now = False
        if side == "long":
            exit_now = close < position["stop"] or (profile_name == "Defensive ETF Core" and _lt(close, ema200[idx])) or (profile_name in crypto_profiles and (close < exit_channel_long or _lt(close, ema50[idx]))) or (profile_name not in {"Defensive ETF Core", *crypto_profiles} and _lt(close, ema50[idx])) or held > max_hold
            pnl = (close / position["entry"] - 1.0) * 100.0
        else:
            exit_now = close > position["stop"] or close > exit_channel_short or _gt(close, ema50[idx]) or held > min(max_hold, 100)
            pnl = (position["entry"] / close - 1.0) * 100.0
        if exit_now:
            trades.append({"entry": position["entry"], "exit": close, "side": side, "pnl_pct": round(pnl, 4), "bars_held": held})
            position = None
    return trades


def _normalize_bars(bars: list[dict[str, Any]]) -> list[dict[str, float]]:
    clean: list[dict[str, float]] = []
    for bar in bars:
        close = _num(bar.get("close"))
        if close is None:
            continue
        clean.append(
            {
                "open": _num(bar.get("open")) or close,
                "high": _num(bar.get("high")) or close,
                "low": _num(bar.get("low")) or close,
                "close": close,
                "volume": _num(bar.get("volume")) or 0.0,
            }
        )
    return clean


def _ema(values: list[float], length: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if len(values) < length:
        return result
    multiplier = 2 / (length + 1)
    ema = sum(values[:length]) / length
    result[length - 1] = ema
    for idx in range(length, len(values)):
        ema = (values[idx] - ema) * multiplier + ema
        result[idx] = ema
    return result


def _sma(values: list[float], length: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    for idx in range(length - 1, len(values)):
        result[idx] = sum(values[idx - length + 1 : idx + 1]) / length
    return result


def _atr(bars: list[dict[str, float]], length: int) -> list[float | None]:
    ranges: list[float] = []
    previous_close = bars[0]["close"]
    for bar in bars:
        true_range = max(bar["high"] - bar["low"], abs(bar["high"] - previous_close), abs(bar["low"] - previous_close))
        ranges.append(true_range)
        previous_close = bar["close"]
    return _sma(ranges, length)


def _equity_curve_from_trades(trades: list[dict[str, Any]]) -> list[float]:
    equity = 100.0
    curve = [equity]
    for trade in trades:
        equity *= 1 + float(trade.get("pnl_pct") or 0) / 100.0
        curve.append(equity)
    return curve


def _notes(profile_name: str, metrics: dict[str, Any], benchmark_return: float) -> list[str]:
    notes = [f"{profile_name}: backtest local para investigacion/paper, no promesa de rentabilidad."]
    if metrics.get("profit_factor", 0) < 1.2:
        notes.append("Profit factor debil: marcar perfil como fragil para este activo/timeframe.")
    if metrics.get("total_trades", 0) < 10:
        notes.append("Muestra insuficiente: no aceptar como ventaja validada.")
    if benchmark_return > 0 and metrics.get("benchmark_capture_ratio", 0) < 0.25:
        notes.append("Captura de benchmark baja: revisar si el perfil esta cortando tendencia.")
    return notes


def _walk_forward(trades: list[dict[str, Any]]) -> dict[str, Any]:
    if len(trades) < 12:
        return {
            "status": "insufficient_sample",
            "train": {},
            "test": {},
            "rolling_windows": [],
            "accepted": False,
            "notes": ["No hay suficientes trades para walk-forward."],
        }
    split = int(len(trades) * 0.6)
    train_trades = trades[:split]
    test_trades = trades[split:]
    train_metrics = calculate_strategy_metrics(train_trades)
    test_metrics = calculate_strategy_metrics(test_trades)
    windows: list[dict[str, Any]] = []
    window_size = max(4, len(trades) // 4)
    for start in range(0, len(trades), window_size):
        chunk = trades[start : start + window_size]
        if len(chunk) >= 3:
            metrics = calculate_strategy_metrics(chunk)
            windows.append({"start": start, "end": start + len(chunk), "profit_factor": metrics["profit_factor"], "net_profit": metrics["net_profit"]})
    accepted = train_metrics["profit_factor"] >= 1.15 and test_metrics["profit_factor"] >= 1.05 and test_metrics["expectancy"] >= 0
    return {
        "status": "evaluated",
        "train": train_metrics,
        "test": test_metrics,
        "rolling_windows": windows,
        "accepted": accepted,
        "notes": ["Walk-forward simple; no aceptar si solo funciono en una ventana."],
    }


def _crosses_above(bars: list[dict[str, float]], line: list[float | None], idx: int) -> bool:
    return idx > 0 and line[idx] is not None and line[idx - 1] is not None and bars[idx - 1]["close"] <= line[idx - 1] and bars[idx]["close"] > line[idx]


def _gt(left: float | None, right: float | None) -> bool:
    return left is not None and right is not None and left > right


def _lt(left: float | None, right: float | None) -> bool:
    return left is not None and right is not None and left < right


def _num(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
