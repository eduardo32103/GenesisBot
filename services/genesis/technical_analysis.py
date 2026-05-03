from __future__ import annotations

import math
from statistics import mean, pstdev
from typing import Any


def compute_technical_indicators(ohlc: list[dict[str, Any]]) -> dict[str, Any]:
    candles = [_shape(row) for row in ohlc if _shape(row)]
    closes = [row["close"] for row in candles]
    highs = [row["high"] for row in candles]
    lows = [row["low"] for row in candles]
    volumes = [row["volume"] for row in candles if row.get("volume") is not None]
    if len(candles) < 2:
        return _empty("No hay datos OHLC suficientes para indicadores tecnicos.")

    last_close = closes[-1]
    volume = volumes[-1] if volumes else None
    avg_volume = mean(volumes[-20:]) if len(volumes) >= 2 else None
    relative_volume = _round(volume / avg_volume, 4) if volume is not None and avg_volume else None
    sma = {str(period): _sma(closes, period) for period in (20, 50, 100, 200)}
    ema = {str(period): _ema(closes, period) for period in (20, 50, 100, 200)}
    macd = _macd(closes)
    bollinger = _bollinger(closes, 20)
    atr = _atr(candles, 14)
    recent_high = max(highs[-20:])
    recent_low = min(lows[-20:])
    fib = _fibonacci(recent_low, recent_high)
    trend = _trend(last_close, sma)
    candle_structure = _candle_structure(candles[-1])
    momentum = _momentum(closes)

    return {
        "ok": True,
        "message": "",
        "volume": volume,
        "avg_volume_20": _round(avg_volume),
        "relative_volume": relative_volume,
        "trend": trend,
        "support": _round(recent_low),
        "resistance": _round(recent_high),
        "sma": sma,
        "ema": ema,
        "rsi": _rsi(closes, 14),
        "macd": macd,
        "vwap": _vwap(candles),
        "bollinger_bands": bollinger,
        "atr": atr,
        "volatility": _volatility(closes),
        "recent_high": _round(recent_high),
        "recent_low": _round(recent_low),
        "fibonacci": fib,
        "golden_pocket": {"from": fib.get("0.65"), "to": fib.get("0.618")},
        "candle_structure": candle_structure,
        "breakout": _breakout(last_close, recent_high, recent_low),
        "divergence": "no concluyente",
        "momentum": momentum,
        "risk": _risk(last_close, recent_low, atr),
    }


def _shape(row: dict[str, Any]) -> dict[str, float] | None:
    open_price = _num(row.get("open"))
    high = _num(row.get("high"))
    low = _num(row.get("low"))
    close = _num(row.get("close"))
    if None in (open_price, high, low, close):
        return None
    return {
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": _num(row.get("volume")),
    }


def _num(value: object) -> float | None:
    try:
        if value in (None, "", "None"):
            return None
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    except Exception:
        return None


def _round(value: float | None, digits: int = 4) -> float | None:
    return round(value, digits) if value is not None and math.isfinite(value) else None


def _empty(message: str) -> dict[str, Any]:
    return {
        "ok": False,
        "message": message,
        "volume": None,
        "relative_volume": None,
        "trend": "no concluyente",
        "support": None,
        "resistance": None,
        "sma": {},
        "ema": {},
        "rsi": None,
        "macd": {"line": None, "signal": None, "histogram": None},
        "vwap": None,
        "bollinger_bands": {"upper": None, "middle": None, "lower": None},
        "atr": None,
        "volatility": None,
        "fibonacci": {},
        "golden_pocket": {"from": None, "to": None},
        "candle_structure": "no concluyente",
        "breakout": "no concluyente",
        "divergence": "no concluyente",
        "momentum": "no concluyente",
        "risk": "no concluyente",
    }


def _sma(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return _round(mean(values[-period:]))


def _ema(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    alpha = 2 / (period + 1)
    current = mean(values[:period])
    for value in values[period:]:
        current = value * alpha + current * (1 - alpha)
    return _round(current)


def _rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for previous, current in zip(values[-period - 1 : -1], values[-period:]):
        change = current - previous
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))
    avg_gain = mean(gains)
    avg_loss = mean(losses)
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return _round(100 - (100 / (1 + rs)), 2)


def _macd(values: list[float]) -> dict[str, float | None]:
    if len(values) < 35:
        return {"line": None, "signal": None, "histogram": None}
    macd_series: list[float] = []
    for idx in range(26, len(values) + 1):
        short = _ema(values[:idx], 12)
        long = _ema(values[:idx], 26)
        if short is not None and long is not None:
            macd_series.append(short - long)
    signal = _ema(macd_series, 9)
    line = macd_series[-1] if macd_series else None
    histogram = line - signal if line is not None and signal is not None else None
    return {"line": _round(line), "signal": _round(signal), "histogram": _round(histogram)}


def _bollinger(values: list[float], period: int) -> dict[str, float | None]:
    if len(values) < period:
        return {"upper": None, "middle": None, "lower": None}
    window = values[-period:]
    middle = mean(window)
    deviation = pstdev(window)
    return {"upper": _round(middle + deviation * 2), "middle": _round(middle), "lower": _round(middle - deviation * 2)}


def _atr(candles: list[dict[str, float]], period: int) -> float | None:
    if len(candles) <= period:
        return None
    ranges: list[float] = []
    for previous, current in zip(candles[-period - 1 : -1], candles[-period:]):
        ranges.append(max(current["high"] - current["low"], abs(current["high"] - previous["close"]), abs(current["low"] - previous["close"])))
    return _round(mean(ranges))


def _vwap(candles: list[dict[str, float]]) -> float | None:
    weighted = 0.0
    total_volume = 0.0
    for row in candles[-30:]:
        volume = row.get("volume")
        if volume is None or volume <= 0:
            continue
        typical = (row["high"] + row["low"] + row["close"]) / 3
        weighted += typical * volume
        total_volume += volume
    return _round(weighted / total_volume) if total_volume else None


def _volatility(values: list[float]) -> float | None:
    if len(values) < 21:
        return None
    returns = [(current - previous) / previous for previous, current in zip(values[-21:-1], values[-20:]) if previous]
    return _round(pstdev(returns) * math.sqrt(252) * 100, 2) if len(returns) >= 2 else None


def _fibonacci(low: float, high: float) -> dict[str, float]:
    span = high - low
    return {
        "0.236": _round(high - span * 0.236),
        "0.382": _round(high - span * 0.382),
        "0.5": _round(high - span * 0.5),
        "0.618": _round(high - span * 0.618),
        "0.65": _round(high - span * 0.65),
        "0.786": _round(high - span * 0.786),
    }


def _trend(last_close: float, sma: dict[str, float | None]) -> str:
    sma20 = sma.get("20")
    sma50 = sma.get("50")
    if sma20 is not None and sma50 is not None:
        if last_close > sma20 > sma50:
            return "alcista"
        if last_close < sma20 < sma50:
            return "bajista"
    return "lateral / no concluyente"


def _candle_structure(row: dict[str, float]) -> str:
    body = abs(row["close"] - row["open"])
    span = max(row["high"] - row["low"], 0.000001)
    if body / span < 0.18:
        return "doji / indecision"
    return "vela alcista" if row["close"] > row["open"] else "vela bajista"


def _breakout(last_close: float, recent_high: float, recent_low: float) -> str:
    if last_close >= recent_high:
        return "ruptura potencial de resistencia"
    if last_close <= recent_low:
        return "perdida potencial de soporte"
    return "sin ruptura confirmada"


def _momentum(values: list[float]) -> str:
    if len(values) < 11:
        return "no concluyente"
    change = values[-1] - values[-10]
    if change > 0:
        return "positivo"
    if change < 0:
        return "negativo"
    return "neutral"


def _risk(last_close: float, support: float, atr: float | None) -> str:
    if atr is None or last_close <= 0:
        return "riesgo no concluyente"
    distance = ((last_close - support) / last_close) * 100
    return f"distancia a soporte {distance:.2f}%, ATR {atr:.2f}"
