from __future__ import annotations

from datetime import datetime, timezone
from statistics import mean
from typing import Any


MIN_BAR_CONTEXT_BARS = 100


def build_runtime_bar_context(
    bars: list[dict[str, Any]],
    *,
    symbol: str,
    timeframe: str,
    tick: dict[str, Any] | None = None,
    min_bars: int = MIN_BAR_CONTEXT_BARS,
) -> dict[str, Any]:
    clean_bars = [_clean_bar(item) for item in bars if isinstance(item, dict)]
    clean_bars = [item for item in clean_bars if item]
    clean_symbol = str(symbol or "").upper().strip()
    clean_timeframe = str(timeframe or "").upper().strip()
    active_tick = dict(tick or {})
    last_bar = clean_bars[-1] if clean_bars else {}
    closes = [_number(item.get("close")) for item in clean_bars]
    closes = [float(value) for value in closes if value is not None]
    last_price = _number(active_tick.get("last") or active_tick.get("price")) or _number(last_bar.get("close"))
    bid = _number(active_tick.get("bid"))
    ask = _number(active_tick.get("ask"))
    spread = _number(active_tick.get("spread"))
    if spread is None and bid is not None and ask is not None:
        spread = abs(ask - bid)
    atr = _atr(clean_bars)
    rsi = _rsi(closes)
    ema20 = _ema(closes, 20)
    ema50 = _ema(closes, 50)
    trend_score = _trend_score(last_price, ema20, ema50, closes)
    momentum_score = _momentum_score(closes)
    volatility_score = _volatility_score(atr, last_price, closes)
    regime = _market_regime(trend_score, momentum_score, volatility_score, last_price, ema20, ema50)
    score = round((trend_score * 0.35) + (momentum_score * 0.35) + (volatility_score * 0.30), 4)
    complete = len(clean_bars) >= max(1, int(min_bars or MIN_BAR_CONTEXT_BARS))
    hour = _bar_hour(last_bar)
    session = _session_name(hour)
    side = "buy"
    if len(closes) >= 2 and closes[-1] < closes[-2]:
        side = "sell"
    enriched_tick = {
        **active_tick,
        "symbol": clean_symbol,
        "normalized_symbol": clean_symbol,
        "timeframe": clean_timeframe,
        "last": last_price,
        "bid": bid,
        "ask": ask,
        "spread": spread,
        "score": score,
        "final_score": score,
        "entry_quality_score": score,
        "momentum_score": momentum_score,
        "trend_score": trend_score,
        "volatility_score": volatility_score,
        "market_regime": regime,
        "regime": regime,
        "rsi": rsi,
        "rsi14": rsi,
        "atr": atr,
        "ema20": ema20,
        "ema50": ema50,
        "hour": hour,
        "session": session,
        "side_hint": side,
        "breakout_confirmed": bool(regime == "trend" and score >= 58 and volatility_score >= 35),
        "runtime_snapshot_available": True,
        "runtime_snapshot_complete": complete,
        "runtime_snapshot_context": "bar_context" if complete else "insufficient_bar_context",
        "bars_count": len(clean_bars),
    }
    return {
        "ok": True,
        "symbol": clean_symbol,
        "timeframe": clean_timeframe,
        "bars": clean_bars,
        "bars_count": len(clean_bars),
        "min_bars_required": max(1, int(min_bars or MIN_BAR_CONTEXT_BARS)),
        "runtime_snapshot_complete": complete,
        "runtime_snapshot_context": "bar_context" if complete else "insufficient_bar_context",
        "last_price": last_price,
        "bid": bid,
        "ask": ask,
        "spread": spread,
        "ohlc_recent": clean_bars[-10:],
        "last_bar_time": str(last_bar.get("time") or last_bar.get("timestamp") or ""),
        "first_bar_time": str((clean_bars[0] if clean_bars else {}).get("time") or (clean_bars[0] if clean_bars else {}).get("timestamp") or ""),
        "volatility_score": volatility_score,
        "momentum_score": momentum_score,
        "trend_score": trend_score,
        "market_regime": regime,
        "regime": regime,
        "rsi": rsi,
        "atr": atr,
        "ema20": ema20,
        "ema50": ema50,
        "hour": hour,
        "session": session,
        "enriched_tick": enriched_tick,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _clean_bar(item: dict[str, Any]) -> dict[str, Any]:
    close = _number(item.get("close") or item.get("c"))
    high = _number(item.get("high") or item.get("h"))
    low = _number(item.get("low") or item.get("l"))
    open_price = _number(item.get("open") or item.get("o"))
    if close is None:
        return {}
    if open_price is None:
        open_price = close
    if high is None:
        high = max(open_price, close)
    if low is None:
        low = min(open_price, close)
    return {
        "time": str(item.get("time") or item.get("timestamp") or item.get("datetime") or ""),
        "open": float(open_price),
        "high": float(high),
        "low": float(low),
        "close": float(close),
        "volume": float(_number(item.get("volume") or item.get("tick_volume") or item.get("real_volume")) or 0.0),
    }


def _atr(bars: list[dict[str, Any]], period: int = 14) -> float | None:
    if len(bars) < 2:
        return None
    ranges: list[float] = []
    for index in range(1, len(bars)):
        current = bars[index]
        previous = bars[index - 1]
        high = _number(current.get("high"))
        low = _number(current.get("low"))
        prev_close = _number(previous.get("close"))
        if high is None or low is None or prev_close is None:
            continue
        ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    if not ranges:
        return None
    return round(mean(ranges[-period:]), 8)


def _rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) <= period:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for index in range(len(closes) - period, len(closes)):
        delta = closes[index] - closes[index - 1]
        gains.append(max(delta, 0.0))
        losses.append(abs(min(delta, 0.0)))
    avg_gain = mean(gains) if gains else 0.0
    avg_loss = mean(losses) if losses else 0.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 4)


def _ema(values: list[float], period: int) -> float | None:
    if not values:
        return None
    alpha = 2.0 / (period + 1.0)
    ema = values[0]
    for value in values[1:]:
        ema = (value * alpha) + (ema * (1.0 - alpha))
    return round(ema, 8)


def _trend_score(last_price: float | None, ema20: float | None, ema50: float | None, closes: list[float]) -> float:
    if last_price is None or ema20 is None or ema50 is None:
        return 50.0
    slope = 0.0
    if len(closes) >= 10:
        slope = (closes[-1] - closes[-10]) / max(abs(closes[-10]), 0.000001)
    structure = 15.0 if last_price >= ema20 >= ema50 else 15.0 if last_price <= ema20 <= ema50 else -5.0
    distance = min(20.0, abs(last_price - ema50) / max(abs(last_price), 0.000001) * 2500.0)
    return round(_clamp(50.0 + structure + distance + slope * 1200.0, 0.0, 100.0), 4)


def _momentum_score(closes: list[float]) -> float:
    if len(closes) < 6:
        return 50.0
    short = (closes[-1] - closes[-4]) / max(abs(closes[-4]), 0.000001)
    medium = (closes[-1] - closes[-8]) / max(abs(closes[-8]), 0.000001) if len(closes) >= 8 else short
    return round(_clamp(50.0 + short * 2400.0 + medium * 1200.0, 0.0, 100.0), 4)


def _volatility_score(atr: float | None, last_price: float | None, closes: list[float]) -> float:
    if atr is None or last_price is None:
        return 50.0
    atr_pct = atr / max(abs(last_price), 0.000001)
    recent_range = 0.0
    if len(closes) >= 20:
        window = closes[-20:]
        recent_range = (max(window) - min(window)) / max(abs(last_price), 0.000001)
    return round(_clamp(35.0 + atr_pct * 2500.0 + recent_range * 650.0, 0.0, 100.0), 4)


def _market_regime(
    trend_score: float,
    momentum_score: float,
    volatility_score: float,
    last_price: float | None,
    ema20: float | None,
    ema50: float | None,
) -> str:
    if trend_score >= 58 and momentum_score >= 50 and volatility_score >= 35:
        return "trend"
    if volatility_score < 35 or abs(trend_score - 50.0) < 8:
        return "chop"
    if last_price is not None and ema20 is not None and ema50 is not None and min(ema20, ema50) <= last_price <= max(ema20, ema50):
        return "range"
    return "trend" if trend_score >= 55 else "chop"


def _bar_hour(bar: dict[str, Any]) -> int | None:
    raw = str(bar.get("time") or bar.get("timestamp") or "")
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.hour


def _session_name(hour: int | None) -> str:
    if hour is None:
        return ""
    if 7 <= hour <= 20:
        return "london_us"
    if 0 <= hour < 7:
        return "asia"
    return "off_session"


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))
