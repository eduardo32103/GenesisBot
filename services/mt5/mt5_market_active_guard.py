from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any


MARKET_ACTIVE_GUARD_VERSION = "2026-07-05.market_active_guard.v1"


class MarketActiveGuard:
    def __init__(self, config: dict[str, Any] | None = None) -> None:
        payload = dict(config or {})
        self.min_bars = max(1, int(_number(payload.get("min_bars")) or 50))
        self.max_bar_age_seconds = _duration_seconds(payload, "max_bar_age", 5400.0)
        self.max_tick_age_seconds = _duration_seconds(payload, "max_tick_age", 5400.0)
        self.require_tick = payload.get("require_tick") is not False
        self.max_spread = _number(payload.get("max_spread"))
        self.min_spread = max(0.0, float(_number(payload.get("min_spread")) or 0.0))
        self.movement_lookback_bars = max(2, int(_number(payload.get("movement_lookback_bars")) or 10))
        self.freeze_lookback_bars = max(2, int(_number(payload.get("freeze_lookback_bars")) or min(5, self.movement_lookback_bars)))
        self.min_price_move_pct = max(0.0, float(_number(payload.get("min_price_move_pct")) or 0.000001))
        self.min_spread_move_multiple = max(0.0, float(_number(payload.get("min_spread_move_multiple")) or 0.1))
        self.min_absolute_move = max(0.0, float(_number(payload.get("min_absolute_move")) or 1e-12))
        self.min_recent_range_pct = max(0.0, float(_number(payload.get("min_recent_range_pct")) or 0.0))
        self.min_atr_pct = max(0.0, float(_number(payload.get("min_atr_pct")) or 0.0))
        self.use_volume_freeze_check = payload.get("use_volume_freeze_check") is not False

    def evaluate(self, snapshot: dict[str, Any] | None) -> dict[str, Any]:
        active = dict(snapshot or {})
        tick = active.get("last_tick") if isinstance(active.get("last_tick"), dict) else {}
        bars = _recent_bars(active)
        bars_count = int(_number(active.get("bars_count")) or len(bars) or 0)
        quote = _quote_values(active, tick)
        spread = quote["spread"]
        latest_bar_time = _latest_bar_time(active, bars)
        latest_tick_at = str(active.get("last_tick_at") or active.get("latest_tick_at") or "")
        bar_age = _age_seconds(latest_bar_time)
        tick_age = _age_seconds(latest_tick_at)
        closes = [_number(bar.get("close") or bar.get("c")) for bar in bars[-self.movement_lookback_bars :] if isinstance(bar, dict)]
        closes = [float(value) for value in closes if value is not None and _finite(value)]
        ranges = [
            max(0.0, float(_number(bar.get("high") or bar.get("h")) or 0.0) - float(_number(bar.get("low") or bar.get("l")) or 0.0))
            for bar in bars[-self.movement_lookback_bars :]
            if isinstance(bar, dict)
        ]
        recent_range = sum(ranges)
        atr = _number(active.get("atr") or (tick or {}).get("atr"))
        atr_pct = (float(atr) / max(abs(quote["current_price"]), 1e-12)) if atr is not None and _finite(atr) and quote["current_price"] else 0.0
        unique_closes = {round(value, 12) for value in closes}
        min_move = max(abs(closes[-1]) * self.min_price_move_pct if closes else 0.0, spread * self.min_spread_move_multiple if spread is not None else 0.0, self.min_absolute_move)
        close_range = max(closes) - min(closes) if len(closes) >= 2 else 0.0
        range_pct = recent_range / max(abs(closes[-1]), 1e-12) if closes else 0.0
        movement_ok = (len(unique_closes) > 1 and close_range > min_move) or recent_range > max(self.min_absolute_move, abs(closes[-1]) * self.min_recent_range_pct if closes else 0.0) or atr_pct > self.min_atr_pct
        frozen = _frozen_ohlc(bars[-self.freeze_lookback_bars :]) or _entry_equals_recent_closes(quote["current_price"], closes) or _timestamps_not_advancing(bars[-self.freeze_lookback_bars :]) or _zero_volume_freeze(bars[-self.freeze_lookback_bars :], enabled=self.use_volume_freeze_check)
        reason = ""
        if bars_count < self.min_bars:
            reason = "insufficient_bars"
        elif latest_bar_time and bar_age is not None and bar_age > self.max_bar_age_seconds:
            reason = "stale_bar"
        elif not latest_bar_time:
            reason = "stale_bar"
        elif self.require_tick and (not tick or not latest_tick_at or tick_age is None or tick_age > self.max_tick_age_seconds):
            reason = "stale_tick"
        elif not quote["quote_valid"]:
            reason = "invalid_quote"
        elif spread is None or spread <= self.min_spread:
            reason = "zero_spread"
        elif self.max_spread is not None and spread > self.max_spread:
            reason = "excessive_spread"
        elif frozen:
            reason = "frozen_ohlc"
        elif not movement_ok:
            reason = "insufficient_recent_movement"
        market_active = not reason
        return {
            "ok": True,
            "guard_version": MARKET_ACTIVE_GUARD_VERSION,
            "market_active": market_active,
            "reason": reason,
            "readiness_state": "market_active" if market_active else "blocked_market_inactive",
            "entry_allowed_for_paper_test": market_active,
            "bars_count": bars_count,
            "min_bars": self.min_bars,
            "latest_bar_time": latest_bar_time,
            "latest_tick_at": latest_tick_at,
            "bar_age_seconds": bar_age,
            "tick_age_seconds": tick_age,
            "current_price": round(quote["current_price"], 8) if quote["current_price"] else 0.0,
            "bid": quote["bid"],
            "ask": quote["ask"],
            "last": quote["last"],
            "spread": round(spread, 8) if spread is not None else 0.0,
            "spread_positive": spread is not None and spread > self.min_spread,
            "spread_valid": spread is not None and spread > self.min_spread and (self.max_spread is None or spread <= self.max_spread),
            "quote_valid": quote["quote_valid"],
            "current_price_valid": quote["quote_valid"],
            "price_moved_recently": bool(movement_ok),
            "recent_close_unique_count": len(unique_closes),
            "recent_close_range": round(close_range, 12),
            "recent_high_low_sum": round(recent_range, 12),
            "atr_pct": round(atr_pct, 12),
            "frozen_ohlc": bool(frozen),
            "market_inactive_or_frozen": not market_active,
            "no_price_movement": not movement_ok,
            "config": {
                "min_bars": self.min_bars,
                "max_bar_age_seconds": self.max_bar_age_seconds,
                "max_tick_age_seconds": self.max_tick_age_seconds,
                "require_tick": self.require_tick,
                "max_spread": self.max_spread,
                "movement_lookback_bars": self.movement_lookback_bars,
                "freeze_lookback_bars": self.freeze_lookback_bars,
                "min_price_move_pct": self.min_price_move_pct,
                "min_spread_move_multiple": self.min_spread_move_multiple,
                "min_absolute_move": self.min_absolute_move,
                "min_recent_range_pct": self.min_recent_range_pct,
                "min_atr_pct": self.min_atr_pct,
            },
            **_safety(),
        }


def evaluate_market_active(snapshot: dict[str, Any] | None, config: dict[str, Any] | None = None) -> dict[str, Any]:
    return MarketActiveGuard(config).evaluate(snapshot)


def _recent_bars(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("ohlc_recent", "bars", "recent_bars"):
        value = snapshot.get(key)
        if isinstance(value, list):
            return [dict(item) for item in value if isinstance(item, dict)]
    return []


def _quote_values(snapshot: dict[str, Any], tick: dict[str, Any]) -> dict[str, Any]:
    bid = _number(tick.get("bid") if tick else None)
    ask = _number(tick.get("ask") if tick else None)
    last = _number((tick or {}).get("last") or (tick or {}).get("price") or snapshot.get("last_price") or snapshot.get("last"))
    spread = _number((tick or {}).get("spread") if tick else None)
    if spread is None:
        spread = _number(snapshot.get("spread"))
    if spread is None and bid is not None and ask is not None:
        spread = abs(ask - bid)
    current = last if last is not None else bid if bid is not None else ask if ask is not None else 0.0
    quote_values = [value for value in (bid, ask, last if last is not None else current) if value is not None]
    quote_valid = bool(quote_values) and all(_finite(value) and value > 0 for value in quote_values)
    if bid is not None and ask is not None and ask < bid:
        quote_valid = False
    return {"bid": bid, "ask": ask, "last": last, "spread": spread, "current_price": float(current or 0.0), "quote_valid": quote_valid}


def _latest_bar_time(snapshot: dict[str, Any], bars: list[dict[str, Any]]) -> str:
    if bars:
        last_bar = bars[-1]
        text = str(last_bar.get("time") or last_bar.get("timestamp") or last_bar.get("datetime") or "").strip()
        if text:
            return text
    return str(snapshot.get("last_bar_time") or snapshot.get("latest_bar_time") or snapshot.get("bars_last_at") or snapshot.get("last_bars_at") or "").strip()


def _frozen_ohlc(bars: list[dict[str, Any]]) -> bool:
    if len(bars) < 2:
        return False
    signatures = []
    for bar in bars:
        values = tuple(round(float(_number(bar.get(key) or bar.get(key[0])) or 0.0), 12) for key in ("open", "high", "low", "close"))
        signatures.append(values)
    return len(set(signatures)) == 1


def _entry_equals_recent_closes(price: float, closes: list[float]) -> bool:
    if not price or len(closes) < 2:
        return False
    return all(abs(float(price) - close) <= 1e-12 for close in closes)


def _timestamps_not_advancing(bars: list[dict[str, Any]]) -> bool:
    stamps = [str(bar.get("time") or bar.get("timestamp") or bar.get("datetime") or "").strip() for bar in bars if isinstance(bar, dict)]
    stamps = [stamp for stamp in stamps if stamp]
    return len(stamps) >= 2 and len(set(stamps)) == 1


def _zero_volume_freeze(bars: list[dict[str, Any]], *, enabled: bool) -> bool:
    if not enabled or len(bars) < 2:
        return False
    volumes = []
    for bar in bars:
        present = any(key in bar for key in ("volume", "tick_volume", "real_volume"))
        if present:
            volumes.append(float(_number(bar.get("volume") or bar.get("tick_volume") or bar.get("real_volume")) or 0.0))
    return bool(volumes) and all(value == 0.0 for value in volumes)


def _age_seconds(value: object) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return round((datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds(), 3)


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if _finite(parsed) else None


def _duration_seconds(payload: dict[str, Any], prefix: str, default: float) -> float:
    seconds = _number(payload.get(f"{prefix}_seconds"))
    if seconds is not None:
        return max(0.0, float(seconds))
    minutes = _number(payload.get(f"{prefix}_minutes"))
    if minutes is not None:
        return max(0.0, float(minutes) * 60.0)
    return max(0.0, float(default))


def _finite(value: object) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
