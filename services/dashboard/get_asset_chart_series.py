from __future__ import annotations

import logging
import math
import re
from typing import Any

from app.settings import load_settings
from integrations.fmp.client import FmpClient
from services.genesis.technical_analysis import compute_technical_indicators

_LOGGER = logging.getLogger("genesis.dashboard.chart")
_TICKER_PATTERN = re.compile(r"^[A-Z0-9.\-=]{1,15}$")
_TIMEFRAMES = {"1D", "1W", "1M", "1Y", "5Y", "MAX"}
_MAX_RENDER_POINTS = 520
_CRYPTO_SYMBOLS = {"BTC", "ETH", "SOL", "XRP", "DOGE"}


def _normalize_ticker(value: object) -> str:
    return str(value or "").strip().upper()


def _normalize_timeframe(value: object) -> str:
    raw = str(value or "").strip().upper()
    return raw if raw in _TIMEFRAMES else "1Y"


def _history_symbol_map(ticker: str) -> dict[str, str] | None:
    if ticker in _CRYPTO_SYMBOLS:
        return {ticker: f"{ticker}USD"}
    if ticker.endswith("-USD"):
        return {ticker: ticker.replace("-USD", "USD")}
    if ticker == "BZ=F":
        return {ticker: "BZUSD"}
    if ticker == "GC=F":
        return {ticker: "GCUSD"}
    return None


def _safe_float(value: object) -> float | None:
    try:
        if value in (None, "", "None"):
            return None
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    except Exception:
        return None


def _row_date(row: dict[str, Any]) -> str:
    return str(row.get("date") or row.get("label") or row.get("datetime") or row.get("timestamp") or "").strip()


def _shape_ohlc(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candles: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        time = _row_date(row)
        open_price = _safe_float(row.get("open"))
        high = _safe_float(row.get("high") or row.get("dayHigh"))
        low = _safe_float(row.get("low") or row.get("dayLow"))
        close = _safe_float(row.get("close") or row.get("price") or row.get("adjClose") or row.get("adj_close"))
        if not time or open_price is None or high is None or low is None or close is None:
            continue
        if min(open_price, high, low, close) <= 0:
            continue
        candles.append(
            {
                "time": time,
                "date": time,
                "open": round(open_price, 6),
                "high": round(high, 6),
                "low": round(low, 6),
                "close": round(close, 6),
                "volume": _safe_float(row.get("volume")),
            }
        )
    candles.sort(key=lambda point: point["time"])
    return candles


def _downsample_ohlc(candles: list[dict[str, Any]], max_points: int = _MAX_RENDER_POINTS) -> list[dict[str, Any]]:
    if len(candles) <= max_points:
        return candles
    bucket_size = max(1, math.ceil(len(candles) / max_points))
    sampled: list[dict[str, Any]] = []
    for start in range(0, len(candles), bucket_size):
        bucket = candles[start : start + bucket_size]
        if not bucket:
            continue
        sampled.append(
            {
                "time": bucket[0]["time"],
                "date": bucket[0]["date"],
                "open": bucket[0]["open"],
                "high": round(max(float(row["high"]) for row in bucket), 6),
                "low": round(min(float(row["low"]) for row in bucket), 6),
                "close": bucket[-1]["close"],
                "volume": round(sum(float(row.get("volume") or 0) for row in bucket), 6),
            }
        )
    return sampled


def _slice_for_timeframe(points: list[dict[str, Any]], timeframe: str) -> list[dict[str, Any]]:
    if timeframe == "1W":
        return points[-7:]
    if timeframe == "1M":
        return points[-23:]
    if timeframe == "1Y":
        return points[-260:]
    if timeframe == "5Y":
        return points[-1260:]
    return points


def _summary(points: list[dict[str, Any]]) -> dict[str, Any]:
    if not points:
        return {"start_price": None, "end_price": None, "change": None, "change_pct": None}
    start = _safe_float(points[0].get("close"))
    end = _safe_float(points[-1].get("close"))
    if start is None or end is None or start == 0:
        return {"start_price": start, "end_price": end, "change": None, "change_pct": None}
    change = end - start
    return {
        "start_price": round(start, 6),
        "end_price": round(end, 6),
        "change": round(change, 6),
        "change_pct": round((change / start) * 100, 4),
    }


def _history_years(points: list[dict[str, Any]]) -> float:
    if len(points) < 2:
        return 0.0
    try:
        from datetime import datetime

        first = datetime.fromisoformat(str(points[0].get("date") or points[0].get("time") or "")[:10])
        last = datetime.fromisoformat(str(points[-1].get("date") or points[-1].get("time") or "")[:10])
        return round(max((last - first).days, 0) / 365.25, 2)
    except Exception:
        return 0.0


def _returns(eod_points: list[dict[str, Any]], intraday_points: list[dict[str, Any]]) -> dict[str, float | None]:
    return {
        "1D": _summary(intraday_points if len(intraday_points) >= 2 else eod_points[-2:]).get("change_pct"),
        "1W": _summary(_slice_for_timeframe(eod_points, "1W")).get("change_pct"),
        "1M": _summary(_slice_for_timeframe(eod_points, "1M")).get("change_pct"),
        "1Y": _summary(_slice_for_timeframe(eod_points, "1Y")).get("change_pct"),
        "5Y": _summary(_slice_for_timeframe(eod_points, "5Y")).get("change_pct"),
        "MAX": _summary(eod_points).get("change_pct"),
    }


def _empty_payload(ticker: str, timeframe: str, status: str, message: str) -> dict[str, Any]:
    return {
        "ok": False,
        "status": status,
        "message": message,
        "ticker": ticker,
        "timeframe": timeframe,
        "range": timeframe,
        "points": [],
        "ohlc": [],
        "returns": {"1D": None, "1W": None, "1M": None, "1Y": None, "5Y": None, "MAX": None},
        "indicators": compute_technical_indicators([]),
        "summary": _summary([]),
        "max_history_years": 0.0,
        "history_points": 0,
        "source": {
            "provider": "FMP",
            "endpoint": "",
            "live_enabled": False,
            "downsampled": False,
            "raw_points": 0,
        },
    }


def get_asset_chart_series(ticker: str = "", timeframe: str = "1Y") -> dict[str, Any]:
    normalized_ticker = _normalize_ticker(ticker)
    normalized_timeframe = _normalize_timeframe(timeframe)
    if not normalized_ticker or not _TICKER_PATTERN.match(normalized_ticker):
        return _empty_payload(normalized_ticker, normalized_timeframe, "invalid", "Ticker no valido.")

    settings = load_settings()
    live_enabled = bool(getattr(settings, "fmp_api_key", "") and getattr(settings, "fmp_live_enabled", False))
    if not live_enabled:
        payload = _empty_payload(normalized_ticker, normalized_timeframe, "fmp_disabled", "Datos historicos no disponibles en este entorno.")
        payload["source"]["live_enabled"] = False
        return payload

    client = FmpClient(settings.fmp_api_key, logger=_LOGGER)
    symbol_map = _history_symbol_map(normalized_ticker)
    quote = client.get_quote(normalized_ticker) or {}
    profile = client.get_profile(normalized_ticker) or {}

    eod_rows = client.get_historical_eod(normalized_ticker, limit=None, symbol_map=symbol_map) or []
    eod_points = _shape_ohlc(eod_rows)
    max_history_years = _history_years(eod_points)
    intraday_points: list[dict[str, Any]] = []
    endpoint_label = "historical-price-eod/full"
    if normalized_timeframe == "1D":
        rows = client.get_intraday_history(normalized_ticker, interval="5min", limit=160, symbol_map=symbol_map) or []
        endpoint_label = "historical-chart/5min"
        intraday_points = _shape_ohlc(rows)
        points = intraday_points
    else:
        points = _slice_for_timeframe(eod_points, normalized_timeframe)

    raw_count = len(points)
    points = _downsample_ohlc(points)
    return_map = _returns(eod_points, intraday_points)
    if not points:
        return {
            **_empty_payload(normalized_ticker, normalized_timeframe, "no_data", "No hay datos OHLC suficientes para esta temporalidad."),
            "quote": quote,
            "name": quote.get("name") or profile.get("companyName") or profile.get("name") or normalized_ticker,
            "returns": return_map,
            "indicators": compute_technical_indicators([]),
            "max_history_years": max_history_years,
            "history_points": len(eod_points),
            "source": {
                "provider": "FMP",
                "endpoint": endpoint_label,
                "live_enabled": True,
                "downsampled": False,
                "raw_points": 0,
            },
        }

    return {
        "ok": True,
        "status": "ready",
        "ticker": normalized_ticker,
        "name": quote.get("name") or profile.get("companyName") or profile.get("name") or normalized_ticker,
        "timeframe": normalized_timeframe,
        "range": normalized_timeframe,
        "points": points,
        "ohlc": points,
        "returns": return_map,
        "indicators": compute_technical_indicators(points),
        "summary": _summary(points),
        "max_history_years": max_history_years,
        "history_points": len(eod_points),
        "quote": {
            "price": quote.get("price"),
            "change": quote.get("change"),
            "changesPercentage": quote.get("changesPercentage"),
            "previousClose": quote.get("previousClose"),
            "timestamp": quote.get("timestamp"),
        },
        "stale": False,
        "source": {
            "provider": "FMP",
            "endpoint": endpoint_label,
            "live_enabled": True,
            "downsampled": raw_count > len(points),
            "raw_points": raw_count,
            "raw_eod_points": len(eod_points),
            "max_history_years": max_history_years,
        },
    }
