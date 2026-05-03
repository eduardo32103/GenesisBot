from __future__ import annotations

import logging
import math
import re
from typing import Any

from app.settings import load_settings
from integrations.fmp.client import FmpClient

_LOGGER = logging.getLogger("genesis.dashboard.chart")
_TICKER_PATTERN = re.compile(r"^[A-Z0-9.\-=]{1,15}$")
_TIMEFRAMES = {"1D", "1W", "1Y", "5Y", "MAX"}
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


def _row_price(row: dict[str, Any]) -> float | None:
    for key in ("close", "price", "adjClose", "adj_close", "vwap"):
        numeric = _safe_float(row.get(key))
        if numeric is not None and numeric > 0:
            return round(numeric, 6)
    return None


def _row_date(row: dict[str, Any]) -> str:
    return str(row.get("date") or row.get("label") or row.get("datetime") or row.get("timestamp") or "").strip()


def _shape_points(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        date = _row_date(row)
        close = _row_price(row)
        if not date or close is None:
            continue
        points.append(
            {
                "date": date,
                "close": close,
                "volume": _safe_float(row.get("volume")),
            }
        )
    points.sort(key=lambda point: point["date"])
    return points


def _downsample(points: list[dict[str, Any]], max_points: int = _MAX_RENDER_POINTS) -> list[dict[str, Any]]:
    if len(points) <= max_points:
        return points
    step = (len(points) - 1) / max(max_points - 1, 1)
    sampled: list[dict[str, Any]] = []
    seen_indexes: set[int] = set()
    for idx in range(max_points):
        source_index = round(idx * step)
        if source_index in seen_indexes:
            continue
        seen_indexes.add(source_index)
        sampled.append(points[source_index])
    return sampled


def _slice_for_timeframe(points: list[dict[str, Any]], timeframe: str) -> list[dict[str, Any]]:
    if timeframe == "1W":
        return points[-7:]
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


def _empty_payload(ticker: str, timeframe: str, status: str, message: str) -> dict[str, Any]:
    return {
        "ok": False,
        "status": status,
        "message": message,
        "ticker": ticker,
        "timeframe": timeframe,
        "points": [],
        "summary": _summary([]),
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

    endpoint_label = "historical-price-eod/full"
    if normalized_timeframe == "1D":
        rows = client.get_intraday_history(normalized_ticker, interval="5min", limit=160, symbol_map=symbol_map) or []
        endpoint_label = "historical-chart/5min"
        points = _shape_points(rows)
    else:
        limit = None if normalized_timeframe == "MAX" else 1260
        rows = client.get_historical_eod(normalized_ticker, limit=limit, symbol_map=symbol_map) or []
        points = _shape_points(rows)
        points = _slice_for_timeframe(points, normalized_timeframe)

    raw_count = len(points)
    points = _downsample(points)
    if not points:
        return {
            **_empty_payload(normalized_ticker, normalized_timeframe, "no_data", "No hay datos suficientes para esta temporalidad."),
            "quote": quote,
            "name": quote.get("name") or profile.get("companyName") or profile.get("name") or normalized_ticker,
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
        "points": points,
        "summary": _summary(points),
        "quote": {
            "price": quote.get("price"),
            "change": quote.get("change"),
            "changesPercentage": quote.get("changesPercentage"),
            "previousClose": quote.get("previousClose"),
            "timestamp": quote.get("timestamp"),
        },
        "source": {
            "provider": "FMP",
            "endpoint": endpoint_label,
            "live_enabled": True,
            "downsampled": raw_count > len(points),
            "raw_points": raw_count,
        },
    }
