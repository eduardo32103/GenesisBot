from __future__ import annotations

import logging
import math
import re
from typing import Any

from app.settings import load_settings
from integrations.fmp.client import FmpClient
from services.genesis.returns_engine import calculate_return_detail, calculate_returns, flatten_return_details, slice_points_for_range
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


def _shape_ohlc(rows: list[dict[str, Any]], *, allow_price_only: bool = False) -> list[dict[str, Any]]:
    candles: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        time = _row_date(row)
        open_price = _safe_float(row.get("open"))
        high = _safe_float(row.get("high") or row.get("dayHigh"))
        low = _safe_float(row.get("low") or row.get("dayLow"))
        close = _safe_float(row.get("close") or row.get("price") or row.get("adjClose") or row.get("adj_close"))
        if allow_price_only and close is not None:
            open_price = open_price if open_price is not None else close
            high = high if high is not None else close
            low = low if low is not None else close
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
    return slice_points_for_range(points, timeframe)


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


def _max_truncation(max_history_years: float, eod_points: list[dict[str, Any]]) -> dict[str, Any]:
    if not eod_points:
        return {"is_max_truncated": True, "truncation_reason": "sin_historico_fmp"}
    if max_history_years <= 5.05:
        return {
            "is_max_truncated": True,
            "truncation_reason": "max_disponible_menor_o_igual_5y",
        }
    return {"is_max_truncated": False, "truncation_reason": ""}


def _max_history_note(max_history_years: float, eod_points: list[dict[str, Any]]) -> str:
    if not eod_points:
        return "MAX sin historico confirmado por FMP."
    if max_history_years <= 5.05:
        return f"MAX disponible: {max_history_years:.2f} anos reales de historico FMP."
    return f"MAX usa {max_history_years:.2f} anos reales de historico FMP."


def _first_date(points: list[dict[str, Any]]) -> str:
    return str((points[0] if points else {}).get("date") or (points[0] if points else {}).get("time") or "")


def _last_date(points: list[dict[str, Any]]) -> str:
    return str((points[-1] if points else {}).get("date") or (points[-1] if points else {}).get("time") or "")


def _empty_payload(ticker: str, timeframe: str, status: str, message: str) -> dict[str, Any]:
    return {
        "ok": False,
        "status": status,
        "message": message,
        "ticker": ticker,
        "selected_range": timeframe,
        "timeframe": timeframe,
        "range": timeframe,
        "points": [],
        "ohlc": [],
        "returns": {"1D": None, "1W": None, "1M": None, "1Y": None, "5Y": None, "MAX": None},
        "return_details": {},
        "indicators": compute_technical_indicators([]),
        "summary": _summary([]),
        "max_history_years": 0.0,
        "history_points": 0,
        "raw_eod_points": 0,
        "selected_range_points": 0,
        "fmp_endpoint_used": "",
        "has_full_history": False,
        "is_max_truncated": True,
        "max_truncated": True,
        "truncation_reason": "sin_historico_fmp",
        "max_history_note": "MAX sin historico confirmado por FMP.",
        "first_date": "",
        "last_date": "",
        "first_close": None,
        "last_close": None,
        "source": {
            "provider": "FMP",
            "endpoint": "",
            "live_enabled": False,
            "downsampled": False,
            "raw_points": 0,
            "fmp_endpoint_used": "",
            "has_full_history": False,
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

    eod_rows = client.get_full_historical_eod(normalized_ticker, symbol_map=symbol_map) or []
    get_meta = getattr(client, "get_full_history_meta", None)
    history_meta = get_meta(normalized_ticker) if callable(get_meta) else {}
    history_meta = history_meta if isinstance(history_meta, dict) else {}
    eod_points = _shape_ohlc(eod_rows)
    price_only_history = False
    if not eod_points:
        get_light_history = getattr(client, "get_historical_price_light", None)
        if callable(get_light_history):
            light_rows = get_light_history(normalized_ticker, symbol_map=symbol_map) or []
            if isinstance(light_rows, list):
                eod_points = _shape_ohlc(light_rows, allow_price_only=True)
                if eod_points:
                    price_only_history = True
                    history_meta = {
                        **history_meta,
                        "fmp_endpoint_used": history_meta.get("fmp_endpoint_used") or "historical-price-eod/light",
                        "raw_eod_points": len(light_rows),
                        "has_full_history": _history_years(eod_points) > 5.05,
                        "max_history_years": _history_years(eod_points),
                        "truncation_reason": "price_only_history",
                    }
    max_history_years = float(history_meta.get("max_history_years") or _history_years(eod_points))
    derived_truncation = _max_truncation(max_history_years, eod_points)
    raw_reason = str(history_meta.get("truncation_reason") or derived_truncation["truncation_reason"])
    if raw_reason.startswith("FMP devolvio solo"):
        raw_reason = "max_disponible_menor_o_igual_5y"
    max_truncation = {
        "is_max_truncated": bool(history_meta.get("is_max_truncated", history_meta.get("max_truncated", derived_truncation["is_max_truncated"]))),
        "truncation_reason": raw_reason,
    }
    max_truncation_alias = {**max_truncation, "max_truncated": max_truncation["is_max_truncated"]}
    max_history_note = _max_history_note(max_history_years, eod_points)
    intraday_points: list[dict[str, Any]] = []
    full_history_endpoint = str(history_meta.get("fmp_endpoint_used") or "historical-price-eod/full")
    endpoint_label = full_history_endpoint
    if normalized_timeframe == "1D":
        rows = client.get_intraday_history(normalized_ticker, interval="5min", limit=160, symbol_map=symbol_map) or []
        endpoint_label = "historical-chart/5min"
        intraday_points = _shape_ohlc(rows)
        points = intraday_points
    else:
        points = _slice_for_timeframe(eod_points, normalized_timeframe)

    selected_points = points
    raw_count = len(selected_points)
    points = _downsample_ohlc(selected_points)
    return_details = calculate_returns(
        eod_points,
        intraday_points,
        source="FMP",
        current_price=quote.get("price"),
        previous_close=quote.get("previousClose"),
        current_date=str(quote.get("timestamp") or "live"),
    )
    return_map = flatten_return_details(return_details)
    selected_return = calculate_return_detail(selected_points, normalized_timeframe, source="FMP")
    if not points:
        return {
            **_empty_payload(normalized_ticker, normalized_timeframe, "no_data", "No hay datos OHLC suficientes para esta temporalidad."),
            "quote": quote,
            "name": quote.get("name") or profile.get("companyName") or profile.get("name") or normalized_ticker,
            "returns": return_map,
            "return_details": return_details,
            "indicators": compute_technical_indicators([]),
            "max_history_years": max_history_years,
            **max_truncation_alias,
            "max_history_note": max_history_note,
            "history_points": len(eod_points),
            "raw_eod_points": len(eod_points),
            "selected_range_points": raw_count,
            "fmp_endpoint_used": full_history_endpoint,
            "has_full_history": bool(history_meta.get("has_full_history", max_history_years > 5.05)),
            "first_date": selected_return.get("first_date") or _first_date(eod_points),
            "last_date": selected_return.get("last_date") or _last_date(eod_points),
            "first_close": selected_return.get("first_close"),
            "last_close": selected_return.get("last_close"),
            "source": {
                "provider": "FMP",
                "endpoint": endpoint_label,
                "live_enabled": True,
                "price_only": price_only_history,
                "downsampled": False,
                "raw_points": 0,
                "selected_range_points": raw_count,
                "raw_eod_points": len(eod_points),
                "fmp_endpoint_used": full_history_endpoint,
                "has_full_history": bool(history_meta.get("has_full_history", max_history_years > 5.05)),
                "max_uses_full_history": True,
                "max_history_note": max_history_note,
                **max_truncation_alias,
            },
        }

    return {
        "ok": True,
        "status": "ready",
        "ticker": normalized_ticker,
        "selected_range": normalized_timeframe,
        "name": quote.get("name") or profile.get("companyName") or profile.get("name") or normalized_ticker,
        "timeframe": normalized_timeframe,
        "range": normalized_timeframe,
        "points": points,
        "ohlc": points,
        "price_only": price_only_history,
        "returns": return_map,
        "return_details": return_details,
        "indicators": compute_technical_indicators(selected_points),
        "summary": _summary(selected_points),
        "max_history_years": max_history_years,
        **max_truncation_alias,
        "max_history_note": max_history_note,
        "history_points": len(eod_points),
        "raw_eod_points": len(eod_points),
        "selected_range_points": raw_count,
        "fmp_endpoint_used": full_history_endpoint,
        "has_full_history": bool(history_meta.get("has_full_history", max_history_years > 5.05)),
        "first_date": selected_return.get("first_date"),
        "last_date": selected_return.get("last_date"),
        "first_close": selected_return.get("first_close"),
        "last_close": selected_return.get("last_close"),
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
            "price_only": price_only_history,
            "downsampled": raw_count > len(points),
            "raw_points": raw_count,
            "selected_range_points": raw_count,
            "raw_eod_points": len(eod_points),
            "fmp_endpoint_used": full_history_endpoint,
            "has_full_history": bool(history_meta.get("has_full_history", max_history_years > 5.05)),
            "max_history_years": max_history_years,
            "max_uses_full_history": True,
            "max_history_note": max_history_note,
            **max_truncation_alias,
        },
    }
