from __future__ import annotations

import logging
import re
from typing import Any

from app.settings import load_settings
from integrations.fmp.client import FmpClient

_TICKER_PATTERN = re.compile(r"^[A-Z0-9.\-=]{1,15}$")


def _normalize_ticker(value: object) -> str:
    return str(value or "").strip().upper()


def _live_ready(settings: Any) -> bool:
    return bool(getattr(settings, "fmp_api_key", "") and getattr(settings, "fmp_live_enabled", False))


def _shape_quote(ticker: str, quote: dict | None, profile: dict | None = None) -> dict:
    quote = quote or {}
    profile = profile or {}
    return {
        "ticker": ticker,
        "name": quote.get("name") or profile.get("companyName") or profile.get("companyName") or ticker,
        "current_price": quote.get("price"),
        "daily_change": quote.get("change"),
        "daily_change_pct": quote.get("changesPercentage"),
        "previous_close": quote.get("previousClose"),
        "day_high": quote.get("dayHigh"),
        "day_low": quote.get("dayLow"),
        "extended_hours_price": quote.get("extendedHoursPrice"),
        "extended_hours_change": quote.get("extendedHoursChange"),
        "extended_hours_change_pct": quote.get("extendedHoursChangePct"),
        "market_session": quote.get("marketSession") or "",
        "volume": quote.get("volume"),
        "quote_timestamp": quote.get("timestamp") or "",
        "source": "datos_directos" if quote.get("price") else "sin_precio",
    }


def search_market_ticker(query: str = "") -> dict:
    ticker = _normalize_ticker(query)
    if not ticker or not _TICKER_PATTERN.match(ticker):
        return {"ok": False, "status": "invalid", "message": "Ticker no valido.", "results": []}

    settings = load_settings()
    if not _live_ready(settings):
        return {
            "ok": True,
            "status": "local_fallback",
            "message": "Sin datos directos en local. Puedes agregarlo a seguimiento sin precio confirmado.",
            "results": [_shape_quote(ticker, None)],
        }

    client = FmpClient(settings.fmp_api_key, logger=logging.getLogger("genesis.dashboard"))
    quote = client.get_quote(ticker)
    if not quote:
        return {
            "ok": False,
            "status": "not_found",
            "message": "No encontre este ticker con la fuente activa.",
            "results": [],
        }

    profile = client.get_profile(ticker) or {}
    return {
        "ok": True,
        "status": "found",
        "message": "Activo encontrado.",
        "results": [_shape_quote(ticker, quote, profile)],
    }
