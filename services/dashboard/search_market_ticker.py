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
        "name": quote.get("name") or profile.get("companyName") or profile.get("companyName") or profile.get("name") or ticker,
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


def _unique_symbols(rows: list[dict]) -> list[dict]:
    seen: set[str] = set()
    unique: list[dict] = []
    for row in rows:
        symbol = _normalize_ticker(row.get("symbol") or row.get("ticker"))
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        shaped = dict(row)
        shaped["symbol"] = symbol
        unique.append(shaped)
    return unique


def search_market_ticker(query: str = "") -> dict:
    raw_query = str(query or "").strip()
    ticker = _normalize_ticker(raw_query)
    is_direct_ticker = bool(ticker and _TICKER_PATTERN.match(ticker))
    if not raw_query:
        return {"ok": False, "status": "invalid", "message": "Ticker no valido.", "results": []}

    settings = load_settings()
    if not _live_ready(settings):
        if not is_direct_ticker:
            return {
                "ok": False,
                "status": "needs_live_search",
                "message": "Necesito datos directos para buscar por nombre.",
                "results": [],
            }
        return {
            "ok": True,
            "status": "local_fallback",
            "message": "Sin datos directos en local. Puedes agregarlo a seguimiento sin precio confirmado.",
            "results": [_shape_quote(ticker, None)],
        }

    client = FmpClient(settings.fmp_api_key, logger=logging.getLogger("genesis.dashboard"))
    if is_direct_ticker:
        quote = client.get_quote(ticker)
        if quote:
            profile = client.get_profile(ticker) or {}
            return {
                "ok": True,
                "status": "found",
                "message": "Activo encontrado.",
                "results": [_shape_quote(ticker, quote, profile)],
            }

    search_rows = _unique_symbols(client.search_symbols(raw_query, limit=5))
    shaped_results: list[dict] = []
    for row in search_rows[:5]:
        symbol = row["symbol"]
        quote = client.get_quote(symbol)
        profile = {"companyName": row.get("name") or symbol}
        shaped = _shape_quote(symbol, quote, profile)
        shaped["exchange"] = row.get("exchange") or ""
        shaped_results.append(shaped)

    if shaped_results:
        return {
            "ok": True,
            "status": "found",
            "message": "Activo encontrado.",
            "results": shaped_results,
        }

    return {
        "ok": False,
        "status": "not_found",
        "message": "No encontre ese ticker en mercado.",
        "results": [],
    }
