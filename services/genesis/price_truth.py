from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from app.settings import load_settings
from integrations.fmp.client import FmpClient
from services.genesis.market_format import format_market_number, number_or_none
from services.genesis.ticker_parser import normalize_ticker

_LOGGER = logging.getLogger("genesis.price_truth")


def validate_price_sanity(ticker: str, current_price: object, previous_close: object = None) -> dict[str, Any]:
    normalized = normalize_ticker(ticker)
    current = number_or_none(current_price)
    previous = number_or_none(previous_close)
    if current is None or current <= 0:
        return {"ok": False, "suspicious": True, "reason": "precio actual invalido"}
    if previous is not None and previous > 0:
        ratio = current / previous
        if ratio > 3 or ratio < 0.33:
            return {
                "ok": False,
                "suspicious": True,
                "reason": f"precio sospechoso: ratio {ratio:.2f} contra cierre previo",
            }
    if normalized == "BNO" and current > 200 and (previous is None or previous < 100):
        return {"ok": False, "suspicious": True, "reason": "BNO fuera de rango esperado; posible error de escala"}
    return {"ok": True, "suspicious": False, "reason": ""}


def get_verified_market_quote(
    ticker: str,
    *,
    quote: dict[str, Any] | None = None,
    snapshot: dict[str, Any] | None = None,
    client: FmpClient | None = None,
    settings: Any | None = None,
) -> dict[str, Any]:
    normalized = normalize_ticker(ticker)
    if not normalized:
        return _empty_quote("", "Ticker no valido.")

    settings = settings or load_settings()
    source = "sin_precio_confirmado"
    raw_quote = quote
    if raw_quote is None and getattr(settings, "fmp_api_key", "") and getattr(settings, "fmp_live_enabled", False):
        try:
            client = client or FmpClient(settings.fmp_api_key, logger=_LOGGER)
            raw_quote = client.get_quote(normalized) or {}
            source = "fmp"
        except Exception:
            _LOGGER.warning("FMP quote unavailable for %s", normalized, exc_info=True)
            raw_quote = {}

    if raw_quote:
        shaped = _shape_fmp_quote(normalized, raw_quote, "fmp" if source == "sin_precio_confirmado" else source)
        sanity = validate_price_sanity(normalized, shaped["current_price"], shaped["previous_close"])
        shaped["sanity"] = sanity
        if sanity["ok"]:
            return shaped
        shaped["current_price"] = None
        shaped["is_live"] = False
        shaped["source"] = "precio_sospechoso"
        shaped["message"] = "Precio sospechoso; no tengo precio confirmado para ese activo."
        return shaped

    fallback = _snapshot_quote(normalized, snapshot)
    if fallback:
        return fallback
    return _empty_quote(normalized, "No tengo precio confirmado para ese activo.")


def _shape_fmp_quote(ticker: str, quote: dict[str, Any], source: str) -> dict[str, Any]:
    current = number_or_none(quote.get("price") or quote.get("current_price"))
    previous = number_or_none(quote.get("previousClose") or quote.get("previous_close"))
    change = number_or_none(quote.get("change") or quote.get("daily_change"))
    pct = number_or_none(quote.get("changesPercentage") or quote.get("daily_change_pct"))
    return {
        "ticker": ticker,
        "name": str(quote.get("name") or quote.get("companyName") or ticker),
        "current_price": current,
        "previous_close": previous,
        "daily_change": change,
        "daily_change_pct": pct,
        "day_low": number_or_none(quote.get("dayLow") or quote.get("day_low") or quote.get("low")),
        "day_high": number_or_none(quote.get("dayHigh") or quote.get("day_high") or quote.get("high")),
        "volume": number_or_none(quote.get("volume") or quote.get("vol")),
        "quote_timestamp": quote.get("timestamp") or quote.get("lastUpdated") or quote.get("date") or datetime.now(timezone.utc).isoformat(),
        "source": source,
        "source_label": "Precio confirmado" if current is not None else "Sin precio confirmado",
        "is_live": source == "fmp" and current is not None,
        "is_stale": False,
        "market_session": quote.get("marketSession") or quote.get("market_session") or "",
        "extended_hours_price": number_or_none(quote.get("extendedHoursPrice") or quote.get("extended_hours_price")),
        "extended_hours_change": number_or_none(quote.get("extendedHoursChange") or quote.get("extended_hours_change")),
        "currency": str(quote.get("currency") or "USD").upper(),
        "formatted_price": format_market_number(current, currency=str(quote.get("currency") or "USD")),
        "sanity": {"ok": True, "suspicious": False, "reason": ""},
        "message": "",
    }


def _snapshot_quote(ticker: str, snapshot: dict[str, Any] | None = None) -> dict[str, Any] | None:
    if snapshot is None:
        try:
            from services.dashboard.get_radar_snapshot import get_radar_snapshot

            snapshot = get_radar_snapshot()
        except Exception:
            snapshot = {}

    items = []
    if isinstance(snapshot, dict):
        for key in ("items", "positions"):
            if isinstance(snapshot.get(key), list):
                items.extend(snapshot[key])
        portfolio = snapshot.get("portfolio") if isinstance(snapshot.get("portfolio"), dict) else {}
        if isinstance(portfolio.get("items"), list):
            items.extend(portfolio["items"])
        if isinstance(portfolio.get("positions"), list):
            items.extend(portfolio["positions"])

    for item in items:
        if not isinstance(item, dict):
            continue
        item_ticker = normalize_ticker(item.get("ticker") or item.get("symbol"))
        if item_ticker != ticker:
            continue
        current = number_or_none(item.get("current_price") or item.get("price"))
        reference = number_or_none(item.get("reference_price") or item.get("entry_price"))
        price = current if current is not None and current > 0 else reference
        if price is None or price <= 0:
            continue
        previous = number_or_none(item.get("previous_close") or item.get("previousClose"))
        sanity = validate_price_sanity(ticker, price, previous)
        if not sanity["ok"]:
            return {
                **_empty_quote(ticker, "Precio sospechoso; no tengo precio confirmado para ese activo."),
                "source": "precio_sospechoso",
                "sanity": sanity,
            }
        is_live = current is not None and current > 0
        return {
            "ticker": ticker,
            "name": str(item.get("name") or item.get("display_name") or ticker),
            "current_price": price,
            "previous_close": previous,
            "daily_change": number_or_none(item.get("daily_change") or item.get("change")),
            "daily_change_pct": number_or_none(item.get("daily_change_pct") or item.get("changesPercentage")),
            "day_low": number_or_none(item.get("day_low") or item.get("dayLow")),
            "day_high": number_or_none(item.get("day_high") or item.get("dayHigh")),
            "volume": number_or_none(item.get("volume")),
            "quote_timestamp": item.get("quote_timestamp") or item.get("updated_at") or "",
            "source": "snapshot_validado" if is_live else "referencia_paper",
            "source_label": "Precio confirmado" if is_live else "Precio de referencia",
            "is_live": is_live,
            "is_stale": not is_live,
            "market_session": item.get("market_session") or "",
            "extended_hours_price": number_or_none(item.get("extended_hours_price")),
            "extended_hours_change": number_or_none(item.get("extended_hours_change")),
            "currency": str(item.get("currency") or "USD").upper(),
            "formatted_price": format_market_number(price, currency=str(item.get("currency") or "USD")),
            "sanity": sanity,
            "message": "",
        }
    return None


def _empty_quote(ticker: str, message: str) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "name": ticker,
        "current_price": None,
        "previous_close": None,
        "daily_change": None,
        "daily_change_pct": None,
        "day_low": None,
        "day_high": None,
        "volume": None,
        "quote_timestamp": "",
        "source": "sin_precio_confirmado",
        "source_label": "Sin precio confirmado",
        "is_live": False,
        "is_stale": True,
        "market_session": "",
        "extended_hours_price": None,
        "extended_hours_change": None,
        "currency": "USD",
        "formatted_price": "Sin precio confirmado",
        "sanity": {"ok": False, "suspicious": False, "reason": message},
        "message": message,
    }
