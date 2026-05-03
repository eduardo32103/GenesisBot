from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.settings import load_settings
from integrations.fmp.client import FmpClient
from services.dashboard.get_operational_health import _connect_database, _safe_iso
from services.portfolio.get_portfolio_snapshot import normalize_portfolio_positions

_ROOT_DIR = Path(__file__).resolve().parents[2]
_PORTFOLIO_FALLBACK_PATH = _ROOT_DIR / "portfolio.json"
_MAX_LIVE_QUOTES = 25
_MAX_LIVE_PROFILES = 12

_SOURCE_LABELS = {
    "live": "live",
    "cache": "cache",
    "contingency": "contingencia",
    "unavailable": "unavailable",
}


def _coerce_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _coerce_optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_optional(value: float | None, digits: int = 2) -> float | None:
    return round(value, digits) if value is not None else None


def _safe_market_timestamp(value: Any) -> str:
    if value in (None, ""):
        return ""
    numeric = _coerce_optional_float(value)
    if numeric is not None:
        timestamp = numeric / 1000 if numeric > 10_000_000_000 else numeric
        try:
            return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
        except (OSError, OverflowError, ValueError):
            return ""
    return _safe_iso(value)


def _max_timestamp(values: list[str]) -> str:
    cleaned = [str(value or "").strip() for value in values if str(value or "").strip()]
    if not cleaned:
        return ""
    try:
        return max(cleaned)
    except Exception:
        return cleaned[-1]


def _infer_item_source(reference_price: float, explicit_source: str = "") -> str:
    normalized = str(explicit_source or "").strip().lower()
    if normalized in _SOURCE_LABELS:
        return normalized
    return "contingency" if reference_price > 0 else "unavailable"


def _source_note(source: str) -> str:
    if source == "live":
        return "Cotizacion directa verificada."
    if source == "cache":
        return "Ultima referencia disponible en datos guardados."
    if source == "contingency":
        return "Ultima referencia persistida; no reemplaza una cotizacion directa."
    return "Sin referencia suficiente para mostrar precio."


def _signal_text(is_investment: bool, source: str) -> str:
    if is_investment and source != "unavailable":
        return "Posicion abierta"
    if is_investment:
        return "Posicion abierta sin referencia"
    if source != "unavailable":
        return "En radar con referencia"
    return "Solo vigilancia"


def _apply_position_metrics(item: dict[str, Any]) -> None:
    units = _coerce_float(item.get("units"))
    entry_price = _coerce_float(item.get("entry_price"))
    current_price = _coerce_float(item.get("current_price"))
    reference_price = _coerce_float(item.get("reference_price"))
    valuation_price = current_price if current_price > 0 else reference_price
    cost_basis = _coerce_float(item.get("cost_basis") or item.get("amount_usd"))
    if cost_basis <= 0 and units > 0 and entry_price > 0:
        cost_basis = units * entry_price

    market_value = units * valuation_price if units > 0 and valuation_price > 0 else 0.0
    daily_change = _coerce_optional_float(item.get("daily_change"))
    daily_change_pct = _coerce_optional_float(item.get("daily_change_pct"))

    daily_pnl = None
    if market_value > 0 and daily_change_pct not in (None, -100):
        previous_value = market_value / (1 + (daily_change_pct / 100))
        daily_pnl = market_value - previous_value
    elif units > 0 and daily_change is not None:
        daily_pnl = units * daily_change

    unrealized_pnl = market_value - cost_basis if market_value > 0 and cost_basis > 0 else None
    unrealized_pnl_pct = (unrealized_pnl / cost_basis) * 100 if unrealized_pnl is not None and cost_basis > 0 else None

    item["cost_basis"] = round(cost_basis, 2) if cost_basis > 0 else 0.0
    item["amount_usd"] = item["cost_basis"]
    item["market_value"] = round(market_value, 2) if market_value > 0 else 0.0
    item["current_value"] = item["market_value"]
    item["unrealized_pnl"] = _round_optional(unrealized_pnl, 2)
    item["unrealized_pnl_pct"] = _round_optional(unrealized_pnl_pct, 2)
    item["daily_pnl"] = _round_optional(daily_pnl, 2)

    if current_price <= 0 and market_value <= 0:
        item["status"] = "no_concluyente"
    elif daily_change_pct is not None and daily_change_pct > 0:
        item["status"] = "en_alza"
    elif daily_change_pct is not None and daily_change_pct < 0:
        item["status"] = "a_la_baja"
    elif daily_change_pct == 0:
        item["status"] = "sin_cambio"
    elif units <= 0:
        item["status"] = "watchlist"
    else:
        item["status"] = "valor_calculado"


def _shape_item(
    ticker: str,
    *,
    display_name: str = "",
    is_investment: bool = False,
    amount_usd: float = 0.0,
    units: float = 0.0,
    entry_price: float = 0.0,
    reference_price: float = 0.0,
    current_price: float = 0.0,
    daily_change: float | None = None,
    daily_change_pct: float | None = None,
    source: str = "",
    updated_at: str = "",
    origin: str = "",
    mode: str = "",
    sector: str = "",
    industry: str = "",
    watchlist: bool | None = None,
    removed_watchlist: bool = False,
) -> dict[str, Any]:
    normalized_ticker = str(ticker or "").strip().upper()
    normalized_units = _coerce_float(units)
    normalized_entry = _coerce_float(entry_price)
    cost_basis = _coerce_float(amount_usd)
    if cost_basis <= 0 and normalized_units > 0 and normalized_entry > 0:
        cost_basis = normalized_units * normalized_entry
    if normalized_units <= 0 and cost_basis > 0 and normalized_entry > 0:
        normalized_units = round(cost_basis / normalized_entry, 8)

    normalized_price = _coerce_float(current_price)
    normalized_reference = _coerce_float(reference_price)
    normalized_source = _infer_item_source(normalized_price or normalized_reference, explicit_source=source)
    shaped = {
        "ticker": normalized_ticker,
        "name": str(display_name or normalized_ticker).strip(),
        "display_name": str(display_name or normalized_ticker).strip(),
        "is_investment": bool(is_investment or normalized_units > 0 or cost_basis > 0),
        "amount_usd": float(cost_basis or 0.0),
        "cost_basis": float(cost_basis or 0.0),
        "units": float(normalized_units or 0.0),
        "entry_price": float(normalized_entry or 0.0),
        "reference_price": float(normalized_reference or 0.0),
        "current_price": float(normalized_price or 0.0),
        "daily_change": _round_optional(_coerce_optional_float(daily_change), 4),
        "daily_change_pct": _round_optional(_coerce_optional_float(daily_change_pct), 4),
        "source": normalized_source,
        "source_label": _SOURCE_LABELS.get(normalized_source, "unavailable"),
        "source_note": _source_note(normalized_source),
        "signal": _signal_text(bool(is_investment or normalized_units > 0), normalized_source),
        "updated_at": _safe_iso(updated_at),
        "origin": origin or "unknown",
        "mode": str(mode or "").strip(),
        "sector": str(sector or "").strip(),
        "industry": str(industry or "").strip(),
        "watchlist": (normalized_units <= 0 and cost_basis <= 0) if watchlist is None else bool(watchlist),
        "removed_watchlist": bool(removed_watchlist),
    }
    _apply_position_metrics(shaped)
    return shaped


def _apply_live_quote(item: dict[str, Any], quote: dict[str, Any]) -> None:
    price = _coerce_float(quote.get("price"))
    if price <= 0:
        return

    item["current_price"] = price
    item["reference_price"] = price
    item["source"] = "live"
    item["source_label"] = _SOURCE_LABELS["live"]
    item["source_note"] = _source_note("live")
    item["signal"] = _signal_text(bool(item.get("is_investment")), "live")
    item["daily_change"] = _round_optional(_coerce_optional_float(quote.get("change")), 4)
    item["daily_change_pct"] = _round_optional(_coerce_optional_float(quote.get("changesPercentage")), 4)
    item["change_pct"] = item["daily_change_pct"]
    item["percent_change"] = item["daily_change_pct"]
    item["previous_close"] = _round_optional(_coerce_optional_float(quote.get("previousClose")), 4)
    item["day_high"] = _round_optional(_coerce_optional_float(quote.get("dayHigh")), 4)
    item["day_low"] = _round_optional(_coerce_optional_float(quote.get("dayLow")), 4)
    item["extended_hours_price"] = _round_optional(_coerce_optional_float(quote.get("extendedHoursPrice")), 4)
    item["extended_hours_change"] = _round_optional(_coerce_optional_float(quote.get("extendedHoursChange")), 4)
    item["extended_hours_change_pct"] = _round_optional(_coerce_optional_float(quote.get("extendedHoursChangePct")), 4)
    item["market_session"] = str(quote.get("marketSession") or "").strip()
    item["volume"] = _round_optional(_coerce_optional_float(quote.get("volume") or quote.get("vol")), 0)
    item["avg_volume"] = _round_optional(_coerce_optional_float(quote.get("avgVolume")), 0)
    item["quote_timestamp"] = _safe_market_timestamp(quote.get("timestamp") or quote.get("lastUpdated") or quote.get("date") or "")
    if item["quote_timestamp"]:
        item["updated_at"] = item["quote_timestamp"]
    if quote.get("name"):
        item["name"] = str(quote.get("name") or "").strip()
        item["display_name"] = item["name"]
    _apply_position_metrics(item)


def _apply_live_profile(item: dict[str, Any], profile: dict[str, Any]) -> None:
    if not isinstance(profile, dict):
        return
    name = profile.get("companyName") or profile.get("name")
    if name:
        item["name"] = str(name).strip()
        item["display_name"] = item["name"]
    item["sector"] = str(profile.get("sector") or item.get("sector") or "").strip()
    item["industry"] = str(profile.get("industry") or item.get("industry") or "").strip()


def _fmp_live_ready(settings: Any) -> bool:
    return bool(getattr(settings, "fmp_api_key", "") and getattr(settings, "fmp_live_enabled", False))


def _fetch_wallet_rows(database_url: str, chat_id: str) -> list[dict[str, Any]]:
    if not database_url or not str(chat_id or "").strip().isdigit():
        return []

    conn = None
    try:
        conn = _connect_database(database_url)
        if not conn:
            return []

        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT ticker, is_investment, amount_usd, entry_price, timestamp
            FROM wallet
            WHERE user_id=%s
            ORDER BY ticker
            """,
            (int(chat_id),),
        )
        rows = cursor.fetchall() or []
        conn.commit()
        shaped = []
        for row in rows:
            amount_usd = _coerce_float(row[2])
            entry_price = _coerce_float(row[3])
            units = round(amount_usd / entry_price, 8) if amount_usd > 0 and entry_price > 0 else 0.0
            shaped.append(
                _shape_item(
                    row[0],
                    is_investment=bool(row[1]) or units > 0,
                    amount_usd=amount_usd,
                    units=units,
                    entry_price=entry_price,
                    reference_price=entry_price,
                    updated_at=row[4] or "",
                    origin="database",
                )
            )
        return shaped
    except Exception:
        return []
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _parse_portfolio_fallback() -> list[dict[str, Any]]:
    if not _PORTFOLIO_FALLBACK_PATH.exists():
        return []

    try:
        raw = json.loads(_PORTFOLIO_FALLBACK_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []

    timestamp = datetime.fromtimestamp(_PORTFOLIO_FALLBACK_PATH.stat().st_mtime, tz=timezone.utc).isoformat()
    items: list[dict[str, Any]] = []

    if isinstance(raw, dict) and ("positions" in raw or "portfolio" in raw):
        for position in normalize_portfolio_positions(raw):
            items.append(
                _shape_item(
                    position["ticker"],
                    display_name=position.get("display_name", ""),
                    is_investment=bool(position.get("is_investment")),
                    amount_usd=_coerce_float(position.get("amount_usd")),
                    units=_coerce_float(position.get("units")),
                    entry_price=_coerce_float(position.get("entry_price")),
                    reference_price=_coerce_float(position.get("reference_price")),
                    updated_at=position.get("opened_at") or timestamp,
                    origin="portfolio_fallback",
                    mode=position.get("mode", ""),
                    watchlist=bool(position.get("watchlist")),
                    removed_watchlist=bool(position.get("removed_watchlist")),
                )
            )
        return sorted(items, key=lambda item: item["ticker"])

    if isinstance(raw, dict):
        for ticker, value in raw.items():
            if isinstance(value, (int, float)):
                items.append(
                    _shape_item(
                        ticker,
                        reference_price=float(value),
                        updated_at=timestamp,
                        origin="portfolio_fallback",
                    )
                )
                continue

            if isinstance(value, dict):
                normalized = normalize_portfolio_positions({"positions": {ticker: value}})
                if not normalized:
                    continue
                position = normalized[0]
                items.append(
                    _shape_item(
                        position["ticker"],
                        display_name=position.get("display_name", ""),
                        is_investment=bool(position.get("is_investment")),
                        amount_usd=_coerce_float(position.get("amount_usd")),
                        units=_coerce_float(position.get("units")),
                        entry_price=_coerce_float(position.get("entry_price")),
                        reference_price=_coerce_float(position.get("reference_price")),
                        source=str(value.get("source", "")),
                        updated_at=position.get("opened_at") or timestamp,
                        origin="portfolio_fallback",
                        mode=position.get("mode", ""),
                        watchlist=bool(position.get("watchlist")),
                        removed_watchlist=bool(position.get("removed_watchlist")),
                    )
                )
        return sorted(items, key=lambda item: item["ticker"])

    if isinstance(raw, list):
        for ticker in raw:
            items.append(_shape_item(str(ticker), updated_at=timestamp, origin="portfolio_fallback"))
    return sorted(items, key=lambda item: item["ticker"])


def _merge_persistent_overlays(database_items: list[dict[str, Any]], fallback_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not database_items:
        return fallback_items
    if not fallback_items:
        return database_items

    merged = {str(item.get("ticker") or "").strip().upper(): dict(item) for item in database_items if str(item.get("ticker") or "").strip()}
    for overlay in fallback_items:
        ticker = str(overlay.get("ticker") or "").strip().upper()
        if not ticker:
            continue

        overlay_units = _coerce_float(overlay.get("units"))
        overlay_cost = _coerce_float(overlay.get("cost_basis") or overlay.get("amount_usd"))
        overlay_entry = _coerce_float(overlay.get("entry_price"))
        overlay_is_position = bool(overlay.get("is_investment")) or overlay_units > 0 or overlay_cost > 0
        overlay_removed = bool(overlay.get("removed_watchlist"))

        if overlay_removed and not overlay_is_position:
            merged.pop(ticker, None)
            continue

        if ticker not in merged:
            merged[ticker] = dict(overlay)
            continue

        item = merged[ticker]
        if overlay_is_position:
            item["is_investment"] = True
            item["units"] = overlay_units
            item["entry_price"] = overlay_entry
            if overlay_cost > 0:
                item["cost_basis"] = overlay_cost
                item["amount_usd"] = overlay_cost
            elif overlay_units > 0 and overlay_entry > 0:
                item["cost_basis"] = round(overlay_units * overlay_entry, 8)
                item["amount_usd"] = item["cost_basis"]
            if overlay.get("mode"):
                item["mode"] = overlay.get("mode")
            if overlay.get("opened_at"):
                item["opened_at"] = overlay.get("opened_at")
            if overlay_removed:
                item["watchlist"] = False
                item["removed_watchlist"] = True
            elif bool(overlay.get("watchlist")):
                item["watchlist"] = True
                item.pop("removed_watchlist", None)

        elif bool(overlay.get("watchlist")):
            item["watchlist"] = True
            item.pop("removed_watchlist", None)

        reference_price = _coerce_float(overlay.get("reference_price"))
        if reference_price > 0 and _coerce_float(item.get("current_price")) <= 0:
            item["reference_price"] = reference_price
        _apply_position_metrics(item)

    return sorted(merged.values(), key=lambda item: str(item.get("ticker") or ""))


def _enrich_items_with_fmp(items: list[dict[str, Any]], settings: Any) -> None:
    if not items or not _fmp_live_ready(settings):
        return

    client = FmpClient(settings.fmp_api_key, logger=logging.getLogger("genesis.dashboard"))
    quoted = 0
    profiled = 0
    for item in items:
        ticker = str(item.get("ticker") or "").strip().upper()
        if not ticker or quoted >= _MAX_LIVE_QUOTES:
            continue
        quote = client.get_quote(ticker) or {}
        quoted += 1
        if isinstance(quote, dict) and quote:
            _apply_live_quote(item, quote)

        if item.get("is_investment") and profiled < _MAX_LIVE_PROFILES:
            profile = client.get_profile(ticker) or {}
            profiled += 1
            _apply_live_profile(item, profile)


def _apply_portfolio_totals(items: list[dict[str, Any]]) -> dict[str, Any]:
    total_value = round(sum(_coerce_float(item.get("market_value")) for item in items), 2)
    total_cost_basis = round(sum(_coerce_float(item.get("cost_basis")) for item in items if _coerce_float(item.get("cost_basis")) > 0), 2)
    unrealized_values = [_coerce_float(item.get("unrealized_pnl")) for item in items if item.get("unrealized_pnl") is not None]
    daily_values = [_coerce_float(item.get("daily_pnl")) for item in items if item.get("daily_pnl") is not None]
    total_unrealized = round(sum(unrealized_values), 2) if unrealized_values else None
    daily_pnl = round(sum(daily_values), 2) if daily_values else None

    for item in items:
        market_value = _coerce_float(item.get("market_value"))
        weight_pct = (market_value / total_value) * 100 if total_value > 0 and market_value > 0 else None
        item["weight_pct"] = _round_optional(weight_pct, 4)

    valued = [item for item in items if _coerce_float(item.get("market_value")) > 0]
    top = max(valued, key=lambda item: _coerce_float(item.get("market_value")), default={})
    top_concentration = {
        "ticker": top.get("ticker", ""),
        "weight_pct": top.get("weight_pct"),
    } if top else {}

    sector_totals: dict[str, float] = {}
    for item in valued:
        sector = str(item.get("sector") or "").strip()
        if not sector:
            continue
        sector_totals[sector] = sector_totals.get(sector, 0.0) + _coerce_float(item.get("market_value"))
    sector_exposure = [
        {
            "sector": sector,
            "value": round(value, 2),
            "weight_pct": _round_optional((value / total_value) * 100 if total_value > 0 else None, 2),
        }
        for sector, value in sorted(sector_totals.items(), key=lambda pair: pair[1], reverse=True)
    ]

    position_count = sum(1 for item in items if _coerce_float(item.get("units")) > 0)
    watchlist_count = sum(1 for item in items if bool(item.get("watchlist")) or _coerce_float(item.get("units")) <= 0)

    live_watchlist = [
        item
        for item in items
        if (bool(item.get("watchlist")) or _coerce_float(item.get("units")) <= 0)
        and _coerce_float(item.get("current_price")) > 0
    ]
    if not valued and position_count:
        perspective = "Genesis: hay posiciones con cantidades, pero falta precio actual para calcular valor y peso."
    elif not valued and live_watchlist:
        movers = [
            item
            for item in live_watchlist
            if _coerce_optional_float(item.get("daily_change_pct")) is not None
        ]
        movers.sort(key=lambda item: abs(_coerce_float(item.get("daily_change_pct"))), reverse=True)
        perspective = "Genesis: watchlist con datos directos activos. Aun no puedo calcular concentracion sin cantidades."
        if movers:
            top = movers[0]
            perspective += f" Movimiento mas visible: {top.get('ticker')} {round(_coerce_float(top.get('daily_change_pct')), 2)}%."
    elif not valued:
        perspective = "Genesis: cartera en modo watchlist. Faltan cantidades para calcular pesos y concentracion."
    elif top_concentration.get("weight_pct") and float(top_concentration["weight_pct"]) >= 45:
        perspective = f"Genesis: cartera concentrada. {top_concentration['ticker']} pesa {round(float(top_concentration['weight_pct']), 1)}% del valor calculado."
    else:
        perspective = "Genesis: cartera calculable sin concentracion extrema visible."
        if watchlist_count:
            perspective += f" Hay {watchlist_count} activos en vigilancia sin cantidades."

    return {
        "total_value": total_value,
        "total_cost_basis": total_cost_basis,
        "total_unrealized_pnl": total_unrealized if total_cost_basis > 0 else None,
        "total_unrealized_pnl_pct": _round_optional((total_unrealized / total_cost_basis) * 100 if total_unrealized is not None and total_cost_basis > 0 else None, 2),
        "daily_pnl": daily_pnl if total_value > 0 else None,
        "daily_pnl_pct": _round_optional((daily_pnl / (total_value - daily_pnl)) * 100 if daily_pnl is not None and total_value > 0 and total_value != daily_pnl else None, 2),
        "number_of_positions": position_count,
        "watchlist_count": watchlist_count,
        "top_concentration": top_concentration,
        "sector_exposure": sector_exposure,
        "genesis_perspective": perspective,
    }


def _build_snapshot_summary(items: list[dict[str, Any]], data_origin: str, portfolio: dict[str, Any] | None = None) -> dict[str, Any]:
    investment_count = sum(1 for item in items if item.get("is_investment"))
    with_reference = sum(1 for item in items if item.get("source") != "unavailable")
    unavailable_count = sum(1 for item in items if item.get("source") == "unavailable")
    last_update = _max_timestamp([str(item.get("quote_timestamp") or item.get("updated_at") or "") for item in items])

    if not items:
        note = "No hay activos vigilados todavia."
    elif data_origin == "database":
        note = "Cartera leida desde la tabla wallet."
    elif data_origin == "portfolio_fallback":
        note = "Cartera leida desde portfolio.json."
    else:
        note = "Cartera sin fuente persistida disponible."

    summary = {
        "tracked_count": len(items),
        "investment_count": investment_count,
        "reference_count": with_reference,
        "unavailable_count": unavailable_count,
        "last_update": last_update,
        "data_origin": data_origin,
        "note": note,
    }
    if portfolio:
        summary["portfolio"] = portfolio
        summary.update(portfolio)
    return summary


def get_radar_snapshot() -> dict[str, Any]:
    settings = load_settings()
    items = _fetch_wallet_rows(settings.database_url, settings.chat_id)
    data_origin = "database" if items else "none"
    fallback_items = _parse_portfolio_fallback()

    if items:
        items = _merge_persistent_overlays(items, fallback_items)
    elif fallback_items:
        items = fallback_items
        data_origin = "portfolio_fallback"

    _enrich_items_with_fmp(items, settings)
    portfolio = _apply_portfolio_totals(items)

    items = sorted(
        items,
        key=lambda item: (
            0 if _coerce_float(item.get("units")) > 0 else 1,
            item.get("source") == "unavailable",
            item.get("ticker") or "",
        ),
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": _build_snapshot_summary(items, data_origin, portfolio),
        "items": items,
    }
