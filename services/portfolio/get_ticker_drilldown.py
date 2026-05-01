from __future__ import annotations

from services.portfolio.get_portfolio_snapshot import _coerce_float, _normalize_ticker, get_portfolio_snapshot


def _resolve_quote(raw_portfolio: dict | None, ticker: str) -> dict:
    raw_portfolio = raw_portfolio or {}
    quotes = raw_portfolio.get("quotes")
    if not isinstance(quotes, dict):
        return {}

    for raw_key, payload in quotes.items():
        if _normalize_ticker(raw_key) == ticker and isinstance(payload, dict):
            return payload
    return {}


def _resolve_status(*, has_position: bool, has_live_price: bool, pnl_pct: float | None, has_entry_price: bool = True) -> str:
    if not has_position:
        return "watchlist"
    if not has_live_price:
        return "unpriced"
    if not has_entry_price:
        return "priced"
    if pnl_pct is None:
        return "unpriced"
    if pnl_pct > 0:
        return "gain"
    if pnl_pct < 0:
        return "loss"
    return "flat"


def get_ticker_drilldown(raw_portfolio: dict | None, ticker: str) -> dict:
    requested_ticker = _normalize_ticker(ticker)
    if not requested_ticker:
        return {
            "symbol": "",
            "ticker": "",
            "found": False,
            "error": "ticker_required",
        }

    snapshot = get_portfolio_snapshot(raw_portfolio or {})
    selected_position = next(
        (position for position in snapshot["positions"] if position["ticker"] == requested_ticker),
        None,
    )
    if selected_position is None:
        return {
            "symbol": requested_ticker,
            "ticker": requested_ticker,
            "found": False,
            "error": "ticker_not_found",
        }

    quote = _resolve_quote(raw_portfolio, requested_ticker)
    current_price = _coerce_float(quote.get("price"))
    has_live_price = current_price > 0
    units_value = _coerce_float(selected_position.get("units"))
    entry_price_value = _coerce_float(selected_position.get("entry_price"))
    amount_value = _coerce_float(selected_position.get("amount_usd"))
    mode = str(selected_position.get("mode") or "").strip()
    if units_value <= 0 and amount_value > 0 and entry_price_value > 0:
        units_value = round(amount_value / entry_price_value, 8)
    has_position = bool(selected_position["is_investment"] and units_value > 0)
    has_entry_price = entry_price_value > 0

    amount_usd = round(units_value * entry_price_value, 2) if has_position and has_entry_price else None
    entry_price = entry_price_value if has_position and has_entry_price else None
    units = units_value if has_position else None
    current_value = round(units * current_price, 2) if units is not None and has_live_price else None
    pnl_usd = round(current_value - amount_usd, 2) if current_value is not None and amount_usd is not None else None
    pnl_pct = round(((current_price - entry_price) / entry_price) * 100, 2) if has_position and has_live_price and entry_price else None

    return {
        "symbol": selected_position["ticker"],
        "ticker": selected_position["ticker"],
        "display_name": selected_position["display_name"],
        "found": True,
        "is_investment": selected_position["is_investment"],
        "mode": mode,
        "position_mode": mode or ("position" if has_position else "watchlist"),
        "amount_usd": amount_usd,
        "entry_price": entry_price,
        "opened_at": selected_position["opened_at"],
        "units": units,
        "current_price": round(current_price, 4) if has_live_price else None,
        "current_value": current_value,
        "daily_change": quote.get("change"),
        "daily_change_pct": quote.get("changesPercentage"),
        "previous_close": quote.get("previousClose"),
        "day_high": quote.get("dayHigh"),
        "day_low": quote.get("dayLow"),
        "extended_hours_price": quote.get("extendedHoursPrice"),
        "extended_hours_change": quote.get("extendedHoursChange"),
        "extended_hours_change_pct": quote.get("extendedHoursChangePct"),
        "market_session": quote.get("marketSession"),
        "volume": quote.get("volume"),
        "pnl_usd": pnl_usd,
        "pnl_pct": pnl_pct,
        "status": _resolve_status(
            has_position=has_position,
            has_live_price=has_live_price,
            pnl_pct=pnl_pct,
            has_entry_price=has_entry_price,
        ),
        "quote_timestamp": quote.get("timestamp") or quote.get("updated_at") or "",
    }
