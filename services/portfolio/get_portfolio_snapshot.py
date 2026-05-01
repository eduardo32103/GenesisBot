from __future__ import annotations


def _coerce_float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _normalize_ticker(value: object) -> str:
    return str(value or "").strip().upper()


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _normalize_position_payload(raw_ticker: object, payload: object) -> dict | None:
    if isinstance(payload, (int, float)):
        ticker = _normalize_ticker(raw_ticker)
        if not ticker:
            return None
        return {
            "ticker": ticker,
            "display_name": ticker,
            "is_investment": False,
            "amount_usd": 0.0,
            "entry_price": 0.0,
            "units": 0.0,
            "reference_price": _coerce_float(payload),
            "opened_at": "",
        }

    if not isinstance(payload, dict):
        return None

    ticker = _normalize_ticker(payload.get("ticker") or payload.get("symbol") or raw_ticker)
    if not ticker:
        return None

    units = _coerce_float(payload.get("units") or payload.get("quantity") or payload.get("shares"))
    entry_price = _coerce_float(payload.get("entry_price") or payload.get("avg_price") or payload.get("average_price"))
    amount_usd = _coerce_float(payload.get("amount_usd") or payload.get("cost_basis"))
    if amount_usd <= 0 and units > 0 and entry_price > 0:
        amount_usd = round(units * entry_price, 8)
    if units <= 0 and amount_usd > 0 and entry_price > 0:
        units = round(amount_usd / entry_price, 8)

    is_investment = bool(
        _coerce_bool(payload.get("is_investment"))
        or units > 0
        or amount_usd > 0
    )
    reference_price = _coerce_float(
        payload.get("reference_price")
        or payload.get("current_price")
        or payload.get("price")
        or entry_price
    )

    return {
        "ticker": ticker,
        "display_name": str(payload.get("display_name") or payload.get("name") or ticker),
        "is_investment": is_investment,
        "amount_usd": amount_usd,
        "entry_price": entry_price,
        "units": units,
        "reference_price": reference_price,
        "mode": str(payload.get("mode") or "").strip(),
        "opened_at": payload.get("timestamp") or payload.get("opened_at") or "",
    }


def normalize_portfolio_positions(raw_portfolio: dict | None) -> list[dict]:
    raw_portfolio = raw_portfolio or {}
    positions_block = raw_portfolio.get("positions")
    if not isinstance(positions_block, (dict, list)):
        positions_block = raw_portfolio.get("portfolio")
    if not isinstance(positions_block, (dict, list)):
        positions_block = {
            key: value
            for key, value in raw_portfolio.items()
            if isinstance(value, (int, float))
            or (
                isinstance(value, dict)
                and (
                    "amount_usd" in value
                    or "entry_price" in value
                    or "units" in value
                    or "is_investment" in value
                    or "reference_price" in value
                )
            )
        }

    normalized_positions: list[dict] = []
    if isinstance(positions_block, list):
        iterable = enumerate(positions_block)
    else:
        iterable = positions_block.items() if isinstance(positions_block, dict) else []

    for raw_ticker, payload in iterable:
        normalized = _normalize_position_payload(raw_ticker, payload)
        if normalized:
            normalized_positions.append(normalized)

    normalized_positions.sort(key=lambda item: item["ticker"])
    return normalized_positions


def get_portfolio_snapshot(raw_portfolio: dict | None) -> dict:
    raw_portfolio = raw_portfolio or {}
    positions = normalize_portfolio_positions(raw_portfolio)
    return {
        "owner_id": raw_portfolio.get("owner_id", "dashboard_web"),
        "tickers": [position["ticker"] for position in positions],
        "positions": positions,
        "summary": {
            "position_count": len(positions),
            "investment_count": sum(1 for position in positions if position["is_investment"]),
            "invested_capital": round(sum(position["amount_usd"] for position in positions), 2),
            "unit_position_count": sum(1 for position in positions if position.get("units", 0.0) > 0),
        },
    }
