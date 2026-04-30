from __future__ import annotations


def _coerce_float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _normalize_ticker(value: object) -> str:
    return str(value or "").strip().upper()


def normalize_portfolio_positions(raw_portfolio: dict | None) -> list[dict]:
    raw_portfolio = raw_portfolio or {}
    positions_block = raw_portfolio.get("positions")
    if not isinstance(positions_block, dict):
        positions_block = raw_portfolio.get("portfolio")
    if not isinstance(positions_block, dict):
        positions_block = {
            key: value
            for key, value in raw_portfolio.items()
            if isinstance(value, dict) and ("amount_usd" in value or "entry_price" in value or "is_investment" in value)
        }

    normalized_positions: list[dict] = []
    for raw_ticker, payload in positions_block.items():
        ticker = _normalize_ticker(raw_ticker)
        if not ticker or not isinstance(payload, dict):
            continue

        amount_usd = _coerce_float(payload.get("amount_usd"))
        entry_price = _coerce_float(payload.get("entry_price"))
        is_investment = bool(payload.get("is_investment") or amount_usd > 0 or entry_price > 0)
        normalized_positions.append(
            {
                "ticker": ticker,
                "display_name": str(payload.get("display_name") or ticker),
                "is_investment": is_investment,
                "amount_usd": amount_usd,
                "entry_price": entry_price,
                "opened_at": payload.get("timestamp") or payload.get("opened_at") or "",
            }
        )

    normalized_positions.sort(key=lambda item: item["ticker"])
    return normalized_positions


def get_portfolio_snapshot(raw_portfolio: dict | None) -> dict:
    raw_portfolio = raw_portfolio or {}
    positions = normalize_portfolio_positions(raw_portfolio)
    return {
        "owner_id": raw_portfolio.get("owner_id", "legacy"),
        "tickers": [position["ticker"] for position in positions],
        "positions": positions,
        "summary": {
            "position_count": len(positions),
            "investment_count": sum(1 for position in positions if position["is_investment"]),
            "invested_capital": round(sum(position["amount_usd"] for position in positions), 2),
        },
    }
