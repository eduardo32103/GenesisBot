from __future__ import annotations

from typing import Any

from services.genesis.price_truth import get_verified_market_quote, validate_price_sanity


class PriceAgent:
    def quote(self, ticker: str) -> dict[str, Any]:
        return get_verified_market_quote(ticker)

    def validate(self, ticker: str, current_price: object, previous_close: object = None) -> dict[str, Any]:
        return validate_price_sanity(ticker, current_price, previous_close)


def get_price_agent() -> PriceAgent:
    return PriceAgent()
