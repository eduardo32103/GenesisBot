from __future__ import annotations

from services.genesis.market_format import format_signed_percent, format_market_number
from services.genesis.price_agent import get_price_agent

_MARKET_TICKERS = ("SPY", "QQQ", "BTC-USD", "BZ=F", "IAU", "SLV")


class MarketOverviewAgent:
    def overview(self) -> dict:
        price_agent = get_price_agent()
        quotes = [price_agent.quote(ticker) for ticker in _MARKET_TICKERS]
        confirmed = [quote for quote in quotes if quote.get("current_price")]
        if not confirmed:
            return {
                "intent": "market_overview",
                "answer": "No tengo datos de mercado suficientes ahora. Falta precio confirmado en la fuente activa.",
                "quotes": quotes,
            }
        parts = [
            f"{quote.get('ticker')}: {format_market_number(quote.get('current_price'), currency=quote.get('currency') or 'USD')} ({format_signed_percent(quote.get('daily_change_pct'))})"
            for quote in confirmed
        ]
        return {
            "intent": "market_overview",
            "answer": "Mercado con datos confirmados: " + " | ".join(parts) + ".",
            "quotes": quotes,
        }


def get_market_overview_agent() -> MarketOverviewAgent:
    return MarketOverviewAgent()
