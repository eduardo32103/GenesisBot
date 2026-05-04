from __future__ import annotations

from services.genesis.market_overview_agent import get_market_overview_agent


class NewsMacroAgent:
    def market_overview(self) -> dict:
        return get_market_overview_agent().overview()

    def daily_briefing(self) -> dict:
        overview = self.market_overview()
        return {
            **overview,
            "intent": "daily_briefing",
            "answer": "Resumen del dia: " + overview["answer"],
        }


def get_news_macro_agent() -> NewsMacroAgent:
    return NewsMacroAgent()
