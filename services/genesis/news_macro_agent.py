from __future__ import annotations

from services.genesis.market_overview_agent import get_market_overview_agent


class NewsMacroAgent:
    def market_overview(self, question: str = "") -> dict:
        return get_market_overview_agent().overview(question)

    def daily_briefing(self, question: str = "") -> dict:
        overview = self.market_overview(question)
        return {
            **overview,
            "intent": "daily_briefing",
            "answer": overview["answer"],
        }


def get_news_macro_agent() -> NewsMacroAgent:
    return NewsMacroAgent()
