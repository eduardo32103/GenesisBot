from __future__ import annotations

from services.genesis.market_briefing import get_portfolio_briefing


class NewsMacroAgent:
    def market_overview(self) -> dict:
        briefing = get_portfolio_briefing()
        return {
            "intent": "market_overview",
            "answer": f"Lectura rapida: {briefing.get('answer')} Ballenas y alertas se tratan como evidencia, no como causalidad garantizada.",
            "portfolio": briefing,
        }

    def daily_briefing(self) -> dict:
        overview = self.market_overview()
        return {
            **overview,
            "intent": "daily_briefing",
            "answer": "Resumen del dia: " + overview["answer"],
        }


def get_news_macro_agent() -> NewsMacroAgent:
    return NewsMacroAgent()
