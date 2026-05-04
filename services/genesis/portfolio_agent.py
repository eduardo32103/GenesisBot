from __future__ import annotations

from services.genesis.market_briefing import get_portfolio_briefing


class PortfolioAgent:
    def summary(self) -> dict:
        return get_portfolio_briefing()


def get_portfolio_agent() -> PortfolioAgent:
    return PortfolioAgent()
