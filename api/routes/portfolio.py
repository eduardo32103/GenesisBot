from __future__ import annotations

from services.portfolio.get_portfolio_snapshot import get_portfolio_snapshot
from services.portfolio.get_ticker_drilldown import get_ticker_drilldown


def get_portfolio(raw_portfolio: dict | None = None) -> dict:
    return get_portfolio_snapshot(raw_portfolio or {})


def get_portfolio_ticker_drilldown(ticker: str, raw_portfolio: dict | None = None) -> dict:
    return get_ticker_drilldown(raw_portfolio or {}, ticker)
