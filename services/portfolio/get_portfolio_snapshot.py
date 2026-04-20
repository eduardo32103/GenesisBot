from __future__ import annotations


def get_portfolio_snapshot(raw_portfolio: dict) -> dict:
    return {
        "owner_id": raw_portfolio.get("owner_id", "legacy"),
        "tickers": raw_portfolio.get("tickers", []),
    }
