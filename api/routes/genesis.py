from __future__ import annotations

from typing import Any

from services.genesis.tradingview_bridge import get_trading_context, receive_tradingview_webhook


def get_genesis_trading_context(ticker: str = "") -> dict[str, Any]:
    return get_trading_context(ticker)


def post_genesis_tradingview_webhook(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return receive_tradingview_webhook(payload)
