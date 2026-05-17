from __future__ import annotations

from typing import Any

from services.genesis.hedge_engine import build_hedge_context
from services.genesis.tradingview_bridge import get_trading_context, receive_tradingview_webhook
from services.mt5.mt5_bridge import (
    mt5_account_sync,
    mt5_config,
    mt5_decision,
    mt5_health,
    mt5_order_request,
    mt5_order_result,
    mt5_signal,
)


def get_genesis_trading_context(ticker: str = "") -> dict[str, Any]:
    return get_trading_context(ticker)


def get_genesis_hedge_plan(ticker: str = "") -> dict[str, Any]:
    return build_hedge_context(ticker, portfolio_mode=False)


def get_genesis_portfolio_hedge() -> dict[str, Any]:
    return build_hedge_context("", portfolio_mode=True)


def post_genesis_tradingview_webhook(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return receive_tradingview_webhook(payload)


def get_genesis_mt5_health() -> dict[str, Any]:
    return mt5_health()


def get_genesis_mt5_config() -> dict[str, Any]:
    return mt5_config()


def get_genesis_mt5_decision(symbol: str = "") -> dict[str, Any]:
    return mt5_decision(symbol)


def post_genesis_mt5_account_sync(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_account_sync(payload)


def post_genesis_mt5_signal(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_signal(payload)


def post_genesis_mt5_order_request(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_order_request(payload)


def post_genesis_mt5_order_result(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_order_result(payload)
