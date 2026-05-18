from __future__ import annotations

from typing import Any

from services.genesis.hedge_engine import build_hedge_context
from services.genesis.tradingview_bridge import get_trading_context, receive_tradingview_webhook
from services.mt5.mt5_bridge import (
    mt5_account_sync,
    mt5_auto_forward_status,
    mt5_config,
    mt5_debug_storage,
    mt5_decision,
    mt5_forward_test,
    mt5_health,
    mt5_journal_recent,
    mt5_manual_tests_reset,
    mt5_order_request,
    mt5_order_result,
    mt5_outcomes_recent,
    mt5_performance,
    mt5_performance_auto,
    mt5_signal,
    mt5_shadow_trades,
    mt5_status,
    mt5_tick,
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


def get_genesis_mt5_status() -> dict[str, Any]:
    return mt5_status()


def get_genesis_mt5_journal_recent(limit: int = 25, symbol: str = "") -> dict[str, Any]:
    return mt5_journal_recent(limit=limit, symbol=symbol)


def get_genesis_mt5_performance(symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return mt5_performance(symbol=symbol, timeframe=timeframe)


def get_genesis_mt5_performance_auto(symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return mt5_performance_auto(symbol=symbol, timeframe=timeframe)


def get_genesis_mt5_forward_test(symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return mt5_forward_test(symbol=symbol, timeframe=timeframe)


def get_genesis_mt5_outcomes_recent(limit: int = 25, symbol: str = "") -> dict[str, Any]:
    return mt5_outcomes_recent(limit=limit, symbol=symbol)


def get_genesis_mt5_shadow_trades(limit: int = 100, symbol: str = "") -> dict[str, Any]:
    return mt5_shadow_trades(limit=limit, symbol=symbol)


def get_genesis_mt5_debug_storage(symbol: str = "") -> dict[str, Any]:
    return mt5_debug_storage(symbol=symbol)


def get_genesis_mt5_auto_forward_status(symbol: str = "") -> dict[str, Any]:
    return mt5_auto_forward_status(symbol=symbol)


def get_genesis_mt5_decision(symbol: str = "") -> dict[str, Any]:
    return mt5_decision(symbol)


def post_genesis_mt5_account_sync(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_account_sync(payload)


def post_genesis_mt5_signal(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_signal(payload)


def post_genesis_mt5_tick(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_tick(payload)


def post_genesis_mt5_order_request(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_order_request(payload)


def post_genesis_mt5_order_result(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_order_result(payload)


def post_genesis_mt5_manual_tests_reset(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_manual_tests_reset(payload)
