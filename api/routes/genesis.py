from __future__ import annotations

from typing import Any

from services.genesis.hedge_engine import build_hedge_context
from services.genesis.tradingview_bridge import get_trading_context, receive_tradingview_webhook
from services.mt5.mt5_bridge import (
    mt5_account_sync,
    mt5_adaptive_recommendations,
    mt5_adaptive_state,
    mt5_autonomous_learning_status,
    mt5_auto_forward_status,
    mt5_backtest_latest,
    mt5_backtest_optimize,
    mt5_backtest_run,
    mt5_bars,
    mt5_capital_protection_status,
    mt5_config,
    mt5_debug_storage,
    mt5_decision,
    mt5_forward_test,
    mt5_forward_replay_run,
    mt5_forward_profile_state,
    mt5_health,
    mt5_instrument,
    mt5_journal_recent,
    mt5_learning_run,
    mt5_learning_status,
    mt5_manual_tests_reset,
    mt5_memory_summary,
    mt5_metrics_exclude_old_proxy,
    mt5_no_trade_report,
    mt5_ops_status,
    mt5_order_request,
    mt5_order_result,
    mt5_paper_defense,
    mt5_persistent_db_doctor_status,
    mt5_persistent_intelligence_bootstrap_status,
    mt5_persistent_intelligence_recent_events,
    mt5_outcomes_recent,
    mt5_performance,
    mt5_performance_auto,
    mt5_persistent_intelligence_status,
    mt5_promoted_profile,
    mt5_risk_recovery,
    mt5_risk_state,
    mt5_runtime_snapshot_inventory,
    mt5_replay_results,
    mt5_replay_reset,
    mt5_replay_run,
    mt5_replay_status,
    mt5_signal,
    mt5_shadow_trade_close,
    mt5_shadow_trades,
    mt5_shadow_trades_close_expired,
    mt5_shadow_trades_open,
    mt5_status,
    mt5_strategy_tournament_status,
    mt5_strategy_profiles,
    mt5_tick,
    mt5_ui_summary,
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


def get_genesis_mt5_ops_status(symbol: str = "") -> dict[str, Any]:
    return mt5_ops_status(symbol=symbol)


def get_genesis_mt5_risk_state(symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return mt5_risk_state(symbol=symbol, timeframe=timeframe)


def get_genesis_mt5_risk_recovery(symbol: str = "ETHUSD", timeframe: str = "M30") -> dict[str, Any]:
    return mt5_risk_recovery(symbol=symbol, timeframe=timeframe)


def get_genesis_mt5_persistent_intelligence_status() -> dict[str, Any]:
    return mt5_persistent_intelligence_status()


def get_genesis_mt5_persistent_intelligence_recent_events(limit: int = 10) -> dict[str, Any]:
    return mt5_persistent_intelligence_recent_events(limit=limit)


def get_genesis_mt5_persistent_intelligence_bootstrap_status() -> dict[str, Any]:
    return mt5_persistent_intelligence_bootstrap_status()


def get_genesis_mt5_persistent_db_doctor_status(
    *,
    repair: bool = False,
    apply_schema: bool = False,
    wait_for_connection: bool = False,
    max_connect_attempts: int = 10,
    verbose_sanitized: bool = False,
) -> dict[str, Any]:
    return mt5_persistent_db_doctor_status(
        repair=repair,
        apply_schema=apply_schema,
        wait_for_connection=wait_for_connection,
        max_connect_attempts=max_connect_attempts,
        verbose_sanitized=verbose_sanitized,
    )


def get_genesis_mt5_capital_protection_status() -> dict[str, Any]:
    return mt5_capital_protection_status()


def get_genesis_mt5_strategy_tournament_status() -> dict[str, Any]:
    return mt5_strategy_tournament_status()


def get_genesis_mt5_ui_summary(symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return mt5_ui_summary(symbol=symbol, timeframe=timeframe)


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


def get_genesis_mt5_no_trade_report(limit: int = 50, symbol: str = "") -> dict[str, Any]:
    return mt5_no_trade_report(limit=limit, symbol=symbol)


def get_genesis_mt5_shadow_trades(limit: int = 100, symbol: str = "") -> dict[str, Any]:
    return mt5_shadow_trades(limit=limit, symbol=symbol)


def get_genesis_mt5_shadow_trades_open(limit: int = 100, symbol: str = "") -> dict[str, Any]:
    return mt5_shadow_trades_open(limit=limit, symbol=symbol)


def post_genesis_mt5_shadow_trades_close_expired(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_shadow_trades_close_expired(payload)


def post_genesis_mt5_shadow_trade_close(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_shadow_trade_close(payload)


def get_genesis_mt5_debug_storage(symbol: str = "", limit: int = 20) -> dict[str, Any]:
    return mt5_debug_storage(symbol=symbol, limit=limit)


def get_genesis_mt5_runtime_snapshot_inventory(symbol: str = "XAUUSD", broker_symbol: str = "XAUUSD.b", timeframe: str = "M15") -> dict[str, Any]:
    return mt5_runtime_snapshot_inventory(symbol=symbol, broker_symbol=broker_symbol, timeframe=timeframe)


def get_genesis_mt5_instrument(symbol: str = "") -> dict[str, Any]:
    return mt5_instrument(symbol=symbol)


def get_genesis_mt5_auto_forward_status(symbol: str = "") -> dict[str, Any]:
    return mt5_auto_forward_status(symbol=symbol)


def get_genesis_mt5_decision(symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return mt5_decision(symbol, timeframe=timeframe)


def post_genesis_mt5_account_sync(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_account_sync(payload)


def post_genesis_mt5_signal(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_signal(payload)


def post_genesis_mt5_tick(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_tick(payload)


def post_genesis_mt5_bars(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_bars(payload)


def post_genesis_mt5_order_request(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_order_request(payload)


def post_genesis_mt5_order_result(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_order_result(payload)


def post_genesis_mt5_manual_tests_reset(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_manual_tests_reset(payload)


def post_genesis_mt5_metrics_exclude_old_proxy(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_metrics_exclude_old_proxy(payload)


def post_genesis_mt5_replay_run(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_replay_run(payload)


def get_genesis_mt5_replay_results(symbol: str = "") -> dict[str, Any]:
    return mt5_replay_results(symbol=symbol)


def get_genesis_mt5_replay_status(symbol: str = "") -> dict[str, Any]:
    return mt5_replay_status(symbol=symbol)


def post_genesis_mt5_replay_reset(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_replay_reset(payload)


def post_genesis_mt5_backtest_run(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_backtest_run(payload)


def post_genesis_mt5_backtest_optimize(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_backtest_optimize(payload)


def get_genesis_mt5_backtest_latest(symbol: str = "") -> dict[str, Any]:
    return mt5_backtest_latest(symbol=symbol)


def post_genesis_mt5_forward_replay_run(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return mt5_forward_replay_run(payload)


def post_genesis_mt5_learning_run(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        return mt5_learning_run(payload)
    except Exception as exc:
        return _mt5_learning_error(exc)


def get_genesis_mt5_learning_status(symbol: str = "") -> dict[str, Any]:
    return mt5_learning_status(symbol=symbol)


def get_genesis_mt5_autonomous_learning_status(symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return mt5_autonomous_learning_status(symbol=symbol, timeframe=timeframe)


def get_genesis_mt5_memory_summary(symbol: str = "", limit: int = 50) -> dict[str, Any]:
    try:
        return mt5_memory_summary(symbol=symbol, limit=limit)
    except Exception as exc:
        return _mt5_learning_error(exc, symbol=symbol)


def _mt5_learning_error(exc: Exception, *, symbol: str = "") -> dict[str, Any]:
    return {
        "ok": False,
        "status": "mt5_learning_error",
        "symbol": str(symbol or "").upper().strip(),
        "error": str(exc)[:500],
        "warnings": [],
        "errors": [{"error": str(exc)[:240]}],
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def get_genesis_mt5_adaptive_state(symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return mt5_adaptive_state(symbol=symbol, timeframe=timeframe)


def get_genesis_mt5_strategy_profiles(symbol: str = "") -> dict[str, Any]:
    return mt5_strategy_profiles(symbol=symbol)


def get_genesis_mt5_adaptive_recommendations(symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return mt5_adaptive_recommendations(symbol=symbol, timeframe=timeframe)


def get_genesis_mt5_paper_defense(symbol: str = "") -> dict[str, Any]:
    return mt5_paper_defense(symbol=symbol)


def get_genesis_mt5_promoted_profile(symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return mt5_promoted_profile(symbol=symbol, timeframe=timeframe)


def get_genesis_mt5_forward_profile_state(symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return mt5_forward_profile_state(symbol=symbol, timeframe=timeframe)
