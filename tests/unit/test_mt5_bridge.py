from __future__ import annotations

import os
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from api.main import create_app
from api.routes.genesis import (
    get_genesis_mt5_adaptive_recommendations,
    get_genesis_mt5_adaptive_state,
    get_genesis_mt5_auto_forward_status,
    get_genesis_mt5_backtest_latest,
    get_genesis_mt5_config,
    get_genesis_mt5_debug_storage,
    get_genesis_mt5_decision,
    get_genesis_mt5_forward_profile_state,
    get_genesis_mt5_forward_test,
    get_genesis_mt5_health,
    get_genesis_mt5_instrument,
    get_genesis_mt5_journal_recent,
    get_genesis_mt5_learning_status,
    get_genesis_mt5_memory_summary,
    get_genesis_mt5_no_trade_report,
    get_genesis_mt5_outcomes_recent,
    get_genesis_mt5_paper_defense,
    get_genesis_mt5_performance,
    get_genesis_mt5_performance_auto,
    get_genesis_mt5_promoted_profile,
    get_genesis_mt5_replay_results,
    get_genesis_mt5_replay_status,
    get_genesis_mt5_shadow_trades,
    get_genesis_mt5_status,
    get_genesis_mt5_strategy_profiles,
    post_genesis_mt5_account_sync,
    post_genesis_mt5_backtest_optimize,
    post_genesis_mt5_backtest_run,
    post_genesis_mt5_forward_replay_run,
    post_genesis_mt5_metrics_exclude_old_proxy,
    post_genesis_mt5_order_request,
    post_genesis_mt5_order_result,
    post_genesis_mt5_manual_tests_reset,
    post_genesis_mt5_learning_run,
    post_genesis_mt5_replay_reset,
    post_genesis_mt5_replay_run,
    post_genesis_mt5_signal,
    post_genesis_mt5_tick,
)
from services.genesis.agent_router import AgentRouter
from services.genesis.memory_store import MemoryStore
from services.genesis.tool_router import route_message
from services.mt5.mt5_bridge import (
    mt5_adaptive_recommendations,
    mt5_adaptive_state,
    mt5_auto_forward_status,
    mt5_account_sync,
    mt5_backtest_latest,
    mt5_backtest_optimize,
    mt5_backtest_run,
    mt5_config,
    mt5_debug_storage,
    mt5_decision,
    mt5_forward_test,
    mt5_forward_replay_run,
    mt5_forward_profile_state,
    mt5_instrument,
    mt5_journal_recent,
    mt5_learning_run,
    mt5_learning_status,
    mt5_manual_tests_reset,
    mt5_memory_summary,
    mt5_metrics_exclude_old_proxy,
    mt5_no_trade_report,
    mt5_order_request,
    mt5_outcomes_recent,
    mt5_paper_defense,
    mt5_performance,
    mt5_performance_auto,
    mt5_promoted_profile,
    mt5_replay_reset,
    mt5_replay_results,
    mt5_replay_run,
    mt5_replay_status,
    mt5_signal,
    mt5_shadow_trades,
    mt5_status,
    mt5_strategy_profiles,
    mt5_tick,
)
from services.mt5.mt5_config import is_paper_exploration_enabled
from services.mt5.mt5_decision_signal_builder import build_actionable_mt5_decision
from services.mt5.mt5_backtester import MT5Backtester
from services.mt5.mt5_journal import MT5Journal
from services.mt5.mt5_ingest_queue import MT5IngestQueue
from services.mt5.mt5_order_model import MT5OrderIntent
from services.mt5.mt5_paper_defense import MT5PaperDefense
from services.mt5.mt5_promoted_profile import record_promoted_profile, reset_promoted_profiles_for_tests
from services.mt5.mt5_risk_guard import MT5BridgeConfig, MT5RiskGuard
from services.mt5.mt5_signal_router import MT5SignalRouter
from services.mt5.mt5_symbol_mapper import MT5SymbolMapper


_LEARNING_ENABLED_ENV = {
    "MT5_FAST_PATH_ONLY": "false",
    "MT5_LEARNING_RUN_ENABLED": "true",
    "MT5_MEMORY_SUMMARY_ENABLED": "true",
    "MT5_ADAPTIVE_LEARNING_ENABLED": "true",
}


def _paper_edge_tick(price: float = 100.0, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "symbol": "BTCUSD",
        "last": price,
        "spread": 10,
        "score": 80,
        "momentum_score": 80,
        "trend_score": 80,
        "volatility_score": 80,
        "regime": "breakout",
    }
    payload.update(overrides)
    return payload


class MT5BridgeTests(unittest.TestCase):
    def test_create_app_exposes_mt5_endpoints(self) -> None:
        app = create_app()

        self.assertEqual(app["genesis_mt5_health_endpoint"], "/api/genesis/mt5/health")
        self.assertEqual(app["genesis_mt5_status_endpoint"], "/api/genesis/mt5/status")
        self.assertEqual(app["genesis_mt5_config_endpoint"], "/api/genesis/mt5/config")
        self.assertEqual(app["genesis_mt5_ops_status_endpoint"], "/api/genesis/mt5/ops/status?symbol={symbol}")
        self.assertEqual(app["genesis_mt5_decision_endpoint"], "/api/genesis/mt5/decision?symbol={symbol}&timeframe={timeframe}")
        self.assertEqual(app["genesis_mt5_journal_recent_endpoint"], "/api/genesis/mt5/journal/recent?symbol={symbol}&limit=25")
        self.assertEqual(app["genesis_mt5_performance_endpoint"], "/api/genesis/mt5/performance?symbol={symbol}&timeframe={timeframe}")
        self.assertEqual(app["genesis_mt5_performance_auto_endpoint"], "/api/genesis/mt5/performance/auto?symbol={symbol}&timeframe={timeframe}")
        self.assertEqual(app["genesis_mt5_forward_test_endpoint"], "/api/genesis/mt5/forward-test?symbol={symbol}")
        self.assertEqual(app["genesis_mt5_outcomes_recent_endpoint"], "/api/genesis/mt5/outcomes/recent?symbol={symbol}&limit=25")
        self.assertEqual(app["genesis_mt5_no_trade_report_endpoint"], "/api/genesis/mt5/no-trade-report?symbol={symbol}&limit=50")
        self.assertEqual(app["genesis_mt5_shadow_trades_endpoint"], "/api/genesis/mt5/shadow-trades?symbol={symbol}")
        self.assertEqual(app["genesis_mt5_debug_storage_endpoint"], "/api/genesis/mt5/debug/storage?symbol={symbol}")
        self.assertEqual(app["genesis_mt5_auto_forward_status_endpoint"], "/api/genesis/mt5/auto-forward-status?symbol={symbol}")
        self.assertEqual(app["genesis_mt5_instrument_endpoint"], "/api/genesis/mt5/instrument?symbol={symbol}")
        self.assertEqual(app["genesis_mt5_metrics_exclude_old_proxy_endpoint"], "/api/genesis/mt5/metrics/exclude-old-proxy")
        self.assertEqual(app["genesis_mt5_replay_run_endpoint"], "/api/genesis/mt5/replay/run")
        self.assertEqual(app["genesis_mt5_replay_results_endpoint"], "/api/genesis/mt5/replay/results?symbol={symbol}")
        self.assertEqual(app["genesis_mt5_replay_status_endpoint"], "/api/genesis/mt5/replay/status?symbol={symbol}")
        self.assertEqual(app["genesis_mt5_replay_reset_endpoint"], "/api/genesis/mt5/replay/reset")
        self.assertEqual(app["genesis_mt5_backtest_run_endpoint"], "/api/genesis/mt5/backtest/run")
        self.assertEqual(app["genesis_mt5_backtest_optimize_endpoint"], "/api/genesis/mt5/backtest/optimize")
        self.assertEqual(app["genesis_mt5_backtest_latest_endpoint"], "/api/genesis/mt5/backtest/latest?symbol={symbol}")
        self.assertEqual(app["genesis_mt5_forward_replay_run_endpoint"], "/api/genesis/mt5/forward-replay/run")
        self.assertEqual(app["genesis_mt5_learning_run_endpoint"], "/api/genesis/mt5/learning/run")
        self.assertEqual(app["genesis_mt5_learning_status_endpoint"], "/api/genesis/mt5/learning/status?symbol={symbol}")
        self.assertEqual(app["genesis_mt5_memory_summary_endpoint"], "/api/genesis/mt5/memory/summary?symbol={symbol}&limit=50")
        self.assertEqual(app["genesis_mt5_adaptive_state_endpoint"], "/api/genesis/mt5/adaptive-state?symbol={symbol}")
        self.assertEqual(app["genesis_mt5_strategy_profiles_endpoint"], "/api/genesis/mt5/strategy-profiles?symbol={symbol}")
        self.assertEqual(app["genesis_mt5_adaptive_recommendations_endpoint"], "/api/genesis/mt5/adaptive-recommendations?symbol={symbol}")
        self.assertEqual(app["genesis_mt5_paper_defense_endpoint"], "/api/genesis/mt5/paper-defense?symbol={symbol}")
        self.assertEqual(app["genesis_mt5_promoted_profile_endpoint"], "/api/genesis/mt5/promoted-profile?symbol={symbol}&timeframe={timeframe}")
        self.assertEqual(app["genesis_mt5_forward_profile_state_endpoint"], "/api/genesis/mt5/forward-profile-state?symbol={symbol}&timeframe={timeframe}")
        self.assertEqual(app["genesis_mt5_account_sync_endpoint"], "/api/genesis/mt5/account-sync")
        self.assertEqual(app["genesis_mt5_signal_endpoint"], "/api/genesis/mt5/signal")
        self.assertEqual(app["genesis_mt5_tick_endpoint"], "/api/genesis/mt5/tick")
        self.assertEqual(app["genesis_mt5_order_request_endpoint"], "/api/genesis/mt5/order-request")
        self.assertEqual(app["genesis_mt5_order_result_endpoint"], "/api/genesis/mt5/order-result")
        self.assertEqual(app["genesis_mt5_manual_tests_reset_endpoint"], "/api/genesis/mt5/manual-tests/reset")

    def test_default_health_and_config_are_journal_only(self) -> None:
        health = get_genesis_mt5_health()
        config = get_genesis_mt5_config()
        status = get_genesis_mt5_status()
        journal = get_genesis_mt5_journal_recent(limit=1)

        self.assertTrue(health["ok"])
        self.assertTrue(status["ok"])
        self.assertEqual(status["status"], "mt5_status_ready")
        self.assertTrue(journal["ok"])
        self.assertEqual(journal["status"], "mt5_journal_ready")
        self.assertFalse(health["mt5_enabled"])
        self.assertTrue(health["demo_only"])
        self.assertFalse(health["live_trading_enabled"])
        self.assertFalse(health["order_execution_enabled"])
        self.assertTrue(health["kill_switch"])
        self.assertFalse(health["broker_touched"])
        self.assertFalse(health["order_executed"])
        self.assertEqual(health["order_policy"], "journal_only_no_broker")
        self.assertIn("MT5_KILL_SWITCH", config["config"])

    def test_paper_exploration_env_values_are_read_by_single_config_loader(self) -> None:
        truthy_values = ("true", "True", "TRUE", "1", "yes", "YES", "on", "ON")
        for value in truthy_values:
            with self.subTest(value=value):
                with patch.dict(
                    "os.environ",
                    {
                        "MT5_ENABLED": "true",
                        "MT5_DEMO_ONLY": "true",
                        "MT5_LIVE_TRADING_ENABLED": "false",
                        "MT5_ORDER_EXECUTION_ENABLED": "false",
                        "MT5_KILL_SWITCH": "false",
                        "MT5_PAPER_EXPLORATION_ENABLED": value,
                    },
                    clear=False,
                ):
                    config = MT5BridgeConfig.from_env()
                    payload = mt5_config()

                    self.assertTrue(config.paper_exploration_enabled)
                    self.assertTrue(is_paper_exploration_enabled())
                    self.assertTrue(payload["MT5_PAPER_EXPLORATION_ENABLED"])
                    self.assertTrue(payload["paper_exploration_enabled"])
                    self.assertFalse(payload["broker_touched"])
                    self.assertFalse(payload["order_executed"])

    def test_symbol_mapper_handles_crypto_and_blocks_unallowed_symbols(self) -> None:
        mapper = MT5SymbolMapper(allowed_symbols=["BTCUSD", "XAUUSD"])

        btc = mapper.map_symbol("BTC-USD")
        gold = mapper.map_symbol("IAU")
        unknown = mapper.map_symbol("XYZ")

        self.assertTrue(btc["ok"])
        self.assertEqual(btc["mt5_symbol"], "BTCUSD")
        self.assertTrue(gold["ok"])
        self.assertEqual(gold["mt5_symbol"], "XAUUSD")
        self.assertFalse(unknown["ok"])
        self.assertEqual(unknown["reason"], "symbol_not_mapped")

    def test_symbol_mapper_accepts_btc_when_broker_allowed_symbol_is_btc(self) -> None:
        mapper = MT5SymbolMapper(allowed_symbols=["BTC"])

        btc = mapper.map_symbol("BTC")

        self.assertTrue(btc["ok"])
        self.assertEqual(btc["raw_symbol"], "BTC")
        self.assertEqual(btc["genesis_symbol"], "BTC-USD")
        self.assertEqual(btc["mt5_symbol"], "BTC")
        self.assertEqual(btc["reason"], "ok")
        self.assertIn("MT5 BTC parece ETF/proxy", btc["instrument_warning"])

    def test_symbol_mapper_env_map_can_map_btc_aliases(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "MT5_ALLOWED_SYMBOLS": "BTC,BTCUSD",
                "MT5_SYMBOL_MAP_JSON": '{"BTC":"BTC","BTC-USD":"BTC","BTCUSD":"BTCUSD","BTCUSDT":"BTCUSD"}',
            },
        ):
            mapper = MT5SymbolMapper()

        self.assertEqual(mapper.map_symbol("BTC")["mt5_symbol"], "BTC")
        self.assertEqual(mapper.map_symbol("BTC-USD")["mt5_symbol"], "BTC")
        self.assertEqual(mapper.map_symbol("BTCUSD")["mt5_symbol"], "BTCUSD")
        self.assertEqual(mapper.map_symbol("BTCUSDT")["mt5_symbol"], "BTCUSD")

    def test_instrument_resolver_separates_btcusd_spot_from_btc_proxy(self) -> None:
        spot = mt5_instrument(
            symbol="BTCUSD",
            payload={
                "symbol": "BTCUSD",
                "symbol_description": "Bitcoin vs. USD",
                "symbol_path": "Crypto\\Bitcoin",
                "currency_base": "BTC",
                "currency_profit": "USD",
            },
        )
        proxy = mt5_instrument(
            symbol="BTC",
            payload={
                "symbol": "BTC",
                "symbol_description": "Grayscale Bitcoin Mini Trust ETF",
                "symbol_path": "ETFs\\Crypto",
            },
        )

        self.assertEqual(spot["normalized_symbol"], "BTCUSD")
        self.assertEqual(spot["instrument_type"], "crypto_spot")
        self.assertTrue(spot["is_spot_crypto"])
        self.assertEqual(spot["underlying"], "BTC")
        self.assertEqual(proxy["normalized_symbol"], "BTC_PROXY")
        self.assertEqual(proxy["instrument_type"], "crypto_etf_proxy")
        self.assertFalse(proxy["is_spot_crypto"])
        self.assertIn("MT5 BTC parece ETF/proxy", proxy["warning"])
        self.assertFalse(spot["broker_touched"])
        self.assertFalse(proxy["order_executed"])

    def test_risk_guard_blocks_without_stop_live_disabled_and_kill_switch(self) -> None:
        mapper = MT5SymbolMapper(allowed_symbols=["BTCUSD"])
        config = MT5BridgeConfig(enabled=True, demo_only=True, live_trading_enabled=False, order_execution_enabled=True, kill_switch=False)
        guard = MT5RiskGuard(config=config, symbol_mapper=mapper)
        intent = MT5OrderIntent(symbol="BTCUSD", action="BUY", entry=100.0, take_profit=110.0, risk_pct=0.25, confidence="high")

        result = guard.evaluate_order(intent, account_state={"is_demo": True})

        self.assertTrue(result["blocked"])
        self.assertIn("stop_loss_required", result["reasons"])
        self.assertIn("live_trading_disabled", result["reasons"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

        kill_guard = MT5RiskGuard(
            config=MT5BridgeConfig(enabled=True, demo_only=True, live_trading_enabled=True, order_execution_enabled=True, kill_switch=True),
            symbol_mapper=mapper,
        )
        kill_result = kill_guard.evaluate_order(
            MT5OrderIntent(symbol="BTCUSD", action="BUY", entry=100.0, stop_loss=95.0, take_profit=110.0, risk_pct=0.25, confidence="high"),
            account_state={"is_demo": True},
        )
        self.assertIn("kill_switch_active", kill_result["reasons"])

    def test_mt5_decision_saves_memory_and_never_executes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            decision = mt5_decision("BTCUSD", memory=store)

            self.assertTrue(decision["ok"])
            self.assertEqual(decision["symbol"], "BTCUSD")
            self.assertIn(decision["decision"], {"WAIT", "NO_TRADE", "HEDGE", "REDUCE", "BUY", "SELL"})
            self.assertFalse(decision["order_executed"])
            self.assertFalse(decision["broker_touched"])
            self.assertEqual(decision["order_policy"], "journal_only_no_broker")
            self.assertTrue(store.get_mt5_events("mt5_decisions", "BTCUSD", limit=5))

    def test_account_sync_signal_order_request_and_result_are_journal_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            account = mt5_account_sync(
                {"account_id": "123", "server": "DemoServer", "balance": 10000, "is_demo": True, "password": "supersecret"},
                memory=store,
            )
            signal = mt5_signal({"symbol": "BTCUSD", "decision": "WAIT", "password": "supersecret"}, memory=store)
            order = mt5_order_request({"symbol": "BTCUSD", "action": "BUY", "entry": 100, "risk_pct": 0.25, "confidence": "high"}, memory=store)

            self.assertTrue(account["ok"])
            self.assertNotIn("supersecret", str(account))
            self.assertTrue(signal["ok"])
            self.assertFalse(signal["order_executed"])
            self.assertEqual(order["status"], "blocked")
            self.assertFalse(order["order_executed"])
            self.assertFalse(order["broker_touched"])
            self.assertIn("stop_loss_required", order["risk_guard"]["reasons"])
            self.assertTrue(store.get_mt5_events("mt5_order_requests", "BTCUSD", limit=5))
            self.assertTrue(store.get_mt5_events("mt5_risk_blocks", "BTCUSD", limit=5))

    def test_mt5_signal_accepts_ea_payload_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            direct = mt5_signal({"symbol": "BTC", "event_type": "mt5_signal", "message": "direct"}, memory=store)
            nested = mt5_signal({"source": "mt5_bridge", "payload": {"symbol": "BTC", "price": 34.95, "is_demo": True}}, memory=store)
            event_nested = mt5_signal({"event": {"payload": {"Symbol": "BTC", "timeframe": "H1"}}}, memory=store)

            self.assertTrue(direct["ok"])
            self.assertEqual(direct["status"], "mt5_signal_recorded")
            self.assertEqual(direct["symbol"], "BTC")
            self.assertTrue(nested["ok"])
            self.assertEqual(nested["symbol"], "BTC")
            self.assertTrue(event_nested["ok"])
            self.assertEqual(event_nested["symbol"], "BTC")
            self.assertFalse(nested["broker_touched"])
            self.assertFalse(nested["order_executed"])

    def test_mt5_signal_accepts_full_ea_signal_payload_without_400_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            signal = mt5_signal(_ea_signal_payload(), memory=store)
            recent = mt5_journal_recent(memory=store, symbol="BTCUSD", limit=5)

            self.assertTrue(signal["ok"])
            self.assertEqual(signal["status"], "mt5_signal_recorded")
            self.assertEqual(signal["symbol"], "BTCUSD")
            self.assertTrue(any(item["event_type"] == "mt5_signal" for item in recent["items"]))
            self.assertFalse(signal["broker_touched"])
            self.assertFalse(signal["order_executed"])

    def test_btc_allowed_symbol_does_not_trigger_symbol_not_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", {"MT5_ALLOWED_SYMBOLS": "BTC"}):
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            decision = mt5_decision("BTC", memory=store)
            order = mt5_order_request({"symbol": "BTC", "action": "BUY", "entry": 100, "risk_pct": 0.25, "confidence": "high"}, memory=store)

            self.assertTrue(decision["ok"])
            self.assertEqual(decision["symbol"], "BTC")
            self.assertEqual(decision["genesis_symbol"], "BTC-USD")
            self.assertNotEqual(decision["reason"], "symbol_not_mapped_or_not_allowed")
            self.assertNotIn("symbol_not_allowed", decision["risk_flags"])
            self.assertIn("MT5 BTC parece ETF/proxy", decision["instrument_warning"])
            self.assertNotIn("symbol_not_allowed", order["risk_guard"]["reasons"])
            self.assertFalse(order["order_executed"])
            self.assertFalse(order["broker_touched"])
            self.assertEqual(order["order_policy"], "journal_only_no_broker")

    def test_mt5_journal_recent_empty_and_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            empty = mt5_journal_recent(memory=store, limit=25)

            self.assertTrue(empty["ok"])
            self.assertEqual(empty["items"], [])
            self.assertEqual(empty["count"], 0)
            self.assertFalse(empty["broker_touched"])
            self.assertFalse(empty["order_executed"])

            mt5_signal({"symbol": "BTC", "decision": "WAIT", "reason": "test", "password": "dont-save"}, memory=store)
            mt5_signal({"symbol": "NVDA", "decision": "WAIT", "reason": "other"}, memory=store)
            recent = mt5_journal_recent(memory=store, limit=1)
            btc_only = mt5_journal_recent(memory=store, limit=25, symbol="BTC")

            self.assertEqual(recent["count"], 1)
            self.assertEqual(btc_only["count"], 1)
            self.assertEqual(btc_only["items"][0]["event_type"], "mt5_signal")
            self.assertEqual(btc_only["items"][0]["symbol"], "BTC")
            self.assertFalse(btc_only["items"][0]["broker_touched"])
            self.assertFalse(btc_only["items"][0]["order_executed"])
            self.assertNotIn("dont-save", str(btc_only))

    def test_mt5_status_uses_latest_journal_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            mt5_account_sync({"account_id": "demo-1", "server": "DemoServer", "is_demo": True, "balance": 10000, "password": "dont-save"}, memory=store)
            mt5_decision("BTC", memory=store)
            mt5_signal({"symbol": "BTC", "decision": "WAIT", "reason": "journal"}, memory=store)
            mt5_order_request({"symbol": "BTC", "action": "BUY", "entry": 100, "risk_pct": 0.25, "confidence": "high"}, memory=store)
            status = mt5_status(memory=store)

            self.assertTrue(status["ok"])
            self.assertEqual(status["status"], "mt5_status_ready")
            self.assertIsNotNone(status["last_account_sync"])
            self.assertIsNotNone(status["last_decision"])
            self.assertIsNotNone(status["last_signal"])
            self.assertIsNotNone(status["last_order_request"])
            self.assertTrue(status["risk_blocks"])
            self.assertIn("BTC", status["symbols"])
            self.assertFalse(status["bridge"]["broker_touched"])
            self.assertFalse(status["bridge"]["order_executed"])
            self.assertNotIn("dont-save", str(status))

    def test_mt5_tick_creates_and_updates_shadow_trade_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            buy = mt5_signal(
                {
                    "symbol": "BTC",
                    "action": "BUY",
                    "entry": 100,
                    "stop_loss": 95,
                    "take_profit": 110,
                    "risk_pct": 0.25,
                    "timeframe": "H1",
                    "strategy_profile": "forward-smoke",
                    "confidence": "high",
                    "password": "dont-save",
                },
                memory=store,
            )
            first_tick = mt5_tick({"symbol": "BTC", "bid": 109.9, "ask": 110.1, "last": 110.2, "timeframe": "H1", "spread": 0.2}, memory=store)
            sell = mt5_signal(
                {
                    "symbol": "BTC",
                    "action": "SELL",
                    "entry": 100,
                    "stop_loss": 105,
                    "take_profit": 90,
                    "risk_pct": 0.25,
                    "timeframe": "H1",
                    "strategy_profile": "forward-smoke",
                    "confidence": "high",
                },
                memory=store,
            )
            second_tick = mt5_tick({"symbol": "BTC", "bid": 105.9, "ask": 106.1, "last": 106, "timeframe": "H1", "spread": 0.2}, memory=store)
            performance = mt5_performance(memory=store, symbol="BTC", timeframe="H1")
            shadow = mt5_shadow_trades(memory=store, symbol="BTC", limit=10)
            forward = mt5_forward_test(memory=store, symbol="BTC", timeframe="H1")
            outcomes = mt5_outcomes_recent(memory=store, symbol="BTC", limit=10)

            self.assertTrue(buy["shadow"]["created"])
            self.assertTrue(sell["shadow"]["created"])
            self.assertEqual(first_tick["status"], "mt5_tick_recorded")
            self.assertEqual(second_tick["status"], "mt5_tick_recorded")
            self.assertTrue(store.get_mt5_events("mt5_ticks", "BTC", limit=5))
            self.assertEqual(performance["summary_manual"]["shadow_trades"], 0)
            self.assertEqual(performance["summary_manual"]["wins"], 0)
            self.assertEqual(performance["summary_manual"]["losses"], 0)
            self.assertEqual(performance["summary_manual"]["win_rate"], 0.0)
            self.assertEqual(performance["summary_manual"]["profit_factor"], 0.0)
            self.assertEqual(performance["summary_manual"]["excluded_from_main_metrics"], 2)
            self.assertEqual(shadow["count"], performance["summary"]["total_shadow_trades"])
            self.assertEqual(shadow["closed"], performance["summary_manual"]["closed"])
            self.assertEqual(shadow["count"], 0)
            self.assertEqual(shadow["excluded_count"], 2)
            self.assertEqual(shadow["excluded_trades"][0]["normalized_symbol"], "BTC_PROXY")
            self.assertIn(shadow["excluded_trades"][0]["instrument_type"], {"crypto_etf_proxy", "legacy_proxy"})
            self.assertEqual(forward["status"], "mt5_forward_test_ready")
            self.assertGreaterEqual(outcomes["count"], 2)
            self.assertFalse(performance["broker_touched"])
            self.assertFalse(performance["order_executed"])
            self.assertNotIn("dont-save", str(performance))

    def test_shadow_trades_and_performance_keep_btcusd_separate_from_proxy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            mt5_signal(
                {
                    "symbol": "BTCUSD",
                    "action": "BUY",
                    "entry": 100,
                    "stop_loss": 95,
                    "take_profit": 110,
                    "risk_pct": 0.25,
                    "timeframe": "H1",
                    "confidence": "high",
                },
                memory=store,
            )
            mt5_tick({"symbol": "BTCUSD", "last": 110.2, "timeframe": "H1", "spread": 0.2}, memory=store)

            perf_btc = mt5_performance(memory=store, symbol="BTC", timeframe="H1")
            perf_btcusd = mt5_performance(memory=store, symbol="BTCUSD", timeframe="H1")
            shadow_btc = mt5_shadow_trades(memory=store, symbol="BTC")
            shadow_btcusd = mt5_shadow_trades(memory=store, symbol="BTCUSD")
            debug = mt5_debug_storage(memory=store, symbol="BTCUSD")

            self.assertEqual(perf_btc["summary"]["total_shadow_trades"], 0)
            self.assertEqual(perf_btcusd["summary"]["total_shadow_trades"], 0)
            self.assertEqual(perf_btcusd["summary_btcusd_auto"]["symbol"], "BTCUSD")
            self.assertEqual(shadow_btc["count"], 0)
            self.assertEqual(shadow_btcusd["count"], 0)
            self.assertEqual(shadow_btcusd["excluded_count"], 1)
            self.assertEqual(shadow_btcusd["excluded_trades"][0]["symbol"], "BTCUSD")
            self.assertEqual(shadow_btcusd["excluded_trades"][0]["normalized_symbol"], "BTCUSD")
            self.assertEqual(shadow_btcusd["excluded_trades"][0]["instrument_type"], "crypto_spot")
            self.assertGreaterEqual(debug["counts"]["mt5_shadow_trades"], 1)
            self.assertEqual(debug["shadow_snapshot_count"], 0)
            self.assertEqual(debug["excluded_count"], 1)
            self.assertEqual(debug["performance_total_shadow_trades"], 0)
            self.assertIn("BTCUSD", debug["symbol_filters_applied"])
            self.assertIn("active_backend", debug)
            self.assertIn("persistence_backend", debug)
            self.assertFalse(shadow_btc["broker_touched"])
            self.assertFalse(debug["order_executed"])

    def test_shadow_trades_survive_store_reload_and_btc_proxy_stays_separate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "memory.sqlite3"
            first_store = MemoryStore(database_url="", sqlite_path=db_path)
            mt5_signal(
                {
                    "symbol": "BTC",
                    "action": "SELL",
                    "entry": 100,
                    "stop_loss": 105,
                    "take_profit": 90,
                    "risk_pct": 0.25,
                    "timeframe": "H1",
                    "confidence": "high",
                },
                memory=first_store,
            )

            reloaded = MemoryStore(database_url="", sqlite_path=db_path)
            perf_btcusd = mt5_performance(memory=reloaded, symbol="BTCUSD", timeframe="H1")
            perf_btc = mt5_performance(memory=reloaded, symbol="BTC", timeframe="H1")
            shadow_btcusd = mt5_shadow_trades(memory=reloaded, symbol="BTCUSD")
            shadow_btc = mt5_shadow_trades(memory=reloaded, symbol="BTC")

            self.assertEqual(perf_btcusd["summary"]["total_shadow_trades"], 0)
            self.assertEqual(shadow_btcusd["count"], 0)
            self.assertEqual(perf_btc["summary"]["total_shadow_trades"], 0)
            self.assertEqual(shadow_btc["count"], 0)
            self.assertEqual(shadow_btc["excluded_count"], 1)
            self.assertEqual(shadow_btc["excluded_trades"][0]["symbol"], "BTC")
            self.assertEqual(shadow_btc["excluded_trades"][0]["normalized_symbol"], "BTC_PROXY")
            self.assertFalse(shadow_btcusd["broker_touched"])
            self.assertFalse(shadow_btcusd["order_executed"])

    def test_auto_forward_tick_creates_decision_and_shadow_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_buy_context()):
                tick = router.tick({"symbol": "BTCUSD", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            trades = router.shadow_trades(symbol="BTCUSD")
            status = router.auto_forward_status(symbol="BTCUSD")

            self.assertTrue(tick["ok"])
            self.assertTrue(tick["auto_forward"]["ok"])
            self.assertEqual(tick["auto_forward"]["decision"]["decision"], "BUY")
            self.assertTrue(tick["auto_shadow_trade_created"])
            self.assertEqual(trades["open"], 1)
            self.assertEqual(status["last_decision"]["decision"], "BUY")
            self.assertEqual(status["open_trades"][0]["source"], "mt5_auto_forward")
            self.assertFalse(tick["broker_touched"])
            self.assertFalse(tick["order_executed"])

    def test_ea_tick_payload_updates_status_and_auto_forward(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)
            signal = router.signal(_ea_signal_payload())

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_generated_buy_context()):
                tick = router.tick(_ea_tick_payload())

            status = router.auto_forward_status(symbol="BTCUSD")

            self.assertTrue(signal["ok"])
            self.assertEqual(tick["status"], "mt5_tick_recorded")
            self.assertEqual(tick["symbol"], "BTCUSD")
            self.assertEqual(tick["tick"]["normalized_symbol"], "BTCUSD")
            self.assertEqual(tick["tick"]["instrument_type"], "crypto_spot")
            self.assertTrue(tick["tick_saved"])
            self.assertTrue(tick["auto_forward_checked"])
            self.assertTrue(tick["auto_shadow_trade_created"])
            self.assertEqual(status["normalized_symbol"], "BTCUSD")
            self.assertEqual(status["instrument_type"], "crypto_spot")
            self.assertEqual(status["last_signal_status"], "mt5_signal_recorded")
            self.assertEqual(status["last_signal_error"], "")
            self.assertEqual(status["last_tick_status"], "mt5_tick_recorded")
            self.assertEqual(status["last_tick_ea_version"], "GenesisBridgeEA_v11_9_FORCE_TICK")
            self.assertIsNotNone(status["last_tick"])
            self.assertIsNotNone(status["last_valid_decision"])
            self.assertFalse(status["broker_touched"])
            self.assertFalse(status["order_executed"])

    def test_actionable_builder_generates_btc_entry_stop_and_take_profit(self) -> None:
        built = build_actionable_mt5_decision(
            "BTC",
            {"symbol": "BTC", "last": 100, "timeframe": "H1"},
            {
                "ok": True,
                "decision": "BUY",
                "confidence": "high",
                "no_trade_score": 0,
                "hedge_score": 0,
                "technical_context": {"atr": 2},
                "recommended_strategy_profile": "BTC Auto",
                "reason": "builder smoke",
            },
            min_rr=1.2,
            risk_pct=0.5,
        )

        self.assertEqual(built["decision"], "BUY")
        self.assertTrue(built["actionable"])
        self.assertEqual(built["entry"], 100)
        self.assertEqual(built["stop_loss"], 98)
        self.assertEqual(built["take_profit"], 102.4)
        self.assertGreaterEqual(built["risk_reward"], 1.2)

    def test_actionable_builder_uses_btc_percent_fallback_without_atr(self) -> None:
        built = build_actionable_mt5_decision(
            "BTC",
            {"symbol": "BTC", "last": 100, "timeframe": "H1"},
            {
                "ok": True,
                "decision": "SELL",
                "confidence": "high",
                "no_trade_score": 0,
                "hedge_score": 0,
                "technical_context": {},
                "recommended_strategy_profile": "BTC Auto",
            },
            min_rr=1.2,
            risk_pct=0.5,
        )

        self.assertEqual(built["decision"], "SELL")
        self.assertTrue(built["actionable"])
        self.assertEqual(built["entry"], 100)
        self.assertEqual(built["stop_loss"], 102)
        self.assertEqual(built["take_profit"], 97.6)
        self.assertGreaterEqual(built["risk_reward"], 1.2)

    def test_actionable_builder_downgrades_to_no_trade_without_entry(self) -> None:
        built = build_actionable_mt5_decision(
            "BTC",
            {"symbol": "BTC"},
            {"ok": True, "decision": "BUY", "confidence": "high", "no_trade_score": 0, "hedge_score": 0},
            min_rr=1.2,
        )

        self.assertEqual(built["decision"], "NO_TRADE")
        self.assertFalse(built["actionable"])
        self.assertIsNone(built["entry"])
        self.assertIsNone(built["stop_loss"])
        self.assertIsNone(built["take_profit"])
        self.assertEqual(built["reason"], "missing_risk_parameters")
        self.assertNotEqual(built["reason"], "missing_entry")

    def test_actionable_builder_does_not_return_partial_risk_on_invalid_signal(self) -> None:
        built = build_actionable_mt5_decision(
            "BTC",
            {"symbol": "BTC", "last": 100, "timeframe": "H1"},
            {
                "ok": True,
                "decision": "BUY",
                "confidence": "high",
                "no_trade_score": 0,
                "hedge_score": 0,
                "stop_loss": 100,
                "take_profit": 101,
                "recommended_strategy_profile": "Invalid Risk Test",
            },
            min_rr=1.2,
        )

        self.assertEqual(built["decision"], "NO_TRADE")
        self.assertFalse(built["actionable"])
        self.assertIsNone(built["entry"])
        self.assertIsNone(built["stop_loss"])
        self.assertIsNone(built["take_profit"])
        self.assertEqual(built["reason"], "invalid_risk_parameters")

    def test_auto_forward_generates_buy_shadow_trade_without_context_stop_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_generated_buy_context()):
                tick = router.tick({"symbol": "BTCUSD", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            trade = router.shadow_trades(symbol="BTCUSD")["open_trades"][0]

            self.assertTrue(tick["auto_shadow_trade_created"])
            self.assertEqual(tick["auto_forward"]["decision"]["decision"], "BUY")
            self.assertTrue(tick["auto_forward"]["decision"]["actionable"])
            self.assertEqual(tick["auto_forward"]["decision"]["stop_loss"], 98)
            self.assertEqual(tick["auto_forward"]["decision"]["take_profit"], 102.4)
            self.assertEqual(trade["source"], "mt5_auto_forward")
            self.assertFalse(tick["order_executed"])
            self.assertFalse(tick["broker_touched"])

    def test_auto_forward_generates_sell_shadow_trade_without_context_stop_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_generated_sell_context()):
                tick = router.tick({"symbol": "BTCUSD", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            trade = router.shadow_trades(symbol="BTCUSD")["open_trades"][0]

            self.assertTrue(tick["auto_shadow_trade_created"])
            self.assertEqual(tick["auto_forward"]["decision"]["decision"], "SELL")
            self.assertTrue(tick["auto_forward"]["decision"]["actionable"])
            self.assertEqual(tick["auto_forward"]["decision"]["stop_loss"], 102)
            self.assertEqual(tick["auto_forward"]["decision"]["take_profit"], 97.6)
            self.assertEqual(trade["action"], "SELL")
            self.assertFalse(tick["order_executed"])
            self.assertFalse(tick["broker_touched"])

    def test_auto_forward_no_trade_does_not_create_shadow_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_no_trade_context()):
                tick = router.tick({"symbol": "BTC", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            performance = router.performance(symbol="BTC")
            status = router.auto_forward_status(symbol="BTC")

            self.assertEqual(tick["auto_forward"]["decision"]["decision"], "NO_TRADE")
            self.assertFalse(tick["auto_shadow_trade_created"])
            self.assertEqual(performance["summary"]["auto_shadow_trades"], 0)
            self.assertFalse(status["last_actionable"])
            self.assertIsNone(status["entry"])
            self.assertIsNone(status["stop_loss"])
            self.assertIsNone(status["take_profit"])
            self.assertTrue(store.get_mt5_events("mt5_no_trade_outcomes", "BTC", limit=5))
            self.assertTrue(store.get_mt5_events("mt5_no_trade_evaluations", "BTC", limit=5))
            self.assertFalse(tick["broker_touched"])
            self.assertFalse(tick["order_executed"])

    def test_no_trade_report_counts_block_reasons(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_no_trade_context()):
                router.tick({"symbol": "BTCUSD", "last": 77000, "timeframe": "H1", "spread": 12, "is_demo": True, "symbol_description": "Bitcoin vs. USD", "currency_base": "BTC", "currency_profit": "USD"})

            report = mt5_no_trade_report(memory=store, symbol="BTCUSD")

            self.assertTrue(report["ok"])
            self.assertEqual(report["total_evaluations"], 1)
            self.assertEqual(report["no_trade_count"], 1)
            self.assertTrue(report["top_block_reasons"])
            self.assertIn(report["top_block_reasons"][0]["reason"], str(report))
            self.assertFalse(report["broker_touched"])
            self.assertFalse(report["order_executed"])

    def test_tick_uses_env_paper_exploration_config_and_records_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            with patch.dict(
                "os.environ",
                {
                    "MT5_ENABLED": "true",
                    "MT5_DEMO_ONLY": "true",
                    "MT5_LIVE_TRADING_ENABLED": "false",
                    "MT5_ORDER_EXECUTION_ENABLED": "false",
                    "MT5_KILL_SWITCH": "false",
                    "MT5_MAX_SPREAD_POINTS": "50",
                    "MT5_PAPER_EXPLORATION_ENABLED": "true",
                },
                clear=False,
            ):
                router = MT5SignalRouter(memory=store, symbol_mapper=MT5SymbolMapper(allowed_symbols=["BTCUSD"]))
                with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_no_trade_context()):
                    warmup = router.tick({"symbol": "BTCUSD", "last": 77000, "timeframe": "H1", "spread": 12, "is_demo": True, "symbol_description": "Bitcoin vs. USD", "currency_base": "BTC", "currency_profit": "USD"})
                    tick = router.tick({"symbol": "BTCUSD", "last": 77025, "timeframe": "H1", "spread": 12, "is_demo": True, "symbol_description": "Bitcoin vs. USD", "currency_base": "BTC", "currency_profit": "USD"})
                report = router.no_trade_report(symbol="BTCUSD")
                performance = router.performance(symbol="BTCUSD")

            self.assertTrue(report["paper_exploration_enabled"])
            self.assertGreaterEqual(report["exploration_attempts"], 1)
            self.assertEqual(report["exploration_created"], 1)
            self.assertTrue(warmup["auto_forward"]["exploration"]["enabled"])
            self.assertNotEqual(warmup["auto_forward"]["exploration"]["reason"], "paper_exploration_disabled")
            self.assertTrue(tick["auto_forward"]["exploration_shadow_trade_created"])
            self.assertIn("summary_exploration", performance)
            self.assertEqual(performance["summary_exploration"]["exploration_trades"], 1)
            self.assertFalse(tick["broker_touched"])
            self.assertFalse(tick["order_executed"])

    def test_paper_exploration_creates_separate_shadow_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _exploration_router(store)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_no_trade_context()):
                warmup = router.tick({"symbol": "BTCUSD", "last": 77000, "timeframe": "H1", "spread": 12, "is_demo": True, "symbol_description": "Bitcoin vs. USD", "currency_base": "BTC", "currency_profit": "USD"})
                tick = router.tick({"symbol": "BTCUSD", "last": 77020, "timeframe": "H1", "spread": 12, "is_demo": True, "symbol_description": "Bitcoin vs. USD", "currency_base": "BTC", "currency_profit": "USD"})

            performance = mt5_performance(memory=store, symbol="BTCUSD", timeframe="H1")
            status = mt5_auto_forward_status(memory=store, symbol="BTCUSD")
            trades = mt5_shadow_trades(memory=store, symbol="BTCUSD")
            no_trade_report = router.no_trade_report(symbol="BTCUSD")

            self.assertFalse(warmup["auto_forward"]["exploration_shadow_trade_created"])
            self.assertNotEqual(warmup["auto_forward"]["exploration"]["reason"], "paper_exploration_disabled")
            self.assertFalse(tick["auto_shadow_trade_created"])
            self.assertTrue(tick["auto_forward"]["exploration_shadow_trade_created"])
            self.assertEqual(performance["summary_strict_auto"]["shadow_trades"], 0)
            self.assertEqual(performance["summary_exploration"]["shadow_trades"], 1)
            self.assertEqual(performance["summary_exploration"]["exploration_trades"], 1)
            self.assertEqual(performance["summary_forward_auto"]["shadow_trades"], 1)
            self.assertEqual(status["exploration_shadow_trades"], 1)
            self.assertEqual(status["strict_shadow_trades"], 0)
            self.assertTrue(no_trade_report["paper_exploration_enabled"])
            self.assertGreaterEqual(no_trade_report["exploration_attempts"], 1)
            self.assertEqual(no_trade_report["exploration_created"], 1)
            self.assertTrue(trades["items"][0]["paper_exploration"])
            self.assertTrue(trades["items"][0]["excluded_from_live_grade"])
            self.assertGreaterEqual(trades["items"][0]["risk_reward"], 1.2)
            self.assertFalse(tick["broker_touched"])
            self.assertFalse(tick["order_executed"])

    def test_paper_exploration_does_not_duplicate_open_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _exploration_router(store)
            wait_context = {**_no_trade_context(), "no_trade_score": 0, "confidence": "medium", "reason": "waiting_confirmation"}

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=wait_context):
                router.tick({"symbol": "BTCUSD", "last": 77000, "timeframe": "H1", "spread": 12, "is_demo": True, "symbol_description": "Bitcoin vs. USD", "currency_base": "BTC", "currency_profit": "USD"})
                first = router.tick({"symbol": "BTCUSD", "last": 77010, "timeframe": "H1", "spread": 12, "is_demo": True, "symbol_description": "Bitcoin vs. USD", "currency_base": "BTC", "currency_profit": "USD"})
                second = router.tick({"symbol": "BTCUSD", "last": 77020, "timeframe": "H1", "spread": 12, "is_demo": True, "symbol_description": "Bitcoin vs. USD", "currency_base": "BTC", "currency_profit": "USD"})

            performance = mt5_performance(memory=store, symbol="BTCUSD", timeframe="H1")

            self.assertTrue(first["auto_forward"]["exploration_shadow_trade_created"])
            self.assertFalse(second["auto_forward"]["exploration_shadow_trade_created"])
            self.assertEqual(second["auto_forward"]["exploration"]["reason"], "active_open_trade")
            self.assertEqual(performance["summary_exploration"]["open"], 1)
            self.assertFalse(second["broker_touched"])
            self.assertFalse(second["order_executed"])

    def test_auto_forward_does_not_create_duplicate_open_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", {"MT5_FAST_PATH_ONLY": "false"}):
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_buy_context()):
                first = router.tick({"symbol": "BTCUSD", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})
                second = router.tick({"symbol": "BTCUSD", "last": 101, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            trades = router.shadow_trades(symbol="BTCUSD")
            status = router.auto_forward_status(symbol="BTCUSD")
            auto_performance = router.performance_auto(symbol="BTCUSD", timeframe="H1")

            self.assertTrue(first["auto_shadow_trade_created"])
            self.assertFalse(second["auto_shadow_trade_created"])
            self.assertEqual(second["auto_forward"]["reason"], "duplicate_open_trade")
            self.assertEqual(trades["open"], 1)
            self.assertEqual(status["last_reason"], "active_open_trade")
            self.assertEqual(status["current_state_reason"], "active_open_trade")
            self.assertEqual(status["last_block_reason"], "duplicate_open_trade")
            self.assertIsNotNone(status["last_valid_decision"])
            self.assertIsNotNone(status["last_invalid_decision"])
            self.assertNotEqual(status["last_reason"], "missing_risk_parameters")
            self.assertEqual(auto_performance["last_reason"], "active_open_trade")
            self.assertEqual(auto_performance["current_state_reason"], "active_open_trade")
            self.assertEqual(auto_performance["last_block_reason"], "duplicate_open_trade")
            self.assertIn("hay una operacion sombra abierta", auto_performance["genesis_reading"])
            self.assertNotEqual(auto_performance["last_reason"], "missing_risk_parameters")
            self.assertFalse(second["order_executed"])
            self.assertFalse(second["broker_touched"])

    def test_auto_forward_low_confidence_blocks_with_clear_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_low_confidence_buy_context()):
                tick = router.tick({"symbol": "BTCUSD", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            performance = router.performance(symbol="BTCUSD", timeframe="H1")

            self.assertFalse(tick["auto_shadow_trade_created"])
            self.assertEqual(tick["auto_forward"]["reason"], "confidence_low")
            self.assertNotIn("stop_loss_missing_from_context", str(performance))
            self.assertFalse(tick["order_executed"])
            self.assertFalse(tick["broker_touched"])

    def test_performance_and_status_alias_legacy_stop_loss_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", {"MT5_FAST_PATH_ONLY": "false"}):
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            MT5Journal(memory=store).save(
                "mt5_decisions",
                "BTC",
                {
                    "symbol": "BTC",
                    "decision": "NO_TRADE",
                    "entry": 34.98,
                    "stop_loss": None,
                    "take_profit": None,
                    "reason": "missing_entry",
                    "broker_touched": False,
                    "order_executed": False,
                    "order_policy": "journal_only_no_broker",
                },
            )

            performance = mt5_performance(memory=store, symbol="BTC")
            status = mt5_auto_forward_status(memory=store, symbol="BTC")

            self.assertEqual(performance["last_reason"], "missing_risk_parameters")
            self.assertEqual(status["last_reason"], "missing_risk_parameters")
            self.assertIsNone(performance["last_decision"]["entry"])
            self.assertIsNone(status["entry"])
            self.assertIsNone(status["stop_loss"])
            self.assertIsNone(status["take_profit"])
            self.assertNotIn("missing_entry", str(performance))
            self.assertNotIn("missing_entry", str(status))
            self.assertFalse(performance["broker_touched"])
            self.assertFalse(status["order_executed"])

    def test_auto_forward_kill_switch_blocks_without_real_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = MT5SignalRouter(
                memory=store,
                config=MT5BridgeConfig(
                    enabled=True,
                    demo_only=True,
                    live_trading_enabled=False,
                    order_execution_enabled=False,
                    kill_switch=True,
                    max_spread_points=10.0,
                    min_rr=1.2,
                ),
                symbol_mapper=MT5SymbolMapper(allowed_symbols=["BTC"]),
            )

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_generated_buy_context()):
                tick = router.tick({"symbol": "BTC", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            self.assertFalse(tick["auto_shadow_trade_created"])
            self.assertEqual(tick["auto_forward"]["reason"], "kill_switch_active")
            self.assertFalse(tick["order_executed"])
            self.assertFalse(tick["broker_touched"])

    def test_auto_forward_tick_closes_take_profit_and_stop_loss(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_buy_context()):
                router.tick({"symbol": "BTCUSD", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_no_trade_context()):
                tp_tick = router.tick({"symbol": "BTCUSD", "last": 110.5, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            loss_store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "loss.sqlite3")
            loss_router = _auto_forward_router(loss_store)
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_buy_context()):
                loss_router.tick({"symbol": "BTCUSD", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_no_trade_context()):
                sl_tick = loss_router.tick({"symbol": "BTCUSD", "last": 94.5, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            win_perf = router.performance(symbol="BTCUSD", timeframe="H1")
            loss_perf = loss_router.performance(symbol="BTCUSD", timeframe="H1")

            self.assertEqual(win_perf["summary"]["wins"], 1)
            self.assertEqual(win_perf["summary"]["losses"], 0)
            self.assertEqual(loss_perf["summary"]["wins"], 0)
            self.assertEqual(loss_perf["summary"]["losses"], 1)
            self.assertFalse(tp_tick["broker_touched"])
            self.assertFalse(sl_tick["order_executed"])

    def test_shadow_trade_arms_breakeven_and_updates_unrealized_pnl(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _managed_shadow_router(store)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_buy_context()):
                router.tick({"symbol": "BTCUSD", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})
                router.tick({"symbol": "BTCUSD", "last": 102.1, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            status = router.auto_forward_status(symbol="BTCUSD")
            trade = status["open_trades"][0]

            self.assertGreater(trade["unrealized_pnl"], 0)
            self.assertGreaterEqual(trade["r_multiple"], 0.4)
            self.assertTrue(trade["breakeven_armed"])
            self.assertEqual(trade["virtual_stop_loss"], 100)
            self.assertFalse(status["broker_touched"])
            self.assertFalse(status["order_executed"])

    def test_shadow_trade_trailing_stop_closes_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _managed_shadow_router(store)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_buy_context()):
                router.tick({"symbol": "BTCUSD", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})
                router.tick({"symbol": "BTCUSD", "last": 103.6, "timeframe": "H1", "spread": 0.2, "is_demo": True})
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_no_trade_context()):
                tick = router.tick({"symbol": "BTCUSD", "last": 102.0, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            status = router.auto_forward_status(symbol="BTCUSD")
            performance = router.performance(symbol="BTCUSD", timeframe="H1")
            closed = status["closed_trades"][0]

            self.assertTrue(closed["trailing_stop_active"])
            self.assertEqual(closed["exit_reason"], "trailing_stop")
            self.assertEqual(closed["status"], "win")
            self.assertEqual(status["open_trades"], [])
            self.assertTrue(status["can_open_new_trade"])
            self.assertEqual(performance["summary"]["wins"], 1)
            self.assertFalse(tick["broker_touched"])
            self.assertFalse(tick["order_executed"])

    def test_shadow_trade_time_stop_closes_and_counts_performance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _managed_shadow_router(store, shadow_time_stop_bars=2, shadow_trail_start_r=9.0)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_buy_context()):
                router.tick({"symbol": "BTCUSD", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})
                router.tick({"symbol": "BTCUSD", "last": 101, "timeframe": "H1", "spread": 0.2, "is_demo": True})
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_no_trade_context()):
                tick = router.tick({"symbol": "BTCUSD", "last": 101.5, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            status = router.auto_forward_status(symbol="BTCUSD")
            performance = router.performance(symbol="BTCUSD", timeframe="H1")
            closed = status["closed_trades"][0]

            self.assertEqual(closed["exit_reason"], "time_stop")
            self.assertEqual(closed["status"], "win")
            self.assertEqual(performance["summary"]["closed"], 1)
            self.assertEqual(performance["summary"]["wins"], 1)
            self.assertFalse(tick["broker_touched"])
            self.assertFalse(tick["order_executed"])

    def test_shadow_trade_signal_flip_closes_and_frees_active_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _managed_shadow_router(store, shadow_trail_start_r=9.0)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_buy_context()):
                router.tick({"symbol": "BTCUSD", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_sell_context()):
                flip_tick = router.tick({"symbol": "BTCUSD", "last": 99, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            status_after_flip = router.auto_forward_status(symbol="BTCUSD")
            performance = router.performance(symbol="BTCUSD", timeframe="H1")

            self.assertTrue(flip_tick["auto_forward"]["signal_flip_closed"])
            self.assertEqual(status_after_flip["open_trades"], [])
            self.assertTrue(status_after_flip["can_open_new_trade"])
            self.assertEqual(status_after_flip["closed_trades"][0]["exit_reason"], "signal_flip")
            self.assertEqual(performance["summary"]["losses"], 1)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_sell_context()):
                next_tick = router.tick({"symbol": "BTCUSD", "last": 99, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            reopened = router.auto_forward_status(symbol="BTCUSD")
            self.assertTrue(next_tick["auto_shadow_trade_created"])
            self.assertEqual(len(reopened["open_trades"]), 1)
            self.assertFalse(next_tick["broker_touched"])
            self.assertFalse(next_tick["order_executed"])

    def test_performance_separates_manual_and_auto_shadow_trades(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)
            mt5_order_request(
                {
                    "symbol": "BTCUSD",
                    "action": "BUY",
                    "entry": 100,
                    "stop_loss": 95,
                    "take_profit": 110,
                    "risk_pct": 0.25,
                    "timeframe": "H1",
                    "confidence": "high",
                    "is_demo": True,
                },
                memory=store,
            )
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_sell_context()):
                router.tick({"symbol": "BTCUSD", "last": 130, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            performance = router.performance(symbol="BTCUSD", timeframe="H1")
            facade = mt5_auto_forward_status(memory=store, symbol="BTCUSD")

            self.assertEqual(performance["summary"]["manual_shadow_trades"], 0)
            self.assertEqual(performance["summary"]["auto_shadow_trades"], 1)
            self.assertEqual(performance["summary"]["total_shadow_trades"], 1)
            self.assertEqual(performance["summary"]["shadow_trades"], 1)
            self.assertEqual(performance["summary_auto"]["shadow_trades"], 1)
            self.assertEqual(performance["summary_manual"]["shadow_trades"], 0)
            self.assertEqual(performance["summary_manual"]["excluded_from_main_metrics"], 1)
            self.assertEqual(performance["summary_total"]["shadow_trades"], 1)
            self.assertTrue(facade["ok"])
            self.assertIn("entry", facade)
            self.assertIn("stop_loss", facade)
            self.assertIn("take_profit", facade)
            self.assertIn("risk_reward", facade)
            self.assertEqual(facade["manual_shadow_trades"], 0)
            self.assertEqual(facade["auto_shadow_trades"], 1)
            self.assertFalse(performance["broker_touched"])
            self.assertFalse(performance["order_executed"])

    def test_auto_performance_endpoint_counts_only_auto_trades(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)
            mt5_order_request(
                {
                    "symbol": "BTCUSD",
                    "action": "BUY",
                    "entry": 100,
                    "stop_loss": 95,
                    "take_profit": 110,
                    "risk_pct": 0.25,
                    "timeframe": "H1",
                    "confidence": "high",
                    "is_demo": True,
                },
                memory=store,
            )
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_generated_buy_context()):
                router.tick({"symbol": "BTCUSD", "last": 120, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            auto = mt5_performance_auto(memory=store, symbol="BTCUSD", timeframe="H1")
            total = mt5_performance(memory=store, symbol="BTCUSD", timeframe="H1")

            self.assertTrue(auto["ok"])
            self.assertEqual(auto["summary"]["shadow_trades"], 1)
            self.assertEqual(total["summary_auto"]["shadow_trades"], 1)
            self.assertEqual(total["summary_manual"]["shadow_trades"], 0)
            self.assertEqual(total["summary_manual"]["excluded_from_main_metrics"], 1)
            self.assertIn("Muestra automatica insuficiente", auto["sample_warning"])
            self.assertFalse(auto["broker_touched"])
            self.assertFalse(auto["order_executed"])

    def test_manual_reset_excludes_manual_tests_without_deleting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)
            mt5_order_request(
                {
                    "symbol": "BTCUSD",
                    "action": "BUY",
                    "entry": 100,
                    "stop_loss": 95,
                    "take_profit": 110,
                    "risk_pct": 0.25,
                    "timeframe": "H1",
                    "confidence": "high",
                    "is_demo": True,
                },
                memory=store,
            )
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_generated_sell_context()):
                router.tick({"symbol": "BTCUSD", "last": 130, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            reset = mt5_manual_tests_reset({"symbol": "BTCUSD"}, memory=store)
            report = mt5_performance(memory=store, symbol="BTCUSD", timeframe="H1")
            trades = mt5_shadow_trades(memory=store, symbol="BTCUSD")

            self.assertTrue(reset["ok"])
            self.assertEqual(reset["updated"], 1)
            self.assertEqual(report["summary_total"]["shadow_trades"], 1)
            self.assertEqual(report["summary_auto"]["shadow_trades"], 1)
            self.assertEqual(report["summary_manual"]["shadow_trades"], 0)
            self.assertEqual(report["summary_manual"]["excluded_from_main_metrics"], 1)
            self.assertEqual(trades["count"], 1)
            self.assertEqual(trades["excluded_count"], 1)
            self.assertTrue(any(item.get("excluded_from_main_metrics") for item in trades["excluded_trades"]))
            self.assertFalse(reset["broker_touched"])
            self.assertFalse(reset["order_executed"])

    def test_old_proxy_exclusion_keeps_btcusd_metrics_clean(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)
            mt5_order_request(
                {
                    "symbol": "BTC",
                    "action": "BUY",
                    "entry": 35.0,
                    "stop_loss": 34.0,
                    "take_profit": 37.0,
                    "risk_pct": 0.25,
                    "timeframe": "H1",
                    "confidence": "high",
                    "is_demo": True,
                    "source": "manual_shadow_test",
                    "symbol_description": "Grayscale Bitcoin Mini Trust",
                },
                memory=store,
            )
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_generated_buy_context()):
                router.tick(_ea_tick_payload())

            exclude = mt5_metrics_exclude_old_proxy({"symbol": "BTC"}, memory=store)
            btcusd_perf = mt5_performance(memory=store, symbol="BTCUSD", timeframe="H1")
            proxy_perf = mt5_performance(memory=store, symbol="BTC", timeframe="H1")
            btcusd_trades = mt5_shadow_trades(memory=store, symbol="BTCUSD")
            proxy_trades = mt5_shadow_trades(memory=store, symbol="BTC")
            debug = mt5_debug_storage(memory=store, symbol="BTCUSD")

            self.assertTrue(exclude["ok"])
            self.assertGreaterEqual(exclude["updated"], 1)
            self.assertEqual(btcusd_perf["summary_auto"]["shadow_trades"], 1)
            self.assertEqual(btcusd_perf["summary"]["total_shadow_trades"], 1)
            self.assertEqual(proxy_perf["summary"]["total_shadow_trades"], 0)
            self.assertEqual(btcusd_trades["count"], 1)
            self.assertEqual(proxy_trades["count"], 0)
            self.assertEqual(proxy_trades["excluded_count"], 1)
            self.assertTrue(proxy_trades["excluded_trades"][0]["excluded_from_main_metrics"])
            self.assertEqual(proxy_trades["excluded_trades"][0]["normalized_symbol"], "BTC_PROXY")
            self.assertIsNotNone(debug["last_proxy_trade"])
            self.assertFalse(exclude["broker_touched"])
            self.assertFalse(btcusd_perf["order_executed"])

    def test_btcusd_shadow_status_ignores_excluded_mixed_legacy_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)
            MT5Journal(memory=store).save(
                "mt5_shadow_trades",
                "BTCUSD",
                {
                    "shadow_trade_id": "legacy-mixed",
                    "symbol": "BTCUSD",
                    "original_symbol": "BTC",
                    "normalized_symbol": "BTCUSD",
                    "instrument_type": "crypto_etf_proxy",
                    "is_spot_crypto": False,
                    "action": "BUY",
                    "entry": 35.0,
                    "stop_loss": 34.0,
                    "take_profit": 37.0,
                    "status": "loss",
                    "exit_price": 35.7888,
                    "last_price": 77047.92,
                    "source": "manual_shadow_test",
                    "manual_test": True,
                    "excluded_from_main_metrics": True,
                    "excluded_reason": "old_proxy_or_manual_test",
                    "broker_touched": False,
                    "order_executed": False,
                    "order_policy": "journal_only_no_broker",
                },
            )
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_generated_buy_context()):
                router.tick(_ea_tick_payload())

            shadow = mt5_shadow_trades(memory=store, symbol="BTCUSD")
            status = mt5_auto_forward_status(memory=store, symbol="BTCUSD")
            auto = mt5_performance_auto(memory=store, symbol="BTCUSD", timeframe="H1")

            self.assertEqual(shadow["count"], 1)
            self.assertEqual(shadow["excluded_count"], 1)
            self.assertEqual(shadow["items"][0]["normalized_symbol"], "BTCUSD")
            self.assertEqual(shadow["items"][0]["instrument_type"], "crypto_spot")
            self.assertEqual(status["last_shadow_trade"]["shadow_trade_id"], shadow["items"][0]["shadow_trade_id"])
            self.assertNotEqual(status["last_shadow_trade"]["shadow_trade_id"], "legacy-mixed")
            self.assertEqual(auto["summary"]["auto_shadow_trades"], 1)
            self.assertFalse(status["broker_touched"])
            self.assertFalse(auto["order_executed"])

    def test_excluded_legacy_trade_only_stays_out_of_btcusd_main_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            MT5Journal(memory=store).save(
                "mt5_shadow_trades",
                "BTCUSD",
                {
                    "shadow_trade_id": "legacy-only",
                    "symbol": "BTCUSD",
                    "original_symbol": "BTC",
                    "normalized_symbol": "BTCUSD",
                    "instrument_type": "crypto_etf_proxy",
                    "is_spot_crypto": False,
                    "action": "BUY",
                    "entry": 35.0,
                    "stop_loss": 34.0,
                    "take_profit": 37.0,
                    "status": "open",
                    "exit_price": 35.7888,
                    "last_price": 77047.92,
                    "source": "manual_shadow_test",
                    "manual_test": True,
                    "excluded_from_main_metrics": True,
                    "excluded_reason": "old_proxy_or_manual_test",
                    "broker_touched": False,
                    "order_executed": False,
                    "order_policy": "journal_only_no_broker",
                },
            )

            shadow = mt5_shadow_trades(memory=store, symbol="BTCUSD")
            auto = mt5_performance_auto(memory=store, symbol="BTCUSD")
            performance = mt5_performance(memory=store, symbol="BTCUSD")
            status = mt5_auto_forward_status(memory=store, symbol="BTCUSD")

            self.assertEqual(shadow["count"], 0)
            self.assertEqual(shadow["open"], 0)
            self.assertEqual(shadow["closed"], 0)
            self.assertEqual(shadow["items"], [])
            self.assertEqual(shadow["open_trades"], [])
            self.assertEqual(shadow["closed_trades"], [])
            self.assertEqual(shadow["excluded_count"], 1)
            self.assertEqual(shadow["excluded_trades"][0]["normalized_symbol"], "BTC_PROXY")
            self.assertEqual(shadow["excluded_trades"][0]["instrument_type"], "legacy_proxy")
            self.assertEqual(auto["auto_shadow_trades"], 0)
            self.assertEqual(auto["open"], 0)
            self.assertEqual(auto["closed"], 0)
            self.assertEqual(auto["win_rate"], 0.0)
            self.assertEqual(auto["profit_factor"], 0.0)
            self.assertEqual(performance["summary"]["total_shadow_trades"], 0)
            self.assertEqual(performance["summary"]["manual_shadow_trades"], 0)
            self.assertEqual(performance["excluded_count"], 1)
            self.assertIsNone(status["last_shadow_trade"])
            self.assertEqual(status["open_trades"], [])
            self.assertEqual(status["closed_trades"], [])
            self.assertEqual(status["auto_shadow_trades"], 0)
            self.assertEqual(status["excluded_count"], 1)
            self.assertFalse(shadow["broker_touched"])
            self.assertFalse(auto["order_executed"])

    def test_genesis_chat_uses_auto_summary_and_warns_small_sample(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_generated_buy_context()):
                router.tick({"symbol": "BTCUSD", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            response = route_message("que porcentaje automatico lleva BTCUSD e ignora pruebas manuales", memory=store)

            self.assertTrue(response["ok"])
            self.assertIn("auto win rate", response["answer"])
            self.assertIn("Muestra automatica insuficiente", response["answer"])
            self.assertIn("order_executed=false", response["answer"])

    def test_order_request_creates_shadow_trade_even_when_execution_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", {"MT5_FAST_PATH_ONLY": "false"}):
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            request = mt5_order_request(
                {
                    "symbol": "BTC",
                    "action": "BUY",
                    "entry": 100,
                    "stop_loss": 95,
                    "take_profit": 110,
                    "risk_pct": 0.25,
                    "timeframe": "H1",
                    "confidence": "high",
                    "is_demo": True,
                    "account_type": "demo",
                },
                memory=store,
            )
            trades = mt5_shadow_trades(memory=store, symbol="BTC")
            performance = mt5_performance(memory=store, symbol="BTC", timeframe="H1")

            self.assertEqual(request["status"], "blocked")
            self.assertFalse(request["order_executed"])
            self.assertFalse(request["broker_touched"])
            self.assertTrue(request["shadow_trade_created"])
            self.assertTrue(request["shadow_trade_id"].startswith("shadow-"))
            self.assertEqual(request["order_policy"], "journal_only_no_broker")
            self.assertEqual(trades["open"], 0)
            self.assertEqual(trades["excluded_count"], 1)
            self.assertEqual(trades["closed"], 0)
            self.assertEqual(performance["summary"]["actionable_signals"], 1)
            self.assertEqual(performance["summary_manual"]["shadow_trades"], 0)
            self.assertEqual(performance["summary_manual"]["excluded_from_main_metrics"], 1)

    def test_order_request_shadow_trade_closes_win_and_loss_from_ticks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            mt5_order_request(
                {
                    "symbol": "BTC",
                    "action": "BUY",
                    "entry": 100,
                    "stop_loss": 95,
                    "take_profit": 110,
                    "risk_pct": 0.25,
                    "timeframe": "H1",
                    "confidence": "high",
                    "is_demo": True,
                },
                memory=store,
            )
            mt5_tick({"symbol": "BTC", "last": 110.2, "timeframe": "H1", "spread": 0.2}, memory=store)
            mt5_order_request(
                {
                    "symbol": "BTC",
                    "action": "SELL",
                    "entry": 100,
                    "stop_loss": 105,
                    "take_profit": 90,
                    "risk_pct": 0.25,
                    "timeframe": "H1",
                    "confidence": "high",
                    "is_demo": True,
                },
                memory=store,
            )
            mt5_tick({"symbol": "BTC", "last": 105.4, "timeframe": "H1", "spread": 0.2}, memory=store)
            trades = mt5_shadow_trades(memory=store, symbol="BTC")
            performance = mt5_performance(memory=store, symbol="BTC", timeframe="H1")

            self.assertEqual(trades["open"], 0)
            self.assertEqual(trades["closed"], 0)
            self.assertEqual(trades["excluded_count"], 2)
            self.assertEqual(performance["summary_manual"]["wins"], 0)
            self.assertEqual(performance["summary_manual"]["losses"], 0)
            self.assertEqual(performance["summary_manual"]["win_rate"], 0.0)
            self.assertEqual(performance["summary_manual"]["profit_factor"], 0.0)
            self.assertEqual(performance["summary_manual"]["excluded_from_main_metrics"], 2)
            self.assertFalse(trades["broker_touched"])
            self.assertFalse(trades["order_executed"])

    def test_mt5_no_trade_and_hedge_outcomes_are_measured(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", {"MT5_FAST_PATH_ONLY": "false"}):
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            no_trade = mt5_signal({"symbol": "BTC", "decision": "NO_TRADE", "price": 100, "reason": "no_edge"}, memory=store)
            hedge = mt5_signal({"symbol": "BTC", "decision": "HEDGE", "price": 100, "hedge_score": 72, "reason": "risk_off"}, memory=store)
            tick = mt5_tick({"symbol": "BTC", "last": 98, "timeframe": "H1", "spread": 0.1}, memory=store)
            performance = mt5_performance(memory=store, symbol="BTC")
            outcomes = mt5_outcomes_recent(memory=store, symbol="BTC", limit=10)

            self.assertEqual(no_trade["shadow"]["status"], "no_trade_outcome_pending")
            self.assertEqual(hedge["shadow"]["status"], "hedge_outcome_pending")
            self.assertEqual(tick["status"], "mt5_tick_recorded")
            self.assertEqual(performance["no_trade_accuracy"]["protected_loss_count"], 1)
            self.assertEqual(performance["no_trade_accuracy"]["accuracy"], 100.0)
            self.assertEqual(performance["hedge_accuracy"]["accuracy"], 100.0)
            self.assertTrue(any(item["event_type"] == "mt5_no_trade_outcome" for item in outcomes["items"]))
            self.assertTrue(any(item["event_type"] == "mt5_hedge_outcome" for item in outcomes["items"]))
            self.assertFalse(outcomes["broker_touched"])
            self.assertFalse(outcomes["order_executed"])

    def test_demo_account_recognition_avoids_false_demo_only_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            mt5_account_sync({"account_id": "demo-1", "server": "DemoServer", "is_demo": True, "balance": 10000}, memory=store)
            request = mt5_order_request({"symbol": "BTC", "action": "BUY", "entry": 100, "risk_pct": 0.25, "confidence": "high"}, memory=store)
            direct_demo = mt5_order_request({"symbol": "BTC", "action": "BUY", "entry": 100, "risk_pct": 0.25, "confidence": "high", "is_demo": True}, memory=store)

            self.assertNotIn("demo_only_account_required", request["risk_guard"]["reasons"])
            self.assertNotIn("demo_only_account_required", direct_demo["risk_guard"]["reasons"])
            self.assertIn("order_execution_disabled", request["risk_guard"]["reasons"])
            self.assertFalse(request["order_executed"])
            self.assertFalse(request["broker_touched"])

    def test_api_route_facades_return_expected_contracts(self) -> None:
        decision = get_genesis_mt5_decision("BTCUSD")
        account = post_genesis_mt5_account_sync({"account_id": "demo", "is_demo": True})
        signal = post_genesis_mt5_signal({"symbol": "BTCUSD", "decision": "WAIT"})
        tick = post_genesis_mt5_tick({"symbol": "BTCUSD", "last": 100, "timeframe": "H1"})
        request = post_genesis_mt5_order_request({"symbol": "BTCUSD", "action": "BUY", "entry": 100, "risk_pct": 0.25})
        result = post_genesis_mt5_order_result({"symbol": "BTCUSD", "result": "simulated"})
        performance = get_genesis_mt5_performance("BTCUSD")
        auto_performance = get_genesis_mt5_performance_auto("BTCUSD")
        forward = get_genesis_mt5_forward_test("BTCUSD")
        outcomes = get_genesis_mt5_outcomes_recent(symbol="BTCUSD", limit=5)
        no_trade_report = get_genesis_mt5_no_trade_report(symbol="BTCUSD", limit=5)
        shadow = get_genesis_mt5_shadow_trades(symbol="BTCUSD", limit=5)
        debug = get_genesis_mt5_debug_storage(symbol="BTCUSD")
        auto_status = get_genesis_mt5_auto_forward_status(symbol="BTCUSD")
        instrument = get_genesis_mt5_instrument("BTCUSD")
        reset = post_genesis_mt5_manual_tests_reset({"symbol": "BTCUSD"})
        exclude_proxy = post_genesis_mt5_metrics_exclude_old_proxy({"symbol": "BTCUSD"})
        replay = post_genesis_mt5_replay_run(
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "bars": 3,
                "bars_data": [
                    {"time": "1", "open": 100, "high": 101, "low": 99, "close": 100},
                    {"time": "2", "open": 100, "high": 105, "low": 99, "close": 103},
                    {"time": "3", "open": 103, "high": 106, "low": 101, "close": 105},
                ],
            }
        )
        replay_results = get_genesis_mt5_replay_results("BTCUSD")
        replay_status = get_genesis_mt5_replay_status("BTCUSD")
        replay_reset = post_genesis_mt5_replay_reset({"symbol": "BTCUSD"})
        learning = post_genesis_mt5_learning_run({"symbol": "BTCUSD", "timeframe": "H1", "mode": "paper"})
        learning_status = get_genesis_mt5_learning_status("BTCUSD")
        memory_summary = get_genesis_mt5_memory_summary("BTCUSD")
        adaptive_state = get_genesis_mt5_adaptive_state("BTCUSD", "H1")
        profiles = get_genesis_mt5_strategy_profiles("BTCUSD")
        recommendations = get_genesis_mt5_adaptive_recommendations("BTCUSD", "H1")
        paper_defense = get_genesis_mt5_paper_defense("BTCUSD")

        self.assertTrue(decision["ok"])
        self.assertTrue(account["ok"])
        self.assertTrue(signal["ok"])
        self.assertTrue(tick["ok"])
        self.assertTrue(request["ok"])
        self.assertTrue(result["ok"])
        self.assertTrue(performance["ok"])
        self.assertTrue(auto_performance["ok"])
        self.assertTrue(forward["ok"])
        self.assertTrue(outcomes["ok"])
        self.assertTrue(no_trade_report["ok"])
        self.assertTrue(shadow["ok"])
        self.assertTrue(debug["ok"])
        self.assertTrue(auto_status["ok"])
        self.assertTrue(instrument["ok"])
        self.assertEqual(instrument["normalized_symbol"], "BTCUSD")
        self.assertTrue(reset["ok"])
        self.assertTrue(exclude_proxy["ok"])
        self.assertTrue(replay["ok"])
        self.assertTrue(replay_results["ok"])
        self.assertTrue(replay_status["ok"])
        self.assertTrue(replay_reset["ok"])
        self.assertFalse(learning["ok"])
        self.assertEqual(learning["status"], "learning_disabled_by_fast_path")
        self.assertTrue(learning_status["ok"])
        self.assertFalse(memory_summary["ok"])
        self.assertEqual(memory_summary["status"], "memory_summary_disabled_by_fast_path")
        self.assertTrue(adaptive_state["ok"])
        self.assertTrue(profiles["ok"])
        self.assertTrue(recommendations["ok"])
        self.assertTrue(paper_defense["ok"])
        self.assertFalse(request["order_executed"])
        self.assertFalse(request["broker_touched"])
        self.assertEqual(request["order_policy"], "journal_only_no_broker")

    def test_replay_run_status_results_and_reset_are_journal_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            bars = [
                {"time": "1", "open": 100, "high": 101, "low": 99, "close": 100},
                {"time": "2", "open": 100, "high": 105, "low": 99, "close": 103},
                {"time": "3", "open": 103, "high": 106, "low": 101, "close": 105},
                {"time": "4", "open": 105, "high": 108, "low": 102, "close": 104},
                {"time": "5", "open": 104, "high": 107, "low": 100, "close": 101},
            ]

            run = mt5_replay_run({"symbol": "BTCUSD", "timeframe": "H1", "bars": 5, "profile": "BTCUSD_PAPER_EXPLORATION_V1", "bars_data": bars}, memory=store)
            results = mt5_replay_results(memory=store, symbol="BTCUSD")
            status = mt5_replay_status(memory=store, symbol="BTCUSD")
            reset = mt5_replay_reset({"symbol": "BTCUSD"}, memory=store)

            self.assertTrue(run["ok"])
            self.assertEqual(run["result"]["normalized_symbol"], "BTCUSD")
            self.assertEqual(run["result"]["profile"], "BTCUSD_PAPER_EXPLORATION_V1")
            self.assertEqual(run["result"]["instrument_type"], "crypto_spot")
            self.assertGreaterEqual(results["replay_trades"], 1)
            self.assertEqual(results["status"], "mt5_replay_results_ready")
            self.assertEqual(status["status"], "mt5_replay_status_ready")
            self.assertTrue(reset["ok"])
            self.assertFalse(run["broker_touched"])
            self.assertFalse(results["order_executed"])
            self.assertEqual(run["order_policy"], "journal_only_no_broker")

    def test_learning_run_creates_trade_memory_lesson_and_profile_stats(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", _LEARNING_ENABLED_ENV):
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            _save_closed_shadow_trade(store, "learn-win", status="win", r_multiple=1.2, pnl=1200, exit_reason="take_profit")

            learning = mt5_learning_run({"symbol": "BTCUSD", "timeframe": "H1", "mode": "paper"}, memory=store)
            summary = mt5_memory_summary(memory=store, symbol="BTCUSD")
            profiles = mt5_strategy_profiles(memory=store, symbol="BTCUSD")

            memories = store.get_mt5_events("mt5_trade_memory", "BTCUSD", limit=10)
            lessons = store.get_mt5_events("mt5_trade_lessons", "BTCUSD", limit=10)

            self.assertTrue(learning["ok"])
            self.assertEqual(learning["status"], "mt5_learning_run_completed")
            self.assertEqual(learning["memories_created"], 1)
            self.assertEqual(learning["lessons_created"], 1)
            self.assertEqual(len(memories), 1)
            self.assertEqual(len(lessons), 1)
            self.assertEqual(lessons[0]["payload"]["trade_quality"], "good")
            self.assertIn("good_risk_control", lessons[0]["payload"]["strengths"])
            self.assertEqual(summary["total_memories"], 1)
            self.assertGreaterEqual(profiles["count"], 1)
            self.assertFalse(learning["broker_touched"])
            self.assertFalse(learning["order_executed"])

    def test_learning_run_marks_loss_lesson_with_mistakes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", _LEARNING_ENABLED_ENV):
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            _save_closed_shadow_trade(
                store,
                "learn-loss",
                status="loss",
                r_multiple=-1.0,
                pnl=-1000,
                exit_reason="time_stop",
                confidence="low",
                regime="chop",
            )

            learning = mt5_learning_run({"symbol": "BTCUSD", "timeframe": "H1"}, memory=store)
            lesson = store.get_mt5_events("mt5_trade_lessons", "BTCUSD", limit=1)[0]["payload"]

            self.assertTrue(learning["ok"])
            self.assertEqual(lesson["trade_quality"], "bad")
            self.assertIn("chop_market", lesson["mistakes"])
            self.assertIn("time_stop_loss", lesson["tags"])
            self.assertFalse(lesson["broker_touched"])
            self.assertFalse(lesson["order_executed"])

    def test_learning_run_empty_is_fast_and_journal_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")

            learning = mt5_learning_run({"symbol": "BTCUSD", "timeframe": "H1", "mode": "paper", "max_trades": 25}, memory=store)
            status = mt5_learning_status(memory=store, symbol="BTCUSD")

            self.assertFalse(learning["ok"])
            self.assertEqual(learning["status"], "learning_disabled_by_fast_path")
            self.assertEqual(learning["trades_seen"], 0)
            self.assertEqual(learning["trades_processed"], 0)
            self.assertLess(learning["duration_ms"], 8000)
            self.assertEqual(status["status"], "mt5_learning_status_ready")
            self.assertFalse(learning["broker_touched"])
            self.assertFalse(status["order_executed"])

    def test_hot_path_tick_does_not_instantiate_memory_and_handles_queue_full(self) -> None:
        with patch("services.mt5.mt5_signal_router.MemoryStore", side_effect=AssertionError("hot path must not open MemoryStore")):
            with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": False, "dropped": True, "warning": "ingest_queue_full", "queue_depth": 5000}):
                tick = mt5_tick({"symbol": "BTCUSD", "last": 77000, "timeframe": "H1"})
                decision = mt5_decision("BTCUSD")

        self.assertTrue(tick["ok"])
        self.assertEqual(tick["status"], "mt5_tick_recorded_fast_path")
        self.assertFalse(tick["tick_saved"])
        self.assertEqual(tick["warning"], "ingest_queue_full")
        self.assertTrue(decision["ok"])
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(tick["order_executed"])

    def test_db_circuit_breaker_returns_snapshot_without_db(self) -> None:
        from services.mt5.mt5_db_circuit_breaker import record_db_error, reset_db_circuit_breaker
        from services.mt5.mt5_runtime_snapshot import update_performance

        reset_db_circuit_breaker()
        try:
            update_performance(
                "BTCUSD",
                {"shadow_trades": 5, "closed": 4, "open": 1, "wins": 3, "losses": 1, "win_rate": 75.0, "profit_factor": 3.0, "expectancy": 0.5},
                {"ok": True, "status": "mt5_performance_ready", "symbol": "BTCUSD", "summary": {"closed": 4, "open": 1}, "broker_touched": False, "order_executed": False},
            )
            record_db_error({"C": "57014", "M": "canceling statement due to statement timeout"}, duration_ms=2000)
            with patch("services.mt5.mt5_signal_router.MemoryStore", side_effect=AssertionError("degraded hot path must not open MemoryStore")):
                performance = mt5_performance(symbol="BTCUSD")
                debug = mt5_debug_storage(symbol="BTCUSD", limit=20)

            self.assertEqual(performance["data_source_used"], "runtime_snapshot")
            self.assertEqual(performance["summary"]["closed"], 4)
            self.assertTrue(debug["db_degraded"])
            self.assertEqual(debug["status"], "mt5_storage_debug_snapshot_only")
            self.assertFalse(performance["broker_touched"])
            self.assertFalse(debug["order_executed"])
        finally:
            reset_db_circuit_breaker()

    def test_fast_path_reads_recent_shadow_trades_for_metrics_and_adaptive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            for index, status in enumerate(("win", "win", "win", "loss")):
                _save_closed_shadow_trade(
                    store,
                    f"fast-path-{index}",
                    status=status,
                    r_multiple=1.0 if status == "win" else -1.0,
                    pnl=1000 if status == "win" else -1000,
                    exit_reason="take_profit" if status == "win" else "stop_loss",
                    opened_minute=index,
                )
            _save_open_shadow_trade(store, "fast-path-open")

            performance = mt5_performance(memory=store, symbol="BTCUSD")
            state = mt5_adaptive_state(memory=store, symbol="BTCUSD", timeframe="H1")
            recommendations = mt5_adaptive_recommendations(memory=store, symbol="BTCUSD", timeframe="H1")

            self.assertEqual(performance["summary"]["closed"], 4)
            self.assertEqual(performance["summary"]["open"], 1)
            self.assertEqual(performance["summary"]["wins"], 3)
            self.assertEqual(performance["summary"]["losses"], 1)
            self.assertGreater(performance["summary"]["profit_factor"], 0)
            self.assertEqual(state["closed_trades"], 4)
            self.assertEqual(state["data_source_used"], "shadow_trades_fast_path")
            self.assertGreater(state["rolling_profit_factor"], 0)
            self.assertEqual(recommendations["closed_trades"], 4)
            self.assertNotEqual(recommendations["data_source_used"], "no_data")
            self.assertFalse(performance["broker_touched"])
            self.assertFalse(state["order_executed"])
            self.assertFalse(recommendations["broker_touched"])

    def test_ingest_queue_rolls_back_dead_letters_bad_event_and_recovers(self) -> None:
        from services.mt5.mt5_db_circuit_breaker import is_db_degraded, reset_db_circuit_breaker

        class FakeConnection:
            def __init__(self) -> None:
                self.rollbacks = 0

            def rollback(self) -> None:
                self.rollbacks += 1

        fake_connection = FakeConnection()
        saved: list[str] = []

        class FailingMemory:
            _pg = fake_connection

            def save_mt5_event(self, collection: str, symbol: str, payload: dict[str, object]) -> None:
                raise RuntimeError("current transaction is aborted, commands ignored until end of transaction block")

        class WorkingMemory:
            _pg = fake_connection

            def save_mt5_event(self, collection: str, symbol: str, payload: dict[str, object]) -> None:
                saved.append(str(payload.get("id")))

        memories = [FailingMemory(), WorkingMemory()]

        def memory_factory():
            return memories.pop(0)

        reset_db_circuit_breaker()
        try:
            ingest = MT5IngestQueue(max_size=10, memory_factory=memory_factory, auto_start_worker=False)
            ingest.enqueue("mt5_ticks", "BTCUSD", {"symbol": "BTCUSD", "id": "bad"})
            ingest.enqueue("mt5_ticks", "BTCUSD", {"symbol": "BTCUSD", "id": "good"})
            ingest.flush_once_for_tests(limit=2)
            status = ingest.status()

            self.assertEqual(fake_connection.rollbacks, 1)
            self.assertEqual(status["failed_flushes"], 1)
            self.assertEqual(status["dead_letter_count"], 1)
            self.assertEqual(status["flushed_events"], 1)
            self.assertEqual(saved, ["good"])
            self.assertTrue(is_db_degraded())
            self.assertIn("current transaction is aborted", status["last_ingest_error"])
            self.assertEqual(ingest.dead_letters_for_tests()[0]["payload"]["id"], "bad")
        finally:
            reset_db_circuit_breaker()

    def test_ops_status_reports_ingest_failures_and_db_degraded(self) -> None:
        from services.mt5.mt5_db_circuit_breaker import record_db_error, reset_db_circuit_breaker

        reset_db_circuit_breaker()
        try:
            record_db_error(RuntimeError("current transaction is aborted, commands ignored until end of transaction block"))
            router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True))
            ingest_payload = {
                "queue_depth": 0,
                "queue_max_size": 5000,
                "dropped_events": 0,
                "enqueued_events": 2,
                "flushed_events": 1,
                "failed_flushes": 1,
                "dead_letter_count": 1,
                "last_enqueue_at": "",
                "last_drop_at": "",
                "last_flush_at": "",
                "last_successful_flush_at": "2026-05-19T00:00:00+00:00",
                "last_failed_flush_at": "2026-05-19T00:01:00+00:00",
                "last_ingest_error": "current transaction is aborted",
            }
            with patch("services.mt5.mt5_signal_router.ingest_status", return_value=ingest_payload):
                status = router.ops_status(symbol="BTCUSD")

            self.assertTrue(status["db_degraded"])
            self.assertEqual(status["failed_flushes"], 1)
            self.assertEqual(status["dead_letter_count"], 1)
            self.assertEqual(status["last_ingest_error"], "current transaction is aborted")
            self.assertFalse(status["broker_touched"])
            self.assertFalse(status["order_executed"])
        finally:
            reset_db_circuit_breaker()

    def test_postgres_migrations_disabled_skips_runtime_schema_ddl(self) -> None:
        with patch.dict("os.environ", {"GENESIS_DB_MIGRATIONS_ENABLED": "false"}):
            with patch.object(MemoryStore, "_connect_postgres", return_value=object()):
                with patch.object(MemoryStore, "_ensure_schema", side_effect=AssertionError("runtime DDL must be disabled")):
                    store = MemoryStore(database_url="postgresql://user:pass@localhost/db")

        self.assertEqual(store.backend, "postgres")

    def test_postgres_migrations_enabled_runs_schema_ddl(self) -> None:
        calls: list[str] = []

        def fake_ensure(self) -> None:
            calls.append("ensure_schema")

        with patch.dict("os.environ", {"GENESIS_DB_MIGRATIONS_ENABLED": "true"}):
            with patch.object(MemoryStore, "_connect_postgres", return_value=object()):
                with patch.object(MemoryStore, "_ensure_schema", fake_ensure):
                    store = MemoryStore(database_url="postgresql://user:pass@localhost/db")

        self.assertEqual(store.backend, "postgres")
        self.assertEqual(calls, ["ensure_schema"])

    def test_ingest_worker_memory_store_does_not_run_runtime_ddl_when_disabled(self) -> None:
        executed_sql: list[str] = []

        class FakeCursor:
            def execute(self, sql: str, params: object = None) -> None:
                executed_sql.append(str(sql))

            def close(self) -> None:
                pass

        class FakeConnection:
            def cursor(self) -> FakeCursor:
                return FakeCursor()

            def commit(self) -> None:
                pass

            def rollback(self) -> None:
                pass

        with patch.dict("os.environ", {"GENESIS_DB_MIGRATIONS_ENABLED": "false"}):
            with patch.object(MemoryStore, "_connect_postgres", return_value=FakeConnection()):
                with patch.object(MemoryStore, "_ensure_schema", side_effect=AssertionError("ingest worker must not run DDL")):
                    ingest = MT5IngestQueue(
                        max_size=10,
                        memory_factory=lambda: MemoryStore(database_url="postgresql://user:pass@localhost/db"),
                        auto_start_worker=False,
                    )
                    ingest.enqueue("mt5_ticks", "BTCUSD", {"symbol": "BTCUSD", "last": 77000})
                    ingest.flush_once_for_tests(limit=1)

        self.assertEqual(ingest.status()["flushed_events"], 1)
        self.assertTrue(any("INSERT INTO genesis_memory_events" in sql for sql in executed_sql))
        self.assertFalse(any(token in "\n".join(executed_sql).upper() for token in ("CREATE TABLE", "CREATE INDEX", "ALTER TABLE", "DROP ")))

    def test_pgrst_ddl_watch_error_trips_circuit_breaker_with_runtime_warning(self) -> None:
        from services.mt5.mt5_db_circuit_breaker import record_db_error, reset_db_circuit_breaker, status_payload

        reset_db_circuit_breaker()
        try:
            record_db_error("57014 canceling statement due to statement timeout PL/pgSQL function pgrst_ddl_watch() line 5 at FOR over SELECT rows")
            status = status_payload()
            self.assertTrue(status["db_degraded"])
            self.assertEqual(status["db_last_sql_operation_type"], "DDL")
            self.assertIn("runtime_ddl_detected", status["warnings"])
        finally:
            reset_db_circuit_breaker()

    def test_paper_exploration_creates_one_shadow_trade_and_updates_performance(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests

        reset_runtime_snapshots_for_tests()
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            with patch("services.mt5.mt5_paper_exploration.enqueue_mt5_event", return_value={"queued": True}):
                tick = router.tick(_paper_edge_tick(100.0, timeframe="H1"))
                second = router.tick(_paper_edge_tick(100.2, timeframe="H1"))
                performance = router.performance(symbol="BTCUSD")

        self.assertTrue(tick["paper_exploration_created"])
        self.assertFalse(second["paper_exploration_created"])
        self.assertEqual(performance["summary"]["shadow_trades"], 1)
        self.assertEqual(performance["summary"]["open"], 1)
        self.assertFalse(tick["broker_touched"])
        self.assertFalse(performance["order_executed"])

    def test_paper_exploration_respects_cooldown_after_open_cleared(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_open_shadow_trade

        reset_runtime_snapshots_for_tests()
        router = MT5SignalRouter(
            config=MT5BridgeConfig(
                fast_path_only=True,
                paper_exploration_enabled=True,
                paper_exploration_cooldown_sec=300,
            )
        )
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            with patch("services.mt5.mt5_paper_exploration.enqueue_mt5_event", return_value={"queued": True}):
                first = router.tick(_paper_edge_tick(100.0))
                update_open_shadow_trade("BTCUSD", None)
                second = router.tick(_paper_edge_tick(100.1))

        self.assertTrue(first["paper_exploration_created"])
        self.assertFalse(second["paper_exploration_created"])
        self.assertEqual(second["paper_exploration_reason"], "cooldown_active")

    def test_paper_exploration_blocks_high_spread(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests

        reset_runtime_snapshots_for_tests()
        router = MT5SignalRouter(
            config=MT5BridgeConfig(
                fast_path_only=True,
                paper_exploration_enabled=True,
                paper_exploration_max_spread_points=5,
            )
        )
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            tick = router.tick({"symbol": "BTCUSD", "last": 100.0, "spread": 10})

        self.assertFalse(tick["paper_exploration_created"])
        self.assertEqual(tick["paper_exploration_reason"], "spread_too_high")
        self.assertFalse(tick["order_executed"])

    def test_paper_exploration_updates_unrealized_pnl_mfe_mae(self) -> None:
        from services.mt5.mt5_runtime_snapshot import get_snapshot, reset_runtime_snapshots_for_tests

        reset_runtime_snapshots_for_tests()
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            with patch("services.mt5.mt5_paper_exploration.enqueue_mt5_event", return_value={"queued": True}):
                router.tick(_paper_edge_tick(100.0))
                router.tick({"symbol": "BTCUSD", "last": 100.6, "spread": 10})
        trade = (get_snapshot("BTCUSD") or {}).get("open_shadow_trade") or {}

        self.assertGreater(trade.get("unrealized_pnl", 0), 0)
        self.assertGreater(trade.get("r_multiple", 0), 0)
        self.assertGreater(trade.get("max_favorable_excursion", 0), 0)
        self.assertEqual(trade.get("max_adverse_excursion"), 0.0)

    def test_paper_exploration_closes_by_time_stop(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests

        reset_runtime_snapshots_for_tests()
        router = MT5SignalRouter(
            config=MT5BridgeConfig(
                fast_path_only=True,
                paper_exploration_enabled=True,
                paper_exploration_time_stop_min=0,
                paper_exploration_cooldown_sec=300,
            )
        )
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            with patch("services.mt5.mt5_paper_exploration.enqueue_mt5_event", return_value={"queued": True}):
                router.tick(_paper_edge_tick(100.0))
                router.tick({"symbol": "BTCUSD", "last": 100.1, "spread": 10})
                performance = router.performance(symbol="BTCUSD")

        self.assertEqual(performance["summary"]["closed"], 1)
        self.assertEqual(performance["summary"]["open"], 0)
        self.assertEqual(performance["recent_closed_trades"][0]["exit_reason"], "time_stop")

    def test_paper_exploration_closes_by_stop_loss_and_take_profit(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests

        reset_runtime_snapshots_for_tests()
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True, paper_exploration_cooldown_sec=300))
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            with patch("services.mt5.mt5_paper_exploration.enqueue_mt5_event", return_value={"queued": True}):
                router.tick(_paper_edge_tick(100.0))
                router.tick({"symbol": "BTCUSD", "last": 98.0, "spread": 10})
                stop_performance = router.performance(symbol="BTCUSD")

        reset_runtime_snapshots_for_tests()
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True, paper_exploration_cooldown_sec=300))
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            with patch("services.mt5.mt5_paper_exploration.enqueue_mt5_event", return_value={"queued": True}):
                router.tick(_paper_edge_tick(100.0))
                router.tick({"symbol": "BTCUSD", "last": 102.0, "spread": 10})
                target_performance = router.performance(symbol="BTCUSD")

        self.assertEqual(stop_performance["recent_closed_trades"][0]["exit_reason"], "stop_loss")
        self.assertEqual(stop_performance["summary"]["losses"], 1)
        self.assertEqual(target_performance["recent_closed_trades"][0]["exit_reason"], "take_profit")
        self.assertEqual(target_performance["summary"]["wins"], 1)

    def test_decision_can_create_paper_probe_without_broker_touch(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_tick

        reset_runtime_snapshots_for_tests()
        update_tick("BTCUSD", _paper_edge_tick(100.0, timeframe="H1"))
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        with patch("services.mt5.mt5_paper_exploration.enqueue_mt5_event", return_value={"queued": True}):
            decision = router.decision("BTCUSD")

        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertTrue(decision["paper_exploration_created"])
        self.assertEqual(decision["reason"], "real_trade_disabled_paper_probe_created")
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])

    def test_promoted_profile_endpoint_returns_degraded_observation_only(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests

        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()
        profile = get_genesis_mt5_promoted_profile(symbol="BTCUSD", timeframe="M30")
        state = get_genesis_mt5_forward_profile_state(symbol="BTCUSD", timeframe="M30")
        repeated = mt5_promoted_profile(symbol="BTCUSD", timeframe="M30")

        self.assertTrue(profile["ok"])
        self.assertEqual(profile["profile"], "quality_loose")
        self.assertEqual(profile["status"], "observation_only")
        self.assertEqual(profile["mode"], "observation_only")
        self.assertFalse(profile["active"])
        self.assertTrue(profile["degraded"])
        self.assertEqual(profile["degradation_reason"], "early_forward_underperformance")
        self.assertEqual(profile["promoted_by"], "walk_forward_optimizer")
        self.assertFalse(profile["applies_to_paper_shadow"])
        self.assertFalse(profile["applies_to_real_trading"])
        self.assertEqual(state["status"], "observation_only")
        self.assertFalse(state["active"])
        self.assertTrue(state["degraded"])
        self.assertEqual(state["degradation_reason"], "early_forward_underperformance")
        self.assertFalse(state["applies_to_paper_shadow"])
        self.assertFalse(state["applies_to_real_trading"])
        self.assertEqual(repeated["profile"], "quality_loose")
        self.assertEqual(repeated["status"], "observation_only")
        self.assertTrue(repeated["degraded"])
        self.assertFalse(profile["broker_touched"])
        self.assertFalse(profile["order_executed"])

    def test_quality_loose_candidate_applies_only_to_btcusd_m30_paper_shadow(self) -> None:
        from services.mt5.mt5_runtime_snapshot import get_snapshot, reset_runtime_snapshots_for_tests

        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()
        record_promoted_profile(symbol="BTCUSD", timeframe="M30", profile="quality_loose", mode="paper_forward_candidate")
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        weak_but_quality_loose_ok = {
            "symbol": "BTCUSD",
            "last": 100.0,
            "spread": 10,
            "score": 50,
            "momentum_score": 40,
            "trend_score": 40,
            "volatility_score": 60,
            "timeframe": "M30",
        }
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            with patch("services.mt5.mt5_paper_exploration.enqueue_mt5_event", return_value={"queued": True}):
                m30_tick = router.tick(weak_but_quality_loose_ok)
                open_trade = (get_snapshot("BTCUSD") or {}).get("open_shadow_trade") or {}

        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()
        router_h1 = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            h1_tick = router_h1.tick({**weak_but_quality_loose_ok, "timeframe": "H1"})

        self.assertTrue(m30_tick["paper_exploration_created"])
        self.assertEqual(m30_tick["paper_forward_candidate_profile"], "quality_loose")
        self.assertEqual(open_trade["strategy_profile"], "quality_loose")
        self.assertEqual(open_trade["profile_mode"], "paper_forward_candidate")
        self.assertFalse(open_trade["broker_touched"])
        self.assertFalse(open_trade["order_executed"])
        self.assertFalse(h1_tick["paper_exploration_created"])
        self.assertIn(h1_tick["paper_exploration_reason"], {"momentum_score_low", "trend_score_low"})

    def test_promoted_profile_does_not_affect_h1_or_m15_endpoint(self) -> None:
        reset_promoted_profiles_for_tests()
        h1 = get_genesis_mt5_promoted_profile(symbol="BTCUSD", timeframe="H1")
        m15 = get_genesis_mt5_promoted_profile(symbol="BTCUSD", timeframe="M15")

        self.assertTrue(h1["ok"])
        self.assertFalse(h1["active"])
        self.assertEqual(h1["status"], "observation_only")
        self.assertEqual(h1["reason"], "no_candidate_for_symbol_timeframe")
        self.assertFalse(h1["broker_touched"])
        self.assertFalse(h1["order_executed"])
        self.assertTrue(m15["ok"])
        self.assertFalse(m15["active"])
        self.assertEqual(m15["status"], "observation_only")
        self.assertFalse(m15["broker_touched"])
        self.assertFalse(m15["order_executed"])

    def test_promoted_profile_early_guardrail_degrades_on_low_profit_factor(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_snapshot

        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()
        record_promoted_profile(symbol="BTCUSD", timeframe="M30", profile="quality_loose", mode="paper_forward_candidate")
        update_snapshot(
            "BTCUSD",
            {
                "latest_performance_summary": {
                    "closed": 10,
                    "wins": 4,
                    "losses": 6,
                    "win_rate": 40.0,
                    "profit_factor": 0.79,
                    "expectancy": 0.01,
                }
            },
        )

        state = get_genesis_mt5_forward_profile_state(symbol="BTCUSD", timeframe="M30")

        self.assertEqual(state["status"], "observation_only")
        self.assertEqual(state["degradation_reason"], "early_forward_underperformance")
        self.assertTrue(state["early_guardrail_active"])
        self.assertEqual(state["early_guardrail_min_trades"], 10)
        self.assertEqual(state["early_guardrail_pf_min"], 0.8)
        self.assertEqual(state["early_guardrail_win_rate_min"], 35.0)
        self.assertFalse(state["broker_touched"])
        self.assertFalse(state["order_executed"])

    def test_promoted_profile_early_guardrail_degrades_on_negative_expectancy(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_snapshot

        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()
        record_promoted_profile(symbol="BTCUSD", timeframe="M30", profile="quality_loose", mode="paper_forward_candidate")
        update_snapshot(
            "BTCUSD",
            {
                "latest_performance_summary": {
                    "closed": 10,
                    "wins": 4,
                    "losses": 6,
                    "win_rate": 40.0,
                    "profit_factor": 0.9,
                    "expectancy": -0.01,
                }
            },
        )

        state = mt5_forward_profile_state(symbol="BTCUSD", timeframe="M30")

        self.assertEqual(state["status"], "observation_only")
        self.assertEqual(state["degradation_reason"], "early_forward_underperformance")
        self.assertFalse(state["broker_touched"])
        self.assertFalse(state["order_executed"])

    def test_promoted_profile_early_guardrail_degrades_on_low_win_rate(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_snapshot

        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()
        record_promoted_profile(symbol="BTCUSD", timeframe="M30", profile="quality_loose", mode="paper_forward_candidate")
        update_snapshot(
            "BTCUSD",
            {
                "latest_performance_summary": {
                    "closed": 10,
                    "wins": 3,
                    "losses": 7,
                    "win_rate": 34.0,
                    "profit_factor": 0.9,
                    "expectancy": 0.01,
                }
            },
        )

        state = mt5_forward_profile_state(symbol="BTCUSD", timeframe="M30")

        self.assertEqual(state["status"], "observation_only")
        self.assertEqual(state["degradation_reason"], "early_forward_underperformance")
        self.assertFalse(state["broker_touched"])
        self.assertFalse(state["order_executed"])

    def test_promoted_profile_early_guardrail_waits_for_min_trades(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_snapshot

        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()
        record_promoted_profile(symbol="BTCUSD", timeframe="M30", profile="quality_loose", mode="paper_forward_candidate")
        update_snapshot(
            "BTCUSD",
            {
                "latest_performance_summary": {
                    "closed": 9,
                    "wins": 1,
                    "losses": 8,
                    "win_rate": 11.11,
                    "profit_factor": 0.1,
                    "expectancy": -0.2,
                }
            },
        )

        state = get_genesis_mt5_forward_profile_state(symbol="BTCUSD", timeframe="M30")

        self.assertEqual(state["status"], "paper_forward_candidate")
        self.assertEqual(state["degradation_reason"], "")
        self.assertEqual(state["trades_forward"], 9)
        self.assertFalse(state["broker_touched"])
        self.assertFalse(state["order_executed"])

    def test_promoted_profile_degrades_when_forward_stats_fail_guardrails(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_snapshot

        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()
        record_promoted_profile(symbol="BTCUSD", timeframe="M30", profile="quality_loose", mode="paper_forward_candidate")
        update_snapshot("BTCUSD", {"latest_performance_summary": {"closed": 50, "profit_factor": 1.05, "expectancy": 0.04}})

        profile = get_genesis_mt5_promoted_profile(symbol="BTCUSD", timeframe="M30")
        state = get_genesis_mt5_forward_profile_state(symbol="BTCUSD", timeframe="M30")

        self.assertEqual(profile["status"], "observation_only")
        self.assertFalse(profile["active"])
        self.assertEqual(profile["degrade_reason"], "forward_pf_below_1_1")
        self.assertEqual(state["status"], "observation_only")
        self.assertEqual(state["degradation_reason"], "forward_pf_below_1_1")
        self.assertEqual(state["trades_forward"], 50)
        self.assertFalse(profile["broker_touched"])
        self.assertFalse(profile["order_executed"])
        self.assertFalse(state["broker_touched"])
        self.assertFalse(state["order_executed"])

    def test_promoted_profile_degrades_when_forward_expectancy_is_not_positive(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_snapshot

        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()
        record_promoted_profile(symbol="BTCUSD", timeframe="M30", profile="quality_loose", mode="paper_forward_candidate")
        update_snapshot("BTCUSD", {"latest_performance_summary": {"closed": 50, "profit_factor": 1.2, "expectancy": 0.0}})

        profile = mt5_promoted_profile(symbol="BTCUSD", timeframe="M30")

        self.assertEqual(profile["status"], "observation_only")
        self.assertEqual(profile["degrade_reason"], "forward_expectancy_not_positive")
        self.assertFalse(profile["broker_touched"])
        self.assertFalse(profile["order_executed"])

    def test_promoted_profile_degrades_when_forward_drawdown_exceeds_limit(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_snapshot

        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()
        record_promoted_profile(symbol="BTCUSD", timeframe="M30", profile="quality_loose", mode="paper_forward_candidate")
        update_snapshot("BTCUSD", {"latest_performance_summary": {"closed": 50, "profit_factor": 1.2, "expectancy": 0.05, "max_drawdown": 6000}})

        state = mt5_forward_profile_state(symbol="BTCUSD", timeframe="M30")

        self.assertEqual(state["status"], "observation_only")
        self.assertEqual(state["degradation_reason"], "forward_drawdown_limit_exceeded")
        self.assertEqual(state["max_drawdown"], 6000.0)
        self.assertFalse(state["broker_touched"])
        self.assertFalse(state["order_executed"])

    def test_decision_m30_does_not_use_h1_snapshot_when_m30_requested(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_tick

        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()
        update_tick("BTCUSD", {"symbol": "BTCUSD", "last": 100.0, "spread": 10, "timeframe": "H1"})
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            decision = router.decision("BTCUSD", timeframe="M30")

        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertEqual(decision["reason"], "no_runtime_snapshot_for_requested_timeframe")
        self.assertEqual(decision["requested_timeframe"], "M30")
        self.assertEqual(decision["available_timeframe"], "H1")
        self.assertEqual(decision["strategy_profile"], "")
        self.assertEqual(decision["paper_forward_candidate_profile"], "")
        self.assertIsNone(decision["promoted_profile"])
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])

    def test_decision_m30_does_not_apply_degraded_quality_loose_when_snapshot_exists(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_tick

        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()
        update_tick("BTCUSD", {"symbol": "BTCUSD", "last": 100.0, "spread": 10, "timeframe": "H1"})
        update_tick(
            "BTCUSD",
            {
                "symbol": "BTCUSD",
                "last": 101.0,
                "spread": 10,
                "timeframe": "M30",
                "score": 50,
                "momentum_score": 40,
                "trend_score": 40,
                "volatility_score": 60,
            },
        )
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            with patch("services.mt5.mt5_paper_exploration.enqueue_mt5_event", return_value={"queued": True}):
                decision = router.decision("BTCUSD", timeframe="M30")

        self.assertEqual(decision["timeframe"], "M30")
        self.assertEqual(decision["requested_timeframe"], "M30")
        self.assertEqual(decision["strategy_profile"], "")
        self.assertEqual(decision["paper_forward_candidate_profile"], "")
        self.assertIsNone(decision["promoted_profile"])
        self.assertFalse(decision["applies_to_real_trading"])
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])

    def test_decision_h1_and_m15_do_not_apply_quality_loose(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_tick

        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()
        tick_base = {"symbol": "BTCUSD", "last": 100.0, "spread": 10, "score": 80, "momentum_score": 80, "trend_score": 80, "volatility_score": 80}
        update_tick("BTCUSD", {**tick_base, "timeframe": "H1"})
        update_tick("BTCUSD", {**tick_base, "timeframe": "M15", "last": 100.5})
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            with patch("services.mt5.mt5_paper_exploration.enqueue_mt5_event", return_value={"queued": True}):
                h1 = router.decision("BTCUSD", timeframe="H1")
                m15 = router.decision("BTCUSD", timeframe="M15")

        self.assertEqual(h1["timeframe"], "H1")
        self.assertEqual(m15["timeframe"], "M15")
        self.assertEqual(h1["strategy_profile"], "")
        self.assertEqual(m15["strategy_profile"], "")
        self.assertEqual(h1["paper_forward_candidate_profile"], "")
        self.assertEqual(m15["paper_forward_candidate_profile"], "")
        self.assertFalse(h1["broker_touched"])
        self.assertFalse(m15["broker_touched"])
        self.assertFalse(h1["order_executed"])
        self.assertFalse(m15["order_executed"])

    def test_paper_exploration_enqueues_shadow_event_and_tick_stays_fast(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests

        reset_runtime_snapshots_for_tests()
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        started = time.monotonic()
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            with patch("services.mt5.mt5_paper_exploration.enqueue_mt5_event", return_value={"queued": True}) as shadow_enqueue:
                tick = router.tick(_paper_edge_tick(100.0))
        elapsed = time.monotonic() - started

        self.assertLess(elapsed, 1.0)
        self.assertTrue(tick["paper_exploration_created"])
        shadow_enqueue.assert_called()
        self.assertEqual(shadow_enqueue.call_args.args[0], "mt5_shadow_trades")
        self.assertFalse(tick["broker_touched"])
        self.assertFalse(tick["order_executed"])

    def test_paper_exploration_blocks_blind_snapshot_probe(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests

        reset_runtime_snapshots_for_tests()
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            with patch("services.mt5.mt5_paper_exploration.enqueue_mt5_event", return_value={"queued": True}) as shadow_enqueue:
                tick = router.tick({"symbol": "BTCUSD", "last": 78007.82, "spread": 27.1, "timeframe": "M30"})
                performance = router.performance(symbol="BTCUSD", timeframe="M30")

        self.assertFalse(tick["paper_exploration_created"])
        self.assertEqual(tick["paper_exploration_reason"], "insufficient_entry_evidence")
        shadow_enqueue.assert_not_called()
        self.assertEqual(performance["summary"]["open"], 0)
        self.assertFalse(tick["broker_touched"])
        self.assertFalse(tick["order_executed"])

    def test_paper_exploration_blocks_low_momentum_trend_volatility_and_chop(self) -> None:
        from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests

        cases = (
            ({"momentum_score": 54, "trend_score": 70, "volatility_score": 60}, "momentum_score_low"),
            ({"momentum_score": 70, "trend_score": 54, "volatility_score": 60}, "trend_score_low"),
            ({"momentum_score": 70, "trend_score": 70, "volatility_score": 30}, "volatility_too_low"),
            ({"momentum_score": 70, "trend_score": 70, "volatility_score": 60, "regime": "chop"}, "regime_chop"),
        )
        for extra, reason in cases:
            reset_runtime_snapshots_for_tests()
            router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
            with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
                tick = router.tick({"symbol": "BTCUSD", "last": 100.0, "spread": 10, **extra})
            self.assertFalse(tick["paper_exploration_created"])
            self.assertEqual(tick["paper_exploration_reason"], reason)

    def test_paper_exploration_caution_blocks_negative_recent_edge(self) -> None:
        from services.mt5.mt5_runtime_snapshot import append_closed_shadow_trade, reset_runtime_snapshots_for_tests

        reset_runtime_snapshots_for_tests()
        for index in range(5):
            append_closed_shadow_trade("BTCUSD", _paper_closed_trade(f"neg-{index}", status="loss", r_multiple=-0.5))
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            tick = router.tick({"symbol": "BTCUSD", "last": 100.0, "spread": 10, "score": 80, "momentum_score": 80, "trend_score": 80, "volatility_score": 80})

        self.assertFalse(tick["paper_exploration_created"])
        self.assertEqual(tick["paper_exploration_reason"], "loss_cluster_cooldown")

    def test_paper_exploration_caution_raises_min_score(self) -> None:
        from services.mt5.mt5_runtime_snapshot import append_closed_shadow_trade, reset_runtime_snapshots_for_tests

        reset_runtime_snapshots_for_tests()
        for index in range(10):
            append_closed_shadow_trade("BTCUSD", _paper_closed_trade(f"caution-old-{index}", status="loss", r_multiple=-1.0))
        for index, status in enumerate(("loss", "win", "win", "loss", "win")):
            append_closed_shadow_trade(
                "BTCUSD",
                _paper_closed_trade(f"caution-recent-{index}", status=status, r_multiple=0.1 if status == "win" else -0.2),
            )
        router = MT5SignalRouter(
            config=MT5BridgeConfig(
                fast_path_only=True,
                paper_exploration_enabled=True,
                paper_exploration_min_score=45,
            )
        )
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            tick = router.tick({"symbol": "BTCUSD", "last": 100.0, "spread": 10, "score": 50, "momentum_score": 80, "trend_score": 80, "volatility_score": 80})

        self.assertFalse(tick["paper_exploration_created"])
        self.assertEqual(tick["paper_exploration_reason"], "score_too_low")

    def test_paper_exploration_detects_time_stop_cluster_and_requires_momentum(self) -> None:
        from services.mt5.mt5_runtime_snapshot import append_closed_shadow_trade, reset_runtime_snapshots_for_tests

        reset_runtime_snapshots_for_tests()
        for index in range(10):
            append_closed_shadow_trade("BTCUSD", _paper_closed_trade(f"time-{index}", status="win", r_multiple=0.1, exit_reason="time_stop"))
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            blocked = router.tick({"symbol": "BTCUSD", "last": 100.0, "spread": 10, "momentum_score": 60, "trend_score": 70, "volatility_score": 70})
        reset_runtime_snapshots_for_tests()
        for index in range(10):
            append_closed_shadow_trade("BTCUSD", _paper_closed_trade(f"time-ok-{index}", status="win", r_multiple=0.1, exit_reason="time_stop"))
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            allowed = router.tick({"symbol": "BTCUSD", "last": 100.0, "spread": 10, "momentum_score": 70, "trend_score": 70, "volatility_score": 70, "regime": "breakout"})

        self.assertFalse(blocked["paper_exploration_created"])
        self.assertEqual(blocked["paper_exploration_reason"], "time_stop_cluster")
        self.assertTrue(allowed["paper_exploration_created"])

    def test_paper_performance_exit_reason_and_side_metrics(self) -> None:
        from services.mt5.mt5_runtime_snapshot import append_closed_shadow_trade, reset_runtime_snapshots_for_tests

        reset_runtime_snapshots_for_tests()
        append_closed_shadow_trade("BTCUSD", _paper_closed_trade("buy-win", status="win", r_multiple=1.2, side="buy", exit_reason="take_profit"))
        append_closed_shadow_trade("BTCUSD", _paper_closed_trade("buy-loss", status="loss", r_multiple=-1.0, side="buy", exit_reason="stop_loss"))
        append_closed_shadow_trade("BTCUSD", _paper_closed_trade("sell-win", status="win", r_multiple=0.4, side="sell", exit_reason="time_stop"))
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        performance = router.performance(symbol="BTCUSD")
        state = router.adaptive_state(symbol="BTCUSD")

        self.assertEqual(performance["summary"]["take_profit_count"], 1)
        self.assertEqual(performance["summary"]["stop_loss_count"], 1)
        self.assertEqual(performance["summary"]["time_stop_count"], 1)
        self.assertEqual(performance["summary"]["buy_win_rate"], 50.0)
        self.assertEqual(performance["summary"]["sell_win_rate"], 100.0)
        self.assertGreater(performance["summary"]["buy_pf"], 0)
        self.assertFalse(state["broker_touched"])
        self.assertFalse(state["order_executed"])

    def test_debug_storage_fast_path_is_limited_and_does_not_call_performance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            journal = MT5Journal(memory=store)
            for index in range(80):
                journal.save(
                    "mt5_ticks",
                    "BTCUSD",
                    {"symbol": "BTCUSD", "last": 77000 + index, "broker_touched": False, "order_executed": False},
                )
            _save_open_shadow_trade(store, "debug-open")

            started = time.perf_counter()
            with patch("services.mt5.mt5_signal_router.MT5SignalRouter.performance", side_effect=AssertionError("debug/storage must not call performance")):
                debug = mt5_debug_storage(memory=store, symbol="BTCUSD", limit=20)
            elapsed = time.perf_counter() - started

            self.assertTrue(debug["ok"])
            self.assertLess(elapsed, 3)
            self.assertLessEqual(debug["limit"], 20)
            self.assertTrue(debug["approximate_counts"])
            self.assertLessEqual(debug["collection_counts"]["mt5_ticks"], 20)
            self.assertFalse(debug["broker_touched"])
            self.assertFalse(debug["order_executed"])

    def test_learning_endpoints_return_controlled_errors(self) -> None:
        with patch("api.routes.genesis.mt5_learning_run", side_effect=RuntimeError("boom")):
            learning = post_genesis_mt5_learning_run({"symbol": "BTCUSD"})
        with patch("api.routes.genesis.mt5_memory_summary", side_effect=RuntimeError("summary boom")):
            summary = get_genesis_mt5_memory_summary("BTCUSD", limit=50)

        self.assertFalse(learning["ok"])
        self.assertEqual(learning["status"], "mt5_learning_error")
        self.assertFalse(learning["broker_touched"])
        self.assertFalse(learning["order_executed"])
        self.assertFalse(summary["ok"])
        self.assertEqual(summary["status"], "mt5_learning_error")
        self.assertEqual(summary["order_policy"], "journal_only_no_broker")

    def test_learning_run_caps_default_max_trades(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", _LEARNING_ENABLED_ENV):
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            for index in range(200):
                _save_closed_shadow_trade(
                    store,
                    f"cap-{index}",
                    status="win" if index % 2 == 0 else "loss",
                    r_multiple=1.0 if index % 2 == 0 else -1.0,
                    pnl=1000 if index % 2 == 0 else -1000,
                    exit_reason="take_profit" if index % 2 == 0 else "stop_loss",
                    opened_minute=index,
                )

            learning = mt5_learning_run({"symbol": "BTCUSD", "timeframe": "H1"}, memory=store)

            self.assertTrue(learning["ok"])
            self.assertEqual(learning["max_trades"], 25)
            self.assertLessEqual(learning["trades_processed"], 25)
            self.assertLessEqual(learning["memories_created"], 25)
            self.assertFalse(learning["broker_touched"])
            self.assertFalse(learning["order_executed"])

    def test_learning_run_incomplete_trade_records_error_and_continues(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", _LEARNING_ENABLED_ENV):
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            _save_closed_shadow_trade(store, "complete-win", status="win", r_multiple=1.0, pnl=1000, exit_reason="take_profit")
            _save_closed_shadow_trade(store, "bad-snapshot", status="win", r_multiple=1.0, pnl=1000, exit_reason="take_profit", opened_minute=1)

            from services.mt5 import mt5_trade_memory as trade_memory_module

            original_snapshot = trade_memory_module.build_trade_memory_snapshot

            def flaky_snapshot(trade: dict) -> dict:
                if trade.get("shadow_trade_id") == "bad-snapshot":
                    raise ValueError("incomplete_trade")
                return original_snapshot(trade)

            with patch("services.mt5.mt5_trade_memory.build_trade_memory_snapshot", side_effect=flaky_snapshot):
                learning = mt5_learning_run({"symbol": "BTCUSD", "timeframe": "H1"}, memory=store)

            self.assertTrue(learning["ok"])
            self.assertGreaterEqual(learning["trades_processed"], 1)
            self.assertTrue(learning["errors"])
            self.assertIn("incomplete_trade", learning["errors"][0]["error"])
            self.assertFalse(learning["broker_touched"])
            self.assertFalse(learning["order_executed"])

    def test_memory_summary_empty_many_and_no_backfill(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", _LEARNING_ENABLED_ENV):
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            empty = mt5_memory_summary(memory=store, symbol="BTCUSD", limit=50)
            self.assertTrue(empty["ok"])
            self.assertEqual(empty["total_memories"], 0)
            self.assertLess(empty["duration_ms"], 5000)

            journal = MT5Journal(memory=store)
            for index in range(150):
                journal.save(
                    "mt5_trade_memory",
                    "BTCUSD",
                    {
                        "trade_id": f"memory-{index}",
                        "symbol": "BTCUSD",
                        "normalized_symbol": "BTCUSD",
                        "timeframe": "H1",
                        "strategy_profile": "BTCUSD_PAPER_EXPLORATION_V1",
                        "status": "win",
                        "r_multiple": 1.0,
                        "pnl": 100,
                        "regime": "bull_trend",
                        "broker_touched": False,
                        "order_executed": False,
                    },
                )

            with patch("services.mt5.mt5_trade_memory.MT5TradeMemoryEngine.run_learning", side_effect=AssertionError("memory summary must be read-only")):
                limited = mt5_memory_summary(memory=store, symbol="BTCUSD", limit=50)

            self.assertTrue(limited["ok"])
            self.assertEqual(limited["limit"], 50)
            self.assertLessEqual(limited["total_memories"], 50)
            self.assertFalse(limited["broker_touched"])
            self.assertFalse(limited["order_executed"])

    def test_adaptive_state_detects_loss_streak_and_hot_streak(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", _LEARNING_ENABLED_ENV):
            loss_store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "loss.sqlite3")
            for index in range(3):
                _save_closed_shadow_trade(loss_store, f"loss-{index}", status="loss", r_multiple=-1.0, pnl=-1000, exit_reason="stop_loss", opened_minute=index)
            mt5_learning_run({"symbol": "BTCUSD", "timeframe": "H1"}, memory=loss_store)
            loss_state = mt5_adaptive_state(memory=loss_store, symbol="BTCUSD", timeframe="H1")

            hot_store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "hot.sqlite3")
            for index in range(31):
                _save_closed_shadow_trade(hot_store, f"win-{index}", status="win", r_multiple=1.0, pnl=1000, exit_reason="take_profit", opened_minute=index)
            mt5_learning_run({"symbol": "BTCUSD", "timeframe": "H1", "max_trades": 50}, memory=hot_store)
            hot_state = mt5_adaptive_state(memory=hot_store, symbol="BTCUSD", timeframe="H1")

            self.assertEqual(loss_state["current_loss_streak"], 3)
            self.assertEqual(loss_state["bot_state"], "drawdown_defense")
            self.assertGreaterEqual(hot_state["current_win_streak"], 30)
            self.assertIn(hot_state["bot_state"], {"hot_streak", "normal"})
            self.assertFalse(loss_state["broker_touched"])
            self.assertFalse(hot_state["order_executed"])

    def test_adaptive_recommendations_do_not_promote_small_sample(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", _LEARNING_ENABLED_ENV):
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            for index in range(5):
                _save_closed_shadow_trade(store, f"sample-{index}", status="win", r_multiple=1.0, pnl=1000, exit_reason="take_profit", opened_minute=index)

            mt5_learning_run({"symbol": "BTCUSD", "timeframe": "H1"}, memory=store)
            recommendations = mt5_adaptive_recommendations(memory=store, symbol="BTCUSD", timeframe="H1")

            self.assertTrue(recommendations["ok"])
            self.assertTrue(any(item["recommendation_type"] == "sample_warning" for item in recommendations["recommendations"]))
            self.assertTrue(all(item["requires_approval"] for item in recommendations["recommendations"]))
            self.assertTrue(all(item["applied"] is False for item in recommendations["recommendations"]))
            self.assertFalse(recommendations["broker_touched"])
            self.assertFalse(recommendations["order_executed"])

    def test_adaptive_recommendations_use_latest_state_and_profile_stats(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            journal = MT5Journal(memory=store)
            journal.save(
                "mt5_adaptive_state",
                "BTCUSD",
                {
                    "symbol": "BTCUSD",
                    "normalized_symbol": "BTCUSD",
                    "timeframe": "H1",
                    "bot_state": "normal",
                    "closed_trades": 16,
                    "current_win_streak": 1,
                    "current_loss_streak": 0,
                    "rolling_win_rate": 68.75,
                    "rolling_profit_factor": 1.7,
                    "rolling_expectancy": 0.0169,
                    "rolling_drawdown": 0.25,
                    "broker_touched": False,
                    "order_executed": False,
                    "order_policy": "journal_only_no_broker",
                },
            )
            journal.save(
                "mt5_strategy_profile_stats",
                "BTCUSD",
                {
                    "symbol": "BTCUSD",
                    "normalized_symbol": "BTCUSD",
                    "timeframe": "H1",
                    "strategy_profile": "BTCUSD_PAPER_EXPLORATION_V1",
                    "regime": "mixed",
                    "trades": 16,
                    "wins": 11,
                    "losses": 5,
                    "broker_touched": False,
                    "order_executed": False,
                    "order_policy": "journal_only_no_broker",
                },
            )

            recommendations = mt5_adaptive_recommendations(memory=store, symbol="BTCUSD", timeframe="H1")

            self.assertEqual(recommendations["closed_trades"], 16)
            self.assertEqual(recommendations["profile_stats_count"], 1)
            self.assertEqual(recommendations["data_source_used"], "adaptive_state")
            self.assertEqual(recommendations["current_loss_streak"], 0)
            self.assertEqual(recommendations["bot_state"], "normal")
            self.assertTrue(any("16 trades cerrados" in item["reason"] for item in recommendations["recommendations"]))
            self.assertTrue(any(item["recommendation_type"] == "sample_warning" for item in recommendations["recommendations"]))
            self.assertFalse(any(item["recommendation_type"] == "risk_adjustment" and "Racha de 3" in item["reason"] for item in recommendations["recommendations"]))
            self.assertFalse(recommendations["broker_touched"])
            self.assertFalse(recommendations["order_executed"])

    def test_paper_defense_activates_from_bad_adaptive_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            _save_adaptive_state(
                store,
                closed_trades=31,
                bot_state="caution",
                rolling_profit_factor=0.749,
                rolling_expectancy=-0.0106,
                rolling_drawdown=0.4424,
                last_10_win_rate=50.0,
                current_loss_streak=0,
            )

            defense = mt5_paper_defense(memory=store, symbol="BTCUSD")
            recommendations = mt5_adaptive_recommendations(memory=store, symbol="BTCUSD", timeframe="H1")

            self.assertTrue(defense["caution_mode_active"])
            self.assertTrue(defense["paper_defense_active"])
            self.assertIn("rolling_pf_below_1", defense["reasons"])
            self.assertIn("negative_expectancy", defense["reasons"])
            self.assertIn("high_drawdown", defense["reasons"])
            self.assertIn("low_recent_win_rate", defense["reasons"])
            self.assertTrue(any("Estado caution" in item["recommendation"] for item in recommendations["recommendations"]))
            self.assertFalse(defense["broker_touched"])
            self.assertFalse(defense["order_executed"])

    def test_paper_defense_blocks_low_score_and_allows_high_score(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            _save_adaptive_state(
                store,
                closed_trades=31,
                bot_state="caution",
                rolling_profit_factor=0.8,
                rolling_expectancy=-0.02,
                rolling_drawdown=0.2,
                last_10_win_rate=55.0,
            )
            defense = MT5PaperDefense(memory=store)

            blocked = defense.evaluate_new_entry(
                symbol="BTCUSD",
                tick={"symbol": "BTCUSD", "spread": 10, "last": 77000},
                market_scores={"score": 58, "trend_score": 50, "momentum_score": 52, "volatility_score": 50, "regime": "not_confirmed"},
                decision={"decision": "BUY"},
                max_spread_points=50,
            )
            allowed = defense.evaluate_new_entry(
                symbol="BTCUSD",
                tick={"symbol": "BTCUSD", "spread": 10, "last": 77000},
                market_scores={"score": 78, "trend_score": 72, "momentum_score": 70, "volatility_score": 60, "regime": "bullish_exploration"},
                decision={"decision": "BUY"},
                max_spread_points=50,
            )
            status = mt5_paper_defense(memory=store, symbol="BTCUSD")

            self.assertFalse(blocked["allowed"])
            self.assertIn("score_too_low", blocked["block_reasons"])
            self.assertIn("trend_not_confirmed", blocked["block_reasons"])
            self.assertTrue(allowed["allowed"])
            self.assertGreaterEqual(status["blocked_count"], 1)
            self.assertGreaterEqual(status["allowed_count"], 1)
            self.assertFalse(blocked["broker_touched"])
            self.assertFalse(allowed["order_executed"])

    def test_genesis_chat_routes_mt5_questions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            self.assertEqual(AgentRouter().route("estado de MT5").intent, "mt5_bridge")
            self.assertEqual(AgentRouter().route("que decision tiene MT5 para BTC").intent, "mt5_bridge")

            status = route_message("estado de MT5", memory=store)
            decision = route_message("que decision tiene MT5 para BTC", memory=store)
            learning = route_message("que aprendio MT5 de BTCUSD", memory=store)

            self.assertTrue(status["ok"])
            self.assertEqual(status["intent"], "mt5_bridge")
            self.assertIn("mt5", status)
            self.assertTrue(decision["ok"])
            self.assertEqual(decision["intent"], "mt5_bridge")
            self.assertIn("order_executed=false", decision["answer"])
            self.assertTrue(learning["ok"])
            self.assertEqual(learning["intent"], "mt5_bridge")
            self.assertIn("Memoria adaptativa MT5", learning["answer"])
            self.assertIn("broker_touched=false", learning["answer"])

    def test_ea_file_exists_with_safety_defaults(self) -> None:
        ea = Path("mt5") / "GenesisBridgeEA.mq5"
        content = ea.read_text(encoding="utf-8")

        self.assertIn("input bool AllowLiveTrading = false", content)
        self.assertIn("input bool JournalOnly = true", content)
        self.assertIn("input bool KillSwitch = false", content)
        self.assertIn("input bool EnableTickPost = true", content)
        self.assertIn("input bool EnableSignalPost = false", content)
        self.assertIn("input bool EnableAccountSync = true", content)
        self.assertIn("input bool EnableDecisionPoll = true", content)
        self.assertIn("GenesisBridgeEA_v11_9_FORCE_TICK", content)
        self.assertIn("AllowedSymbols = \"BTC,BTCUSD", content)
        self.assertIn("bool SendTick()", content)
        self.assertIn("MT5 SendTick start", content)
        self.assertIn("MT5 SendTick JSON=", content)
        self.assertIn("MT5 SendTick HTTP=", content)
        self.assertIn("MT5 SendTick response=", content)
        self.assertIn("SendAccountSync()", content)
        self.assertIn("/api/genesis/mt5/tick", content)
        self.assertIn("/api/genesis/mt5/signal", content)
        self.assertIn("Content-Type: application/json", content)
        self.assertIn("StringToUtf8Body", content)
        self.assertIn("JsonToCharArray", content)
        self.assertIn("int PostJson(string path, string json, string &response)", content)
        self.assertIn("int GetJson(string path, string &response)", content)
        self.assertIn("ea_version", content)
        self.assertIn("EA version", content)
        self.assertIn("LastTickTime", content)
        self.assertIn("LastSignalHttpCode", content)
        self.assertIn("LastTickHttpCode", content)
        self.assertIn("LastTickError", content)
        self.assertIn("LastDecisionHttpCode", content)
        self.assertIn("LastAccountSyncHttpCode", content)
        self.assertIn("DemoOnly", content)
        self.assertIn("WebRequest", content)
        self.assertNotIn("FMP_API_KEY", content)
        self.assertNotIn("OPENAI_API_KEY", content)

    def test_mt5_history_export_scripts_are_read_only(self) -> None:
        exporter = Path("scripts") / "export_mt5_history.py"
        runner = Path("scripts") / "run_backtest_from_csv.ps1"
        forward_replay_runner = Path("scripts") / "run_forward_replay_from_csv.ps1"
        exporter_content = exporter.read_text(encoding="utf-8")
        runner_content = runner.read_text(encoding="utf-8")
        forward_replay_content = forward_replay_runner.read_text(encoding="utf-8")

        self.assertIn("copy_rates_from_pos", exporter_content)
        self.assertIn('"time", "open", "high", "low", "close", "volume"', exporter_content)
        self.assertIn("data/backtests/BTCUSD_H1.csv", exporter_content)
        self.assertIn("/api/genesis/mt5/backtest/run", runner_content)
        self.assertIn("/api/genesis/mt5/forward-replay/run", forward_replay_content)
        self.assertIn("data/backtests/BTCUSD_M30_5000.csv", forward_replay_content)
        self.assertIn("checkpoints", forward_replay_content)
        self.assertIn("[int]$TimeoutSec = 60", forward_replay_content)
        self.assertIn("-TimeoutSec $TimeoutSec -ErrorAction Stop", forward_replay_content)
        self.assertIn("CSV size bytes", forward_replay_content)
        self.assertIn("CSV lines", forward_replay_content)
        self.assertIn("Max bars", forward_replay_content)
        self.assertIn("Timeout sec", forward_replay_content)
        self.assertIn("save_results    = $true", runner_content)
        self.assertIn("broker_touched", runner_content)
        self.assertIn("order_executed", runner_content)
        self.assertIn("broker_touched", forward_replay_content)
        self.assertIn("order_executed", forward_replay_content)
        combined = f"{exporter_content}\n{runner_content}\n{forward_replay_content}".casefold()
        self.assertNotIn("order_send", combined)
        self.assertNotIn("ordersend", combined)
        self.assertNotIn("mt5.login", combined)
        self.assertNotIn("password", combined)
        self.assertNotIn("api_key", combined)

    def test_mt5_backtest_endpoint_is_paper_only_and_exposes_latest(self) -> None:
        result = post_genesis_mt5_backtest_run(
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "bars_data": _backtest_buy_take_profit_bars(),
                "filter_profile": "baseline",
                "spread_points": 0,
                "slippage_points": 0,
                "commission": 0,
                "save_results": False,
            }
        )
        latest = get_genesis_mt5_backtest_latest(symbol="BTCUSD")

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "mt5_backtest_completed")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")
        self.assertGreaterEqual(result["total_trades"], 1)
        self.assertEqual(latest["result"]["symbol"], "BTCUSD")
        self.assertFalse(latest["broker_touched"])
        self.assertFalse(latest["order_executed"])

    def test_mt5_forward_replay_endpoint_is_isolated_and_paper_only(self) -> None:
        from services.mt5.mt5_runtime_snapshot import get_snapshot, reset_runtime_snapshots_for_tests

        reset_runtime_snapshots_for_tests()
        result = post_genesis_mt5_forward_replay_run(
            {
                "symbol": "BTCUSD",
                "timeframe": "M30",
                "profile": "quality_loose",
                "bars_data": _forward_replay_loss_bars(160),
                "spread_points": 0,
                "slippage_points": 0,
                "commission": 0,
                "checkpoints": [10, 25, 50, 100],
                "persist": False,
            }
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["api_status"], "mt5_forward_replay_completed")
        self.assertEqual(result["status"], "observation_only")
        self.assertEqual(result["profile"], "quality_loose")
        self.assertEqual(result["timeframe"], "M30")
        self.assertEqual(result["closed"], 10)
        self.assertTrue(result["degraded"])
        self.assertEqual(result["degradation_reason"], "early_forward_underperformance")
        self.assertTrue(result["checkpoints"][0]["reached"])
        self.assertTrue(result["checkpoints"][0]["degraded"])
        self.assertIn("stop_loss", result["exit_reason_counts"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")
        self.assertFalse(result["live_runtime_mutated"])
        self.assertFalse(result["promoted_profile_mutated"])
        self.assertFalse(result["shadow_trades_mutated"])
        self.assertIsNone(get_snapshot("BTCUSD", "M30"))

    def test_mt5_forward_replay_accepts_csv_text_and_defaults_to_no_persist(self) -> None:
        csv_text = _bars_to_csv(_forward_replay_loss_bars(80))
        result = mt5_forward_replay_run(
            {
                "symbol": "BTCUSD",
                "timeframe": "M30",
                "profile": "quality_loose",
                "csv_text": csv_text,
                "spread_points": 0,
                "slippage_points": 0,
                "checkpoints": [10],
            }
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["api_status"], "mt5_forward_replay_completed")
        self.assertGreaterEqual(result["bars_loaded"], 10)
        self.assertFalse(result["persist"])
        self.assertFalse(result["saved"])
        self.assertIn("recent_trades", result)
        self.assertIn("buy_win_rate", result)
        self.assertIn("sell_pf", result)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_mt5_forward_replay_timeout_guard_returns_json(self) -> None:
        with patch("services.mt5.mt5_forward_replay._timed_out", return_value=True):
            result = mt5_forward_replay_run(
                {
                    "symbol": "BTCUSD",
                    "timeframe": "M30",
                    "profile": "quality_loose",
                    "bars_data": _forward_replay_loss_bars(80),
                    "spread_points": 0,
                    "slippage_points": 0,
                    "checkpoints": [10],
                }
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "forward_replay_timeout_or_loop_guard")
        self.assertEqual(result["guard_reason"], "timeout_guard")
        self.assertGreaterEqual(result["max_iterations"], result["bars_loaded"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_mt5_backtest_buy_take_profit_calculates_pnl(self) -> None:
        result = mt5_backtest_run(
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "bars_data": _backtest_buy_take_profit_bars(),
                "filter_profile": "baseline",
                "spread_points": 0,
                "slippage_points": 0,
            }
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["wins"], 1)
        self.assertEqual(result["losses"], 0)
        self.assertEqual(result["exit_reason_counts"].get("take_profit"), 1)
        self.assertGreater(result["net_pnl"], 0)
        self.assertGreaterEqual(result["profit_factor"], 1.0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_mt5_backtest_sell_take_profit_calculates_pnl(self) -> None:
        result = mt5_backtest_run(
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "bars_data": _backtest_sell_take_profit_bars(),
                "filter_profile": "baseline",
                "spread_points": 0,
                "slippage_points": 0,
            }
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["wins"], 1)
        self.assertEqual(result["losses"], 0)
        self.assertEqual(result["exit_reason_counts"].get("take_profit"), 1)
        self.assertEqual(result["recent_trades"][0]["side"], "sell")
        self.assertGreater(result["net_pnl"], 0)

    def test_mt5_backtest_closes_by_stop_loss(self) -> None:
        result = mt5_backtest_run(
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "bars_data": _backtest_buy_stop_loss_bars(),
                "filter_profile": "baseline",
                "spread_points": 0,
                "slippage_points": 0,
            }
        )

        self.assertEqual(result["losses"], 1)
        self.assertEqual(result["exit_reason_counts"].get("stop_loss"), 1)
        self.assertLess(result["net_pnl"], 0)
        self.assertFalse(result["order_executed"])

    def test_mt5_backtest_closes_by_time_stop(self) -> None:
        result = mt5_backtest_run(
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "bars_data": _backtest_time_stop_bars(),
                "filter_profile": "baseline",
                "spread_points": 0,
                "slippage_points": 0,
                "time_stop_min": 60,
            }
        )

        self.assertEqual(result["closed"], 1)
        self.assertEqual(result["exit_reason_counts"].get("time_stop"), 1)
        self.assertIn(result["recent_trades"][0]["status"], {"win", "loss", "breakeven"})

    def test_mt5_backtest_metrics_include_profit_factor_drawdown_and_sides(self) -> None:
        bars = _backtest_buy_take_profit_bars() + _backtest_sell_take_profit_bars()[1:] + _backtest_buy_stop_loss_bars()[1:]
        result = MT5Backtester().run({"symbol": "BTCUSD", "timeframe": "H1", "bars_data": bars, "filter_profile": "baseline", "spread_points": 0})

        self.assertTrue(result["ok"])
        self.assertIn("profit_factor", result)
        self.assertIn("max_drawdown", result)
        self.assertIn("buy_win_rate", result)
        self.assertIn("sell_win_rate", result)
        self.assertIn("side_stats", result)
        self.assertIn("regime_stats", result)
        self.assertIn("hour_stats", result)
        self.assertIn("trades_by_hour", result)
        self.assertIn("trades_by_regime", result)
        self.assertIsInstance(result["equity_curve"], list)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_mt5_backtest_quality_v2_blocks_sell_with_extreme_oversold_rsi(self) -> None:
        result = mt5_backtest_run(
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "filter_profile": "quality_v2",
                "bars_data": _backtest_oversold_sell_bars(),
                "spread_points": 0,
            }
        )

        self.assertEqual(result["total_trades"], 0)
        self.assertGreaterEqual(result["blocked_reason_counts"].get("rsi_extreme_block", 0), 1)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_mt5_backtest_quality_v2_blocks_buy_with_extreme_overbought_rsi(self) -> None:
        result = mt5_backtest_run(
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "filter_profile": "quality_v2",
                "bars_data": _backtest_overbought_buy_bars(),
                "spread_points": 0,
            }
        )

        self.assertEqual(result["total_trades"], 0)
        self.assertGreaterEqual(result["blocked_reason_counts"].get("rsi_extreme_block", 0), 1)

    def test_mt5_backtest_quality_v2_blocks_weak_internal_scores_and_score_inflation(self) -> None:
        result = mt5_backtest_run(
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "filter_profile": "quality_v2",
                "bars_data": _backtest_weak_internal_high_vol_bars(),
                "spread_points": 0,
            }
        )

        self.assertEqual(result["total_trades"], 0)
        self.assertGreaterEqual(result["weak_internal_scores_count"], 1)
        self.assertGreaterEqual(result["blocked_reason_counts"].get("weak_internal_scores", 0), 1)

    def test_mt5_backtest_filter_profiles_return_comparable_metrics(self) -> None:
        result = mt5_backtest_run(
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "filter_profile": "quality_v2",
                "bars_data": _backtest_weak_internal_high_vol_bars(),
                "spread_points": 0,
            }
        )
        comparison = result["filter_comparison"]

        self.assertIn("baseline_pf", comparison)
        self.assertIn("quality_v2_pf", comparison)
        self.assertIn("baseline_drawdown", comparison)
        self.assertIn("quality_v2_drawdown", comparison)
        self.assertIn("baseline_trades", comparison)
        self.assertIn("quality_v2_trades", comparison)
        self.assertGreaterEqual(comparison["baseline_trades"], comparison["quality_v2_trades"])

    def test_mt5_backtest_avoids_first_bar_lookahead(self) -> None:
        result = mt5_backtest_run(
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "bars_data": [
                    {"time": "2026-01-01T00:00:00+00:00", "open": 100, "high": 200, "low": 100, "close": 100},
                    {"time": "2026-01-01T01:00:00+00:00", "open": 100, "high": 100, "low": 100, "close": 100},
                    {"time": "2026-01-01T02:00:00+00:00", "open": 100, "high": 100, "low": 100, "close": 100},
                    {"time": "2026-01-01T03:00:00+00:00", "open": 100, "high": 100, "low": 100, "close": 100},
                ],
                "spread_points": 0,
            }
        )

        self.assertEqual(result["total_trades"], 0)
        self.assertGreaterEqual(result["no_trade_count"], 1)
        self.assertNotIn("take_profit", result["exit_reason_counts"])

    def test_mt5_backtest_walk_forward_and_fast_path_remain_separate(self) -> None:
        with patch.dict(os.environ, {"MT5_FAST_PATH_ONLY": "true", "MT5_PAPER_EXPLORATION_ENABLED": "false"}, clear=False):
            backtest = mt5_backtest_run(
                {
                    "symbol": "BTCUSD",
                    "timeframe": "H1",
                    "bars_data": _backtest_buy_take_profit_bars() * 4,
                    "filter_profile": "baseline",
                    "walk_forward": True,
                    "train_months": 1,
                    "test_months": 1,
                    "spread_points": 0,
                }
            )
            tick = mt5_tick({"symbol": "BTCUSD", "bid": 100, "ask": 100.1, "last": 100.05, "timeframe": "H1", "source": "unit"})
            decision = mt5_decision("BTCUSD")

        self.assertTrue(backtest["ok"])
        self.assertTrue(backtest["walk_forward"])
        self.assertIn("walk_forward_results", backtest)
        self.assertTrue(tick["ok"])
        self.assertTrue(decision["ok"])
        self.assertFalse(tick["broker_touched"])
        self.assertFalse(decision["order_executed"])

    def test_mt5_backtest_optimize_runs_profiles_without_broker(self) -> None:
        result = mt5_backtest_optimize(
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "bars_data": _backtest_optimizer_bars(90),
                "profiles": ["baseline", "quality_v2", "quality_loose"],
                "walk_forward": True,
                "spread_points": 0,
                "slippage_points": 0,
            }
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "mt5_backtest_optimize_completed")
        self.assertEqual(len(result["ranking"]), 3)
        self.assertIn("table_markdown", result)
        for row in result["ranking"]:
            self.assertIn("test_profit_factor", row)
            self.assertIn("test_expectancy", row)
            self.assertIn("robustness_score", row)
            self.assertFalse(row["broker_touched"])
            self.assertFalse(row["order_executed"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_mt5_backtest_walk_forward_reports_train_and_test_metrics(self) -> None:
        result = mt5_backtest_run(
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "bars_data": _backtest_optimizer_bars(80),
                "filter_profile": "baseline",
                "walk_forward": True,
                "train_ratio": 0.6,
                "spread_points": 0,
            }
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["walk_forward"])
        self.assertIn("train_pf", result)
        self.assertIn("test_pf", result)
        self.assertIn("train_expectancy", result)
        self.assertIn("test_expectancy", result)
        self.assertGreater(result["train_bars"], 0)
        self.assertGreater(result["test_bars"], 0)

    def test_mt5_backtest_optimizer_does_not_promote_weak_or_small_samples(self) -> None:
        result = post_genesis_mt5_backtest_optimize(
            {
                "symbol": "BTCUSD",
                "timeframe": "M30",
                "bars_data": _backtest_buy_take_profit_bars() * 4,
                "profiles": ["baseline", "quality_v2"],
                "walk_forward": True,
                "spread_points": 0,
            }
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["timeframe"], "M30")
        for row in result["ranking"]:
            self.assertFalse(row["promoted"])
            self.assertIn("test_trades_below_50", row["promotion_reasons"])

    def test_mt5_backtest_optimizer_rejects_low_test_pf(self) -> None:
        result = mt5_backtest_optimize(
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "bars_data": _backtest_buy_stop_loss_bars() * 30,
                "profiles": ["baseline", "quality_v2"],
                "walk_forward": True,
                "spread_points": 0,
            }
        )

        self.assertTrue(result["ok"])
        low_pf_rows = [row for row in result["ranking"] if row["test_profit_factor"] < 1.25]
        self.assertTrue(low_pf_rows)
        self.assertTrue(all(not row["promoted"] for row in low_pf_rows))

    def test_mt5_backtest_optimizer_script_exists_and_is_paper_only(self) -> None:
        content = Path("scripts/run_backtest_optimize_from_csv.ps1").read_text(encoding="utf-8")

        self.assertIn("/api/genesis/mt5/backtest/optimize", content)
        self.assertIn("profiles", content)
        self.assertIn("save_results", content)
        self.assertNotIn("order_send", content.lower())
        self.assertNotIn("OrderSend", content)


def _backtest_optimizer_bars(count: int = 120) -> list[dict[str, object]]:
    bars: list[dict[str, object]] = []
    price = 100.0
    for index in range(count):
        wave = (index % 18) - 9
        drift = 0.18 if (index // 18) % 2 == 0 else -0.12
        open_price = price
        close = max(20.0, open_price + drift + wave * 0.03)
        high = max(open_price, close) + 1.2
        low = min(open_price, close) - 1.2
        bars.append(
            {
                "time": f"2026-06-{(index // 24) + 1:02d}T{index % 24:02d}:00:00+00:00",
                "open": round(open_price, 6),
                "high": round(high, 6),
                "low": round(low, 6),
                "close": round(close, 6),
                "volume": 1000 + index,
            }
        )
        price = close
    return bars


def _forward_replay_loss_bars(count: int = 160) -> list[dict[str, object]]:
    bars: list[dict[str, object]] = []
    price = 100.0
    for index in range(count):
        close = price + 1.0 if index % 5 < 4 else price - 4.0
        open_price = price
        bars.append(
            {
                "time": f"2026-07-{(index // 48) + 1:02d}T{(index % 48) // 2:02d}:{'30' if index % 2 else '00'}:00+00:00",
                "open": round(open_price, 6),
                "high": round(max(open_price, close) + 0.5, 6),
                "low": round(min(open_price, close) - 2.0, 6),
                "close": round(close, 6),
                "volume": 1000 + index,
            }
        )
        price = close
    return bars


def _bars_to_csv(bars: list[dict[str, object]]) -> str:
    lines = ["time,open,high,low,close,volume"]
    for bar in bars:
        lines.append(
            ",".join(
                [
                    str(bar.get("time") or ""),
                    str(bar.get("open") or ""),
                    str(bar.get("high") or ""),
                    str(bar.get("low") or ""),
                    str(bar.get("close") or ""),
                    str(bar.get("volume") or 0),
                ]
            )
        )
    return "\n".join(lines)


def _backtest_buy_take_profit_bars() -> list[dict[str, object]]:
    return [
        {"time": "2026-01-01T00:00:00+00:00", "open": 100.0, "high": 100.5, "low": 99.5, "close": 100.0},
        {"time": "2026-01-01T01:00:00+00:00", "open": 100.5, "high": 101.5, "low": 100.2, "close": 101.0},
        {"time": "2026-01-01T02:00:00+00:00", "open": 101.5, "high": 102.5, "low": 101.2, "close": 102.0},
        {"time": "2026-01-01T03:00:00+00:00", "open": 103.0, "high": 103.4, "low": 102.8, "close": 103.0},
        {"time": "2026-01-01T04:00:00+00:00", "open": 103.2, "high": 105.8, "low": 103.0, "close": 105.2},
    ]


def _backtest_sell_take_profit_bars() -> list[dict[str, object]]:
    return [
        {"time": "2026-02-01T00:00:00+00:00", "open": 100.0, "high": 100.5, "low": 99.5, "close": 100.0},
        {"time": "2026-02-01T01:00:00+00:00", "open": 99.5, "high": 99.8, "low": 98.8, "close": 99.0},
        {"time": "2026-02-01T02:00:00+00:00", "open": 98.5, "high": 98.8, "low": 97.8, "close": 98.0},
        {"time": "2026-02-01T03:00:00+00:00", "open": 97.0, "high": 97.2, "low": 96.8, "close": 97.0},
        {"time": "2026-02-01T04:00:00+00:00", "open": 96.8, "high": 97.1, "low": 94.8, "close": 95.2},
    ]


def _backtest_buy_stop_loss_bars() -> list[dict[str, object]]:
    return [
        {"time": "2026-03-01T00:00:00+00:00", "open": 100.0, "high": 100.5, "low": 99.5, "close": 100.0},
        {"time": "2026-03-01T01:00:00+00:00", "open": 100.5, "high": 101.5, "low": 100.2, "close": 101.0},
        {"time": "2026-03-01T02:00:00+00:00", "open": 101.5, "high": 102.5, "low": 101.2, "close": 102.0},
        {"time": "2026-03-01T03:00:00+00:00", "open": 103.0, "high": 103.4, "low": 102.8, "close": 103.0},
        {"time": "2026-03-01T04:00:00+00:00", "open": 102.8, "high": 103.0, "low": 101.0, "close": 101.2},
    ]


def _backtest_time_stop_bars() -> list[dict[str, object]]:
    return [
        {"time": "2026-04-01T00:00:00+00:00", "open": 100.0, "high": 100.5, "low": 99.5, "close": 100.0},
        {"time": "2026-04-01T01:00:00+00:00", "open": 100.5, "high": 101.5, "low": 100.2, "close": 101.0},
        {"time": "2026-04-01T02:00:00+00:00", "open": 101.5, "high": 102.5, "low": 101.2, "close": 102.0},
        {"time": "2026-04-01T03:00:00+00:00", "open": 103.0, "high": 103.4, "low": 102.8, "close": 103.0},
        {"time": "2026-04-01T04:00:00+00:00", "open": 103.1, "high": 103.6, "low": 102.5, "close": 103.3},
    ]


def _backtest_overbought_buy_bars() -> list[dict[str, object]]:
    return [
        {"time": "2026-05-01T00:00:00+00:00", "open": 100.0, "high": 100.5, "low": 99.8, "close": 100.0},
        {"time": "2026-05-01T01:00:00+00:00", "open": 101.0, "high": 101.5, "low": 100.8, "close": 101.0},
        {"time": "2026-05-01T02:00:00+00:00", "open": 102.0, "high": 102.5, "low": 101.8, "close": 102.0},
        {"time": "2026-05-01T03:00:00+00:00", "open": 103.0, "high": 103.5, "low": 102.8, "close": 103.0},
        {"time": "2026-05-01T04:00:00+00:00", "open": 103.2, "high": 104.0, "low": 102.8, "close": 103.5},
    ]


def _backtest_oversold_sell_bars() -> list[dict[str, object]]:
    return [
        {"time": "2026-06-01T00:00:00+00:00", "open": 100.0, "high": 100.2, "low": 99.5, "close": 100.0},
        {"time": "2026-06-01T01:00:00+00:00", "open": 99.0, "high": 99.2, "low": 98.5, "close": 99.0},
        {"time": "2026-06-01T02:00:00+00:00", "open": 98.0, "high": 98.2, "low": 97.5, "close": 98.0},
        {"time": "2026-06-01T03:00:00+00:00", "open": 97.0, "high": 97.2, "low": 96.5, "close": 97.0},
        {"time": "2026-06-01T04:00:00+00:00", "open": 96.8, "high": 97.1, "low": 96.0, "close": 96.5},
    ]


def _backtest_weak_internal_high_vol_bars() -> list[dict[str, object]]:
    return [
        {"time": "2026-07-01T00:00:00+00:00", "open": 100.0, "high": 110.0, "low": 90.0, "close": 100.0},
        {"time": "2026-07-01T01:00:00+00:00", "open": 101.0, "high": 112.0, "low": 91.0, "close": 101.0},
        {"time": "2026-07-01T02:00:00+00:00", "open": 99.0, "high": 113.0, "low": 90.0, "close": 99.0},
        {"time": "2026-07-01T03:00:00+00:00", "open": 98.0, "high": 115.0, "low": 89.0, "close": 98.0},
        {"time": "2026-07-01T04:00:00+00:00", "open": 98.5, "high": 100.0, "low": 96.0, "close": 97.5},
    ]


def _save_closed_shadow_trade(
    store: MemoryStore,
    trade_id: str,
    *,
    status: str,
    r_multiple: float,
    pnl: float,
    exit_reason: str,
    confidence: str = "high",
    regime: str = "bull_trend",
    opened_minute: int = 0,
) -> None:
    opened_at = f"2026-01-01T00:{opened_minute % 60:02d}:00+00:00"
    closed_at = f"2026-01-01T01:{opened_minute % 60:02d}:00+00:00"
    action = "BUY"
    entry = 100.0
    stop = 98.0
    target = 102.4
    exit_price = target if status == "win" else stop if status == "loss" else entry
    MT5Journal(memory=store).save(
        "mt5_shadow_trades",
        "BTCUSD",
        {
            "shadow_trade_id": trade_id,
            "symbol": "BTCUSD",
            "original_symbol": "BTCUSD",
            "normalized_symbol": "BTCUSD",
            "instrument_type": "crypto_spot",
            "is_spot_crypto": True,
            "action": action,
            "entry": entry,
            "stop_loss": stop,
            "take_profit": target,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "last_exit_reason": exit_reason,
            "status": status,
            "lifecycle_status": "closed",
            "timeframe": "H1",
            "pnl": pnl,
            "pnl_pct": round((pnl / entry) * 100, 4),
            "r_multiple": r_multiple,
            "opened_at": opened_at,
            "closed_at": closed_at,
            "updated_at": closed_at,
            "bars_open": 3,
            "spread_at_entry": 8.0,
            "spread_at_exit": 7.0,
            "trend_score": 70,
            "momentum_score": 65,
            "volatility_score": 55,
            "regime": regime,
            "confidence": confidence,
            "decision_reason": "unit test closed trade",
            "strategy_profile": "BTCUSD_PAPER_EXPLORATION_V1",
            "source": "mt5_auto_forward_exploration",
            "auto_forward": True,
            "paper_exploration": True,
            "manual_test": False,
            "excluded_from_main_metrics": False,
            "risk_reward": 1.2,
            "initial_risk": 2.0,
            "breakeven_armed": r_multiple >= 0.4,
            "trailing_stop_active": False,
            "virtual_stop_loss": entry if r_multiple >= 0.4 else stop,
            "max_favorable_excursion": max(pnl, 0),
            "max_adverse_excursion": min(pnl, 0),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        },
    )


def _save_open_shadow_trade(store: MemoryStore, trade_id: str, *, opened_minute: int = 30) -> None:
    opened_at = f"2026-01-01T00:{opened_minute % 60:02d}:00+00:00"
    MT5Journal(memory=store).save(
        "mt5_shadow_trades",
        "BTCUSD",
        {
            "shadow_trade_id": trade_id,
            "symbol": "BTCUSD",
            "original_symbol": "BTCUSD",
            "normalized_symbol": "BTCUSD",
            "instrument_type": "crypto_spot",
            "is_spot_crypto": True,
            "action": "BUY",
            "entry": 100.0,
            "stop_loss": 98.0,
            "take_profit": 102.4,
            "status": "open",
            "lifecycle_status": "open",
            "timeframe": "H1",
            "pnl": 0.0,
            "pnl_pct": 0.0,
            "r_multiple": 0.0,
            "opened_at": opened_at,
            "updated_at": opened_at,
            "strategy_profile": "BTCUSD_PAPER_EXPLORATION_V1",
            "source": "mt5_auto_forward_exploration",
            "auto_forward": True,
            "paper_exploration": True,
            "manual_test": False,
            "excluded_from_main_metrics": False,
            "risk_reward": 1.2,
            "initial_risk": 2.0,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        },
    )


def _save_adaptive_state(
    store: MemoryStore,
    *,
    closed_trades: int,
    bot_state: str,
    rolling_profit_factor: float,
    rolling_expectancy: float,
    rolling_drawdown: float,
    last_10_win_rate: float,
    current_loss_streak: int = 0,
) -> None:
    MT5Journal(memory=store).save(
        "mt5_adaptive_state",
        "BTCUSD",
        {
            "symbol": "BTCUSD",
            "normalized_symbol": "BTCUSD",
            "timeframe": "H1",
            "bot_state": bot_state,
            "closed_trades": closed_trades,
            "current_win_streak": 0,
            "current_loss_streak": current_loss_streak,
            "last_10_win_rate": last_10_win_rate,
            "last_20_win_rate": last_10_win_rate,
            "rolling_win_rate": last_10_win_rate,
            "rolling_profit_factor": rolling_profit_factor,
            "rolling_expectancy": rolling_expectancy,
            "rolling_drawdown": rolling_drawdown,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        },
    )


def _ea_signal_payload() -> dict[str, object]:
    return {
        "symbol": "BTCUSD",
        "source": "mt5_bridge",
        "event_type": "mt5_signal",
        "timeframe": "H1",
        "price": 77047.92,
        "message": "MT5 bridge signal",
        "payload": {
            "symbol": "BTCUSD",
            "timeframe": "H1",
            "price": 77047.92,
            "bid": 77042.0,
            "ask": 77053.84,
            "spread": 11.84,
            "account": "5050589561",
            "broker": "MetaQuotes-Demo",
            "server": "MetaQuotes-Demo",
            "is_demo": True,
            "symbol_description": "Bitcoin vs. USD",
            "symbol_path": "Crypto\\Bitcoin",
            "currency_base": "BTC",
            "currency_profit": "USD",
        },
    }


def _ea_tick_payload() -> dict[str, object]:
    return {
        "symbol": "BTCUSD",
        "bid": 77042.0,
        "ask": 77053.84,
        "last": 77047.92,
        "timeframe": "H1",
        "spread": 11.84,
        "account": "5050589561",
        "broker": "MetaQuotes-Demo",
        "server": "MetaQuotes-Demo",
        "is_demo": True,
        "source": "mt5_bridge",
        "ea_version": "GenesisBridgeEA_v11_9_FORCE_TICK",
        "symbol_description": "Bitcoin vs. USD",
        "symbol_path": "Crypto\\Bitcoin",
        "currency_base": "BTC",
        "currency_profit": "USD",
        "digits": 2,
        "point": 0.01,
        "contract_size": 1.0,
    }


def _auto_forward_router(store: MemoryStore) -> MT5SignalRouter:
    return MT5SignalRouter(
        memory=store,
        config=MT5BridgeConfig(
            enabled=True,
            demo_only=True,
            live_trading_enabled=False,
            order_execution_enabled=False,
            kill_switch=False,
            max_spread_points=50.0,
            min_rr=1.2,
            fast_path_only=False,
        ),
        symbol_mapper=MT5SymbolMapper(allowed_symbols=["BTC", "BTCUSD"]),
    )


def _managed_shadow_router(
    store: MemoryStore,
    *,
    shadow_time_stop_bars: int = 99,
    shadow_trail_start_r: float = 0.70,
) -> MT5SignalRouter:
    return MT5SignalRouter(
        memory=store,
        config=MT5BridgeConfig(
            enabled=True,
            demo_only=True,
            live_trading_enabled=False,
            order_execution_enabled=False,
            kill_switch=False,
            max_spread_points=50.0,
            min_rr=1.2,
            shadow_time_stop_hours=99.0,
            shadow_time_stop_bars=shadow_time_stop_bars,
            shadow_breakeven_r=0.40,
            shadow_trail_start_r=shadow_trail_start_r,
            shadow_trail_distance_r=0.30,
            shadow_signal_flip_close=True,
            fast_path_only=False,
        ),
        symbol_mapper=MT5SymbolMapper(allowed_symbols=["BTCUSD"]),
    )


def _exploration_router(store: MemoryStore) -> MT5SignalRouter:
    return MT5SignalRouter(
        memory=store,
        config=MT5BridgeConfig(
            enabled=True,
            demo_only=True,
            live_trading_enabled=False,
            order_execution_enabled=False,
            kill_switch=False,
            max_spread_points=50.0,
            min_rr=1.2,
            paper_exploration_enabled=True,
            fast_path_only=False,
        ),
        symbol_mapper=MT5SymbolMapper(allowed_symbols=["BTCUSD"]),
    )


def _buy_context() -> dict[str, object]:
    return {
        "ok": True,
        "decision": "BUY",
        "bias": "bullish",
        "confidence": "high",
        "reason": "auto forward buy test",
        "no_trade_score": 0,
        "hedge_score": 0,
        "genesis_context_score": 80,
        "technical_context": {"price": 100, "atr": 2.5},
        "stop_loss": 95,
        "take_profit": 110,
        "recommended_strategy_profile": "Auto Forward Test",
        "recommended_timeframe": "H1",
        "risk_flags": [],
        "what_to_watch": [],
    }


def _sell_context() -> dict[str, object]:
    return {
        "ok": True,
        "decision": "SELL",
        "bias": "bearish",
        "confidence": "high",
        "reason": "auto forward sell test",
        "no_trade_score": 0,
        "hedge_score": 0,
        "genesis_context_score": -55,
        "technical_context": {"price": 130, "atr": 3},
        "stop_loss": 136,
        "take_profit": 118,
        "recommended_strategy_profile": "Auto Forward Test",
        "recommended_timeframe": "H1",
    }


def _generated_buy_context() -> dict[str, object]:
    return {
        "ok": True,
        "decision": "BUY",
        "bias": "bullish",
        "confidence": "high",
        "reason": "auto forward generated buy",
        "no_trade_score": 0,
        "hedge_score": 0,
        "genesis_context_score": 80,
        "technical_context": {"atr": 2},
        "recommended_strategy_profile": "Generated Auto Forward Test",
        "recommended_timeframe": "H1",
        "risk_flags": [],
        "what_to_watch": [],
    }


def _generated_sell_context() -> dict[str, object]:
    return {
        "ok": True,
        "decision": "SELL",
        "bias": "bearish",
        "confidence": "high",
        "reason": "auto forward generated sell",
        "no_trade_score": 0,
        "hedge_score": 0,
        "genesis_context_score": -50,
        "technical_context": {},
        "recommended_strategy_profile": "Generated Auto Forward Test",
        "recommended_timeframe": "H1",
        "risk_flags": [],
        "what_to_watch": [],
    }


def _low_confidence_buy_context() -> dict[str, object]:
    return {
        "ok": True,
        "decision": "BUY",
        "bias": "bullish",
        "confidence": "low",
        "reason": "",
        "no_trade_score": 0,
        "hedge_score": 0,
        "genesis_context_score": 40,
        "technical_context": {},
        "recommended_strategy_profile": "Low Confidence Auto Forward Test",
        "recommended_timeframe": "H1",
    }


def _no_trade_context() -> dict[str, object]:
    return {
        "ok": True,
        "decision": "NO_TRADE",
        "bias": "neutral",
        "confidence": "low",
        "reason": "no edge in auto test",
        "no_trade_score": 90,
        "hedge_score": 0,
        "genesis_context_score": 0,
        "technical_context": {"price": 100, "atr": 2.5},
        "recommended_strategy_profile": "No Trade Test",
        "recommended_timeframe": "H1",
    }


def _paper_closed_trade(
    trade_id: str,
    *,
    status: str = "win",
    r_multiple: float = 1.0,
    side: str = "buy",
    exit_reason: str = "take_profit",
) -> dict[str, object]:
    pnl = r_multiple
    closed_at = datetime.now(timezone.utc).isoformat()
    return {
        "shadow_trade_id": trade_id,
        "symbol": "BTCUSD",
        "normalized_symbol": "BTCUSD",
        "instrument_type": "crypto_spot",
        "side": side,
        "action": side.upper(),
        "entry_price": 100.0,
        "stop_loss": 98.5 if side == "buy" else 101.5,
        "take_profit": 101.8 if side == "buy" else 98.2,
        "risk_reward": 1.2,
        "risk_pct": 0.1,
        "opened_at": "2026-05-19T00:00:00+00:00",
        "closed_at": closed_at,
        "status": status,
        "lifecycle_status": "closed",
        "exit_price": 101.0 if pnl >= 0 else 99.0,
        "exit_reason": exit_reason,
        "pnl": pnl,
        "pnl_pct": pnl,
        "r_multiple": r_multiple,
        "source": "mt5_paper_exploration",
        "auto_forward": True,
        "paper_exploration": True,
        "included_in_exploration_metrics": True,
        "manual_test": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


if __name__ == "__main__":
    unittest.main()
