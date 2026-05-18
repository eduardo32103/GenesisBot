from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from api.main import create_app
from api.routes.genesis import (
    get_genesis_mt5_auto_forward_status,
    get_genesis_mt5_config,
    get_genesis_mt5_debug_storage,
    get_genesis_mt5_decision,
    get_genesis_mt5_forward_test,
    get_genesis_mt5_health,
    get_genesis_mt5_instrument,
    get_genesis_mt5_journal_recent,
    get_genesis_mt5_no_trade_report,
    get_genesis_mt5_outcomes_recent,
    get_genesis_mt5_performance,
    get_genesis_mt5_performance_auto,
    get_genesis_mt5_replay_results,
    get_genesis_mt5_replay_status,
    get_genesis_mt5_shadow_trades,
    get_genesis_mt5_status,
    post_genesis_mt5_account_sync,
    post_genesis_mt5_metrics_exclude_old_proxy,
    post_genesis_mt5_order_request,
    post_genesis_mt5_order_result,
    post_genesis_mt5_manual_tests_reset,
    post_genesis_mt5_replay_reset,
    post_genesis_mt5_replay_run,
    post_genesis_mt5_signal,
    post_genesis_mt5_tick,
)
from services.genesis.agent_router import AgentRouter
from services.genesis.memory_store import MemoryStore
from services.genesis.tool_router import route_message
from services.mt5.mt5_bridge import (
    mt5_auto_forward_status,
    mt5_account_sync,
    mt5_debug_storage,
    mt5_decision,
    mt5_forward_test,
    mt5_instrument,
    mt5_journal_recent,
    mt5_manual_tests_reset,
    mt5_metrics_exclude_old_proxy,
    mt5_no_trade_report,
    mt5_order_request,
    mt5_outcomes_recent,
    mt5_performance,
    mt5_performance_auto,
    mt5_replay_reset,
    mt5_replay_results,
    mt5_replay_run,
    mt5_replay_status,
    mt5_signal,
    mt5_shadow_trades,
    mt5_status,
    mt5_tick,
)
from services.mt5.mt5_decision_signal_builder import build_actionable_mt5_decision
from services.mt5.mt5_journal import MT5Journal
from services.mt5.mt5_order_model import MT5OrderIntent
from services.mt5.mt5_risk_guard import MT5BridgeConfig, MT5RiskGuard
from services.mt5.mt5_signal_router import MT5SignalRouter
from services.mt5.mt5_symbol_mapper import MT5SymbolMapper


class MT5BridgeTests(unittest.TestCase):
    def test_create_app_exposes_mt5_endpoints(self) -> None:
        app = create_app()

        self.assertEqual(app["genesis_mt5_health_endpoint"], "/api/genesis/mt5/health")
        self.assertEqual(app["genesis_mt5_status_endpoint"], "/api/genesis/mt5/status")
        self.assertEqual(app["genesis_mt5_config_endpoint"], "/api/genesis/mt5/config")
        self.assertEqual(app["genesis_mt5_decision_endpoint"], "/api/genesis/mt5/decision?symbol={symbol}")
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
            self.assertEqual(performance["summary_manual"]["shadow_trades"], 2)
            self.assertEqual(performance["summary_manual"]["wins"], 1)
            self.assertEqual(performance["summary_manual"]["losses"], 1)
            self.assertEqual(performance["summary_manual"]["win_rate"], 50.0)
            self.assertEqual(performance["summary_manual"]["profit_factor"], 2.0)
            self.assertEqual(shadow["count"], performance["summary"]["total_shadow_trades"])
            self.assertEqual(shadow["closed"], performance["summary_manual"]["closed"])
            self.assertEqual(shadow["items"][0]["normalized_symbol"], "BTC_PROXY")
            self.assertEqual(shadow["items"][0]["instrument_type"], "crypto_etf_proxy")
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
            self.assertEqual(perf_btcusd["summary"]["total_shadow_trades"], 1)
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
            self.assertEqual(debug["performance_total_shadow_trades"], 1)
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
            self.assertEqual(perf_btc["summary"]["total_shadow_trades"], 1)
            self.assertEqual(shadow_btc["count"], 1)
            self.assertEqual(shadow_btc["items"][0]["symbol"], "BTC")
            self.assertEqual(shadow_btc["items"][0]["normalized_symbol"], "BTC_PROXY")
            self.assertFalse(shadow_btcusd["broker_touched"])
            self.assertFalse(shadow_btcusd["order_executed"])

    def test_auto_forward_tick_creates_decision_and_shadow_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_buy_context()):
                tick = router.tick({"symbol": "BTC", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            trades = router.shadow_trades(symbol="BTC")
            status = router.auto_forward_status(symbol="BTC")

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
                tick = router.tick({"symbol": "BTC", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            trade = router.shadow_trades(symbol="BTC")["open_trades"][0]

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
                tick = router.tick({"symbol": "BTC", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            trade = router.shadow_trades(symbol="BTC")["open_trades"][0]

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

    def test_paper_exploration_creates_separate_shadow_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _exploration_router(store)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_no_trade_context()):
                tick = router.tick({"symbol": "BTCUSD", "last": 77000, "timeframe": "H1", "spread": 12, "is_demo": True, "symbol_description": "Bitcoin vs. USD", "currency_base": "BTC", "currency_profit": "USD"})

            performance = mt5_performance(memory=store, symbol="BTCUSD", timeframe="H1")
            status = mt5_auto_forward_status(memory=store, symbol="BTCUSD")
            trades = mt5_shadow_trades(memory=store, symbol="BTCUSD")

            self.assertFalse(tick["auto_shadow_trade_created"])
            self.assertTrue(tick["auto_forward"]["exploration_shadow_trade_created"])
            self.assertEqual(performance["summary_strict_auto"]["shadow_trades"], 0)
            self.assertEqual(performance["summary_exploration"]["shadow_trades"], 1)
            self.assertEqual(performance["summary_forward_auto"]["shadow_trades"], 1)
            self.assertEqual(status["exploration_shadow_trades"], 1)
            self.assertEqual(status["strict_shadow_trades"], 0)
            self.assertTrue(trades["items"][0]["paper_exploration"])
            self.assertTrue(trades["items"][0]["excluded_from_live_grade"])
            self.assertGreaterEqual(trades["items"][0]["risk_reward"], 1.2)
            self.assertFalse(tick["broker_touched"])
            self.assertFalse(tick["order_executed"])

    def test_paper_exploration_does_not_duplicate_open_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _exploration_router(store)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_no_trade_context()):
                first = router.tick({"symbol": "BTCUSD", "last": 77000, "timeframe": "H1", "spread": 12, "is_demo": True, "symbol_description": "Bitcoin vs. USD", "currency_base": "BTC", "currency_profit": "USD"})
                second = router.tick({"symbol": "BTCUSD", "last": 77010, "timeframe": "H1", "spread": 12, "is_demo": True, "symbol_description": "Bitcoin vs. USD", "currency_base": "BTC", "currency_profit": "USD"})

            performance = mt5_performance(memory=store, symbol="BTCUSD", timeframe="H1")

            self.assertTrue(first["auto_forward"]["exploration_shadow_trade_created"])
            self.assertFalse(second["auto_forward"]["exploration_shadow_trade_created"])
            self.assertEqual(second["auto_forward"]["exploration"]["reason"], "active_open_trade")
            self.assertEqual(performance["summary_exploration"]["open"], 1)
            self.assertFalse(second["broker_touched"])
            self.assertFalse(second["order_executed"])

    def test_auto_forward_does_not_create_duplicate_open_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)

            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_buy_context()):
                first = router.tick({"symbol": "BTC", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})
                second = router.tick({"symbol": "BTC", "last": 101, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            trades = router.shadow_trades(symbol="BTC")
            status = router.auto_forward_status(symbol="BTC")
            auto_performance = router.performance_auto(symbol="BTC", timeframe="H1")

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
                tick = router.tick({"symbol": "BTC", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            performance = router.performance(symbol="BTC", timeframe="H1")

            self.assertFalse(tick["auto_shadow_trade_created"])
            self.assertEqual(tick["auto_forward"]["reason"], "confidence_low")
            self.assertNotIn("stop_loss_missing_from_context", str(performance))
            self.assertFalse(tick["order_executed"])
            self.assertFalse(tick["broker_touched"])

    def test_performance_and_status_alias_legacy_stop_loss_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
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
                router.tick({"symbol": "BTC", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_no_trade_context()):
                tp_tick = router.tick({"symbol": "BTC", "last": 110.5, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            loss_store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "loss.sqlite3")
            loss_router = _auto_forward_router(loss_store)
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_buy_context()):
                loss_router.tick({"symbol": "BTC", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_no_trade_context()):
                sl_tick = loss_router.tick({"symbol": "BTC", "last": 94.5, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            win_perf = router.performance(symbol="BTC", timeframe="H1")
            loss_perf = loss_router.performance(symbol="BTC", timeframe="H1")

            self.assertEqual(win_perf["summary"]["wins"], 1)
            self.assertEqual(win_perf["summary"]["losses"], 0)
            self.assertEqual(loss_perf["summary"]["wins"], 0)
            self.assertEqual(loss_perf["summary"]["losses"], 1)
            self.assertFalse(tp_tick["broker_touched"])
            self.assertFalse(sl_tick["order_executed"])

    def test_performance_separates_manual_and_auto_shadow_trades(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)
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
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_sell_context()):
                router.tick({"symbol": "BTC", "last": 130, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            performance = router.performance(symbol="BTC", timeframe="H1")
            facade = mt5_auto_forward_status(memory=store, symbol="BTC")

            self.assertEqual(performance["summary"]["manual_shadow_trades"], 1)
            self.assertEqual(performance["summary"]["auto_shadow_trades"], 1)
            self.assertEqual(performance["summary"]["total_shadow_trades"], 2)
            self.assertEqual(performance["summary"]["shadow_trades"], 1)
            self.assertEqual(performance["summary_auto"]["shadow_trades"], 1)
            self.assertEqual(performance["summary_manual"]["shadow_trades"], 1)
            self.assertEqual(performance["summary_total"]["shadow_trades"], 2)
            self.assertTrue(facade["ok"])
            self.assertIn("entry", facade)
            self.assertIn("stop_loss", facade)
            self.assertIn("take_profit", facade)
            self.assertIn("risk_reward", facade)
            self.assertEqual(facade["manual_shadow_trades"], 1)
            self.assertEqual(facade["auto_shadow_trades"], 1)
            self.assertFalse(performance["broker_touched"])
            self.assertFalse(performance["order_executed"])

    def test_auto_performance_endpoint_counts_only_auto_trades(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)
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
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_generated_buy_context()):
                router.tick({"symbol": "BTC", "last": 120, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            auto = mt5_performance_auto(memory=store, symbol="BTC", timeframe="H1")
            total = mt5_performance(memory=store, symbol="BTC", timeframe="H1")

            self.assertTrue(auto["ok"])
            self.assertEqual(auto["summary"]["shadow_trades"], 1)
            self.assertEqual(total["summary_auto"]["shadow_trades"], 1)
            self.assertEqual(total["summary_manual"]["shadow_trades"], 1)
            self.assertIn("Muestra automatica insuficiente", auto["sample_warning"])
            self.assertFalse(auto["broker_touched"])
            self.assertFalse(auto["order_executed"])

    def test_manual_reset_excludes_manual_tests_without_deleting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)
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
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_generated_sell_context()):
                router.tick({"symbol": "BTC", "last": 130, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            reset = mt5_manual_tests_reset({"symbol": "BTC"}, memory=store)
            report = mt5_performance(memory=store, symbol="BTC", timeframe="H1")
            trades = mt5_shadow_trades(memory=store, symbol="BTC")

            self.assertTrue(reset["ok"])
            self.assertEqual(reset["updated"], 1)
            self.assertEqual(report["summary_total"]["shadow_trades"], 1)
            self.assertEqual(report["summary_auto"]["shadow_trades"], 1)
            self.assertEqual(report["summary_manual"]["shadow_trades"], 1)
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

    def test_genesis_chat_uses_auto_summary_and_warns_small_sample(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            router = _auto_forward_router(store)
            with patch("services.mt5.mt5_auto_forward.GenesisBrain.build_trading_context", return_value=_generated_buy_context()):
                router.tick({"symbol": "BTC", "last": 100, "timeframe": "H1", "spread": 0.2, "is_demo": True})

            response = route_message("que porcentaje automatico lleva BTC e ignora pruebas manuales", memory=store)

            self.assertTrue(response["ok"])
            self.assertIn("auto win rate", response["answer"])
            self.assertIn("Muestra automatica insuficiente", response["answer"])
            self.assertIn("order_executed=false", response["answer"])

    def test_order_request_creates_shadow_trade_even_when_execution_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
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
            self.assertEqual(trades["open"], 1)
            self.assertEqual(trades["closed"], 0)
            self.assertEqual(performance["summary"]["actionable_signals"], 1)
            self.assertEqual(performance["summary_manual"]["shadow_trades"], 1)

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
            self.assertEqual(trades["closed"], 2)
            self.assertEqual(performance["summary_manual"]["wins"], 1)
            self.assertEqual(performance["summary_manual"]["losses"], 1)
            self.assertEqual(performance["summary_manual"]["win_rate"], 50.0)
            self.assertEqual(performance["summary_manual"]["profit_factor"], 2.0)
            self.assertFalse(trades["broker_touched"])
            self.assertFalse(trades["order_executed"])

    def test_mt5_no_trade_and_hedge_outcomes_are_measured(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
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

    def test_genesis_chat_routes_mt5_questions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            self.assertEqual(AgentRouter().route("estado de MT5").intent, "mt5_bridge")
            self.assertEqual(AgentRouter().route("que decision tiene MT5 para BTC").intent, "mt5_bridge")

            status = route_message("estado de MT5", memory=store)
            decision = route_message("que decision tiene MT5 para BTC", memory=store)

            self.assertTrue(status["ok"])
            self.assertEqual(status["intent"], "mt5_bridge")
            self.assertIn("mt5", status)
            self.assertTrue(decision["ok"])
            self.assertEqual(decision["intent"], "mt5_bridge")
            self.assertIn("order_executed=false", decision["answer"])

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
        ),
        symbol_mapper=MT5SymbolMapper(allowed_symbols=["BTC", "BTCUSD"]),
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


if __name__ == "__main__":
    unittest.main()
