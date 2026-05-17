from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from api.main import create_app
from api.routes.genesis import (
    get_genesis_mt5_config,
    get_genesis_mt5_decision,
    get_genesis_mt5_forward_test,
    get_genesis_mt5_health,
    get_genesis_mt5_journal_recent,
    get_genesis_mt5_outcomes_recent,
    get_genesis_mt5_performance,
    get_genesis_mt5_status,
    post_genesis_mt5_account_sync,
    post_genesis_mt5_order_request,
    post_genesis_mt5_order_result,
    post_genesis_mt5_signal,
    post_genesis_mt5_tick,
)
from services.genesis.agent_router import AgentRouter
from services.genesis.memory_store import MemoryStore
from services.genesis.tool_router import route_message
from services.mt5.mt5_bridge import (
    mt5_account_sync,
    mt5_decision,
    mt5_forward_test,
    mt5_journal_recent,
    mt5_order_request,
    mt5_outcomes_recent,
    mt5_performance,
    mt5_signal,
    mt5_status,
    mt5_tick,
)
from services.mt5.mt5_order_model import MT5OrderIntent
from services.mt5.mt5_risk_guard import MT5BridgeConfig, MT5RiskGuard
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
        self.assertEqual(app["genesis_mt5_forward_test_endpoint"], "/api/genesis/mt5/forward-test?symbol={symbol}")
        self.assertEqual(app["genesis_mt5_outcomes_recent_endpoint"], "/api/genesis/mt5/outcomes/recent?symbol={symbol}&limit=25")
        self.assertEqual(app["genesis_mt5_account_sync_endpoint"], "/api/genesis/mt5/account-sync")
        self.assertEqual(app["genesis_mt5_signal_endpoint"], "/api/genesis/mt5/signal")
        self.assertEqual(app["genesis_mt5_tick_endpoint"], "/api/genesis/mt5/tick")
        self.assertEqual(app["genesis_mt5_order_request_endpoint"], "/api/genesis/mt5/order-request")
        self.assertEqual(app["genesis_mt5_order_result_endpoint"], "/api/genesis/mt5/order-result")

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
        self.assertIn("BTC en este broker puede ser ETF/proxy", btc["instrument_warning"])

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
            self.assertIn("BTC en este broker puede ser ETF/proxy", decision["instrument_warning"])
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
            forward = mt5_forward_test(memory=store, symbol="BTC", timeframe="H1")
            outcomes = mt5_outcomes_recent(memory=store, symbol="BTC", limit=10)

            self.assertTrue(buy["shadow"]["created"])
            self.assertTrue(sell["shadow"]["created"])
            self.assertEqual(first_tick["status"], "mt5_tick_recorded")
            self.assertEqual(second_tick["status"], "mt5_tick_recorded")
            self.assertTrue(store.get_mt5_events("mt5_ticks", "BTC", limit=5))
            self.assertEqual(performance["summary"]["shadow_trades"], 2)
            self.assertEqual(performance["summary"]["wins"], 1)
            self.assertEqual(performance["summary"]["losses"], 1)
            self.assertEqual(performance["summary"]["win_rate"], 50.0)
            self.assertEqual(performance["summary"]["profit_factor"], 2.0)
            self.assertEqual(forward["status"], "mt5_forward_test_ready")
            self.assertGreaterEqual(outcomes["count"], 2)
            self.assertFalse(performance["broker_touched"])
            self.assertFalse(performance["order_executed"])
            self.assertNotIn("dont-save", str(performance))

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
        forward = get_genesis_mt5_forward_test("BTCUSD")
        outcomes = get_genesis_mt5_outcomes_recent(symbol="BTCUSD", limit=5)

        self.assertTrue(decision["ok"])
        self.assertTrue(account["ok"])
        self.assertTrue(signal["ok"])
        self.assertTrue(tick["ok"])
        self.assertTrue(request["ok"])
        self.assertTrue(result["ok"])
        self.assertTrue(performance["ok"])
        self.assertTrue(forward["ok"])
        self.assertTrue(outcomes["ok"])
        self.assertFalse(request["order_executed"])
        self.assertFalse(request["broker_touched"])
        self.assertEqual(request["order_policy"], "journal_only_no_broker")

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
        self.assertIn("input bool KillSwitch = true", content)
        self.assertIn("/api/genesis/mt5/tick", content)
        self.assertIn("Last tick sent", content)
        self.assertIn("WebRequest", content)
        self.assertNotIn("FMP_API_KEY", content)
        self.assertNotIn("OPENAI_API_KEY", content)


if __name__ == "__main__":
    unittest.main()
