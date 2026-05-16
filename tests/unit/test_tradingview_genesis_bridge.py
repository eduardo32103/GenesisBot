from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from api.main import create_app
from services.genesis.agent_router import AgentRouter
from services.genesis.memory_store import MemoryStore
from services.genesis.tradingview_bridge import get_trading_context, receive_tradingview_webhook


class TradingViewGenesisBridgeTests(unittest.TestCase):
    def test_create_app_exposes_tradingview_bridge_endpoints(self) -> None:
        app_config = create_app()

        self.assertEqual(app_config["genesis_trading_context_endpoint"], "/api/genesis/trading-context?ticker={symbol}")
        self.assertEqual(app_config["genesis_tradingview_webhook_endpoint"], "/api/genesis/tradingview-webhook")

    def test_tradingview_signal_questions_route_to_memory(self) -> None:
        self.assertEqual(AgentRouter().route("que senales de TradingView tengo").intent, "memory_query")

    def test_trading_context_endpoint_contract_score_and_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            store.save_signal_event(
                "NVDA",
                {
                    "event_type": "strategy_signal",
                    "asset_name": "NVIDIA Corporation",
                    "expected_direction": "bullish",
                    "genesis_reading": "Breakout long con volumen; paper only.",
                    "status": "watching",
                },
                "test",
                "alta",
            )
            store.save_news_event("NVDA", {"title": "NVDA demand remains strong", "summary": "bullish catalyst"}, "test", "media")

            payload = get_trading_context("NVDA", memory=store)

            self.assertTrue(payload["ok"])
            self.assertEqual(payload["ticker"], "NVDA")
            self.assertEqual(payload["asset_name"], "NVIDIA Corporation")
            self.assertGreaterEqual(payload["genesis_context_score"], -100)
            self.assertLessEqual(payload["genesis_context_score"], 100)
            self.assertEqual(payload["bias"], "bullish")
            self.assertIn("relevant_news", payload)
            self.assertIn("active_alerts", payload)
            self.assertIn("whale_flow", payload)
            self.assertIn("memory_notes", payload)
            self.assertIn("risk_flags", payload)
            self.assertIn("what_to_watch", payload)

    def test_webhook_receives_alert_and_persists_memory_without_orders(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = receive_tradingview_webhook(
                {
                    "source": "tradingview",
                    "strategy": "Genesis Advantage Strategy v1",
                    "ticker": "NVDA",
                    "time": "2026-05-15T20:00:00Z",
                    "action": "long_signal",
                    "price": "100",
                    "score": "82",
                    "setup": "Breakout long",
                    "risk": "paper_only",
                    "stop": "95",
                    "target": "110",
                    "regime": "Alcista fuerte",
                    "genesis_context": "45",
                    "notes": "paper alert",
                },
                memory=store,
                now=datetime(2026, 5, 15, 20, 0, tzinfo=timezone.utc),
            )

            self.assertTrue(payload["ok"])
            self.assertFalse(payload["order_executed"])
            self.assertFalse(payload["broker_touched"])
            self.assertEqual(payload["execution_policy"], "journal_only_no_broker")
            self.assertIn("strategy_signals", payload["collections_saved"])
            self.assertIn("tradingview_alerts", payload["collections_saved"])
            self.assertIn("strategy_outcomes", payload["collections_saved"])
            self.assertIn("backtest_notes", payload["collections_saved"])
            self.assertIn("trade_journal", payload["collections_saved"])

            recent = store.get_recent_events(5, "tradingview_alert")
            signals = store.get_signal_events("NVDA", limit=5)
            outcomes = store.get_outcome_tracking("NVDA", limit=5)
            journal = store.get_asset_memory("NVDA", limit=5)
            notes = store.get_hypotheses("NVDA", limit=5)

            self.assertEqual(recent[0]["payload"]["ticker"], "NVDA")
            self.assertEqual(signals[0]["payload"]["event_type"], "strategy_signal")
            self.assertEqual(outcomes[0]["payload"]["event_type"], "strategy_outcome")
            self.assertIn("actual_outcome_1h", outcomes[0]["payload"])
            self.assertIn("actual_outcome_24h", outcomes[0]["payload"])
            self.assertIn("actual_outcome_7d", outcomes[0]["payload"])
            self.assertEqual(journal[0]["payload"]["event_type"], "trade_journal")
            self.assertEqual(notes[0]["payload"]["event_type"], "backtest_note")

    def test_webhook_does_not_store_or_echo_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = receive_tradingview_webhook(
                {
                    "ticker": "MSFT",
                    "action": "watch_only",
                    "score": 61,
                    "FMP_API_KEY": "should-not-persist",
                    "OPENAI_API_KEY": "should-not-persist",
                    "notes": "contains fmp_api_key=should-not-persist",
                },
                memory=store,
            )

            encoded = json.dumps(payload, sort_keys=True)
            self.assertNotIn("should-not-persist", encoded)
            self.assertNotIn("FMP_API_KEY", encoded)
            self.assertNotIn("OPENAI_API_KEY", encoded)
            self.assertFalse(payload["order_executed"])

    def test_pine_script_contract(self) -> None:
        root = Path(__file__).resolve().parents[2]
        script = (root / "tradingview" / "genesis_advantage_strategy_v1.pine").read_text(encoding="utf-8")
        docs = (root / "docs" / "TRADINGVIEW_GENESIS_STRATEGY.md").read_text(encoding="utf-8")

        self.assertIn("//@version=6", script)
        self.assertIn("strategy(", script)
        self.assertIn("Genesis Advantage Strategy v1", script)
        self.assertIn("freePlanMode", script)
        self.assertIn("validationMode", script)
        self.assertIn("debugMode", script)
        self.assertIn("longConditionRaw", script)
        self.assertIn("shortConditionRaw", script)
        self.assertIn("longCondition =", script)
        self.assertIn("shortCondition =", script)
        self.assertIn("strategy.entry(", script)
        self.assertIn("strategy.exit(", script)
        self.assertIn("buildAlertJson", script)
        self.assertIn("plotshape(", script)
        self.assertIn("table.new", script)
        self.assertIn("alertcondition", script)
        self.assertIn("alert_message", script)
        self.assertIn("broker_touched", script)
        self.assertIn("journal_only_no_broker", script)
        self.assertIn("blockReason", script)
        self.assertIn("Modo plan gratis / barras cargadas", script)
        self.assertIn("Modo validacion: generar senales", script)
        self.assertIn("Genesis Context Score", script)
        self.assertIn("marketRegimeScore", script)
        self.assertIn("finalSignalScore", script)
        self.assertIn("strategy.risk.max_intraday_loss", script)
        self.assertIn("strategy.risk.max_drawdown", script)
        self.assertIn("request.security", script)
        self.assertIn("Uso sin pagar Deep Backtesting", docs)
        self.assertIn("can't parse argument number", docs)
        self.assertIn("strategy.order.alert_message", script)
        self.assertIn("strategy.order.alert_message", docs)
        self.assertNotIn("str.format(", script)
        self.assertNotIn("str.format(\"{\\\"source\\\"", script)
        self.assertNotIn("str.format('{\"source\"", script)
        self.assertNotIn("message=longAlertMessage", script)
        self.assertNotIn("message=shortAlertMessage", script)
        self.assertNotIn("FMP_API_KEY", script)
        self.assertNotIn("OPENAI_API_KEY", script)


if __name__ == "__main__":
    unittest.main()
