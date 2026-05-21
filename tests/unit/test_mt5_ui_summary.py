from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from api.main import create_app
from api.routes.genesis import get_genesis_mt5_ui_summary
from services.genesis.memory_store import MemoryStore
from services.genesis.tool_router import route_message
from services.mt5.mt5_bridge import mt5_ui_summary
from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_decision, update_snapshot
from services.mt5.mt5_ui_summary import humanize_mt5_reason


class MT5UiSummaryTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_runtime_snapshots_for_tests()

    def test_create_app_exposes_mt5_ui_summary_endpoint(self) -> None:
        app = create_app()
        self.assertEqual(app["genesis_mt5_ui_summary_endpoint"], "/api/genesis/mt5/ui-summary?symbol={symbol}&timeframe={timeframe}")

    def test_reason_translations_are_human_readable(self) -> None:
        self.assertEqual(humanize_mt5_reason("no_runtime_snapshot_for_requested_timeframe"), "No hay lectura reciente del timeframe solicitado.")
        self.assertEqual(humanize_mt5_reason("risk_governor_pass"), "Riesgo dentro de limites.")
        self.assertIn("Perfil degradado", humanize_mt5_reason("early_forward_underperformance"))
        self.assertIn("Bloqueo total", humanize_mt5_reason("lockdown"))

    def test_ui_summary_builds_four_cards_and_safety_fields(self) -> None:
        update_decision(
            "BTCUSD",
            {
                "symbol": "BTCUSD",
                "timeframe": "M30",
                "decision": "NO_TRADE",
                "reason": "risk_governor_block:spread_too_high",
                "strategy_profile": "",
                "paper_forward_candidate_profile": "",
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            },
        )
        update_snapshot("BTCUSD", {"latest_performance_summary": {"closed": 12, "profit_factor": 0.7, "expectancy": -0.01}}, timeframe="M30")
        payload = mt5_ui_summary(symbol="BTCUSD", timeframe="M30")

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "mt5_ui_summary_ready")
        self.assertEqual(len(payload["cards"]), 4)
        self.assertEqual({card["title"] for card in payload["cards"]}, {"Estado de Riesgo", "Decision MT5", "Perfil Forward", "Robust Optimizer"})
        self.assertFalse(payload["broker_touched"])
        self.assertFalse(payload["order_executed"])
        self.assertEqual(payload["order_policy"], "journal_only_no_broker")
        self.assertIn("Broker protegido", payload["genesis_reading"])

    def test_route_wrapper_returns_ui_summary(self) -> None:
        payload = get_genesis_mt5_ui_summary("BTCUSD", "M30")
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["structured"]["kind"], "mt5_dashboard")
        self.assertFalse(payload["structured"]["cards"][0]["broker_protected"] is False)

    def test_genesis_chat_returns_mt5_dashboard_cards_for_risk_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = route_message("estado de riesgo MT5 BTCUSD M30", memory=store)

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["intent"], "mt5_bridge")
        self.assertEqual(payload["structured"]["kind"], "mt5_dashboard")
        self.assertIn("Broker protegido", payload["answer"])
        self.assertFalse(payload["mt5"]["broker_touched"])
        self.assertFalse(payload["mt5"]["order_executed"])


if __name__ == "__main__":
    unittest.main()
