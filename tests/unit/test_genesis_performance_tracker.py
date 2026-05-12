import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from services.genesis.memory_store import MemoryStore
from services.genesis.performance_tracker import build_genesis_performance_report
from services.genesis.tool_router import route_message


class GenesisPerformanceTrackerTests(unittest.TestCase):
    def test_scores_saved_decisions_and_persists_outcomes(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            store.save_decision_note(
                "NVDA",
                "comprar con cautela",
                {
                    "event_id": "decision-nvda-1",
                    "ticker": "NVDA",
                    "asset_name": "NVIDIA Corporation",
                    "price_at_decision": 100,
                    "expected_direction": "bullish",
                    "verdict": "comprar con cautela",
                    "created_at": "2026-05-12T12:00:00+00:00",
                },
                "test",
                "alta",
            )

            report = build_genesis_performance_report(
                "que tanto esta acertando genesis",
                memory=store,
                quote_loader=lambda ticker: {"ticker": ticker, "current_price": 104, "name": "NVIDIA Corporation"},
                now=datetime(2026, 5, 12, 15, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(report["metrics"]["hits"], 1)
            self.assertEqual(report["metrics"]["misses"], 0)
            self.assertEqual(report["today"]["hits"], 1)
            self.assertEqual(store.get_outcome_tracking("NVDA", limit=5)[0]["payload"]["outcome_label"], "hit")
            self.assertTrue(store.get_learned_context(5))

    def test_router_exposes_performance_review_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = route_message("que tanto esta acertando genesis", memory=store)

            self.assertTrue(payload["ok"])
            self.assertEqual(payload["intent"], "performance_review")
            self.assertEqual(payload["response_type"], "performance_review")
            self.assertEqual(payload["structured"]["kind"], "performance_review")


if __name__ == "__main__":
    unittest.main()
