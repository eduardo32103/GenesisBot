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

    def test_collapses_repeated_same_setup_and_ignores_non_asset_words(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            for index in range(3):
                store.save_decision_note(
                    "NVDA",
                    "vigilar confirmacion",
                    {
                        "event_id": f"decision-nvda-repeat-{index}",
                        "ticker": "NVDA",
                        "asset_name": "NVIDIA Corporation",
                        "price_at_decision": 215.217,
                        "current_price": 215.217,
                        "expected_direction": "watch",
                        "verdict": "vigilar confirmacion",
                        "created_at": f"2026-05-12T12:0{index}:00+00:00",
                    },
                    "test",
                    "media",
                )
            store.save_decision_note(
                "CAUTELA",
                "esperar fuente",
                {
                    "event_id": "decision-not-asset",
                    "ticker": "CAUTELA",
                    "price_at_decision": None,
                    "expected_direction": "watch",
                    "verdict": "esperar fuente",
                    "created_at": "2026-05-12T12:05:00+00:00",
                },
                "test",
                "baja",
            )

            report = build_genesis_performance_report(
                "que tanto esta acertando genesis",
                memory=store,
                quote_loader=lambda ticker: {"ticker": ticker, "current_price": 215.217, "name": "NVIDIA Corporation"},
                now=datetime(2026, 5, 12, 15, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(report["metrics"]["priced_decisions"], 1)
            self.assertEqual(report["metrics"]["watching"], 1)
            self.assertEqual(report["metrics"]["missing_price"], 0)
            self.assertEqual(report["metrics"]["ignored_rows"], 1)
            self.assertEqual(len(report["recent"]), 1)
            self.assertEqual(report["recent"][0]["ticker"], "NVDA")

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
