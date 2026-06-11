from __future__ import annotations

import unittest
from datetime import datetime, timezone

from services.mt5.mt5_runtime_context_diagnostics import run_runtime_context_diagnostics


class MT5RuntimeContextDiagnosticsTests(unittest.TestCase):
    def test_reports_missing_context_without_inventing_data(self) -> None:
        result = run_runtime_context_diagnostics(symbol="BTCUSD", timeframe="M30", snapshot={}, generic_snapshot={})

        self.assertEqual(result["runtime_context_status"], "runtime_context_incomplete")
        self.assertIn("runtime_snapshot", result["runtime_context_missing_fields"])
        self.assertFalse(result["data_invented"])
        self.assertFalse(result["forced_context"])
        _assert_safety(self, result)

    def test_reports_complete_bar_context(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        result = run_runtime_context_diagnostics(
            symbol="BTCUSD",
            timeframe="M30",
            snapshot={
                "symbol": "BTCUSD",
                "timeframe": "M30",
                "last_tick_at": now,
                "bars_last_at": now,
                "bars_count": 60,
                "min_bars_required": 50,
                "runtime_snapshot_complete": True,
                "runtime_snapshot_context": "bar_context",
                "tick_merged_into_bar_context": True,
                "ohlc_recent": [{"close": 1.0}],
                "last_tick": {"last": 1.0},
            },
            generic_snapshot={},
        )

        self.assertEqual(result["runtime_context_status"], "runtime_context_ready")
        self.assertEqual(result["runtime_context_missing_fields"], [])
        self.assertTrue(result["runtime_snapshot_recent"])
        _assert_safety(self, result)

    def test_reports_incomplete_bar_context_fields(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        result = run_runtime_context_diagnostics(
            symbol="ETHUSD",
            timeframe="M30",
            snapshot={
                "symbol": "ETHUSD",
                "timeframe": "M30",
                "last_tick_at": now,
                "bars_count": 10,
                "min_bars_required": 50,
                "runtime_snapshot_complete": False,
                "runtime_snapshot_context": "tick_only",
            },
            generic_snapshot={},
        )

        missing = set(result["runtime_context_missing_fields"])
        self.assertIn("runtime_snapshot_complete", missing)
        self.assertIn("bar_context", missing)
        self.assertIn("bars_count", missing)
        self.assertIn("bars_last_at", missing)
        self.assertIn("tick_merged_into_bar_context", missing)
        _assert_safety(self, result)


def _assert_safety(test: unittest.TestCase, result: dict[str, object]) -> None:
    test.assertFalse(result["broker_touched"])
    test.assertFalse(result["order_executed"])
    test.assertEqual(result["order_policy"], "journal_only_no_broker")


if __name__ == "__main__":
    unittest.main()
