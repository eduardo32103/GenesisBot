from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from services.mt5.mt5_xau_m15_fast_observation_parameter_sweep import run_xau_m15_fast_observation_parameter_sweep


class MT5XauM15FastObservationParameterSweepTests(unittest.TestCase):
    def test_parameter_sweep_returns_ranked_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "XAUUSD.b_M15_20000.csv"
            csv_path.write_text(_csv_rows(2600), encoding="utf-8")

            result = run_xau_m15_fast_observation_parameter_sweep(csv_path=csv_path, max_rows=2600)

        self.assertTrue(result["ok"])
        self.assertTrue(result["csv_found"])
        self.assertEqual(result["rows_loaded"], 2600)
        self.assertGreater(result["evaluations_count"], 0)
        self.assertTrue(result["top_parameter_sets"])
        self.assertIn("time_stop_bars", result["recommended_live_paper_parameters"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")


def _csv_rows(count: int) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 4200.0
    for idx in range(count):
        price += 0.02
        rows.append(f"2026-01-01T00:{idx % 60:02d}:00,{price - 0.1},{price + 0.5},{price - 0.5},{price},100")
    return "\n".join(rows)


if __name__ == "__main__":
    unittest.main()
