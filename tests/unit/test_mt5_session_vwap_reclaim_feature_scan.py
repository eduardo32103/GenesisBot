from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.run_session_vwap_reclaim_feature_scan import main as scan_main
from services.mt5.mt5_session_vwap_reclaim_feature_scan import run_session_vwap_reclaim_feature_scan


class MT5SessionVwapReclaimFeatureScanTests(unittest.TestCase):
    def test_clean_session_vwap_reclaim_feature_edge_is_hardening_candidate_only(self) -> None:
        result = run_session_vwap_reclaim_feature_scan(
            evaluations=[{"symbol": "US500", "timeframe": "M30", "bars": _reclaim_bars()}],
            symbols=["US500"],
            timeframes=["M30"],
            max_evaluations=4,
        )

        self.assertEqual(result["recommendation"], "hardening_candidate_found")
        self.assertEqual(result["recommended_next_research_phase"], "session_vwap_reclaim_hardening")
        self.assertEqual(result["scanned_symbols"], ["US500"])
        self.assertEqual(result["scanned_timeframes"], ["M30"])
        self.assertEqual(result["evaluations_count"], 4)
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")
        edge = result["top_feature_edges"][0]
        self.assertEqual(edge["scan_status"], "hardening_candidate")
        self.assertGreaterEqual(edge["signal_count"], 50)
        self.assertGreaterEqual(edge["recent_signal_count"], 15)
        self.assertGreaterEqual(edge["profit_factor_proxy"], 1.15)
        self.assertGreater(edge["expectancy_proxy"], 0.0)
        self.assertGreater(edge["recent_expectancy_proxy"], 0.0)
        self.assertEqual(edge["data_quality"], "ok")
        self.assertFalse(edge["degraded_by_registry"])
        self.assertFalse(edge["rejected_by_research_registry"])
        self.assertFalse(edge["sibling_risk"])

    def test_missing_volume_blocks_scan_promotion(self) -> None:
        bars = [{key: value for key, value in bar.items() if key != "volume"} for bar in _reclaim_bars(days=10)]
        result = run_session_vwap_reclaim_feature_scan(
            evaluations=[{"symbol": "EURUSD", "timeframe": "M15", "bars": bars}],
            symbols=["EURUSD"],
            timeframes=["M15"],
            max_evaluations=1,
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertEqual(result["top_feature_edges"], [])
        self.assertTrue(result["data_quality_issues"])
        row = result["data_quality_issues"][0]
        self.assertEqual(row["data_quality"], "missing_volume")
        self.assertIn("missing_volume", row["rejection_reasons"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_eth_m30_is_avoided_by_default_fast_scan(self) -> None:
        result = run_session_vwap_reclaim_feature_scan(
            evaluations=[
                {"symbol": "ETHUSD", "timeframe": "M30", "bars": _reclaim_bars()},
                {"symbol": "ETHUSD", "timeframe": "M15", "bars": _reclaim_bars()},
            ],
            symbols=["ETHUSD"],
            timeframes=["M15", "M30"],
            max_evaluations=8,
            run_deep_scan=False,
        )

        self.assertEqual(result["scanned_symbols"], ["ETHUSD"])
        self.assertEqual(result["scanned_timeframes"], ["M15"])
        self.assertTrue(result["rejected_by_registry"])
        skipped = result["rejected_by_registry"][0]
        self.assertEqual(skipped["symbol"], "ETHUSD")
        self.assertEqual(skipped["timeframe"], "M30")
        self.assertEqual(skipped["candidate_status"], "skipped_default_failed_cluster_pair")
        self.assertFalse(skipped["broker_touched"])
        self.assertFalse(skipped["order_executed"])
        self.assertEqual(skipped["order_policy"], "journal_only_no_broker")

    def test_feature_scan_does_not_mark_new_family_as_rejected_registry_sibling(self) -> None:
        result = run_session_vwap_reclaim_feature_scan(
            evaluations=[{"symbol": "BTCUSD", "timeframe": "M30", "bars": _reclaim_bars()}],
            symbols=["BTCUSD"],
            timeframes=["M30"],
            max_evaluations=1,
        )

        row = result["results"][0]
        self.assertFalse(row["degraded_by_registry"])
        self.assertFalse(row["rejected_by_research_registry"])
        self.assertFalse(row["sibling_risk"])
        self.assertNotIn("research_rejection_registry", row["rejection_reasons"])
        self.assertNotIn("sibling_risk", row["rejection_reasons"])

    def test_script_runs_fast_with_local_csv_without_activation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_csv(root / "US500.b_M30_20000.csv", _reclaim_bars())
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = scan_main(["--csv-dir", str(root), "--symbols", "US500", "--timeframes", "M30"])

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("MT5 session VWAP reclaim feature scan", text)
        self.assertIn("scanned_symbols=US500", text)
        self.assertIn("scanned_timeframes=M30", text)
        self.assertIn("candidate_activated=False", text)
        self.assertIn("paper_forward_onboarding_started=False", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_executed=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)


def _reclaim_bars(*, days: int = 60) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    start = datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)
    for day in range(days):
        base = 100.0 + day * 0.1
        closes = [base, base - 1.0, base + 1.0, base + 2.0, base + 3.0, base + 4.0, base + 5.0, base + 6.0, base + 7.0, base + 8.0, base + 9.0]
        for offset, close in enumerate(closes):
            timestamp = start + timedelta(days=day, minutes=30 * offset)
            rows.append(
                {
                    "time": timestamp.isoformat(),
                    "open": close,
                    "high": close + 0.5,
                    "low": close - 0.5,
                    "close": close,
                    "volume": 100 + offset,
                }
            )
    return rows


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write("time,open,high,low,close,volume\n")
        for row in rows:
            handle.write(
                f"{row['time']},{row['open']},{row['high']},{row['low']},{row['close']},{row['volume']}\n"
            )


if __name__ == "__main__":
    unittest.main()
