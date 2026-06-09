from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.run_multi_timeframe_trend_pullback_feature_scan import main as scan_main
from services.mt5.mt5_multi_timeframe_trend_pullback_feature_scan import run_multi_timeframe_trend_pullback_feature_scan


class MT5MultiTimeframeTrendPullbackFeatureScanTests(unittest.TestCase):
    def test_clean_multi_timeframe_pullback_edge_is_hardening_candidate_only(self) -> None:
        result = run_multi_timeframe_trend_pullback_feature_scan(
            evaluations=[
                {
                    "symbol": "EURUSD",
                    "timeframe": "M30",
                    "higher_timeframe": "H1",
                    "bars": _pullback_bars(minutes=30, count=720),
                    "higher_bars": _higher_trend_bars(count=360),
                }
            ],
            symbols=["EURUSD"],
            timeframes=["M30"],
            max_evaluations=5,
        )

        self.assertEqual(result["recommendation"], "hardening_candidate_found")
        self.assertEqual(result["recommended_next_research_phase"], "multi_timeframe_trend_pullback_hardening")
        self.assertEqual(result["scanned_symbols"], ["EURUSD"])
        self.assertEqual(result["scanned_timeframes"], ["M30"])
        self.assertEqual(result["evaluations_count"], 5)
        self.assertTrue(result["proxy_only"])
        self.assertTrue(result["requires_real_hardening"])
        self.assertTrue(result["hardening_required_before_candidate"])
        self.assertTrue(result["cannot_be_paper_forward_candidate"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

        edge = result["top_feature_edges"][0]
        self.assertEqual(edge["scan_status"], "hardening_candidate")
        self.assertEqual(edge["family"], "multi_timeframe_trend_pullback")
        self.assertEqual(edge["higher_timeframe"], "H1")
        self.assertGreaterEqual(edge["signal_count"], 50)
        self.assertGreaterEqual(edge["recent_signal_count"], 15)
        self.assertGreaterEqual(edge["profit_factor_proxy"], 1.20)
        self.assertGreater(edge["expectancy_proxy"], 0.0)
        self.assertGreater(edge["recent_expectancy_proxy"], 0.0)
        self.assertEqual(edge["data_quality"], "ok")
        self.assertFalse(edge["degraded_by_registry"])
        self.assertFalse(edge["rejected_by_research_registry"])
        self.assertFalse(edge["sibling_risk"])
        self.assertTrue(edge["proxy_only"])
        self.assertTrue(edge["requires_real_hardening"])
        self.assertTrue(edge["cannot_be_paper_forward_candidate"])

    def test_missing_h4_for_h1_marks_missing_higher_timeframe_without_activation(self) -> None:
        result = run_multi_timeframe_trend_pullback_feature_scan(
            evaluations=[
                {
                    "symbol": "GBPUSD",
                    "timeframe": "H1",
                    "higher_timeframe": "H4",
                    "bars": _pullback_bars(minutes=60, count=320),
                    "higher_bars": [],
                }
            ],
            symbols=["GBPUSD"],
            timeframes=["H1"],
            max_evaluations=1,
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertEqual(result["top_feature_edges"], [])
        self.assertTrue(result["data_quality_issues"])
        row = result["data_quality_issues"][0]
        self.assertEqual(row["data_quality"], "missing_higher_timeframe")
        self.assertIn("missing_higher_timeframe", row["rejection_reasons"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_eurusd_h1_vwap_rejection_does_not_block_new_multitimeframe_family(self) -> None:
        result = run_multi_timeframe_trend_pullback_feature_scan(
            evaluations=[
                {
                    "symbol": "EURUSD",
                    "timeframe": "H1",
                    "higher_timeframe": "H4",
                    "bars": _pullback_bars(minutes=60, count=720),
                    "higher_bars": _higher_trend_bars(minutes=240, count=360),
                }
            ],
            symbols=["EURUSD"],
            timeframes=["H1"],
            max_evaluations=1,
        )

        row = result["results"][0]
        self.assertFalse(row["degraded_by_registry"])
        self.assertFalse(row["rejected_by_research_registry"])
        self.assertFalse(row["sibling_risk"])
        self.assertNotIn("research_rejection_registry", row["rejection_reasons"])
        self.assertNotIn("sibling_risk", row["rejection_reasons"])

    def test_ustec_m30_h1_trend_pullback_is_excluded_but_m15_remains_available(self) -> None:
        result = run_multi_timeframe_trend_pullback_feature_scan(
            evaluations=[
                {
                    "symbol": "NAS100",
                    "timeframe": "M30",
                    "higher_timeframe": "H1",
                    "bars": _pullback_bars(minutes=30, count=720),
                    "higher_bars": _higher_trend_bars(count=360),
                },
                {
                    "symbol": "USTEC",
                    "timeframe": "M15",
                    "higher_timeframe": "H1",
                    "bars": _pullback_bars(minutes=15, count=720),
                    "higher_bars": _higher_trend_bars(count=360),
                },
            ],
            symbols=["USTEC"],
            timeframes=["M15", "M30"],
            max_evaluations=10,
        )

        self.assertTrue(result["rejected_by_registry"])
        self.assertEqual(result["proxy_reliability_warning"], "proxy_false_positive_after_monte_carlo_failure")
        rejected = [row for row in result["rejected_by_registry"] if row["timeframe"] == "M30"]
        self.assertTrue(rejected)
        for row in rejected:
            self.assertEqual(row["symbol"], "USTEC")
            self.assertEqual(row["higher_timeframe"], "H1")
            self.assertTrue(row["rejected_by_research_registry"])
            self.assertEqual(row["research_rejection_reason"], "proxy_false_positive_after_monte_carlo_failure")
            self.assertEqual(row["scan_status"], "excluded_by_registry_or_sibling_risk")
            self.assertEqual(row["proxy_reliability_warning"], "proxy_false_positive_after_monte_carlo_failure")
            self.assertFalse(row["candidate_activated"])
            self.assertFalse(row["paper_forward_onboarding_started"])
            self.assertFalse(row["broker_touched"])
            self.assertFalse(row["order_executed"])
            self.assertEqual(row["order_policy"], "journal_only_no_broker")

        m15_rows = [row for row in result["results"] if row["timeframe"] == "M15"]
        self.assertTrue(m15_rows)
        self.assertTrue(all(not row["rejected_by_research_registry"] for row in m15_rows))
        self.assertTrue(all("research_rejection_registry" not in row["rejection_reasons"] for row in m15_rows))
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])

    def test_script_runs_fast_with_local_csv_without_activation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_csv(root / "EURUSD_M30_20000.csv", _pullback_bars(minutes=30, count=720))
            _write_csv(root / "EURUSD_H1_20000.csv", _higher_trend_bars(count=360))
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = scan_main(["--csv-dir", str(root), "--symbols", "EURUSD", "--timeframes", "M30"])

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("MT5 multi-timeframe trend pullback feature scan", text)
        self.assertIn("scanned_symbols=EURUSD", text)
        self.assertIn("scanned_timeframes=M30", text)
        self.assertIn("candidate_activated=False", text)
        self.assertIn("paper_forward_onboarding_started=False", text)
        self.assertIn("proxy_only=True", text)
        self.assertIn("requires_real_hardening=True", text)
        self.assertIn("cannot_be_paper_forward_candidate=True", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_executed=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)


def _higher_trend_bars(*, minutes: int = 60, count: int = 360) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    for index in range(count):
        close = 100.0 + index * 0.18
        timestamp = start + timedelta(minutes=minutes * index)
        rows.append(_bar(timestamp, close, high_extra=0.4, low_extra=0.4))
    return rows


def _pullback_bars(*, minutes: int, count: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    cycle_offsets = (1.0, 0.45, -1.0, 1.55, 2.25, 2.9)
    for index in range(count):
        base = 100.0 + index * 0.35
        close = base + cycle_offsets[index % len(cycle_offsets)]
        timestamp = start + timedelta(minutes=minutes * index)
        rows.append(_bar(timestamp, close, high_extra=0.7, low_extra=8.0))
    return rows


def _bar(timestamp: datetime, close: float, *, high_extra: float, low_extra: float) -> dict[str, object]:
    return {
        "time": timestamp.isoformat(),
        "open": close - 0.15,
        "high": close + high_extra,
        "low": close - low_extra,
        "close": close,
        "volume": 1000,
    }


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write("time,open,high,low,close,volume\n")
        for row in rows:
            handle.write(
                f"{row['time']},{row['open']},{row['high']},{row['low']},{row['close']},{row['volume']}\n"
            )


if __name__ == "__main__":
    unittest.main()
