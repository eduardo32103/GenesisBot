from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.run_volatility_compression_breakout_feature_scan import main as scan_main
from services.mt5.mt5_volatility_compression_breakout_feature_scan import (
    run_volatility_compression_breakout_feature_scan,
)


class MT5VolatilityCompressionBreakoutFeatureScanTests(unittest.TestCase):
    def test_clean_precomputed_row_becomes_deep_validation_candidate(self) -> None:
        result = run_volatility_compression_breakout_feature_scan(
            evaluations=[{"precomputed_result": _clean_row()}],
            persistent_events={},
        )

        self.assertEqual(result["recommendation"], "deep_validation_candidate_found")
        self.assertEqual(len(result["deep_validation_candidates"]), 1)
        self.assertEqual(result["recommended_next_candidate"]["candidate_status"], "deep_validation_candidate")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_sample_gates_are_enforced(self) -> None:
        row = {**_clean_row(), "total_closed": 10, "recent_closed": 3}
        result = run_volatility_compression_breakout_feature_scan(
            evaluations=[{"precomputed_result": row}],
            persistent_events={},
        )

        reasons = result["top_rejected"][0]["rejection_reasons"]
        self.assertIn("total_closed_below_50", reasons)
        self.assertIn("recent_closed_below_20", reasons)
        self.assertEqual(result["recommendation"], "continue_research")

    def test_pf_and_expectancy_gates_are_enforced(self) -> None:
        row = {
            **_clean_row(),
            "total_pf": 1.01,
            "recent_pf": 1.03,
            "expectancy": -0.01,
            "recent_expectancy": 0.0,
        }
        result = run_volatility_compression_breakout_feature_scan(
            evaluations=[{"precomputed_result": row}],
            persistent_events={},
        )

        reasons = result["top_rejected"][0]["rejection_reasons"]
        self.assertIn("total_pf_below_1_15", reasons)
        self.assertIn("recent_pf_below_1_15", reasons)
        self.assertIn("expectancy_not_positive", reasons)
        self.assertIn("recent_expectancy_not_positive", reasons)

    def test_spread_and_remove_best_gates_are_enforced(self) -> None:
        row = {**_clean_row(), "spread_x2_pf": 0.9, "remove_best_5_pf": 0.8}
        result = run_volatility_compression_breakout_feature_scan(
            evaluations=[{"precomputed_result": row}],
            persistent_events={},
        )

        reasons = result["top_rejected"][0]["rejection_reasons"]
        self.assertIn("spread_x2_pf_below_0_95", reasons)
        self.assertIn("remove_best_5_pf_below_1", reasons)

    def test_dependency_gates_are_enforced(self) -> None:
        row = {**_clean_row(), "single_trade_dependency": True, "fragile_regime_dependency": True}
        result = run_volatility_compression_breakout_feature_scan(
            evaluations=[{"precomputed_result": row}],
            persistent_events={},
        )

        reasons = result["top_rejected"][0]["rejection_reasons"]
        self.assertIn("single_trade_dependency", reasons)
        self.assertIn("fragile_regime_dependency", reasons)

    def test_unknown_profile_is_rejected(self) -> None:
        row = {**_clean_row(), "profile": "unknown_profile", "source_identity_resolved": False}
        result = run_volatility_compression_breakout_feature_scan(
            evaluations=[{"precomputed_result": row}],
            persistent_events={},
        )

        reasons = result["top_rejected"][0]["rejection_reasons"]
        self.assertIn("unknown_profile", reasons)
        self.assertIn("source_identity_unresolved", reasons)

    def test_degradation_registry_and_sibling_risk_are_enforced(self) -> None:
        degraded = {
            **_clean_row(),
            "symbol": "ETHUSD",
            "timeframe": "M30",
            "profile": "eth_m30_vol_breakout_chop_guard_v1",
        }
        sibling = {
            **_clean_row(),
            "symbol": "ETHUSD",
            "timeframe": "M30",
            "profile": "volatility_compression_breakout|mode=atr_compression_breakout",
        }
        result = run_volatility_compression_breakout_feature_scan(
            evaluations=[{"precomputed_result": degraded}, {"precomputed_result": sibling}],
            persistent_events={},
        )

        all_reasons = [reason for row in result["top_rejected"] for reason in row["rejection_reasons"]]
        self.assertIn("degraded_by_registry", all_reasons)
        self.assertIn("sibling_risk", all_reasons)
        self.assertEqual(result["recommendation"], "continue_research")

    def test_rejected_summary_is_deduped(self) -> None:
        row = {**_clean_row(), "total_closed": 10, "recent_closed": 5}
        result = run_volatility_compression_breakout_feature_scan(
            evaluations=[
                {"precomputed_result": row},
                {"precomputed_result": dict(row)},
                {"precomputed_result": dict(row)},
            ],
            persistent_events={},
        )

        self.assertEqual(result["evaluations_count"], 1)
        self.assertEqual(result["rejected_summary"][0]["rejected_count"], 1)
        self.assertEqual(len(result["top_rejected"]), 1)

    def test_reads_local_ohlc_csv_without_activation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "GBPUSD_H1_20000.csv"
            path.write_text(_synthetic_csv(280), encoding="utf-8")

            result = run_volatility_compression_breakout_feature_scan(
                csv_dirs=[tmp],
                symbols="GBPUSD",
                timeframes="H1",
                max_rows_per_file=300,
                max_evaluations=5,
                persistent_events={},
            )

        self.assertIn(str(path), result["scanned_csvs"])
        self.assertEqual(result["missing_csvs"], [])
        self.assertGreater(result["evaluations_count"], 0)
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["broker_touched"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_script_smoke_runs_without_broker(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = scan_main(["--symbols", "GBPUSD", "--timeframes", "H1", "--max-evaluations", "3", "--no-persistent"])

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("MT5 Volatility Compression Breakout Feature Scan", text)
        self.assertIn("candidate_activated=False", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)


def _clean_row() -> dict[str, object]:
    return {
        "symbol": "GBPUSD",
        "timeframe": "H1",
        "family": "volatility_compression_breakout",
        "profile": "volatility_compression_breakout|mode=atr_compression_breakout",
        "mode": "atr_compression_breakout",
        "source_identity_resolved": True,
        "total_closed": 72,
        "recent_closed": 24,
        "total_pf": 1.34,
        "recent_pf": 1.22,
        "expectancy": 0.0012,
        "recent_expectancy": 0.0008,
        "spread_x2_pf": 1.02,
        "remove_best_5_pf": 1.01,
        "single_trade_dependency": False,
        "fragile_regime_dependency": False,
    }


def _synthetic_csv(rows: int) -> str:
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    lines = ["time,open,high,low,close,volume"]
    price = 1.2500
    for index in range(rows):
        timestamp = start + timedelta(hours=index)
        drift = 0.00008 if index % 30 < 20 else -0.00002
        if index % 45 in {20, 21, 22, 23, 24}:
            spread = 0.00008
        elif index % 45 == 25:
            spread = 0.0012
            drift = 0.0010
        else:
            spread = 0.00035
        open_price = price
        close = price + drift
        high = max(open_price, close) + spread
        low = min(open_price, close) - spread
        price = close
        lines.append(
            f"{timestamp.isoformat()},{open_price:.5f},{high:.5f},{low:.5f},{close:.5f},100"
        )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    unittest.main()
