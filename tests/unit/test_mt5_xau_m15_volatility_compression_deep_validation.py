from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.run_xau_m15_volatility_compression_deep_validation import main as script_main
from services.mt5.mt5_xau_m15_volatility_compression_deep_validation import (
    run_xau_m15_volatility_compression_deep_validation,
)


class MT5XauM15VolatilityCompressionDeepValidationTests(unittest.TestCase):
    def test_clean_precomputed_variant_becomes_paper_observation_review(self) -> None:
        result = run_xau_m15_volatility_compression_deep_validation(
            variant_results=[_clean_variant()],
            load_persistent=False,
        )

        self.assertEqual(result["recommendation"], "paper_observation_review")
        self.assertTrue(result["paper_observation_ready"])
        self.assertTrue(result["requires_human_approval"])
        self.assertEqual(result["best_variant"]["mode"], "nr7_trailing_defensive")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

        payload = result["compact_persistence_payload"]
        self.assertEqual(payload["payload_type"], "paper_observation_candidate_review")
        self.assertTrue(payload["paper_observation_ready"])
        self.assertFalse(payload["csv_payload_included"])
        self.assertFalse(payload["raw_trades_included"])

    def test_failed_variant_prepares_compact_research_lesson(self) -> None:
        weak = {
            **_clean_variant(),
            "total_pf": 1.01,
            "recent_pf": 0.92,
            "monte_carlo_stressed_pf": 0.71,
            "monte_carlo_stressed_expectancy": -0.0001,
            "spread_x2_pf": 0.82,
            "remove_best_5_pf": 0.78,
        }
        result = run_xau_m15_volatility_compression_deep_validation(
            variant_results=[weak],
            load_persistent=False,
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertFalse(result["paper_observation_ready"])
        self.assertIn("recent_pf", result["rejection_reasons"])
        self.assertIn("monte_carlo_stressed_pf", result["rejection_reasons"])
        self.assertIn("monte_carlo_stressed_expectancy_positive", result["rejection_reasons"])
        self.assertEqual(result["compact_persistence_payload"]["payload_type"], "research_lesson")
        self.assertFalse(result["compact_persistence_payload"]["candidate_activated"])
        self.assertFalse(result["compact_persistence_payload"]["paper_forward_onboarding_started"])

    def test_dependency_and_registry_gates_are_enforced(self) -> None:
        row = {
            **_clean_variant(),
            "symbol": "ETHUSD",
            "timeframe": "M30",
            "profile": "eth_m30_vol_breakout_chop_guard_v1",
            "single_trade_dependency": True,
            "fragile_regime_dependency": True,
        }
        result = run_xau_m15_volatility_compression_deep_validation(
            variant_results=[row],
            load_persistent=False,
        )

        reasons = result["rejection_reasons"]
        self.assertIn("single_trade_dependency_false", reasons)
        self.assertIn("fragile_regime_dependency_false", reasons)
        self.assertIn("no_registry_hit", reasons)
        self.assertIn("no_degradation_hit", reasons)
        self.assertFalse(result["candidate_activated"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_payload_does_not_include_large_research_artifacts(self) -> None:
        result = run_xau_m15_volatility_compression_deep_validation(
            variant_results=[_clean_variant()],
            load_persistent=False,
        )

        text = json.dumps(result["compact_persistence_payload"], sort_keys=True)
        self.assertNotIn("bars_loaded", text)
        self.assertNotIn("metrics_by_window", text)
        self.assertNotIn("trade_list", text)
        self.assertFalse(result["csv_payload_included"])
        self.assertFalse(result["raw_trades_included"])

    def test_missing_csvs_are_reported_without_failure(self) -> None:
        result = run_xau_m15_volatility_compression_deep_validation(
            csv_paths=["missing_xau_m15_file.csv"],
            max_bars=800,
            monte_carlo_simulations=50,
            load_persistent=False,
        )

        self.assertEqual(result["source_csvs_used"], [])
        self.assertEqual(result["missing_csvs"], ["missing_xau_m15_file.csv"])
        self.assertEqual(result["recommendation"], "continue_research")
        self.assertFalse(result["paper_observation_ready"])
        self.assertFalse(result["broker_touched"])

    def test_script_smoke_runs_with_local_temp_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "XAUUSD.b_M15_20000.csv"
            path.write_text(_synthetic_csv(520), encoding="utf-8")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = script_main(
                    [
                        "--csv-paths",
                        str(path),
                        "--max-bars",
                        "520",
                        "--monte-carlo-simulations",
                        "50",
                    ]
                )

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("MT5 XAUUSD M15 Volatility Compression Deep Validation", text)
        self.assertIn("candidate_activated=False", text)
        self.assertIn("paper_forward_onboarding_started=False", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)


def _clean_variant() -> dict[str, object]:
    return {
        "symbol": "XAUUSD",
        "timeframe": "M15",
        "family": "volatility_compression_breakout",
        "profile": "volatility_compression_breakout|mode=nr7_trailing_defensive",
        "mode": "nr7_trailing_defensive",
        "source_identity_resolved": True,
        "data_quality": "ok",
        "total_closed": 86,
        "recent_closed": 24,
        "win_rate": 56.0,
        "recent_win_rate": 58.0,
        "total_pf": 1.42,
        "recent_pf": 1.31,
        "expectancy": 0.0012,
        "recent_expectancy": 0.0009,
        "max_drawdown": 0.008,
        "consecutive_losses": 4,
        "monte_carlo_stressed_pf": 1.09,
        "monte_carlo_stressed_expectancy": 0.0002,
        "monte_carlo_p95_drawdown": 0.015,
        "spread_x1_5_pf": 1.18,
        "spread_x2_pf": 1.03,
        "remove_best_1_pf": 1.24,
        "remove_best_5_pf": 1.05,
        "single_trade_dependency": False,
        "fragile_regime_dependency": False,
        "sample_stability_score": 68.0,
        "cost_model_confidence": "medium",
        "metrics_by_window": [
            {
                "window": "total_sample",
                "closed": 86,
                "win_rate": 56.0,
                "profit_factor": 1.42,
                "expectancy": 0.0012,
            },
            {
                "window": "recent_25_pct",
                "closed": 24,
                "win_rate": 58.0,
                "profit_factor": 1.31,
                "expectancy": 0.0009,
            },
        ],
    }


def _synthetic_csv(rows: int) -> str:
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    lines = ["time,open,high,low,close,volume"]
    price = 2350.0
    for index in range(rows):
        timestamp = start + timedelta(minutes=15 * index)
        drift = 0.18 if index % 40 < 24 else -0.08
        if index % 34 in {20, 21, 22, 23, 24, 25, 26}:
            spread = 0.18
        elif index % 34 == 27:
            spread = 1.3
            drift = 1.1
        else:
            spread = 0.55
        open_price = price
        close = price + drift
        high = max(open_price, close) + spread
        low = min(open_price, close) - spread
        price = close
        lines.append(f"{timestamp.isoformat()},{open_price:.2f},{high:.2f},{low:.2f},{close:.2f},100")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    unittest.main()
