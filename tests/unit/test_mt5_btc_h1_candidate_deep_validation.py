from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts.run_btc_h1_candidate_deep_validation import main as deep_validation_main
from services.mt5.mt5_btc_h1_candidate_deep_validation import (
    CANDIDATE_PROFILE,
    run_btc_h1_candidate_deep_validation,
)


class MT5BtcH1CandidateDeepValidationTests(unittest.TestCase):
    def test_clean_resolved_source_is_paper_observation_review_only(self) -> None:
        result = run_btc_h1_candidate_deep_validation(
            {
                "rows": [_clean_row()],
                "source_identity": _resolved_source(),
                "load_persistent_memory": False,
                "persist_research_lesson": False,
            }
        )

        self.assertEqual(result["recommendation"], "paper_observation_review")
        self.assertTrue(result["paper_observation_ready"])
        self.assertTrue(result["requires_human_approval"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["paper_rotation_applied"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")
        best = result["best_variant"]
        self.assertEqual(best["candidate_status"], "paper_observation_review_ready")
        self.assertEqual(best["rejection_reasons"], [])
        self.assertEqual(best["validation_window"], "recent_25_pct")

    def test_unknown_source_identity_blocks_small_sample_hype(self) -> None:
        result = run_btc_h1_candidate_deep_validation(
            {
                "rows": [_clean_row()],
                "load_persistent_memory": False,
                "persist_research_lesson": False,
            }
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertFalse(result["paper_observation_ready"])
        self.assertFalse(result["source_identity_resolved"])
        self.assertEqual(result["source_identity_status"], "unresolved_unknown_profile_from_tournament_shadow_grouping")
        self.assertIn("source_identity_unresolved", result["best_variant"]["rejection_reasons"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["broker_touched"])

    def test_gates_report_full_rejection_set_for_fragile_row(self) -> None:
        weak = {
            **_clean_row(),
            "profile": "btc_h1_ema_reclaim_volatility_guard",
            "family": "recent_ema_reclaim",
            "total_closed": 49,
            "profit_factor": 1.05,
            "expectancy": -0.01,
            "monte_carlo_stressed_pf": 0.72,
            "spread_x2_pf": 0.8,
            "remove_best_5_pf": 0.9,
            "fragile_regime_dependency": True,
            "single_trade_dependency": True,
            "window_results": [
                {"window": "recent_25_pct", "closed": 18, "win_rate": 60.0, "profit_factor": 1.0, "expectancy": -0.02},
            ],
        }

        result = run_btc_h1_candidate_deep_validation(
            {
                "rows": [weak],
                "source_identity": _resolved_source(),
                "load_persistent_memory": False,
                "persist_research_lesson": False,
            }
        )

        reasons = result["best_variant"]["rejection_reasons"]
        self.assertIn("rejected_by_research_registry", reasons)
        self.assertIn("total_closed_below_50", reasons)
        self.assertIn("recent_closed_below_20", reasons)
        self.assertIn("recent_profit_factor_below_1_15", reasons)
        self.assertIn("profit_factor_below_1_15", reasons)
        self.assertIn("expectancy_not_positive", reasons)
        self.assertIn("recent_expectancy_not_positive", reasons)
        self.assertIn("monte_carlo_stressed_pf_below_1_05", reasons)
        self.assertIn("spread_x2_pf_below_0_95", reasons)
        self.assertIn("remove_best_5_pf_below_1", reasons)
        self.assertIn("single_trade_dependency", reasons)
        self.assertIn("fragile_regime_dependency", reasons)
        self.assertEqual(result["recommendation"], "continue_research")
        self.assertFalse(result["applies_to_real_trading"])

    def test_missing_deep_csvs_are_reported_without_broker_touch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing_40k = Path(tmp) / "BTCUSD_H1_40000.csv"
            missing_60k = Path(tmp) / "BTCUSD_H1_60000.csv"
            result = run_btc_h1_candidate_deep_validation(
                {
                    "csv_paths": f"{missing_40k},{missing_60k}",
                    "processed_source_paths": str(Path(tmp) / "missing_results.csv"),
                    "load_persistent_memory": False,
                    "persist_research_lesson": False,
                }
            )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertEqual(result["variants_evaluated"], 0)
        self.assertEqual(result["csv_used"], [])
        self.assertEqual(len(result["missing_csvs"]), 2)
        self.assertEqual(result["useful_processed_rows"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_failure_prepares_and_persists_compact_research_lesson_when_db_ready(self) -> None:
        store = _FakeStore()
        result = run_btc_h1_candidate_deep_validation(
            {
                "rows": [{**_clean_row(), "total_closed": 30}],
                "source_identity": _resolved_source(),
                "load_persistent_memory": False,
                "store": store,
            }
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertTrue(result["research_lesson_persisted"])
        self.assertEqual(len(store.lessons), 1)
        lesson = store.lessons[0]
        self.assertEqual(lesson["symbol"], "BTCUSD")
        self.assertEqual(lesson["timeframe"], "H1")
        self.assertEqual(lesson["lesson_type"], "paper_candidate_deep_validation")
        self.assertLessEqual(len(lesson["summary"]), 500)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_script_smoke_runs_without_activation_for_missing_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "BTCUSD_H1_40000.csv"
            processed_missing = Path(tmp) / "missing_results.csv"
            with contextlib.redirect_stdout(io.StringIO()):
                code = deep_validation_main(
                    [
                        "--csv-paths",
                        str(missing),
                        "--processed-source-paths",
                        str(processed_missing),
                        "--no-persistent-memory",
                        "--no-persist-research-lesson",
                    ]
                )

        self.assertEqual(code, 0)


def _clean_row() -> dict[str, object]:
    return {
        "symbol": "BTCUSD",
        "timeframe": "H1",
        "profile": CANDIDATE_PROFILE,
        "family": "tournament_edge",
        "total_closed": 72,
        "win_rate": 58.3,
        "profit_factor": 1.42,
        "expectancy": 0.16,
        "max_drawdown": 480.0,
        "consecutive_losses": 2,
        "monte_carlo_stressed_pf": 1.12,
        "monte_carlo_stressed_expectancy": 3.2,
        "monte_carlo_p95_drawdown": 940.0,
        "spread_x1_5_pf": 1.21,
        "spread_x2_pf": 1.08,
        "remove_best_1_pf": 1.31,
        "remove_best_5_pf": 1.03,
        "fragile_regime_dependency": False,
        "single_trade_dependency": False,
        "sample_stability_score": 76.0,
        "cost_model_confidence": "medium",
        "window_results": [
            {"window": "total_sample", "closed": 72, "win_rate": 58.3, "profit_factor": 1.42, "expectancy": 0.16},
            {"window": "recent_25_pct", "closed": 24, "win_rate": 62.5, "profit_factor": 1.31, "expectancy": 0.13},
            {"window": "recent_10_pct", "closed": 10, "win_rate": 60.0, "profit_factor": 1.4, "expectancy": 0.12},
        ],
    }


def _resolved_source() -> dict[str, object]:
    return {
        "candidate_profile_name": CANDIDATE_PROFILE,
        "source_profile_before": "unknown_profile",
        "source_family": "tournament_edge",
        "source_profile": CANDIDATE_PROFILE,
        "source_identity_resolved": True,
        "source_identity_status": "resolved_from_persistent_shadow_context",
    }


class _FakeStore:
    def __init__(self) -> None:
        self.lessons: list[dict[str, object]] = []

    def healthcheck(self, *, write_test_event: bool = False) -> dict[str, object]:
        return {
            "db_available": True,
            "tables_ready": True,
            "db_degraded": False,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def record_research_lesson(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        self.lessons.append(dict(payload))
        return {"ok": True, "db_degraded": False, "critical": bool(critical)}


if __name__ == "__main__":
    unittest.main()
