from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts.run_research_intelligence_core import main as intelligence_main
from services.mt5.mt5_research_intelligence_core import run_research_intelligence_core


class MT5ResearchIntelligenceCoreTests(unittest.TestCase):
    def test_core_builds_safe_research_plan_from_registries_and_processed_outputs(self) -> None:
        result = run_research_intelligence_core(
            rotation_result=_rotation_result(),
            expansion_result=_expansion_result(),
            discovery_result=_discovery_result(),
        )

        self.assertEqual(result["recommendation"], "research_plan_ready")
        self.assertEqual(result["mode"], "processed_sources_and_registries_only")
        self.assertFalse(result["offline_backtests_run"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")
        self.assertTrue(result["rejected_clusters"])
        self.assertTrue(result["failure_patterns"])
        self.assertTrue(result["avoid_next"])
        self.assertTrue(result["next_hypotheses"])
        self.assertTrue(result["priority_queue"])
        self.assertIn(
            result["priority_queue"][0]["family_name"],
            {"multi_timeframe_trend_pullback", "volatility_compression_breakout"},
        )
        self.assertIn(
            result["recommended_next_research_phase"],
            {
                "design_multi_timeframe_trend_pullback_processed_feature_scan",
                "design_volatility_compression_breakout_processed_feature_scan",
            },
        )

    def test_failure_patterns_cover_closed_research_branches(self) -> None:
        result = run_research_intelligence_core(
            rotation_result=_rotation_result(),
            expansion_result=_expansion_result(),
            discovery_result=_discovery_result(),
        )
        categories = {row["category"] for row in result["failure_patterns"]}

        for category in {
            "degraded_forward_profile",
            "sibling_of_failed_profile",
            "monte_carlo_fragility",
            "remove_best_dependency",
            "fragile_regime_dependency",
            "single_trade_dependency",
            "total_pf_weakness",
            "insufficient_recent_sample",
            "unstable_deep_sample",
            "proxy_false_positive_after_costs",
            "cost_adjusted_edge_failure",
            "feature_scan_to_hardening_decay",
        }:
            self.assertIn(category, categories)

        rejected_labels = {f"{row['symbol']} {row['timeframe']}" for row in result["rejected_clusters"]}
        self.assertIn("ETHUSD M30", rejected_labels)
        self.assertIn("XAUUSD M15", rejected_labels)
        self.assertIn("BTCUSD H1", rejected_labels)
        self.assertIn("BTCUSD M30", rejected_labels)
        self.assertIn("EURUSD H1", rejected_labels)

    def test_hypotheses_do_not_repeat_rejected_family_markers(self) -> None:
        result = run_research_intelligence_core(
            rotation_result=_rotation_result(),
            expansion_result=_expansion_result(),
            discovery_result=_discovery_result(),
        )
        names = {row["family_name"] for row in result["next_hypotheses"]}

        self.assertIn("session_vwap_reclaim", names)
        self.assertIn("volatility_compression_breakout", names)
        self.assertIn("multi_timeframe_trend_pullback", names)
        session = [row for row in result["priority_queue"] if row["family_name"] == "session_vwap_reclaim"]
        self.assertEqual(len(session), 1)
        self.assertGreater(result["priority_queue"][0]["priority_score"], session[0]["priority_score"])
        for rejected in {
            "recent_session_open_continuation",
            "recent_ema_reclaim",
            "recent_london_us_breakout",
            "opening_range_fakeout",
            "volatility_breakout",
        }:
            self.assertNotIn(rejected, names)
        for hypothesis in result["next_hypotheses"]:
            self.assertIn("required_metrics", hypothesis)
            self.assertIn("priority_score", hypothesis)
            self.assertIn("max_offline_evaluations_suggested", hypothesis)
            self.assertIn("heavy_backtest_required", hypothesis)

    def test_watchlist_logic_marks_sample_only_opportunity_for_revisit(self) -> None:
        result = run_research_intelligence_core(
            rotation_result=_rotation_result(),
            expansion_result=_expansion_result(),
            discovery_result=_discovery_result(),
        )

        watchlist = [
            row
            for row in result["unresolved_opportunities"]
            if row["profile"] == "us500_m30_failed_breakout_reversal_sample_watch"
        ]
        self.assertEqual(len(watchlist), 1)
        self.assertEqual(watchlist[0]["recommended_action"], "watchlist_revisit_later")

    def test_script_runs_fast_without_heavy_backtests_or_activation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = intelligence_main(["--results-dir", str(Path(tmp))])

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("Genesis Research Intelligence Core", text)
        self.assertIn("mode=processed_sources_and_registries_only", text)
        self.assertIn("recommendation=research_plan_ready", text)
        self.assertIn("offline_backtests_run=False", text)
        self.assertIn("candidate_activated=False", text)
        self.assertIn("paper_forward_onboarding_started=False", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_executed=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)


def _rotation_result() -> dict[str, object]:
    return {
        "status": "paper_forward_candidate_rotation_ready",
        "recommendation": "continue_research",
        "useful_rows": 4,
        "loaded_sources": ["processed.csv"],
        "ranking": [
            {
                "symbol": "XAUUSD",
                "timeframe": "M15",
                "profile": "xauusd_m15_recent_session_open_continuation",
                "family": "recent_session_open_continuation",
                "candidate_status": "excluded_by_research_rejection_registry",
                "research_rejection_reason": "xau_m15_session_open_continuation_failed_mc_and_remove_best_5",
                "rejection_reasons": ["research_rejection_registry"],
                "rejected_by_research_registry": True,
            }
        ],
        "excluded_by_research_rejection_registry": [],
    }


def _expansion_result() -> dict[str, object]:
    return {
        "status": "paper_forward_research_expansion_ready",
        "recommendation": "continue_research",
        "useful_rows": 4,
        "loaded_sources": ["processed.csv"],
        "near_misses": [
            {
                "symbol": "US500",
                "timeframe": "M30",
                "profile": "us500_m30_failed_breakout_reversal_sample_watch",
                "family": "recent_failed_breakout_reversal",
                "candidate_status": "near_miss_hardening_candidate",
                "failed_gates": ["recent_closed_below_15"],
                "rejection_reasons": ["recent_closed_below_15"],
            }
        ],
        "ranking": [],
    }


def _discovery_result() -> dict[str, object]:
    return {
        "status": "new_family_edge_discovery_ready",
        "recommendation": "continue_research",
        "mode": "processed_sources_only",
        "useful_rows": 4,
        "loaded_sources": ["processed.csv"],
        "offline_backtests_run": False,
        "ranking": [
            {
                "symbol": "BTCUSD",
                "timeframe": "M30",
                "profile": "btcusd_m30_recent_liquidity_sweep",
                "family": "recent_liquidity_sweep",
                "conceptual_family": "liquidity_sweep_reversal",
                "candidate_status": "research_gate_failed",
                "rejection_reasons": [
                    "monte_carlo_stressed_pf_below_1_05",
                    "fragile_regime_dependency",
                ],
                "fragile_regime_dependency": True,
            }
        ],
        "top_near_misses": [
            {
                "symbol": "US500",
                "timeframe": "M30",
                "profile": "us500_m30_failed_breakout_reversal_sample_watch",
                "family": "recent_failed_breakout_reversal",
                "conceptual_family": "range_breakout_failed_retest",
                "candidate_status": "near_miss",
                "rejection_reasons": ["recent_closed_below_15"],
            }
        ],
        "excluded_by_registry_or_sibling_risk": [
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "profile": "btc_h1_ema_reclaim_volatility_guard",
                "family": "recent_ema_reclaim",
                "candidate_status": "excluded_by_research_rejection_registry",
                "research_rejection_reason": "btc_h1_ema_reclaim_failed_pf_mc_remove_best_and_dependency_gates",
                "rejected_by_research_registry": True,
            }
        ],
        "skipped_family_ideas": [
            {
                "family": "session_vwap_reclaim",
                "reason": "not_implemented_in_current_offline_signal_set",
                "next_step": "add VWAP feature generation before backtest",
            }
        ],
    }


if __name__ == "__main__":
    unittest.main()
