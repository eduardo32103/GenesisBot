from __future__ import annotations

import unittest

from services.mt5.mt5_eurusd_h1_vwap_reclaim_hardening import run_eurusd_h1_vwap_reclaim_hardening
from services.mt5.mt5_research_rejection_registry import (
    research_rejection,
    research_rejection_registry_status,
)
from services.mt5.mt5_ustec_m30_h1_trend_pullback_hardening import run_ustec_m30_h1_trend_pullback_hardening


class MT5ResearchRejectionRegistryTests(unittest.TestCase):
    def test_registry_status_lists_rejected_paper_only_families(self) -> None:
        status = research_rejection_registry_status()

        self.assertTrue(status["ok"])
        self.assertEqual(status["count"], 9)
        self.assertFalse(status["broker_touched"])
        self.assertFalse(status["order_executed"])
        self.assertEqual(status["order_policy"], "journal_only_no_broker")
        for entry in status["research_rejections"]:
            self.assertTrue(entry["applies_to_paper_forward_candidate"])
            self.assertFalse(entry["applies_to_real_trading"])
            self.assertFalse(entry["allow_future_research"])
            self.assertTrue(entry["allow_manual_override"])
            self.assertTrue(entry["reviewed_at_version"])

    def test_eth_m30_volatility_breakout_cluster_matches_by_profile_or_family(self) -> None:
        by_profile = research_rejection("ETHUSD", "M30", "eth_m30_vol_breakout_regime_filtered_v1")
        by_family = research_rejection("ETHUSD", "M30", "candidate_v2", "recent_volatility_breakout")

        self.assertEqual(
            by_profile["rejection_reason"],
            "eth_m30_volatility_breakout_cluster_degraded_or_sibling_risk",
        )
        self.assertEqual(by_family["rejection_status"], "rejected_after_forward_degradation")
        self.assertFalse(by_profile["broker_touched"])
        self.assertFalse(by_profile["order_executed"])
        self.assertEqual(by_profile["order_policy"], "journal_only_no_broker")

    def test_xau_btc_h1_and_btc_m30_rejected_families_match(self) -> None:
        xau = research_rejection("XAUUSD.b", "M15", "xau_m15_session_baseline", "recent_session_open_continuation")
        btc_h1 = research_rejection("BTCUSD", "H1", "btc_h1_ema_reclaim_volatility_guard", "recent_ema_reclaim")
        btc_h1_tournament = research_rejection("BTCUSD", "H1", "btcusd_h1_tournament_edge_candidate_paper_review_v1")
        btc_h1_liquidity = research_rejection("BTCUSD", "H1", "btcusd_h1_recent_liquidity_sweep_baseline_source_1_deep_validation")
        btc_m30_london = research_rejection("BTCUSD", "M30", "btc_m30_london_us_breakout_strict_trailing")
        btc_m30_fakeout = research_rejection("BTCUSD", "M30", "profile", "opening_range_fakeout")

        self.assertEqual(xau["rejection_reason"], "xau_m15_session_open_continuation_failed_mc_and_remove_best_5")
        self.assertEqual(
            btc_h1["rejection_reason"],
            "btc_h1_ema_reclaim_failed_pf_mc_remove_best_and_dependency_gates",
        )
        self.assertEqual(
            btc_h1_tournament["rejection_reason"],
            "source_identity_unresolved_and_deep_validation_failed",
        )
        self.assertEqual(
            btc_h1_liquidity["rejection_reason"],
            "monte_carlo_fragility_single_trade_dependency",
        )
        self.assertEqual(
            btc_m30_london["rejection_reason"],
            "btc_m30_london_us_breakout_failed_deep_sample_validation",
        )
        self.assertEqual(
            btc_m30_fakeout["rejection_reason"],
            "btc_m30_opening_range_fakeout_correlated_with_failed_london_us_breakout",
        )

    def test_eurusd_h1_session_vwap_reclaim_false_positive_matches_only_cluster(self) -> None:
        rejected = research_rejection("EURUSD", "H1", "session_vwap_reclaim|mode=distance_filter")
        unrelated_timeframe = research_rejection("EURUSD", "M30", "session_vwap_reclaim|mode=distance_filter")
        unrelated_family = research_rejection("EURUSD", "H1", "multi_timeframe_trend_pullback")

        self.assertEqual(rejected["rejection_status"], "rejected_after_real_hardening")
        self.assertEqual(rejected["rejection_reason"], "proxy_false_positive_after_costs_and_mc_failure")
        self.assertTrue(rejected["applies_to_paper_forward_candidate"])
        self.assertFalse(rejected["applies_to_real_trading"])
        self.assertFalse(rejected["allow_future_research"])
        self.assertTrue(rejected["allow_manual_override"])
        self.assertFalse(rejected["broker_touched"])
        self.assertFalse(rejected["order_executed"])
        self.assertEqual(rejected["order_policy"], "journal_only_no_broker")
        self.assertEqual(unrelated_timeframe, {})
        self.assertEqual(unrelated_family, {})

    def test_ustec_m30_trend_pullback_false_positive_matches_aliases_only_cluster(self) -> None:
        rejected = research_rejection("USTEC", "M30", "multi_timeframe_trend_pullback|mode=rsi_filter|higher=H1")
        alias = research_rejection("NAS100", "M30", "ustec_m30_h1_trend_pullback_fast_loss_cut", "trend_pullback")
        unrelated_timeframe = research_rejection("USTEC.b", "M15", "multi_timeframe_trend_pullback|mode=baseline|higher=H1")
        unrelated_symbol = research_rejection("US500", "M30", "multi_timeframe_trend_pullback|mode=baseline|higher=H1")

        self.assertEqual(rejected["rejection_status"], "rejected_after_real_hardening")
        self.assertEqual(rejected["rejection_reason"], "proxy_false_positive_after_monte_carlo_failure")
        self.assertEqual(rejected["higher_timeframe"], "H1")
        self.assertEqual(alias["rejection_reason"], "proxy_false_positive_after_monte_carlo_failure")
        self.assertFalse(rejected["applies_to_real_trading"])
        self.assertFalse(rejected["broker_touched"])
        self.assertFalse(rejected["order_executed"])
        self.assertEqual(rejected["order_policy"], "journal_only_no_broker")
        self.assertEqual(unrelated_timeframe, {})
        self.assertEqual(unrelated_symbol, {})

    def test_rejection_registry_allows_eurusd_session_vwap_reclaim_candidate_review(self) -> None:
        rejected = research_rejection(
            "EURUSD",
            "H1",
            "eurusd_h1_vwap_reclaim_distance_filter",
            "session_vwap_reclaim",
            "session_vwap_reclaim",
        )

        self.assertEqual(rejected, {})

    def test_rejection_registry_allows_ustec_multi_timeframe_trend_pullback_candidate_review(self) -> None:
        rejected = research_rejection(
            "USTEC",
            "M30",
            "ustec_m30_h1_trend_pullback_rsi_filter",
            "multi_timeframe_trend_pullback",
            "multi_timeframe_trend_pullback",
        )

        self.assertEqual(rejected, {})

    def test_rejection_registry_rejects_explicit_sample_valid_false(self) -> None:
        rejected = research_rejection(
            "EURUSD",
            "H1",
            "eurusd_h1_vwap_reclaim_distance_filter",
            "session_vwap_reclaim",
            candidate={"sample_valid": False},
        )

        self.assertEqual(rejected["rejection_status"], "rejected_invalid_sample")
        self.assertIn("sample_valid_false", rejected["rejection_reason"])
        self.assertFalse(rejected["broker_touched"])
        self.assertFalse(rejected["order_executed"])
        self.assertEqual(rejected["order_policy"], "journal_only_no_broker")

    def test_rejection_registry_rejects_frozen_market_candidate(self) -> None:
        rejected = research_rejection(
            "USTEC",
            "M30",
            "ustec_m30_h1_trend_pullback_rsi_filter",
            "multi_timeframe_trend_pullback",
            candidate={"frozen_market_detected": True},
        )

        self.assertEqual(rejected["rejection_status"], "rejected_invalid_sample")
        self.assertIn("frozen_market_detected", rejected["rejection_reason"])
        self.assertFalse(rejected["broker_touched"])
        self.assertFalse(rejected["order_executed"])
        self.assertEqual(rejected["order_policy"], "journal_only_no_broker")

    def test_candidate_review_does_not_activate_candidate(self) -> None:
        result = run_eurusd_h1_vwap_reclaim_hardening(
            {
                "rows": [
                    {
                        "profile": "eurusd_h1_vwap_reclaim_distance_filter",
                        "hardening_actions": ["distance_filter"],
                        "recent_closed": 24,
                        "total_closed": 80,
                        "recent_pf": 1.25,
                        "total_pf": 1.36,
                        "expectancy": 0.00012,
                        "monte_carlo_stressed_pf": 1.09,
                        "monte_carlo_stressed_expectancy": 0.00004,
                        "spread_x2_pf": 1.2,
                        "remove_best_5_pf": 1.05,
                        "max_drawdown": 0.002,
                        "fragile_regime_dependency": False,
                        "single_trade_dependency": False,
                        "data_quality": "ok",
                    }
                ]
            }
        )

        self.assertEqual(result["recommendation"], "paper_forward_candidate_review")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_candidate_review_does_not_start_paper_forward_onboarding(self) -> None:
        result = run_ustec_m30_h1_trend_pullback_hardening(
            {
                "rows": [
                    {
                        "profile": "ustec_m30_h1_trend_pullback_rsi_filter",
                        "hardening_actions": ["rsi_filter"],
                        "recent_closed": 24,
                        "total_closed": 80,
                        "recent_pf": 1.22,
                        "total_pf": 1.34,
                        "expectancy": 0.0008,
                        "monte_carlo_stressed_pf": 1.08,
                        "monte_carlo_stressed_expectancy": 0.0002,
                        "spread_x2_pf": 1.14,
                        "remove_best_5_pf": 1.04,
                        "max_drawdown": 0.01,
                        "fragile_regime_dependency": False,
                        "single_trade_dependency": False,
                        "data_quality": "ok",
                    }
                ]
            }
        )

        self.assertEqual(result["recommendation"], "paper_forward_candidate_review")
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_unrelated_candidate_is_not_rejected(self) -> None:
        self.assertEqual(
            research_rejection("US500", "M30", "us500_m30_failed_breakout_reversal_clean", "recent_failed_breakout_reversal"),
            {},
        )
        self.assertEqual(research_rejection("BTCUSD", "M30", "btcusd_m30_recent_liquidity_sweep"), {})


if __name__ == "__main__":
    unittest.main()
