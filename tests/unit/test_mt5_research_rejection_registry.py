from __future__ import annotations

import unittest

from services.mt5.mt5_research_rejection_registry import (
    research_rejection,
    research_rejection_registry_status,
)


class MT5ResearchRejectionRegistryTests(unittest.TestCase):
    def test_registry_status_lists_rejected_paper_only_families(self) -> None:
        status = research_rejection_registry_status()

        self.assertTrue(status["ok"])
        self.assertEqual(status["count"], 6)
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
        btc_m30_london = research_rejection("BTCUSD", "M30", "btc_m30_london_us_breakout_strict_trailing")
        btc_m30_fakeout = research_rejection("BTCUSD", "M30", "profile", "opening_range_fakeout")

        self.assertEqual(xau["rejection_reason"], "xau_m15_session_open_continuation_failed_mc_and_remove_best_5")
        self.assertEqual(
            btc_h1["rejection_reason"],
            "btc_h1_ema_reclaim_failed_pf_mc_remove_best_and_dependency_gates",
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

    def test_unrelated_candidate_is_not_rejected(self) -> None:
        self.assertEqual(
            research_rejection("US500", "M30", "us500_m30_failed_breakout_reversal_clean", "recent_failed_breakout_reversal"),
            {},
        )


if __name__ == "__main__":
    unittest.main()
