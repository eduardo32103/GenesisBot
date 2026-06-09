from __future__ import annotations

import unittest
from unittest.mock import patch

from services.mt5.instrument_resolver import normalize_mt5_symbol, resolve_instrument
from services.mt5.mt5_bridge import mt5_decision, mt5_forward_profile_state, mt5_tick
from services.mt5.mt5_eth_m30_paper_forward_candidate import ETH_M30_CANDIDATE_PROFILE
from services.mt5.mt5_promoted_profile import reset_promoted_profiles_for_tests
from services.mt5.mt5_runtime_snapshot import get_snapshot, reset_runtime_snapshots_for_tests, update_tick


class MT5EthM30RuntimeSnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()

    def test_eth_alias_normalizes_to_ethusd(self) -> None:
        info = resolve_instrument({"symbol": "ETHUSD.b", "currency_base": "ETH", "currency_profit": "USD"})

        self.assertEqual(info["normalized_symbol"], "ETHUSD")
        self.assertEqual(normalize_mt5_symbol("ETHUSD.b"), "ETHUSD")
        self.assertEqual(info["instrument_type"], "crypto_spot")
        self.assertTrue(info["is_spot_crypto"])

    def test_fast_tick_without_timeframe_promotes_eth_to_m30_minimal_snapshot(self) -> None:
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            tick = mt5_tick({"symbol": "ETHUSD.b", "bid": 3199.9, "ask": 3200.1, "last": 3200.0, "spread": 0.2})

        snapshot = get_snapshot("ETHUSD", "M30") or {}
        forward = mt5_forward_profile_state(symbol="ETHUSD", timeframe="M30")

        self.assertTrue(tick["ok"])
        self.assertEqual(tick["status"], "mt5_tick_recorded_fast_path")
        self.assertEqual(tick["tick"]["symbol"], "ETHUSD")
        self.assertEqual(tick["tick"]["timeframe"], "M30")
        self.assertEqual(snapshot["symbol"], "ETHUSD")
        self.assertEqual(snapshot["normalized_symbol"], "ETHUSD")
        self.assertEqual(snapshot["timeframe"], "M30")
        self.assertTrue(snapshot["runtime_snapshot_available"])
        self.assertTrue(snapshot["runtime_snapshot_recent"])
        self.assertFalse(snapshot["runtime_snapshot_complete"])
        self.assertEqual(forward["status"], "observation_only")
        self.assertTrue(forward["runtime_snapshot_available"])
        self.assertTrue(forward["runtime_snapshot_recent"])
        self.assertFalse(forward["runtime_snapshot_complete"])
        self.assertFalse(forward["active"])
        self.assertFalse(forward["applies_to_paper_shadow"])
        self.assertFalse(forward["applies_to_real_trading"])
        self.assertEqual(forward["reason"], "early_forward_edge_failed")
        self.assertEqual(forward.get("degradation_reason"), "early_forward_edge_failed")
        self.assertTrue(forward.get("registry_degraded"))
        self.assertFalse(forward["broker_touched"])
        self.assertFalse(forward["order_executed"])

    def test_decision_uses_fresh_minimal_eth_m30_snapshot_without_creating_trade(self) -> None:
        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            mt5_tick({"symbol": "ETHUSD", "bid": 3199.9, "ask": 3200.1, "last": 3200.0})
            decision = mt5_decision("ETHUSD", timeframe="M30")

        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertNotEqual(decision["reason"], "no_runtime_snapshot_for_requested_timeframe")
        self.assertEqual(decision["reason"], "forward_degraded:early_forward_edge_failed")
        self.assertTrue(decision["runtime_snapshot_available"])
        self.assertTrue(decision["runtime_snapshot_recent"])
        self.assertFalse(decision["runtime_snapshot_complete"])
        self.assertEqual(decision["paper_forward_candidate_profile"], ETH_M30_CANDIDATE_PROFILE)
        self.assertFalse(decision["paper_forward_candidate_active"])
        self.assertFalse(decision["paper_exploration_created"])
        self.assertEqual(decision["shadow_trade_id"], "")
        self.assertTrue(decision.get("registry_degraded"))
        self.assertFalse(decision["applies_to_real_trading"])
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])
        self.assertEqual(decision["order_policy"], "journal_only_no_broker")

    def test_decision_uses_complete_eth_m30_snapshot_when_indicator_context_exists(self) -> None:
        update_tick(
            "ETHUSD",
            {
                "symbol": "ETHUSD",
                "timeframe": "PERIOD_M30",
                "last": 3200.0,
                "spread": 20,
                "score": 62,
                "momentum_score": 61,
                "trend_score": 64,
                "volatility_score": 58,
                "regime": "trend",
                "breakout_confirmed": True,
            },
        )

        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            with patch("services.mt5.mt5_paper_exploration.enqueue_mt5_event", return_value={"queued": True}):
                decision = mt5_decision("ETHUSD", timeframe="M30")

        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertTrue(decision["runtime_snapshot_available"])
        self.assertTrue(decision["runtime_snapshot_recent"])
        self.assertTrue(decision["runtime_snapshot_complete"])
        self.assertEqual(decision["reason"], "forward_degraded:early_forward_edge_failed")
        self.assertEqual(decision["strategy_profile"], "")
        self.assertEqual(decision["paper_forward_candidate_profile"], ETH_M30_CANDIDATE_PROFILE)
        self.assertFalse(decision["paper_forward_candidate_active"])
        self.assertFalse(decision["paper_exploration_created"])
        self.assertEqual(decision["shadow_trade_id"], "")
        self.assertTrue(decision.get("registry_degraded"))
        self.assertFalse(decision.get("paper_probe_allowed"))
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])


if __name__ == "__main__":
    unittest.main()
