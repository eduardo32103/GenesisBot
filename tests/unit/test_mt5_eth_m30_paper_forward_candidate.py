from __future__ import annotations

import unittest
from unittest.mock import patch

from scripts.run_eth_m30_paper_forward_onboarding import main as onboarding_main
from services.mt5.mt5_bridge import mt5_forward_profile_state, mt5_promoted_profile
from services.mt5.mt5_eth_m30_paper_forward_candidate import (
    ETH_M30_CANDIDATE_PROFILE,
    eth_m30_forward_profile_state,
)
from services.mt5.mt5_promoted_profile import reset_promoted_profiles_for_tests
from services.mt5.mt5_risk_guard import MT5BridgeConfig
from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_open_shadow_trade, update_tick
from services.mt5.mt5_signal_router import MT5SignalRouter


class MT5EthM30PaperForwardCandidateTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()

    def test_candidate_state_exists_without_real_promotion(self) -> None:
        state = eth_m30_forward_profile_state(symbol="ETHUSD", timeframe="M30")
        promoted = mt5_promoted_profile(symbol="ETHUSD", timeframe="M30")

        self.assertEqual(state["status"], "paper_forward_candidate")
        self.assertEqual(state["profile"], ETH_M30_CANDIDATE_PROFILE)
        self.assertFalse(state["active"])
        self.assertFalse(state["applies_to_paper_shadow"])
        self.assertFalse(state["applies_to_real_trading"])
        self.assertEqual(state["reason"], "no_runtime_snapshot_for_requested_timeframe")
        self.assertTrue(state["metadata"]["capital_preservation_passed"])
        self.assertFalse(state["automatic_promotion"])
        self.assertFalse(state["promoted_profile_mutated"])
        self.assertFalse(state["forward_state_mutated"])
        self.assertEqual(promoted["status"], "observation_only")
        self.assertEqual(promoted["profile"], "")
        self.assertFalse(promoted["active"])
        self.assertFalse(state["broker_touched"])
        self.assertFalse(state["order_executed"])

    def test_forward_state_activates_only_with_runtime_snapshot_and_risk_pass(self) -> None:
        update_tick("ETHUSD", _eth_tick(last=3200.0, previous=False))

        state = mt5_forward_profile_state(symbol="ETHUSD", timeframe="M30")

        self.assertEqual(state["status"], "paper_forward_candidate")
        self.assertTrue(state["active"])
        self.assertTrue(state["applies_to_paper_shadow"])
        self.assertFalse(state["applies_to_real_trading"])
        self.assertTrue(state["risk_governor_allowed"])
        self.assertEqual(state["risk_governor_reason"], "risk_governor_pass")
        self.assertFalse(state["broker_touched"])
        self.assertFalse(state["order_executed"])

    def test_decision_without_snapshot_is_no_trade_but_exposes_candidate_metadata(self) -> None:
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))

        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            decision = router.decision("ETHUSD", timeframe="M30")

        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertEqual(decision["reason"], "no_runtime_snapshot_for_requested_timeframe")
        self.assertEqual(decision["paper_forward_candidate_profile"], ETH_M30_CANDIDATE_PROFILE)
        self.assertFalse(decision["paper_forward_candidate_active"])
        self.assertEqual(decision["strategy_profile"], "")
        self.assertFalse(decision["applies_to_real_trading"])
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])

    def test_decision_creates_only_paper_shadow_when_snapshot_and_risk_allow(self) -> None:
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        update_tick("ETHUSD", _eth_tick(last=3200.0, previous=True))
        update_tick("ETHUSD", _eth_tick(last=3212.0, previous=False))

        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            with patch("services.mt5.mt5_paper_exploration.enqueue_mt5_event", return_value={"queued": True}):
                decision = router.decision("ETHUSD", timeframe="M30")

        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertEqual(decision["strategy_profile"], ETH_M30_CANDIDATE_PROFILE)
        self.assertEqual(decision["paper_forward_candidate_profile"], ETH_M30_CANDIDATE_PROFILE)
        self.assertTrue(decision["paper_forward_candidate_active"])
        self.assertTrue(decision["paper_exploration_created"])
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])
        self.assertEqual(decision["order_policy"], "journal_only_no_broker")

    def test_risk_governor_blocks_high_spread_candidate(self) -> None:
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        update_tick("ETHUSD", _eth_tick(last=3200.0, spread=99.0))

        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            decision = router.decision("ETHUSD", timeframe="M30")

        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertFalse(decision["risk_governor_allowed"])
        self.assertEqual(decision["risk_governor_reason"], "spread_too_high")
        self.assertIn("risk_governor_block", decision["reason"])
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])

    def test_open_shadow_trade_blocks_new_eth_m30_entry(self) -> None:
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        update_tick("ETHUSD", _eth_tick(last=3200.0))
        update_open_shadow_trade(
            "ETHUSD",
            {
                "shadow_trade_id": "eth-open-1",
                "symbol": "ETHUSD",
                "timeframe": "M30",
                "status": "open",
                "lifecycle_status": "open",
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            },
            timeframe="M30",
        )

        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            decision = router.decision("ETHUSD", timeframe="M30")

        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertFalse(decision["risk_governor_allowed"])
        self.assertEqual(decision["risk_governor_reason"], "max_open_trades_reached")
        self.assertFalse(decision["paper_exploration_created"])
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])

    def test_btc_and_other_timeframes_remain_unchanged(self) -> None:
        btc_state = mt5_forward_profile_state(symbol="BTCUSD", timeframe="M30")
        h1_state = mt5_forward_profile_state(symbol="ETHUSD", timeframe="H1")
        m15_state = mt5_forward_profile_state(symbol="ETHUSD", timeframe="M15")

        self.assertNotEqual(btc_state.get("profile"), ETH_M30_CANDIDATE_PROFILE)
        self.assertEqual(h1_state["status"], "observation_only")
        self.assertEqual(m15_state["status"], "observation_only")
        self.assertFalse(h1_state["applies_to_real_trading"])
        self.assertFalse(m15_state["applies_to_real_trading"])

    def test_onboarding_script_smoke(self) -> None:
        code = onboarding_main(["--symbol", "ETHUSD", "--timeframe", "M30"])
        self.assertEqual(code, 0)


def _eth_tick(*, last: float, spread: float = 20.0, previous: bool = False) -> dict[str, object]:
    return {
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "last": last,
        "spread": spread,
        "score": 62,
        "momentum_score": 61,
        "trend_score": 64,
        "volatility_score": 58,
        "regime": "trend",
        "hour": 14,
        "breakout_confirmed": not previous,
    }


if __name__ == "__main__":
    unittest.main()
