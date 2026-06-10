from __future__ import annotations

import unittest
from unittest.mock import patch

from services.mt5.mt5_adaptive_strategy_governor import adaptive_governor_enforcement
from services.mt5.mt5_paper_exploration import evaluate_paper_exploration
from services.mt5.mt5_risk_guard import MT5BridgeConfig
from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests


class MT5AdaptiveStrategyGovernorEnforcementTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_runtime_snapshots_for_tests()

    def test_open_shadow_over_limit_blocks_new_shadow(self) -> None:
        result = adaptive_governor_enforcement(
            symbol="BTCUSD",
            timeframe="M30",
            profile="btc_m30_test_profile",
            governor_result=_governor_result(
                "kill_switch",
                "kill_switch",
                [{"name": "max_open_shadow_trades", "active": True, "critical": True}],
            ),
        )

        self.assertTrue(result["blocked"])
        self.assertFalse(result["safe_to_open_new_shadow"])
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "adaptive_governor:max_open_shadow_trades")
        self.assertFalse(result["paper_exploration_created"])
        self.assertEqual(result["shadow_trade_id"], "")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_global_kill_switch_returns_no_trade(self) -> None:
        result = adaptive_governor_enforcement(
            symbol="BTCUSD",
            timeframe="M30",
            profile="btc_m30_test_profile",
            governor_result=_governor_result("kill_switch", "kill_switch", []),
        )

        self.assertTrue(result["blocked"])
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "adaptive_governor:kill_switch")
        self.assertFalse(result["paper_exploration_created"])
        self.assertEqual(result["shadow_trade_id"], "")

    def test_missing_data_returns_no_trade(self) -> None:
        result = adaptive_governor_enforcement(
            symbol="BTCUSD",
            timeframe="M30",
            profile="btc_m30_test_profile",
            governor_result=_governor_result("no_trade", "continue_research", [{"name": "missing_shadow_trade_data", "active": True, "critical": False}]),
        )

        self.assertTrue(result["blocked"])
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "adaptive_governor:missing_data")
        self.assertFalse(result["paper_exploration_created"])
        self.assertEqual(result["shadow_trade_id"], "")

    def test_degradation_registry_profile_blocks_new_shadow(self) -> None:
        result = adaptive_governor_enforcement(
            symbol="ETHUSD",
            timeframe="M30",
            profile="eth_m30_vol_breakout_chop_guard_v1",
            governor_result=_governor_result("watch", "rotate_candidate_review", []),
        )

        self.assertTrue(result["blocked"])
        self.assertTrue(result["degraded_by_registry"])
        self.assertEqual(result["reason"], "adaptive_governor:observation_only")
        self.assertFalse(result["paper_exploration_created"])
        self.assertEqual(result["shadow_trade_id"], "")

    def test_research_rejection_registry_profile_blocks_new_shadow(self) -> None:
        result = adaptive_governor_enforcement(
            symbol="EURUSD",
            timeframe="H1",
            profile="eurusd_h1_session_vwap_reclaim_distance_filter",
            governor_result=_governor_result("watch", "rotate_candidate_review", []),
        )

        self.assertTrue(result["blocked"])
        self.assertTrue(result["rejected_by_research_registry"])
        self.assertEqual(result["reason"], "adaptive_governor:skip_rejected_family")
        self.assertFalse(result["paper_exploration_created"])
        self.assertEqual(result["shadow_trade_id"], "")

    def test_sibling_risk_blocks_new_shadow(self) -> None:
        result = adaptive_governor_enforcement(
            symbol="US500",
            timeframe="M30",
            profile="us500_m30_sibling_candidate",
            governor_result={
                **_governor_result("watch", "rotate_candidate_review", []),
                "rejected_candidates": [
                    {
                        "symbol": "US500",
                        "timeframe": "M30",
                        "profile": "us500_m30_sibling_candidate",
                        "sibling_risk": True,
                        "sibling_risk_reason": "similar_to_degraded_forward_profile",
                    }
                ],
            },
        )

        self.assertTrue(result["blocked"])
        self.assertTrue(result["sibling_risk"])
        self.assertEqual(result["reason"], "adaptive_governor:sibling_risk")
        self.assertFalse(result["paper_exploration_created"])
        self.assertEqual(result["shadow_trade_id"], "")

    def test_paper_exploration_uses_adaptive_enforcement_before_opening_shadow(self) -> None:
        blocked = {
            "ok": True,
            "blocked": True,
            "allowed": False,
            "decision": "NO_TRADE",
            "reason": "adaptive_governor:kill_switch",
            "adaptive_governor_global_state": "kill_switch",
            "adaptive_governor_recommended_next_action": "kill_switch",
            "circuit_breakers": [{"name": "max_open_shadow_trades", "active": True, "critical": True}],
            "paper_exploration_created": False,
            "shadow_trade_id": "",
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
        with patch("services.mt5.mt5_paper_exploration.adaptive_governor_enforcement", return_value=blocked):
            result = evaluate_paper_exploration(
                "BTCUSD",
                tick={
                    "symbol": "BTCUSD",
                    "timeframe": "M30",
                    "last": 100000.0,
                    "spread": 1.0,
                    "score": 80.0,
                    "momentum_score": 80.0,
                    "trend_score": 80.0,
                    "volatility_score": 80.0,
                    "regime": "trend",
                },
                config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True),
                trigger="decision",
                timeframe="M30",
            )

        self.assertFalse(result["paper_exploration_created"])
        self.assertTrue(result["adaptive_governor_blocked"])
        self.assertEqual(result["paper_exploration_reason"], "adaptive_governor:kill_switch")
        self.assertEqual(result["shadow_trade_id"], "")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")


def _governor_result(global_state: str, recommended_next_action: str, circuit_breakers: list[dict[str, object]]) -> dict[str, object]:
    return {
        "ok": True,
        "global_state": global_state,
        "recommended_next_action": recommended_next_action,
        "circuit_breakers": circuit_breakers,
        "active_profiles": [],
        "paused_profiles": [],
        "degraded_profiles": [],
        "rotation_candidates": [],
        "rejected_candidates": [],
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


if __name__ == "__main__":
    unittest.main()
