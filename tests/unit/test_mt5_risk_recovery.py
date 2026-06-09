from __future__ import annotations

import unittest

from api.main import create_app
from api.routes.genesis import get_genesis_mt5_risk_recovery
from services.mt5.mt5_eth_m30_forward_degradation import eth_m30_forward_degradation_status
from services.mt5.mt5_eth_m30_paper_forward_candidate import eth_m30_forward_profile_state
from services.mt5.mt5_promoted_profile import get_promoted_profile, reset_promoted_profiles_for_tests
from services.mt5.mt5_risk_recovery import mt5_risk_recovery_status
from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_snapshot


class MT5RiskRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()

    def test_explicit_performance_flag_blocks_despite_positive_metrics(self) -> None:
        _seed_eth_m30(
            summary={
                "closed": 7,
                "wins": 3,
                "losses": 4,
                "win_rate": 42.86,
                "profit_factor": 3.697,
                "expectancy": 0.1133,
                "negative_recent_edge": True,
            },
            adaptive={"current_loss_streak": 3, "negative_edge": False},
        )

        result = mt5_risk_recovery_status("ETHUSD", timeframe="M30")

        self.assertFalse(result["risk_governor_allowed"])
        self.assertEqual(result["risk_governor_reason"], "recent_edge_negative")
        self.assertEqual(result["recovery_status"], "blocked_by_explicit_recent_edge_flag")
        self.assertTrue(result["blocker_source"]["latest_performance_summary.negative_recent_edge"])
        self.assertFalse(result["blocker_source"]["computed_recent_pf_rule"])
        self.assertTrue(result["recovery_requirements"]["clear_explicit_negative_edge_flag"]["required"])
        self.assertTrue(result["recovery_requirements"]["recent_profit_factor_at_least_1"]["satisfied"])
        self.assertTrue(result["recovery_requirements"]["recent_expectancy_above_0"]["satisfied"])
        self.assertTrue(result["indefinite_block_risk"])
        self.assertIn("review_flag_source", result["recommended_action"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_computed_recent_pf_rule_blocks_until_recent_metrics_recover(self) -> None:
        _seed_eth_m30(
            summary={
                "closed": 10,
                "wins": 4,
                "losses": 6,
                "win_rate": 40.0,
                "profit_factor": 0.9,
                "expectancy": 0.0,
                "negative_recent_edge": False,
            }
        )

        result = mt5_risk_recovery_status("ETHUSD", timeframe="M30")

        self.assertFalse(result["risk_governor_allowed"])
        self.assertEqual(result["recovery_status"], "blocked_by_computed_recent_pf_rule")
        self.assertTrue(result["blocker_source"]["computed_recent_pf_rule"])
        self.assertFalse(result["blocker_source"]["latest_performance_summary.negative_recent_edge"])
        self.assertTrue(result["recovery_requirements"]["recent_closed_less_than_10"]["required"])
        self.assertTrue(result["recovery_requirements"]["recent_profit_factor_at_least_1"]["required"])
        self.assertTrue(result["recovery_requirements"]["recent_expectancy_above_0"]["required"])
        self.assertFalse(result["indefinite_block_risk"])

    def test_adaptive_negative_edge_flag_is_reported_as_exact_source(self) -> None:
        _seed_eth_m30(
            summary={
                "closed": 7,
                "wins": 5,
                "losses": 2,
                "win_rate": 71.43,
                "profit_factor": 2.0,
                "expectancy": 0.2,
                "negative_recent_edge": False,
            },
            adaptive={"current_loss_streak": 0, "negative_edge": True},
        )

        result = get_genesis_mt5_risk_recovery(symbol="ETHUSD", timeframe="M30")

        self.assertFalse(result["risk_governor_allowed"])
        self.assertEqual(result["risk_governor_reason"], "recent_edge_negative")
        self.assertTrue(result["blocker_source"]["latest_adaptive_state.negative_edge"])
        self.assertEqual(
            result["recovery_requirements"]["clear_explicit_negative_edge_flag"]["sources"],
            ["latest_adaptive_state.negative_edge"],
        )
        self.assertTrue(result["indefinite_block_risk"])

    def test_recovered_metrics_without_explicit_flag_pass_observation_only(self) -> None:
        _seed_eth_m30(
            summary={
                "closed": 7,
                "wins": 5,
                "losses": 2,
                "win_rate": 71.43,
                "profit_factor": 1.2,
                "expectancy": 0.1,
                "negative_recent_edge": False,
            },
            adaptive={"current_loss_streak": 0, "negative_edge": False},
        )

        result = mt5_risk_recovery_status("ETHUSD", timeframe="M30")

        self.assertTrue(result["risk_governor_allowed"])
        self.assertEqual(result["risk_governor_reason"], "risk_governor_pass")
        self.assertEqual(result["recovery_status"], "risk_governor_pass")
        self.assertFalse(result["indefinite_block_risk"])
        self.assertEqual(result["recommended_action"][0], "continue_observation")
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["automatic_promotion"])

    def test_create_app_exposes_risk_recovery_endpoint(self) -> None:
        app = create_app()

        self.assertEqual(
            app["genesis_mt5_risk_recovery_endpoint"],
            "/api/genesis/mt5/risk-recovery?symbol={symbol}&timeframe={timeframe}",
        )

    def test_forward_degradation_guardrail_does_not_degrade_with_open_shadow_trade(self) -> None:
        _seed_eth_m30(summary=_failed_early_forward_summary(), open_shadow=True)

        result = eth_m30_forward_degradation_status(symbol="ETHUSD", timeframe="M30")

        self.assertFalse(result["should_degrade"])
        self.assertEqual(result["open_shadow_count"], 1)
        self.assertEqual(result["recommendation"], "continue_observation")
        self.assertFalse(result["promoted_profile_mutated"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_forward_degradation_guardrail_degrades_failed_early_forward(self) -> None:
        _seed_eth_m30(summary=_failed_early_forward_summary(), adaptive={"current_loss_streak": 3, "negative_edge": True})

        result = eth_m30_forward_degradation_status(symbol="ETHUSD", timeframe="M30")
        state = eth_m30_forward_profile_state(symbol="ETHUSD", timeframe="M30")
        recovery = mt5_risk_recovery_status("ETHUSD", timeframe="M30")

        self.assertTrue(result["should_degrade"])
        self.assertEqual(result["open_shadow_count"], 0)
        self.assertEqual(result["recommendation"], "degrade_to_observation_only")
        self.assertTrue(result["whether_degradation_is_safe"])
        self.assertEqual(result["degradation_reason"], "early_forward_edge_failed")
        self.assertEqual(result["new_status"], "observation_only")
        self.assertEqual(state["status"], "observation_only")
        self.assertFalse(state["active"])
        self.assertFalse(state["applies_to_paper_shadow"])
        self.assertFalse(state["applies_to_real_trading"])
        self.assertEqual(state["degradation_reason"], "early_forward_edge_failed")
        self.assertIn("degrade_to_observation_only", recovery["recommended_action"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_forward_degradation_guardrail_does_not_mutate_promoted_profile(self) -> None:
        before = get_promoted_profile(symbol="ETHUSD", timeframe="M30")
        _seed_eth_m30(summary=_failed_early_forward_summary())

        state = eth_m30_forward_profile_state(symbol="ETHUSD", timeframe="M30")
        after = get_promoted_profile(symbol="ETHUSD", timeframe="M30")

        self.assertEqual(before["status"], "observation_only")
        self.assertEqual(after["status"], "observation_only")
        self.assertEqual(after["profile"], "")
        self.assertEqual(state["status"], "observation_only")
        self.assertFalse(state["automatic_promotion"])
        self.assertFalse(state["promoted_profile_mutated"])
        self.assertFalse(state["forward_state_mutated"])
        self.assertFalse(state["broker_touched"])
        self.assertFalse(state["order_executed"])
        self.assertEqual(state["order_policy"], "journal_only_no_broker")


def _seed_eth_m30(
    *,
    summary: dict[str, object],
    adaptive: dict[str, object] | None = None,
    open_shadow: bool = False,
) -> None:
    update_snapshot(
        "ETHUSD",
        {
            "last_tick": {
                "symbol": "ETHUSD",
                "timeframe": "M30",
                "last": 2500.0,
                "spread": 1.5,
                "regime": "trend",
            },
            "open_shadow_trade": {"shadow_trade_id": "eth-m30-open", "status": "open"} if open_shadow else {},
            "latest_performance_summary": dict(summary),
            "latest_adaptive_state": dict(adaptive or {}),
            "last_decision": {
                "paper_forward_candidate_profile": "eth_m30_vol_breakout_chop_guard_v1",
                "risk_governor_reason": "recent_edge_negative",
            },
        },
        timeframe="M30",
    )


def _failed_early_forward_summary() -> dict[str, object]:
    return {
        "trades_forward": 5,
        "closed": 5,
        "wins": 1,
        "losses": 4,
        "win_rate": 20.0,
        "profit_factor": 0.0144,
        "expectancy": -0.1025,
        "negative_recent_edge": True,
    }


if __name__ == "__main__":
    unittest.main()
