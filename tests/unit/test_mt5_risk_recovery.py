from __future__ import annotations

import unittest

from api.main import create_app
from api.routes.genesis import get_genesis_mt5_risk_recovery
from services.mt5.mt5_eth_m30_forward_degradation import eth_m30_forward_degradation_status
from services.mt5.mt5_eth_m30_paper_forward_candidate import eth_m30_forward_profile_state
from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation
from services.mt5.mt5_paper_exploration import evaluate_paper_exploration
from services.mt5.mt5_promoted_profile import get_promoted_profile, reset_promoted_profiles_for_tests
from services.mt5.mt5_risk_recovery import mt5_risk_recovery_status
from services.mt5.mt5_risk_guard import MT5BridgeConfig
from services.mt5.mt5_runtime_snapshot import get_snapshot, reset_runtime_snapshots_for_tests, update_snapshot
from services.mt5.mt5_signal_router import MT5SignalRouter


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
        self.assertEqual(result["recommendation"], "pending_degradation_until_shadow_closes")
        self.assertTrue(result["degradation_guardrail_active"])
        self.assertTrue(result["pending_degradation_until_shadow_closes"])
        self.assertFalse(result["paper_probe_allowed"])
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
        self.assertTrue(result["registry_degraded"])
        self.assertEqual(result["degradation_source"], "forward_profile_degradation_registry")
        self.assertEqual(result["new_status"], "observation_only")
        self.assertTrue(result["degradation_guardrail_active"])
        self.assertFalse(result["pending_degradation_until_shadow_closes"])
        self.assertFalse(result["paper_probe_allowed"])
        self.assertEqual(state["status"], "observation_only")
        self.assertFalse(state["active"])
        self.assertFalse(state["applies_to_paper_shadow"])
        self.assertFalse(state["applies_to_real_trading"])
        self.assertEqual(state["degradation_reason"], "early_forward_edge_failed")
        self.assertTrue(state["degradation_guardrail_active"])
        self.assertFalse(state["paper_probe_allowed"])
        self.assertTrue(recovery["degradation_guardrail_active"])
        self.assertFalse(recovery["pending_degradation_until_shadow_closes"])
        self.assertFalse(recovery["paper_probe_allowed"])
        self.assertIn("degrade_to_observation_only", recovery["recommended_action"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_persistent_degradation_registry_blocks_zero_runtime_metrics(self) -> None:
        _seed_eth_m30(summary=_zero_forward_summary())

        registry = forward_profile_degradation("ETHUSD", "M30", "eth_m30_vol_breakout_chop_guard_v1")
        result = eth_m30_forward_degradation_status(symbol="ETHUSD", timeframe="M30")
        state = eth_m30_forward_profile_state(symbol="ETHUSD", timeframe="M30")
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))
        decision = router.decision("ETHUSD", timeframe="M30")
        exploration = evaluate_paper_exploration(
            "ETHUSD",
            tick=(get_snapshot("ETHUSD", "M30") or {}).get("last_tick"),
            config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True),
            trigger="decision",
            timeframe="M30",
        )

        self.assertEqual(registry["status"], "observation_only")
        self.assertEqual(registry["degradation_reason"], "early_forward_edge_failed")
        self.assertTrue(result["registry_degraded"])
        self.assertFalse(result["edge_failed"])
        self.assertEqual(result["trades_forward"], 0)
        self.assertEqual(result["wins"], 0)
        self.assertEqual(result["losses"], 0)
        self.assertEqual(result["profit_factor"], 0.0)
        self.assertEqual(result["expectancy"], 0.0)
        self.assertEqual(result["recommendation"], "degrade_to_observation_only")
        self.assertEqual(result["new_status"], "observation_only")
        self.assertFalse(result["paper_probe_allowed"])
        self.assertEqual(state["status"], "observation_only")
        self.assertFalse(state["active"])
        self.assertFalse(state["applies_to_paper_shadow"])
        self.assertFalse(state["applies_to_real_trading"])
        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertEqual(decision["reason"], "forward_degraded:early_forward_edge_failed")
        self.assertFalse(decision["paper_exploration_created"])
        self.assertEqual(decision["shadow_trade_id"], "")
        self.assertTrue(decision["registry_degraded"])
        self.assertFalse(exploration["paper_exploration_created"])
        self.assertEqual(exploration["paper_exploration_reason"], "forward_degraded:early_forward_edge_failed")
        self.assertEqual(exploration["shadow_trade_id"], "")
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])
        self.assertEqual(decision["order_policy"], "journal_only_no_broker")

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

    def test_router_does_not_create_paper_probe_when_forward_degraded(self) -> None:
        _seed_eth_m30(summary=_failed_early_forward_summary(), adaptive={"current_loss_streak": 3, "negative_edge": True})
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))

        decision = router.decision("ETHUSD", timeframe="M30")
        snapshot = get_snapshot("ETHUSD", "M30")

        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertEqual(decision["reason"], "forward_degraded:early_forward_edge_failed")
        self.assertFalse(decision["paper_exploration_created"])
        self.assertFalse(decision["paper_probe_allowed"])
        self.assertTrue(decision["degradation_guardrail_active"])
        self.assertEqual(decision["degradation_reason"], "early_forward_edge_failed")
        self.assertEqual(decision["shadow_trade_id"], "")
        self.assertEqual(snapshot.get("open_shadow_trade"), {})
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])
        self.assertEqual(decision["order_policy"], "journal_only_no_broker")

    def test_router_reports_pending_degradation_without_closing_open_shadow(self) -> None:
        _seed_eth_m30(summary=_zero_forward_summary(), open_shadow=True)
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))

        decision = router.decision("ETHUSD", timeframe="M30")
        snapshot = get_snapshot("ETHUSD", "M30")

        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertEqual(decision["reason"], "pending_degradation_until_shadow_closes:early_forward_edge_failed")
        self.assertFalse(decision["paper_exploration_created"])
        self.assertTrue(decision["degradation_guardrail_active"])
        self.assertTrue(decision["registry_degraded"])
        self.assertTrue(decision["pending_degradation_until_shadow_closes"])
        self.assertFalse(decision["paper_probe_allowed"])
        self.assertEqual(snapshot["open_shadow_trade"]["shadow_trade_id"], "eth-m30-open")
        self.assertEqual(snapshot["open_shadow_trade"]["status"], "open")
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])
        self.assertEqual(decision["order_policy"], "journal_only_no_broker")


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
                "score": 70.0,
                "momentum_score": 70.0,
                "trend_score": 70.0,
                "volatility_score": 50.0,
                "runtime_snapshot_complete": True,
                "runtime_snapshot_context": "bar_context",
            },
            "runtime_snapshot_complete": True,
            "runtime_snapshot_context": "bar_context",
            "bars_count": 120,
            "min_bars_required": 100,
            "open_shadow_trade": {"shadow_trade_id": "eth-m30-open", "status": "open", "lifecycle_status": "open"} if open_shadow else {},
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


def _zero_forward_summary() -> dict[str, object]:
    return {
        "trades_forward": 0,
        "closed": 0,
        "wins": 0,
        "losses": 0,
        "win_rate": 0.0,
        "profit_factor": 0.0,
        "expectancy": 0.0,
        "negative_recent_edge": False,
    }


if __name__ == "__main__":
    unittest.main()
