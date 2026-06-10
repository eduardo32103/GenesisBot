from __future__ import annotations

import contextlib
import io
import unittest

from scripts.run_adaptive_strategy_governor import main as governor_main
from services.mt5.mt5_adaptive_strategy_governor import run_adaptive_strategy_governor


class MT5AdaptiveStrategyGovernorTests(unittest.TestCase):
    def test_degrades_strategy_with_low_pf_and_negative_expectancy(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[
                _trade("ETHUSD", "H1", "eth_h1_test_profile", 1.0, "win"),
                _trade("ETHUSD", "H1", "eth_h1_test_profile", -2.0, "loss"),
                _trade("ETHUSD", "H1", "eth_h1_test_profile", -2.0, "loss"),
                _trade("ETHUSD", "H1", "eth_h1_test_profile", -2.0, "loss"),
                _trade("ETHUSD", "H1", "eth_h1_test_profile", -2.0, "loss"),
            ],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            limits={"max_profile_drawdown": 99.0, "max_consecutive_losses_global": 99},
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["global_state"], "degrade_to_observation_only")
        self.assertEqual(len(result["degraded_profiles"]), 1)
        profile = result["degraded_profiles"][0]
        self.assertEqual(profile["recommended_action"], "degrade_to_observation_only")
        self.assertEqual(profile["active_state"], "observation_only")
        self.assertLess(profile["profit_factor"], 0.9)
        self.assertLessEqual(profile["expectancy"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_pauses_strategy_after_three_consecutive_losses(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[
                _trade("US500", "H1", "us500_h1_pause_profile", 5.0, "win"),
                _trade("US500", "H1", "us500_h1_pause_profile", -1.0, "loss"),
                _trade("US500", "H1", "us500_h1_pause_profile", -1.0, "loss"),
                _trade("US500", "H1", "us500_h1_pause_profile", -1.0, "loss"),
            ],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertEqual(result["global_state"], "pause_new_entries")
        self.assertEqual(len(result["paused_profiles"]), 1)
        profile = result["paused_profiles"][0]
        self.assertEqual(profile["consecutive_losses"], 3)
        self.assertEqual(profile["recommended_action"], "pause_new_entries")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_kill_switch_when_profile_drawdown_exceeds_limit(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[
                _trade("BTCUSD", "M30", "btc_m30_drawdown_profile", 2.0, "win"),
                _trade("BTCUSD", "M30", "btc_m30_drawdown_profile", -3.0, "loss"),
                _trade("BTCUSD", "M30", "btc_m30_drawdown_profile", -3.0, "loss"),
            ],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            limits={"max_profile_drawdown": 2.0},
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertEqual(result["global_state"], "kill_switch")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertTrue(any(row["name"] == "max_profile_drawdown" and row["active"] for row in result["circuit_breakers"]))
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_does_not_rotate_to_rejected_degraded_or_sibling_candidates(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[_trade("US500", "H1", "us500_h1_healthy_profile", 1.0, "win")],
            open_trades=[],
            rotation_result={
                "recommendation": "paper_forward_candidate_review",
                "recommended_candidate": None,
                "ranking": [
                    _rotation_row("ETHUSD", "M30", "eth_m30_vol_breakout_chop_guard_v1", "excluded_by_degradation_registry", degraded=True),
                    _rotation_row("EURUSD", "H1", "eurusd_h1_session_vwap_reclaim", "excluded_by_research_rejection_registry", rejected=True),
                    _rotation_row("ETHUSD", "M30", "eth_m30_vol_breakout_regime_filtered_v1", "blocked_by_sibling_risk", sibling=True),
                ],
                "candidate_activated": False,
                "paper_forward_onboarding_started": False,
            },
            intelligence_result=_empty_intelligence(),
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertEqual(result["rotation_candidates"], [])
        self.assertEqual(len(result["rejected_candidates"]), 3)
        self.assertEqual(result["recommended_next_action"], "continue_research")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_clean_rotation_candidate_is_review_only(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[_trade("US500", "H1", "us500_h1_healthy_profile", 1.0, "win")],
            open_trades=[],
            rotation_result={
                "recommendation": "paper_forward_candidate_review",
                "recommended_candidate": _rotation_row("US500", "H1", "us500_h1_clean_profile", "paper_forward_review_ready"),
                "ranking": [],
                "candidate_activated": False,
                "paper_forward_onboarding_started": False,
            },
            intelligence_result=_empty_intelligence(),
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertEqual(result["recommended_next_action"], "rotate_candidate_review")
        self.assertEqual(len(result["rotation_candidates"]), 1)
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["rotation_candidates"][0]["candidate_activated"])
        self.assertFalse(result["rotation_candidates"][0]["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_safety_flags_are_false_and_journal_only_policy_is_preserved(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[_trade("GBPUSD", "H1", "gbpusd_h1_safe_profile", 1.0, "win")],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertFalse(result["safety_state"]["broker_touched"])
        self.assertFalse(result["safety_state"]["order_executed"])
        self.assertEqual(result["safety_state"]["order_policy"], "journal_only_no_broker")
        self.assertFalse(result["safety_state"]["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_missing_data_returns_no_trade(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertEqual(result["global_state"], "no_trade")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "adaptive_governor:missing_data")
        self.assertTrue(any(row["name"] == "missing_shadow_trade_data" and row["active"] for row in result["circuit_breakers"]))
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_script_runs_without_activation(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = governor_main(["--no-shadow-snapshot", "--no-rotation", "--no-intelligence"])

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("MT5 Adaptive Strategy Governor", text)
        self.assertIn("global_state=no_trade", text)
        self.assertIn("decision=NO_TRADE", text)
        self.assertIn("candidate_activated=False", text)
        self.assertIn("paper_forward_onboarding_started=False", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_executed=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)


def _trade(symbol: str, timeframe: str, profile: str, pnl: float, status: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_profile": profile,
        "pnl": pnl,
        "status": status,
    }


def _rotation_row(
    symbol: str,
    timeframe: str,
    profile: str,
    status: str,
    *,
    degraded: bool = False,
    rejected: bool = False,
    sibling: bool = False,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "profile": profile,
        "family": profile,
        "candidate_status": status,
        "recommended_next_action": "paper_forward_candidate_review",
        "degraded_by_registry": degraded,
        "rejected_by_research_registry": rejected,
        "sibling_risk": sibling,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _empty_rotation() -> dict[str, object]:
    return {
        "recommendation": "continue_research",
        "recommended_candidate": None,
        "ranking": [],
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _empty_intelligence() -> dict[str, object]:
    return {
        "recommendation": "research_plan_ready",
        "recommended_next_research_phase": "continue_research",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


if __name__ == "__main__":
    unittest.main()
