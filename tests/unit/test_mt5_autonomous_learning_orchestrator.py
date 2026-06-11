from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from services.mt5.mt5_autonomous_learning_orchestrator import (
    run_autonomous_learning_loop,
    run_autonomous_learning_orchestrator,
)


class MT5AutonomousLearningOrchestratorTests(unittest.TestCase):
    def test_db_degraded_pauses_learning_and_never_rotates(self) -> None:
        result = _run(persistent_status=_db_degraded())

        self.assertEqual(result["learning_state"], "paused_by_db_degraded")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertFalse(result["safe_to_learn"])
        self.assertFalse(result["paper_rotation_applied"])
        self.assertEqual(result["paper_rotation_recommendation"], "db_degraded_no_rotation")
        _assert_safety(self, result)

    def test_capital_kill_switch_pauses_learning(self) -> None:
        result = _run(capital_result=_capital("kill_switch", safe=False, breaker="daily_loss_kill_switch"))

        self.assertEqual(result["learning_state"], "paused_by_capital_protection")
        self.assertFalse(result["safe_to_learn"])
        self.assertFalse(result["safe_to_open_new_shadow"])
        self.assertEqual(result["recommended_next_action"], "kill_switch")
        _assert_safety(self, result)

    def test_adaptive_kill_switch_pauses_learning(self) -> None:
        result = _run(adaptive_result=_adaptive("kill_switch", breaker="adaptive_global_kill_switch"))

        self.assertEqual(result["learning_state"], "paused_by_adaptive_governor")
        self.assertFalse(result["safe_to_learn"])
        self.assertFalse(result["paper_rotation_applied"])
        self.assertEqual(result["recommended_next_action"], "NO_TRADE")
        _assert_safety(self, result)

    def test_max_open_shadow_trades_blocks_new_entries(self) -> None:
        result = _run(
            hygiene_result=_hygiene(open_shadow_trades=4, safe=False),
            tournament_result=_tournament(top=_candidate()),
            active_profile=_active_profile(),
            apply_paper_rotation=True,
        )

        self.assertFalse(result["safe_to_open_new_shadow"])
        self.assertFalse(result["paper_rotation_applied"])
        self.assertEqual(result["paper_rotation_recommendation"], "cleanup_open_shadows_before_rotation")
        self.assertIn("max_open_shadow_trades", {row["name"] for row in result["circuit_breakers"]})
        _assert_safety(self, result)

    def test_low_pf_negative_expectancy_profile_degrades(self) -> None:
        result = _run(
            profile_performance=[
                _profile("BTCUSD", "M30", "btc_m30_bad_edge", trades=6, win_rate=30, pf=0.5, expectancy=-0.1)
            ],
            tournament_result=None,
        )

        self.assertEqual(result["learning_state"], "learning")
        self.assertEqual(result["degraded_profiles"][0]["recommended_action"], "degrade_profile")
        self.assertFalse(result["paper_rotation_applied"])
        _assert_safety(self, result)

    def test_three_consecutive_losses_pause_profile(self) -> None:
        result = _run(
            profile_performance=[
                _profile("US500", "H1", "us500_h1_pause_me", trades=12, win_rate=55, pf=1.3, expectancy=0.1, losses=3)
            ],
            tournament_result=None,
        )

        self.assertEqual(result["learning_state"], "learning")
        self.assertEqual(result["paused_profiles"][0]["recommended_action"], "pause_profile")
        self.assertFalse(result["paper_rotation_applied"])
        _assert_safety(self, result)

    def test_degradation_registry_candidate_never_rotates(self) -> None:
        result = _run(
            tournament_result=_tournament(top=_candidate("ETHUSD", "M30", "eth_m30_vol_breakout_chop_guard_v1")),
            active_profile=_active_profile(),
            apply_paper_rotation=True,
        )

        self.assertEqual(result["learning_state"], "paused_by_registry")
        self.assertEqual(result["paper_rotation_recommendation"], "paused_by_registry")
        self.assertIn("degradation_registry_block", result["paper_rotation_decision"]["rejection_reasons"])
        self.assertFalse(result["paper_rotation_applied"])
        _assert_safety(self, result)

    def test_research_rejection_registry_candidate_never_rotates(self) -> None:
        result = _run(
            tournament_result=_tournament(top=_candidate("EURUSD", "H1", "eurusd_h1_session_vwap_reclaim_distance_filter")),
            active_profile=_active_profile(),
            apply_paper_rotation=True,
        )

        self.assertEqual(result["learning_state"], "paused_by_registry")
        self.assertIn("research_rejection_registry_block", result["paper_rotation_decision"]["rejection_reasons"])
        self.assertFalse(result["paper_rotation_applied"])
        _assert_safety(self, result)

    def test_sibling_risk_candidate_never_rotates(self) -> None:
        result = _run(
            tournament_result=_tournament(top={**_candidate(), "sibling_risk": True}),
            active_profile=_active_profile(),
            apply_paper_rotation=True,
        )

        self.assertEqual(result["learning_state"], "paused_by_registry")
        self.assertIn("sibling_risk", result["paper_rotation_decision"]["rejection_reasons"])
        self.assertFalse(result["paper_rotation_applied"])
        _assert_safety(self, result)

    def test_high_winrate_negative_expectancy_does_not_win(self) -> None:
        result = _run(
            tournament_result=_tournament(top=_candidate(win_rate=75, expectancy=-0.05, pf=1.4)),
            active_profile=_active_profile(),
            apply_paper_rotation=True,
        )

        reasons = result["paper_rotation_decision"]["rejection_reasons"]
        self.assertIn("expectancy_not_positive", reasons)
        self.assertIn("high_winrate_negative_expectancy", reasons)
        self.assertFalse(result["paper_rotation_applied"])
        _assert_safety(self, result)

    def test_high_pf_high_drawdown_does_not_win(self) -> None:
        result = _run(
            tournament_result=_tournament(top=_candidate(pf=2.4, drawdown=20.0)),
            active_profile=_active_profile(),
            apply_paper_rotation=True,
        )

        self.assertIn("drawdown_too_high", result["paper_rotation_decision"]["rejection_reasons"])
        self.assertFalse(result["paper_rotation_applied"])
        _assert_safety(self, result)

    def test_paper_rotation_only_applies_with_apply_flag(self) -> None:
        review = _run(tournament_result=_tournament(top=_candidate()), active_profile=_active_profile())
        applied = _run(
            tournament_result=_tournament(top=_candidate()),
            active_profile=_active_profile(),
            apply_paper_rotation=True,
        )

        self.assertEqual(review["learning_state"], "paper_rotation_review")
        self.assertFalse(review["paper_rotation_applied"])
        self.assertEqual(applied["learning_state"], "paper_rotation_applied")
        self.assertTrue(applied["paper_rotation_applied"])
        self.assertFalse(applied["candidate_activated"])
        self.assertFalse(applied["paper_forward_onboarding_started"])
        _assert_safety(self, review)
        _assert_safety(self, applied)

    def test_dry_run_does_not_apply_paper_rotation(self) -> None:
        result = _run(
            tournament_result=_tournament(top=_candidate()),
            active_profile=_active_profile(),
            apply_paper_rotation=True,
            dry_run=True,
        )

        self.assertEqual(result["learning_state"], "paper_rotation_review")
        self.assertFalse(result["paper_rotation_applied"])
        self.assertFalse(result["mutations_allowed"])
        _assert_safety(self, result)

    def test_loop_uses_lock_file(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            lock_path = Path(folder) / "learning.lock"
            lock_path.write_text("already-running", encoding="utf-8")

            result = run_autonomous_learning_loop(lock_path=lock_path, max_cycles=1)

        self.assertFalse(result["ok"])
        self.assertTrue(result["lock_active"])
        self.assertEqual(result["learning_state"], "idle")
        _assert_safety(self, result)


def _run(**overrides: object) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "persistent_status": _db_ready(),
        "capital_result": _capital(),
        "adaptive_result": _adaptive(),
        "hygiene_result": _hygiene(),
        "risk_governor_result": _risk(),
        "tournament_result": _tournament(top=None),
        "active_profile": None,
        "run_trade_learning": False,
        "persist_events": False,
        "load_persistent": False,
        "load_shadow_snapshot": False,
        "load_rotation": False,
    }
    kwargs.update(overrides)
    return run_autonomous_learning_orchestrator(**kwargs)  # type: ignore[arg-type]


def _db_ready() -> dict[str, object]:
    return {"provider": "unit", "db_available": True, "db_degraded": False, "tables_ready": True}


def _db_degraded() -> dict[str, object]:
    return {"provider": "unit", "db_available": False, "db_degraded": True, "tables_ready": False, "recommendation": "apply_schema_sql"}


def _capital(state: str = "normal", *, safe: bool = True, breaker: str = "") -> dict[str, object]:
    breakers = []
    if breaker:
        breakers.append({"name": breaker, "active": True, "critical": True, "reason": breaker})
    return {
        "capital_state": state,
        "safe_to_trade": safe,
        "recommended_action": "kill_switch" if state == "kill_switch" else "continue_learning",
        "circuit_breakers": breakers,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _adaptive(state: str = "watch", *, breaker: str = "") -> dict[str, object]:
    breakers = []
    if breaker:
        breakers.append({"name": breaker, "active": True, "critical": True, "reason": breaker})
    return {
        "global_state": state,
        "active_profiles": [],
        "paused_profiles": [],
        "degraded_profiles": [],
        "circuit_breakers": breakers,
        "recommended_next_action": "NO_TRADE" if state == "kill_switch" else "continue_learning",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _hygiene(*, open_shadow_trades: int = 0, safe: bool = True) -> dict[str, object]:
    return {
        "open_shadow_trades": open_shadow_trades,
        "safe_to_open_new_shadow": safe,
        "recommended_cleanup_action": "cleanup_open_shadows_before_rotation" if not safe else "none",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _risk(*, allowed: bool = True) -> dict[str, object]:
    return {
        "allowed": allowed,
        "risk_governor_allowed": allowed,
        "reason": "" if allowed else "unit_block",
        "risk_governor_reason": "" if allowed else "unit_block",
        "risk_state": "normal" if allowed else "blocked",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _tournament(*, top: dict[str, object] | None) -> dict[str, object]:
    ranked = [top] if top else []
    return {
        "top_candidate": top,
        "ranked_profiles": ranked,
        "paused_profiles": [],
        "degraded_profiles": [],
        "rejected_profiles": [],
        "recommended_action": "rotate_to_better_candidate_review" if top else "continue_research",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _candidate(
    symbol: str = "US500",
    timeframe: str = "H1",
    profile: str = "us500_h1_clean_breakout",
    *,
    trades: int = 45,
    win_rate: float = 52.0,
    pf: float = 1.35,
    expectancy: float = 0.18,
    recent_pf: float = 1.2,
    drawdown: float = 1.0,
    score: float = 80.0,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "profile": profile,
        "trades_forward": trades,
        "win_rate": win_rate,
        "profit_factor": pf,
        "expectancy": expectancy,
        "recent_profit_factor": recent_pf,
        "max_drawdown": drawdown,
        "tournament_score": score,
        "recommended_action": "allow_paper_probe",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _active_profile() -> dict[str, object]:
    return {
        "symbol": "BTCUSD",
        "timeframe": "M30",
        "profile": "btc_m30_current_paper_profile",
        "tournament_score": 60.0,
        "win_rate": 45.0,
        "expectancy": 0.08,
        "profit_factor": 1.1,
    }


def _profile(
    symbol: str,
    timeframe: str,
    profile: str,
    *,
    trades: int,
    win_rate: float,
    pf: float,
    expectancy: float,
    losses: int = 0,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "profile": profile,
        "trades_forward": trades,
        "win_rate": win_rate,
        "profit_factor": pf,
        "expectancy": expectancy,
        "max_drawdown": 0.5,
        "consecutive_losses": losses,
        "recent_win_rate": win_rate,
        "recent_profit_factor": pf,
        "monte_carlo_stressed_pf": 1.2,
    }


def _assert_safety(testcase: unittest.TestCase, result: dict[str, object]) -> None:
    testcase.assertFalse(result["broker_touched"])
    testcase.assertFalse(result["order_executed"])
    testcase.assertEqual(result["order_policy"], "journal_only_no_broker")
    testcase.assertFalse(result.get("candidate_activated", False))
    testcase.assertFalse(result.get("paper_forward_onboarding_started", False))


if __name__ == "__main__":
    unittest.main()
