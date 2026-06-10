from __future__ import annotations

import contextlib
import io
import unittest

from api.main import create_app
from scripts.run_strategy_tournament import main as tournament_main
from services.mt5.mt5_strategy_tournament import run_strategy_tournament, strategy_tournament_enforcement


class MT5StrategyTournamentTests(unittest.TestCase):
    def test_consecutive_losses_pause_profile(self) -> None:
        result = run_strategy_tournament(
            profile_performance=[
                _profile("BTCUSD", "M30", "btc_m30_pause", trades=12, win_rate=45, pf=1.2, expectancy=0.1, losses=3)
            ],
            persistent_status=_persistent_ready(),
            load_shadow_snapshot=False,
            load_persistent=False,
            load_rotation=False,
            persist_events=False,
        )

        row = result["ranked_profiles"][0]
        self.assertEqual(row["recommended_action"], "pause_profile")
        self.assertEqual(result["recommended_action"], "pause_profile")
        self.assertEqual(len(result["paused_profiles"]), 1)
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_registry_rejected_profile_cannot_rank_above_clean_candidate(self) -> None:
        result = run_strategy_tournament(
            profile_performance=[
                _profile("EURUSD", "H1", "eurusd_h1_session_vwap_reclaim_distance_filter", trades=80, win_rate=70, pf=2.5, expectancy=0.6),
                _profile("US500", "H1", "us500_h1_clean_candidate", trades=45, win_rate=48, pf=1.25, expectancy=0.12),
            ],
            persistent_status=_persistent_ready(),
            load_shadow_snapshot=False,
            load_persistent=False,
            load_rotation=False,
            persist_events=False,
        )

        top = result["top_candidate"]
        self.assertEqual(top["profile"], "us500_h1_clean_candidate")
        rejected = [row for row in result["ranked_profiles"] if row["profile"].startswith("eurusd_h1")]
        self.assertTrue(rejected[0]["rejected_by_research_registry"])
        self.assertLess(rejected[0]["tournament_score"], top["tournament_score"])
        self.assertFalse(rejected[0]["candidate_activated"])
        self.assertFalse(rejected[0]["paper_forward_onboarding_started"])

    def test_degradation_registry_profile_cannot_activate(self) -> None:
        enforcement = strategy_tournament_enforcement(
            symbol="ETHUSD",
            timeframe="M30",
            profile="eth_m30_vol_breakout_chop_guard_v1",
            tournament_result=run_strategy_tournament(
                profile_performance=[
                    _profile("ETHUSD", "M30", "eth_m30_vol_breakout_chop_guard_v1", trades=82, win_rate=60, pf=2.0, expectancy=0.4),
                ],
                persistent_status=_persistent_ready(),
                load_shadow_snapshot=False,
                load_persistent=False,
                load_rotation=False,
                persist_events=False,
            ),
        )

        self.assertTrue(enforcement["blocked"])
        self.assertEqual(enforcement["decision"], "NO_TRADE")
        self.assertTrue(enforcement["matching_profile"]["degraded_by_registry"])
        self.assertFalse(enforcement["paper_exploration_created"])
        self.assertEqual(enforcement["shadow_trade_id"], "")
        self.assertFalse(enforcement["broker_touched"])
        self.assertFalse(enforcement["order_executed"])
        self.assertEqual(enforcement["order_policy"], "journal_only_no_broker")

    def test_sibling_risk_cannot_rotate(self) -> None:
        result = run_strategy_tournament(
            profile_performance=[
                {
                    **_profile("ETHUSD", "M30", "eth_m30_vol_breakout_regime_filtered_v1", trades=82, win_rate=60, pf=1.88, expectancy=0.3),
                    "sibling_risk": True,
                },
                _profile("GBPUSD", "H1", "gbpusd_h1_clean_candidate", trades=45, win_rate=49, pf=1.2, expectancy=0.1),
            ],
            persistent_status=_persistent_ready(),
            load_shadow_snapshot=False,
            load_persistent=False,
            load_rotation=False,
            persist_events=False,
        )

        sibling = [row for row in result["ranked_profiles"] if row["profile"].startswith("eth_m30")][0]
        self.assertTrue(sibling["sibling_risk"])
        self.assertEqual(sibling["recommended_action"], "continue_research")
        self.assertNotEqual(result["top_candidate"]["profile"], sibling["profile"])
        self.assertFalse(sibling["candidate_activated"])
        self.assertFalse(sibling["paper_forward_onboarding_started"])

    def test_better_winrate_but_bad_expectancy_does_not_win(self) -> None:
        result = run_strategy_tournament(
            profile_performance=[
                _profile("BTCUSD", "H1", "high_win_bad_expectancy", trades=50, win_rate=75, pf=1.1, expectancy=-0.2),
                _profile("BTCUSD", "M30", "lower_win_positive_expectancy", trades=50, win_rate=48, pf=1.25, expectancy=0.2),
            ],
            persistent_status=_persistent_ready(),
            load_shadow_snapshot=False,
            load_persistent=False,
            load_rotation=False,
            persist_events=False,
        )

        self.assertEqual(result["top_candidate"]["profile"], "lower_win_positive_expectancy")
        self.assertGreater(result["top_candidate"]["expectancy"], 0)

    def test_better_pf_but_high_drawdown_does_not_win(self) -> None:
        result = run_strategy_tournament(
            profile_performance=[
                _profile("US500", "H1", "high_pf_high_drawdown", trades=60, win_rate=50, pf=2.4, expectancy=0.2, drawdown=20.0),
                _profile("US500", "M30", "lower_pf_survivable", trades=60, win_rate=50, pf=1.35, expectancy=0.15, drawdown=1.0),
            ],
            persistent_status=_persistent_ready(),
            load_shadow_snapshot=False,
            load_persistent=False,
            load_rotation=False,
            persist_events=False,
        )

        self.assertEqual(result["top_candidate"]["profile"], "lower_pf_survivable")
        self.assertLess(result["top_candidate"]["max_drawdown"], 5.0)

    def test_db_degraded_never_rotates(self) -> None:
        result = run_strategy_tournament(
            profile_performance=[
                _profile("US500", "H1", "us500_h1_clean_candidate", trades=60, win_rate=55, pf=1.5, expectancy=0.3),
            ],
            persistent_status={"db_available": True, "db_degraded": True, "tables_ready": True},
            load_shadow_snapshot=False,
            load_persistent=False,
            load_rotation=False,
            persist_events=False,
        )

        self.assertIsNone(result["top_candidate"])
        self.assertEqual(result["recommended_action"], "continue_research")
        self.assertTrue(result["ranked_profiles"][0]["db_degraded"])
        self.assertEqual(result["ranked_profiles"][0]["recommended_action"], "continue_research")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])

    def test_script_runs_without_activation(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = tournament_main(["--no-shadow-snapshot", "--no-persistent", "--no-rotation", "--no-persist-events"])

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("MT5 Strategy Tournament", text)
        self.assertIn("Ranked profiles:", text)
        self.assertIn("candidate_activated=False", text)
        self.assertIn("paper_forward_onboarding_started=False", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_executed=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)

    def test_endpoint_is_exposed(self) -> None:
        app = create_app()

        self.assertEqual(
            app["genesis_mt5_strategy_tournament_status_endpoint"],
            "/api/genesis/mt5/strategy-tournament/status",
        )


def _profile(
    symbol: str,
    timeframe: str,
    profile: str,
    *,
    trades: int,
    win_rate: float,
    pf: float,
    expectancy: float,
    drawdown: float = 0.0,
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
        "max_drawdown": drawdown,
        "consecutive_losses": losses,
        "recent_win_rate": win_rate,
        "recent_profit_factor": pf,
        "monte_carlo_stressed_pf": 1.2,
    }


def _persistent_ready() -> dict[str, object]:
    return {"db_available": True, "db_degraded": False, "tables_ready": True}


if __name__ == "__main__":
    unittest.main()
