from __future__ import annotations

import contextlib
import io
import unittest

from api.main import create_app
from scripts.run_capital_protection_governor import main as capital_main
from services.mt5.mt5_capital_protection_governor import (
    capital_protection_enforcement,
    run_capital_protection_governor,
)


class MT5CapitalProtectionGovernorTests(unittest.TestCase):
    def test_daily_loss_breaker_activates_kill_switch(self) -> None:
        result = run_capital_protection_governor(
            closed_trades=[
                _trade("BTCUSD", "M30", "btc_m30_test", -1.6, "loss"),
                _trade("BTCUSD", "M30", "btc_m30_test", -1.5, "loss"),
            ],
            open_trades=[],
            limits={"max_daily_loss_pct": 3.0},
            load_shadow_snapshot=False,
            load_persistent=False,
            persist_events=False,
        )

        self.assertEqual(result["capital_state"], "kill_switch")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertFalse(result["safe_to_trade"])
        self.assertTrue(_breaker_active(result, "max_daily_loss_pct"))
        self.assertEqual(result["recommended_action"], "kill_switch")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_weekly_loss_breaker_activates_kill_switch(self) -> None:
        result = run_capital_protection_governor(
            closed_trades=[
                _trade("XAUUSD", "M15", "xau_m15_test", -3.5, "loss"),
                _trade("XAUUSD", "M15", "xau_m15_test", -3.6, "loss"),
            ],
            open_trades=[],
            limits={"max_daily_loss_pct": 99.0, "max_weekly_loss_pct": 7.0},
            load_shadow_snapshot=False,
            load_persistent=False,
            persist_events=False,
        )

        self.assertEqual(result["capital_state"], "kill_switch")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertTrue(_breaker_active(result, "max_weekly_loss_pct"))
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_drawdown_breaker_activates_kill_switch(self) -> None:
        result = run_capital_protection_governor(
            closed_trades=[
                _trade("EURUSD", "H1", "eur_h1_test", 2.0, "win"),
                _trade("EURUSD", "H1", "eur_h1_test", -6.0, "loss"),
                _trade("EURUSD", "H1", "eur_h1_test", -6.0, "loss"),
            ],
            open_trades=[],
            limits={"max_daily_loss_pct": 99.0, "max_weekly_loss_pct": 99.0, "max_drawdown_pct": 10.0},
            load_shadow_snapshot=False,
            load_persistent=False,
            persist_events=False,
        )

        self.assertEqual(result["capital_state"], "kill_switch")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertTrue(_breaker_active(result, "max_drawdown_pct"))
        self.assertGreaterEqual(result["max_drawdown_pct"], 10.0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_db_degraded_blocks_critical_trade(self) -> None:
        result = capital_protection_enforcement(
            symbol="BTCUSD",
            timeframe="M30",
            profile="btc_m30_test",
            governor_result=run_capital_protection_governor(
                closed_trades=[_trade("BTCUSD", "M30", "btc_m30_test", 1.0, "win")],
                open_trades=[],
                persistent_status={"db_available": True, "db_degraded": True, "tables_ready": True},
                load_shadow_snapshot=False,
                persist_events=False,
            ),
        )

        self.assertTrue(result["blocked"])
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "capital_protection:persistent_db_degraded")
        self.assertFalse(result["safe_to_open_new_shadow"])
        self.assertFalse(result["paper_exploration_created"])
        self.assertEqual(result["shadow_trade_id"], "")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_open_shadow_and_profile_exposure_breakers_are_safe(self) -> None:
        result = run_capital_protection_governor(
            closed_trades=[],
            open_trades=[
                _open_trade("BTCUSD", "M30", "btc_m30_test"),
                _open_trade("BTCUSD", "M30", "btc_m30_test"),
            ],
            limits={"max_profile_exposure": 1, "max_open_shadow_trades": 3},
            load_shadow_snapshot=False,
            load_persistent=False,
            persist_events=False,
        )

        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertTrue(_breaker_active(result, "max_profile_exposure"))
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_normal_state_is_review_only_and_never_real_trading(self) -> None:
        result = run_capital_protection_governor(
            closed_trades=[_trade("US500", "H1", "us500_h1_test", 0.5, "win")],
            open_trades=[],
            load_shadow_snapshot=False,
            load_persistent=False,
            persist_events=False,
        )

        self.assertEqual(result["capital_state"], "normal")
        self.assertEqual(result["decision"], "ALLOW_PAPER_REVIEW")
        self.assertTrue(result["safe_to_trade"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_script_runs_and_prints_safety(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = capital_main(["--no-shadow-snapshot", "--no-persistent", "--no-persist-events"])

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("MT5 Capital Protection Governor", text)
        self.assertIn("capital_state=", text)
        self.assertIn("safe_to_trade=", text)
        self.assertIn("recommended_action=", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_executed=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)

    def test_endpoint_is_exposed(self) -> None:
        app = create_app()

        self.assertEqual(
            app["genesis_mt5_capital_protection_status_endpoint"],
            "/api/genesis/mt5/capital-protection/status",
        )


def _trade(symbol: str, timeframe: str, profile: str, pnl_pct: float, status: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_profile": profile,
        "pnl": pnl_pct,
        "pnl_pct": pnl_pct,
        "status": status,
    }


def _open_trade(symbol: str, timeframe: str, profile: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_profile": profile,
        "risk_pct": 0.5,
        "lifecycle_status": "open",
    }


def _breaker_active(result: dict[str, object], name: str) -> bool:
    return any(
        isinstance(row, dict) and row.get("name") == name and row.get("active")
        for row in result.get("circuit_breakers", [])
    )


if __name__ == "__main__":
    unittest.main()
