from __future__ import annotations

import unittest

from services.mt5.mt5_risk_governor import MT5RiskGovernor


class MT5RiskGovernorTests(unittest.TestCase):
    def test_blocks_daily_loss(self) -> None:
        result = MT5RiskGovernor().assess(account_state={"daily_loss_pct": 1.1}, market={"regime": "trend"})

        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "daily_loss_limit_reached")
        self.assertEqual(result["risk_state"], "lockdown")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_blocks_consecutive_losses(self) -> None:
        result = MT5RiskGovernor().assess(performance={"consecutive_losses": 4}, market={"regime": "trend"})

        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "consecutive_loss_lockdown")
        self.assertEqual(result["suggested_lot_multiplier"], 0.0)

    def test_blocks_max_drawdown(self) -> None:
        result = MT5RiskGovernor().assess(account_state={"total_drawdown_pct": 5.1}, market={"regime": "trend"})

        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "drawdown_limit_reached")
        self.assertEqual(result["risk_state"], "lockdown")

    def test_blocks_high_spread(self) -> None:
        result = MT5RiskGovernor().assess(market={"spread_points": 61, "regime": "trend"})

        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "spread_too_high")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_blocks_negative_recent_edge(self) -> None:
        result = MT5RiskGovernor().assess(
            performance={"recent_closed": 12, "recent_profit_factor": 0.8, "recent_expectancy": -0.01},
            market={"regime": "trend"},
        )

        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "recent_edge_negative")
        self.assertEqual(result["risk_state"], "defensive")

    def test_no_martingale_after_loss(self) -> None:
        result = MT5RiskGovernor().assess(
            signal={"lot_multiplier": 1.5},
            performance={"consecutive_losses": 1},
            market={"regime": "trend"},
        )

        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "martingale_or_loss_scaling_blocked")
        self.assertEqual(result["risk_state"], "lockdown")

    def test_hedge_cannot_increase_exposure_without_limit(self) -> None:
        result = MT5RiskGovernor().assess_hedge(
            open_trade={"exposure": 1.0},
            hedge_signal={
                "hedge_fraction": 0.75,
                "contrary_regime_confirmed": True,
                "stop_loss": 99,
                "max_life_minutes": 30,
            },
            market={"volatility_elevated": True},
        )

        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "hedge_size_limit")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_hedge_allowed_only_when_it_reduces_net_risk(self) -> None:
        result = MT5RiskGovernor().assess_hedge(
            open_trade={"exposure": 1.0},
            hedge_signal={
                "hedge_fraction": 0.25,
                "contrary_regime_confirmed": True,
                "breakdown_confirmed": True,
                "expected_drawdown_change": -0.1,
                "stop_loss": 99,
                "max_life_minutes": 30,
            },
            market={"volatility_elevated": True},
        )

        self.assertTrue(result["allowed"])
        self.assertTrue(result["hedge_needed"])
        self.assertLessEqual(result["suggested_hedge_fraction"], 0.5)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])


if __name__ == "__main__":
    unittest.main()
