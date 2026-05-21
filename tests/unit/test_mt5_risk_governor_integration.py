from __future__ import annotations

import unittest

from api.main import create_app
from api.routes.genesis import get_genesis_mt5_decision, get_genesis_mt5_risk_state
from services.mt5.mt5_promoted_profile import reset_promoted_profiles_for_tests
from services.mt5.mt5_risk_guard import MT5BridgeConfig
from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_snapshot, update_tick
from services.mt5.mt5_signal_router import MT5SignalRouter


def _strong_tick(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "symbol": "BTCUSD",
        "timeframe": "M30",
        "last": 100.0,
        "spread": 10,
        "score": 90,
        "trend_score": 80,
        "momentum_score": 80,
        "volatility_score": 80,
        "rsi": 50,
        "regime": "trend",
        "source": "unit_test",
    }
    payload.update(overrides)
    return payload


class MT5RiskGovernorIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()

    def test_create_app_exposes_risk_state_endpoint(self) -> None:
        app = create_app()

        self.assertEqual(app["genesis_mt5_risk_state_endpoint"], "/api/genesis/mt5/risk-state?symbol={symbol}&timeframe={timeframe}")

    def test_buy_signal_blocked_by_daily_loss_before_shadow_trade(self) -> None:
        update_snapshot("BTCUSD", {"last_account_sync": {"daily_loss_pct": 1.25}}, timeframe="M30")
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))

        result = router.tick(_strong_tick(last=100.0))

        self.assertFalse(result["paper_exploration_created"])
        self.assertFalse(result["risk_governor_allowed"])
        self.assertEqual(result["risk_governor_reason"], "daily_loss_limit_reached")
        self.assertIn("risk_governor_block", result["paper_exploration_reason"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_sell_signal_blocked_by_drawdown_before_shadow_trade(self) -> None:
        update_tick("BTCUSD", _strong_tick(last=101.0))
        update_snapshot("BTCUSD", {"last_account_sync": {"total_drawdown_pct": 5.25}}, timeframe="M30")
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))

        result = router.tick(_strong_tick(last=100.0))

        self.assertFalse(result["paper_exploration_created"])
        self.assertFalse(result["risk_governor_allowed"])
        self.assertEqual(result["risk_governor_reason"], "drawdown_limit_reached")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_signal_blocked_by_high_spread_visible_in_decision(self) -> None:
        update_tick("BTCUSD", _strong_tick(last=100.0, spread=100))

        decision = get_genesis_mt5_decision("BTCUSD", timeframe="M30")

        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertFalse(decision["risk_governor_allowed"])
        self.assertEqual(decision["risk_governor_reason"], "spread_too_high")
        self.assertIn("risk_governor_block", decision["reason"])
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])

    def test_signal_blocked_by_consecutive_losses(self) -> None:
        update_snapshot("BTCUSD", {"latest_adaptive_state": {"current_loss_streak": 4}}, timeframe="M30")
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))

        result = router.tick(_strong_tick())

        self.assertFalse(result["paper_exploration_created"])
        self.assertFalse(result["risk_governor_allowed"])
        self.assertEqual(result["risk_governor_reason"], "consecutive_loss_lockdown")
        self.assertEqual(result["risk_state"], "lockdown")

    def test_degraded_profile_does_not_create_paper_forward_candidate(self) -> None:
        router = MT5SignalRouter(config=MT5BridgeConfig(fast_path_only=True, paper_exploration_enabled=True))

        result = router.tick(_strong_tick())

        self.assertTrue(result["paper_exploration_created"])
        self.assertEqual(result["paper_forward_candidate_profile"], "")
        self.assertFalse((result["auto_forward"].get("open_shadow_trade") or {}).get("paper_forward_candidate"))
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_risk_state_endpoint_reports_snapshot_risk(self) -> None:
        update_snapshot("BTCUSD", {"last_account_sync": {"daily_loss_pct": 1.2, "weekly_loss_pct": 0.5}}, timeframe="M30")

        state = get_genesis_mt5_risk_state(symbol="BTCUSD", timeframe="M30")

        self.assertTrue(state["ok"])
        self.assertFalse(state["allowed"])
        self.assertEqual(state["reason"], "daily_loss_limit_reached")
        self.assertEqual(state["risk_state"], "lockdown")
        self.assertEqual(state["daily_loss_pct"], 1.2)
        self.assertEqual(state["weekly_loss_pct"], 0.5)
        self.assertFalse(state["broker_touched"])
        self.assertFalse(state["order_executed"])


if __name__ == "__main__":
    unittest.main()
