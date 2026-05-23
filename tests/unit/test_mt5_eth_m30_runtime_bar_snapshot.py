from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from services.mt5.mt5_bridge import mt5_bars, mt5_decision, mt5_forward_profile_state, mt5_promoted_profile, mt5_tick
from services.mt5.mt5_eth_m30_paper_forward_candidate import ETH_M30_CANDIDATE_PROFILE
from services.mt5.mt5_promoted_profile import reset_promoted_profiles_for_tests
from services.mt5.mt5_runtime_snapshot import get_snapshot, reset_runtime_snapshots_for_tests, update_open_shadow_trade, update_snapshot


class MT5EthM30RuntimeBarSnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_runtime_snapshots_for_tests()
        reset_promoted_profiles_for_tests()

    def test_bar_snapshot_read_only_builds_complete_runtime_context(self) -> None:
        result = mt5_bars(_bars_payload(_bars(100)))
        snapshot = get_snapshot("ETHUSD", "M30") or {}
        forward = mt5_forward_profile_state(symbol="ETHUSD", timeframe="M30")

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "mt5_bars_recorded_fast_path")
        self.assertEqual(result["bars_loaded"], 100)
        self.assertTrue(result["runtime_snapshot_available"])
        self.assertTrue(result["runtime_snapshot_recent"])
        self.assertTrue(result["runtime_snapshot_complete"])
        self.assertEqual(result["runtime_snapshot_context"], "bar_context")
        self.assertEqual(snapshot["runtime_snapshot_context"], "bar_context")
        self.assertTrue(snapshot["runtime_snapshot_complete"])
        self.assertIn("ohlc_recent", snapshot)
        self.assertIn("trend_score", snapshot)
        self.assertIn("momentum_score", snapshot)
        self.assertIn("volatility_score", snapshot)
        self.assertEqual(forward["status"], "paper_forward_candidate")
        self.assertTrue(forward["runtime_snapshot_available"])
        self.assertTrue(forward["runtime_snapshot_recent"])
        self.assertTrue(forward["runtime_snapshot_complete"])
        self.assertTrue(forward["active"])
        self.assertTrue(forward["applies_to_paper_shadow"])
        self.assertFalse(forward["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_insufficient_bars_keep_candidate_in_insufficient_context(self) -> None:
        result = mt5_bars(_bars_payload(_bars(20)))
        forward = mt5_forward_profile_state(symbol="ETHUSD", timeframe="M30")
        decision = mt5_decision("ETHUSD", timeframe="M30")

        self.assertTrue(result["ok"])
        self.assertFalse(result["runtime_snapshot_complete"])
        self.assertEqual(result["runtime_snapshot_context"], "insufficient_bar_context")
        self.assertFalse(forward["active"])
        self.assertEqual(forward["reason"], "insufficient_bar_context")
        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertEqual(decision["reason"], "insufficient_bar_context")
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])

    def test_decision_with_complete_bars_reports_profile_conditions_not_fast_path(self) -> None:
        mt5_bars(_bars_payload(_bars(100)))

        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            decision = mt5_decision("ETHUSD", timeframe="M30")

        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertTrue(decision["runtime_snapshot_complete"])
        self.assertTrue(decision["reason"].startswith("profile_conditions_not_met:"))
        self.assertNotEqual(decision["reason"], "fast_path_snapshot_only")
        self.assertNotEqual(decision["reason"], "insufficient_bar_context")
        self.assertEqual(decision["paper_forward_candidate_profile"], ETH_M30_CANDIDATE_PROFILE)
        self.assertIn("strategy_score", decision)
        self.assertIn("min_score", decision)
        self.assertIn("score_gap_to_threshold", decision)
        self.assertIn("failed_components", decision)
        self.assertIn("component_thresholds", decision)
        self.assertIsInstance(decision["failed_components"], list)
        self.assertIsInstance(decision["component_thresholds"], dict)
        self.assertEqual(decision["min_score"], 58.0)
        self.assertEqual(decision["min_momentum_score"], 50.0)
        self.assertEqual(decision["min_trend_score"], 50.0)
        self.assertEqual(decision["min_volatility_score"], 35.0)
        self.assertIn("momentum_gap_to_threshold", decision)
        self.assertIn("trend_gap_to_threshold", decision)
        self.assertIn("volatility_gap_to_threshold", decision)
        self.assertIn("risk_governor_open_trades_count", decision)
        self.assertIn("risk_governor_open_trades_source", decision)
        self.assertIn("score", decision["component_thresholds"])
        self.assertFalse(decision["applies_to_real_trading"])
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])
        self.assertEqual(decision["order_policy"], "journal_only_no_broker")

    def test_tick_after_complete_bars_preserves_bar_context_and_updates_price(self) -> None:
        mt5_bars(_bars_payload(_bars(100), spread=0.3))
        before = get_snapshot("ETHUSD", "M30") or {}

        tick_result = mt5_tick(
            {
                "symbol": "ETHUSD",
                "timeframe": "M30",
                "bid": 3333.1,
                "ask": 3333.5,
                "last": 3333.3,
                "spread": 0.4,
                "source": "unit_tick_after_bars",
            }
        )
        snapshot = get_snapshot("ETHUSD", "M30") or {}
        forward = mt5_forward_profile_state(symbol="ETHUSD", timeframe="M30")

        self.assertTrue(tick_result["ok"])
        self.assertTrue(tick_result["snapshot"]["runtime_snapshot_complete"])
        self.assertEqual(tick_result["snapshot"]["runtime_snapshot_context"], "bar_context")
        self.assertTrue(tick_result["snapshot"]["tick_merged_into_bar_context"])
        self.assertTrue(snapshot["runtime_snapshot_complete"])
        self.assertEqual(snapshot["runtime_snapshot_context"], "bar_context")
        self.assertTrue(snapshot["tick_merged_into_bar_context"])
        self.assertEqual(snapshot["snapshot_context_source"], "bar_context")
        self.assertEqual(snapshot["bars_count"], 100)
        self.assertEqual(snapshot["last_tick"]["last"], 3333.3)
        self.assertEqual(snapshot["last_tick"]["spread"], 0.4)
        self.assertEqual(snapshot["trend_score"], before["trend_score"])
        self.assertEqual(snapshot["last_tick"]["trend_score"], before["trend_score"])
        self.assertIn("ohlc_recent", snapshot)
        self.assertTrue(forward["active"])
        self.assertTrue(forward["runtime_snapshot_complete"])
        self.assertEqual(forward["runtime_snapshot_context"], "bar_context")
        self.assertFalse(forward["applies_to_real_trading"])
        self.assertFalse(tick_result["broker_touched"])
        self.assertFalse(tick_result["order_executed"])
        self.assertEqual(tick_result["order_policy"], "journal_only_no_broker")

        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            decision = mt5_decision("ETHUSD", timeframe="M30")

        self.assertTrue(decision["runtime_snapshot_complete"])
        self.assertEqual(decision["runtime_snapshot_context"], "bar_context")
        self.assertNotEqual(decision["reason"], "insufficient_bar_context")
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])

    def test_expired_bar_context_can_degrade_to_tick_only(self) -> None:
        mt5_bars(_bars_payload(_bars(100), spread=0.3))
        expired = (datetime.now(timezone.utc) - timedelta(hours=4)).isoformat()
        update_snapshot("ETHUSD", {"last_bars_at": expired, "bars_last_at": expired}, timeframe="M30")

        mt5_tick(
            {
                "symbol": "ETHUSD",
                "timeframe": "M30",
                "bid": 3333.1,
                "ask": 3333.5,
                "last": 3333.3,
                "spread": 0.4,
                "source": "unit_tick_after_expired_bars",
            }
        )
        snapshot = get_snapshot("ETHUSD", "M30") or {}
        forward = mt5_forward_profile_state(symbol="ETHUSD", timeframe="M30")

        self.assertFalse(snapshot["runtime_snapshot_complete"])
        self.assertEqual(snapshot["runtime_snapshot_context"], "tick_only")
        self.assertFalse(snapshot["tick_merged_into_bar_context"])
        self.assertFalse(forward["active"])
        self.assertEqual(forward["reason"], "insufficient_bar_context")
        self.assertFalse(forward["broker_touched"])
        self.assertFalse(forward["order_executed"])

    def test_risk_governor_blocks_before_paper_shadow_when_spread_high(self) -> None:
        mt5_bars(_bars_payload(_trend_bars(100), spread=99.0))

        with patch("services.mt5.mt5_signal_router.enqueue_mt5_event", return_value={"queued": True}):
            decision = mt5_decision("ETHUSD", timeframe="M30")

        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertFalse(decision["risk_governor_allowed"])
        self.assertEqual(decision["risk_governor_reason"], "spread_too_high")
        self.assertIn("risk_governor_block", decision["reason"])
        self.assertFalse(decision["paper_exploration_created"])
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])

    def test_max_open_trades_blocks_new_paper_shadow(self) -> None:
        mt5_bars(_bars_payload(_trend_bars(100)))
        update_open_shadow_trade(
            "ETHUSD",
            {
                "shadow_trade_id": "eth-m30-open",
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
            decision = mt5_decision("ETHUSD", timeframe="M30")

        self.assertEqual(decision["decision"], "NO_TRADE")
        self.assertFalse(decision["risk_governor_allowed"])
        self.assertEqual(decision["risk_governor_reason"], "max_open_trades_reached")
        self.assertFalse(decision["paper_exploration_created"])
        self.assertFalse(decision["broker_touched"])
        self.assertFalse(decision["order_executed"])

    def test_btc_and_other_eth_timeframes_remain_observation_only(self) -> None:
        mt5_bars(_bars_payload(_bars(100)))

        btc = mt5_forward_profile_state(symbol="BTCUSD", timeframe="M30")
        h1 = mt5_forward_profile_state(symbol="ETHUSD", timeframe="H1")
        m15 = mt5_forward_profile_state(symbol="ETHUSD", timeframe="M15")
        promoted = mt5_promoted_profile(symbol="ETHUSD", timeframe="M30")

        self.assertNotEqual(btc.get("profile"), ETH_M30_CANDIDATE_PROFILE)
        self.assertEqual(h1["status"], "observation_only")
        self.assertEqual(m15["status"], "observation_only")
        self.assertEqual(promoted["status"], "observation_only")
        self.assertEqual(promoted["profile"], "")
        self.assertFalse(h1["applies_to_real_trading"])
        self.assertFalse(m15["applies_to_real_trading"])
        self.assertFalse(bool(promoted.get("applies_to_real_trading")))


def _bars_payload(bars: list[dict[str, object]], *, spread: float = 0.3) -> dict[str, object]:
    last = float(bars[-1]["close"])
    return {
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "bars_data": bars,
        "bid": last - spread / 2.0,
        "ask": last + spread / 2.0,
        "last": last,
        "spread": spread,
        "source": "unit_test",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _bars(count: int) -> list[dict[str, object]]:
    start = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows: list[dict[str, object]] = []
    for index in range(count):
        price = 3200.0 + ((index % 6) * 0.15)
        rows.append(_bar(start + timedelta(minutes=30 * index), price, price + 1.0, price - 1.0, price + 0.05))
    return rows


def _trend_bars(count: int) -> list[dict[str, object]]:
    start = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows: list[dict[str, object]] = []
    for index in range(count):
        price = 3000.0 + index * 3.0
        rows.append(_bar(start + timedelta(minutes=30 * index), price, price + 9.0, price - 3.0, price + 7.0))
    return rows


def _bar(time_value: datetime, open_price: float, high: float, low: float, close: float) -> dict[str, object]:
    return {
        "time": time_value.isoformat(),
        "open": round(open_price, 6),
        "high": round(high, 6),
        "low": round(low, 6),
        "close": round(close, 6),
        "volume": 10.0,
    }


if __name__ == "__main__":
    unittest.main()
