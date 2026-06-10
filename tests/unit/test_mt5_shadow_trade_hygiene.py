from __future__ import annotations

import unittest

from services.mt5.mt5_shadow_trade_hygiene import run_shadow_trade_hygiene


class MT5ShadowTradeHygieneTests(unittest.TestCase):
    def test_reports_open_shadow_over_limit_as_not_safe_to_open(self) -> None:
        result = run_shadow_trade_hygiene(
            open_trades=[
                _open_trade("shadow-1", "BTCUSD", "M30", "btc_m30_profile", "buy"),
                _open_trade("shadow-2", "ETHUSD", "M30", "eth_m30_profile", "buy"),
                _open_trade("shadow-3", "US500", "H1", "us500_h1_profile", "sell"),
                _open_trade("shadow-4", "XAUUSD", "M15", "xau_m15_profile", "buy"),
            ],
            max_open_shadow_trades=3,
            load_shadow_snapshot=False,
        )

        self.assertEqual(result["open_shadow_trades"], 4)
        self.assertFalse(result["safe_to_open_new_shadow"])
        self.assertEqual(result["recommended_cleanup_action"], "review_open_shadow_over_limit_before_new_entries")
        self.assertFalse(result["closed_shadow_trades"])
        self.assertFalse(result["deleted_shadow_trades"])
        self.assertFalse(result["shadow_trades_mutated"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_reports_duplicate_shadow_clusters_and_profile_overcrowding(self) -> None:
        result = run_shadow_trade_hygiene(
            open_trades=[
                _open_trade("shadow-1", "BTCUSD", "M30", "btc_m30_profile", "buy"),
                _open_trade("shadow-2", "BTCUSD", "M30", "btc_m30_profile", "buy"),
                _open_trade("shadow-3", "BTCUSD", "M30", "btc_m30_profile", "sell"),
            ],
            max_open_shadow_trades=5,
            max_profile_open_shadows=1,
            load_shadow_snapshot=False,
        )

        self.assertTrue(result["safe_to_open_new_shadow"])
        self.assertEqual(len(result["duplicate_shadow_clusters"]), 1)
        self.assertEqual(result["duplicate_shadow_clusters"][0]["open_count"], 2)
        self.assertEqual(len(result["profiles_with_too_many_open_shadows"]), 1)
        self.assertEqual(result["profiles_with_too_many_open_shadows"][0]["open_count"], 3)
        self.assertEqual(result["recommended_cleanup_action"], "review_duplicate_shadow_clusters")

    def test_reports_stale_shadow_trades_without_closing(self) -> None:
        result = run_shadow_trade_hygiene(
            open_trades=[
                _open_trade("shadow-old", "EURUSD", "H1", "eurusd_h1_profile", "buy", opened_at="2000-01-01T00:00:00+00:00"),
            ],
            max_open_shadow_trades=3,
            stale_hours=1,
            load_shadow_snapshot=False,
        )

        self.assertEqual(len(result["stale_shadow_trades"]), 1)
        self.assertEqual(result["stale_shadow_trades"][0]["shadow_trade_id"], "shadow-old")
        self.assertEqual(result["recommended_cleanup_action"], "review_stale_shadow_trades")
        self.assertFalse(result["closed_shadow_trades"])
        self.assertFalse(result["deleted_shadow_trades"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_empty_open_shadows_are_safe_and_report_only(self) -> None:
        result = run_shadow_trade_hygiene(open_trades=[], load_shadow_snapshot=False)

        self.assertEqual(result["open_shadow_trades"], 0)
        self.assertTrue(result["safe_to_open_new_shadow"])
        self.assertEqual(result["stale_shadow_trades"], [])
        self.assertEqual(result["duplicate_shadow_clusters"], [])
        self.assertEqual(result["profiles_with_too_many_open_shadows"], [])
        self.assertEqual(result["recommended_cleanup_action"], "no_open_shadow_cleanup_needed")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")


def _open_trade(
    trade_id: str,
    symbol: str,
    timeframe: str,
    profile: str,
    side: str,
    *,
    opened_at: str = "2026-06-10T12:00:00+00:00",
) -> dict[str, object]:
    return {
        "shadow_trade_id": trade_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_profile": profile,
        "side": side,
        "status": "open",
        "lifecycle_status": "open",
        "opened_at": opened_at,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


if __name__ == "__main__":
    unittest.main()
