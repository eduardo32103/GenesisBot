from __future__ import annotations

import unittest

from services.portfolio.get_portfolio_snapshot import get_portfolio_snapshot
from services.portfolio.get_ticker_drilldown import get_ticker_drilldown


class PortfolioDrilldownTests(unittest.TestCase):
    def test_portfolio_snapshot_normalizes_positions(self) -> None:
        snapshot = get_portfolio_snapshot(
            {
                "owner_id": "dashboard",
                "positions": {
                    "nvda": {
                        "display_name": "NVIDIA",
                        "is_investment": True,
                        "amount_usd": 1500,
                        "entry_price": 500,
                        "timestamp": "2026-04-20T10:00:00+00:00",
                    },
                    "iau": {
                        "display_name": "iShares Gold",
                        "is_investment": False,
                        "amount_usd": 0,
                        "entry_price": 0,
                    },
                },
            }
        )

        self.assertEqual(snapshot["owner_id"], "dashboard")
        self.assertEqual(snapshot["tickers"], ["IAU", "NVDA"])
        self.assertEqual(snapshot["summary"]["position_count"], 2)
        self.assertEqual(snapshot["summary"]["investment_count"], 1)
        self.assertEqual(snapshot["summary"]["invested_capital"], 1500.0)

    def test_portfolio_snapshot_accepts_positions_list_with_units(self) -> None:
        snapshot = get_portfolio_snapshot(
            {
                "positions": [
                    {
                        "ticker": "nvda",
                        "name": "NVIDIA",
                        "units": 10,
                        "entry_price": 150,
                    },
                    {
                        "ticker": "bno",
                    },
                ],
            }
        )

        nvda = next(position for position in snapshot["positions"] if position["ticker"] == "NVDA")
        bno = next(position for position in snapshot["positions"] if position["ticker"] == "BNO")
        self.assertEqual(nvda["units"], 10.0)
        self.assertEqual(nvda["amount_usd"], 1500.0)
        self.assertTrue(nvda["is_investment"])
        self.assertFalse(bno["is_investment"])
        self.assertEqual(snapshot["summary"]["unit_position_count"], 1)

    def test_drilldown_returns_live_position_metrics(self) -> None:
        detail = get_ticker_drilldown(
            {
                "positions": {
                    "NVDA": {
                        "display_name": "NVIDIA",
                        "is_investment": True,
                        "amount_usd": 1500,
                        "entry_price": 500,
                        "timestamp": "2026-04-20T10:00:00+00:00",
                    }
                },
                "quotes": {
                    "NVDA": {
                        "price": 575.25,
                        "timestamp": "2026-04-23T14:30:00+00:00",
                    }
                },
            },
            "nvda",
        )

        self.assertTrue(detail["found"])
        self.assertEqual(detail["symbol"], "NVDA")
        self.assertEqual(detail["status"], "gain")
        self.assertEqual(detail["amount_usd"], 1500.0)
        self.assertEqual(detail["entry_price"], 500.0)
        self.assertEqual(detail["units"], 3.0)
        self.assertEqual(detail["current_price"], 575.25)
        self.assertEqual(detail["current_value"], 1725.75)
        self.assertEqual(detail["pnl_usd"], 225.75)
        self.assertEqual(detail["pnl_pct"], 15.05)
        self.assertEqual(detail["quote_timestamp"], "2026-04-23T14:30:00+00:00")

    def test_drilldown_keeps_watchlist_fields_clean_when_no_position(self) -> None:
        detail = get_ticker_drilldown(
            {
                "positions": {
                    "IAU": {
                        "display_name": "iShares Gold",
                        "is_investment": False,
                        "amount_usd": 0,
                        "entry_price": 0,
                        "timestamp": "2026-04-20T10:00:00+00:00",
                    }
                },
                "quotes": {
                    "IAU": {
                        "price": 63.1,
                        "timestamp": "2026-04-23T14:30:00+00:00",
                    }
                },
            },
            "IAU",
        )

        self.assertTrue(detail["found"])
        self.assertEqual(detail["status"], "watchlist")
        self.assertIsNone(detail["amount_usd"])
        self.assertIsNone(detail["entry_price"])
        self.assertIsNone(detail["units"])
        self.assertEqual(detail["current_price"], 63.1)
        self.assertIsNone(detail["current_value"])
        self.assertIsNone(detail["pnl_usd"])
        self.assertIsNone(detail["pnl_pct"])

    def test_drilldown_calculates_value_without_total_pnl_when_entry_missing(self) -> None:
        detail = get_ticker_drilldown(
            {
                "positions": [
                    {
                        "ticker": "NVDA",
                        "display_name": "NVIDIA",
                        "units": 5,
                    }
                ],
                "quotes": {
                    "NVDA": {
                        "price": 200.0,
                        "timestamp": "2026-05-01T14:30:00+00:00",
                    }
                },
            },
            "NVDA",
        )

        self.assertTrue(detail["found"])
        self.assertEqual(detail["units"], 5.0)
        self.assertEqual(detail["current_value"], 1000.0)
        self.assertIsNone(detail["entry_price"])
        self.assertIsNone(detail["amount_usd"])
        self.assertIsNone(detail["pnl_usd"])
        self.assertIsNone(detail["pnl_pct"])
        self.assertEqual(detail["status"], "priced")

    def test_drilldown_returns_not_found_for_unknown_ticker(self) -> None:
        detail = get_ticker_drilldown({"positions": {}}, "MSFT")

        self.assertFalse(detail["found"])
        self.assertEqual(detail["error"], "ticker_not_found")
        self.assertEqual(detail["symbol"], "MSFT")


if __name__ == "__main__":
    unittest.main()
