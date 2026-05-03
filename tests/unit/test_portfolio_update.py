from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from services.dashboard.search_market_ticker import search_market_ticker
from services.portfolio.get_ticker_drilldown import get_ticker_drilldown
from services.portfolio.update_portfolio import (
    add_ticker_to_portfolio,
    remove_paper_position,
    remove_watchlist_ticker,
    simulate_paper_position,
)


class PortfolioUpdateTests(unittest.TestCase):
    def test_add_ticker_preserves_existing_watchlist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "portfolio.json"
            path.write_text(json.dumps({"BNO": 61.39}), encoding="utf-8")

            result = add_ticker_to_portfolio("nvda", path=path)
            payload = json.loads(path.read_text(encoding="utf-8"))
            tickers = [item["ticker"] for item in payload["positions"]]

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "added")
            self.assertEqual(tickers, ["BNO", "NVDA"])

    def test_add_ticker_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "portfolio.json"
            path.write_text(json.dumps({"positions": [{"ticker": "NVDA"}]}), encoding="utf-8")

            result = add_ticker_to_portfolio("NVDA", path=path)
            payload = json.loads(path.read_text(encoding="utf-8"))

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "exists")
            self.assertEqual(result["message"], "Este activo ya esta en tu cartera/watchlist.")
            self.assertEqual(len(payload["positions"]), 1)

    def test_simulate_paper_position_saves_units_entry_and_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "portfolio.json"
            path.write_text(json.dumps({"positions": [{"ticker": "NVDA"}]}), encoding="utf-8")

            result = simulate_paper_position("NVDA", units=10, entry_price=199.57, path=path)
            payload = json.loads(path.read_text(encoding="utf-8"))
            position = payload["positions"][0]

            self.assertTrue(result["ok"])
            self.assertEqual(result["mode"], "paper")
            self.assertEqual(position["ticker"], "NVDA")
            self.assertEqual(position["units"], 10.0)
            self.assertEqual(position["entry_price"], 199.57)
            self.assertEqual(position["mode"], "paper")

    def test_simulate_new_paper_position_does_not_force_watchlist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "portfolio.json"
            path.write_text(json.dumps({"positions": []}), encoding="utf-8")

            result = simulate_paper_position("META", units=10, entry_price=608.75, path=path)
            payload = json.loads(path.read_text(encoding="utf-8"))
            position = payload["positions"][0]

            self.assertTrue(result["ok"])
            self.assertEqual(position["ticker"], "META")
            self.assertEqual(position["mode"], "paper")
            self.assertNotIn("watchlist", position)

    def test_paper_position_drilldown_calculates_value_and_pnl_with_quote(self) -> None:
        raw_portfolio = {
            "positions": [
                {"ticker": "NVDA", "units": 10, "entry_price": 150, "mode": "paper"},
            ],
            "quotes": {
                "NVDA": {
                    "price": 200,
                    "change": 3.25,
                    "changesPercentage": 1.65,
                    "dayHigh": 202,
                    "dayLow": 195,
                    "volume": 123456,
                    "timestamp": "2026-05-01T18:00:00+00:00",
                }
            },
        }

        detail = get_ticker_drilldown(raw_portfolio, "NVDA")

        self.assertTrue(detail["found"])
        self.assertEqual(detail["position_mode"], "paper")
        self.assertEqual(detail["current_value"], 2000)
        self.assertEqual(detail["pnl_usd"], 500)
        self.assertEqual(detail["pnl_pct"], 33.33)
        self.assertEqual(detail["daily_change"], 3.25)
        self.assertEqual(detail["daily_change_pct"], 1.65)

    def test_remove_watchlist_ticker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "portfolio.json"
            path.write_text(json.dumps({"positions": [{"ticker": "META"}, {"ticker": "NVDA"}]}), encoding="utf-8")

            result = remove_watchlist_ticker("META", path=path)
            payload = json.loads(path.read_text(encoding="utf-8"))
            tickers = [item["ticker"] for item in payload["positions"]]

            self.assertTrue(result["ok"])
            self.assertEqual(tickers, ["NVDA"])

    def test_remove_watchlist_from_paper_position_keeps_paper(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "portfolio.json"
            path.write_text(
                json.dumps(
                    {
                        "positions": [
                            {
                                "ticker": "META",
                                "units": 10,
                                "entry_price": 608.75,
                                "mode": "paper",
                                "watchlist": True,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            result = remove_watchlist_ticker("META", path=path)
            payload = json.loads(path.read_text(encoding="utf-8"))
            position = payload["positions"][0]

            self.assertTrue(result["ok"])
            self.assertEqual(position["ticker"], "META")
            self.assertEqual(position["units"], 10.0)
            self.assertNotIn("watchlist", position)

    def test_add_ticker_restores_watchlist_on_existing_paper(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "portfolio.json"
            path.write_text(
                json.dumps({"positions": [{"ticker": "META", "units": 2, "entry_price": 10, "mode": "paper"}]}),
                encoding="utf-8",
            )

            result = add_ticker_to_portfolio("META", path=path)
            payload = json.loads(path.read_text(encoding="utf-8"))

            self.assertTrue(result["ok"])
            self.assertTrue(payload["positions"][0]["watchlist"])

    def test_remove_paper_position(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "portfolio.json"
            path.write_text(
                json.dumps({"positions": [{"ticker": "META", "units": 2, "entry_price": 10, "mode": "paper"}]}),
                encoding="utf-8",
            )

            result = remove_paper_position("META", path=path)
            payload = json.loads(path.read_text(encoding="utf-8"))

            self.assertTrue(result["ok"])
            self.assertEqual(payload["positions"], [])

    def test_close_paper_position_keeps_prior_watchlist_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "portfolio.json"
            path.write_text(json.dumps({"positions": [{"ticker": "META", "watchlist": True}]}), encoding="utf-8")

            simulate_paper_position("META", units=10, entry_price=20, path=path)
            result = remove_paper_position("META", path=path)
            payload = json.loads(path.read_text(encoding="utf-8"))

            self.assertTrue(result["ok"])
            self.assertEqual(payload["positions"], [{"display_name": "META", "ticker": "META", "watchlist": True}])

    def test_market_search_local_fallback_does_not_need_secret(self) -> None:
        fake_settings = SimpleNamespace(fmp_api_key="", fmp_live_enabled=False)
        with patch("services.dashboard.search_market_ticker.load_settings", return_value=fake_settings):
            result = search_market_ticker("meta")

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "local_fallback")
        self.assertEqual(result["results"][0]["ticker"], "META")

    def test_market_search_by_company_name_uses_fmp_search(self) -> None:
        fake_settings = SimpleNamespace(fmp_api_key="secret", fmp_live_enabled=True)
        with (
            patch("services.dashboard.search_market_ticker.load_settings", return_value=fake_settings),
            patch("services.dashboard.search_market_ticker.FmpClient") as client_class,
        ):
            client = client_class.return_value
            client.search_symbols.return_value = [{"symbol": "META", "name": "Meta Platforms", "exchange": "NASDAQ"}]
            client.get_quote.return_value = {
                "price": 430.25,
                "change": 4.1,
                "changesPercentage": 0.96,
                "previousClose": 426.15,
            }

            result = search_market_ticker("Meta Platforms")

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "found")
        self.assertEqual(result["results"][0]["ticker"], "META")
        self.assertEqual(result["results"][0]["name"], "Meta Platforms")
        self.assertEqual(result["results"][0]["current_price"], 430.25)


if __name__ == "__main__":
    unittest.main()
