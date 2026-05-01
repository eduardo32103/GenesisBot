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

    def test_market_search_local_fallback_does_not_need_secret(self) -> None:
        fake_settings = SimpleNamespace(fmp_api_key="", fmp_live_enabled=False)
        with patch("services.dashboard.search_market_ticker.load_settings", return_value=fake_settings):
            result = search_market_ticker("meta")

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "local_fallback")
        self.assertEqual(result["results"][0]["ticker"], "META")


if __name__ == "__main__":
    unittest.main()
