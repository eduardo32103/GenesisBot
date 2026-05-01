from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from services.portfolio.update_portfolio import add_ticker_to_portfolio, simulate_paper_position


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


if __name__ == "__main__":
    unittest.main()
