from __future__ import annotations

import unittest
from unittest.mock import patch

from api import main as api_main


class DashboardLiveMarketProxyTests(unittest.TestCase):
    @patch("api.main._market_search_for_proxy")
    @patch("api.main.search_dashboard_market_ticker")
    def test_market_search_uses_live_proxy_when_local_has_no_price(self, mock_local, mock_proxy) -> None:
        mock_local.return_value = {
            "ok": True,
            "status": "local_fallback",
            "results": [{"ticker": "SPY", "current_price": None, "source": "sin_precio"}],
        }
        mock_proxy.return_value = {
            "ok": True,
            "status": "found",
            "results": [{"ticker": "SPY", "current_price": 737.62, "source": "datos_directos"}],
        }

        payload = api_main._search_market_with_live_fallback("SPY")

        self.assertEqual(payload["results"][0]["current_price"], 737.62)
        self.assertEqual(payload["provider_used"], "railway_fmp_proxy")

    @patch("api.main._market_search_for_proxy")
    def test_portfolio_snapshot_keeps_local_paper_and_enriches_live_quotes(self, mock_proxy) -> None:
        quotes = {
            "MARA": {"ticker": "MARA", "name": "Marathon Digital Holdings, Inc.", "current_price": 12.94, "daily_change": 0.24, "daily_change_pct": 0.0, "volume": 47_962_656, "source": "datos_directos"},
            "BNO": {"ticker": "BNO", "name": "United States Brent Oil Fund LP", "current_price": 53.11, "daily_change": -0.56, "daily_change_pct": 0.0, "volume": 4_016_556, "source": "datos_directos"},
        }
        mock_proxy.side_effect = lambda ticker: {"ok": True, "results": [quotes[str(ticker).upper()]]} if str(ticker).upper() in quotes else {"ok": False, "results": []}
        snapshot = {
            "summary": {"data_origin": "portfolio_fallback", "portfolio": {}},
            "items": [
                {"ticker": "MARA", "units": 12, "entry_price": 1212, "cost_basis": 14544, "current_price": 0, "is_investment": True, "mode": "paper", "watchlist": False},
                {"ticker": "BNO", "units": 0, "reference_price": 61.39, "current_price": 0, "watchlist": True, "source": "contingency"},
            ],
        }

        enriched = api_main._enrich_portfolio_snapshot_with_live_quotes(snapshot)

        self.assertTrue(enriched["live_proxy_enriched"])
        self.assertEqual(enriched["items"][0]["current_price"], 12.94)
        self.assertEqual(enriched["items"][0]["daily_change_pct"], 0.0)
        self.assertEqual(enriched["items"][0]["market_value"], 155.28)
        self.assertAlmostEqual(enriched["items"][0]["unrealized_pnl"], -14388.72)
        self.assertEqual(enriched["items"][1]["current_price"], 53.11)
        self.assertEqual(enriched["items"][1]["source_label"], "FMP / Railway")
        self.assertEqual(enriched["summary"]["number_of_positions"], 1)
        self.assertEqual(enriched["summary"]["watchlist_count"], 1)
        self.assertIn("live_proxy", enriched["summary"]["data_origin"])


if __name__ == "__main__":
    unittest.main()
