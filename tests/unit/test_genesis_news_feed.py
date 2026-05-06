from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from services.genesis.news_feed import get_news_source_status, get_recent_market_news


class GenesisNewsFeedTests(unittest.TestCase):
    @patch("services.genesis.news_feed.FmpClient")
    @patch("services.genesis.news_feed.load_settings")
    def test_recent_news_are_deduplicated_and_visual(self, mock_settings: Mock, mock_client_cls: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="fmp-key", fmp_live_enabled=True)
        client = mock_client_cls.return_value
        client.get_market_news.return_value = [
            {
                "title": "NVIDIA rallies after AI demand update",
                "text": "Demand update lifts chip sentiment.",
                "site": "Market Source",
                "publishedDate": "2026-05-05T12:00:00Z",
                "symbol": "NVDA",
                "image": "https://example.com/nvda.jpg",
                "url": "https://example.com/nvda",
            },
            {
                "title": "NVIDIA rallies after AI demand update",
                "text": "Duplicate title.",
                "site": "Market Source",
                "publishedDate": "2026-05-05T12:05:00Z",
                "symbol": "NVDA",
            },
            {
                "title": "Very old headline",
                "text": "Old.",
                "publishedDate": "2026-01-01T12:00:00Z",
                "symbol": "NVDA",
            },
        ]
        client.get_stock_news.return_value = []

        items = get_recent_market_news(["NVDA"], limit=5, max_age_days=30)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["tickers"], ["NVDA"])
        self.assertEqual(items[0]["impact"], "bullish")
        self.assertEqual(items[0]["image_url"], "https://example.com/nvda.jpg")
        self.assertEqual(items[0]["source"], "Market Source")
        self.assertTrue(items[0]["is_important"])
        self.assertGreaterEqual(items[0]["recency_score"], 1)
        self.assertGreaterEqual(items[0]["relevance_score"], 1)
        self.assertIn("why_it_matters", items[0])
        self.assertEqual(get_news_source_status()["fmp_market_news"]["status"], "ok")

    @patch("services.genesis.news_feed.FmpClient")
    @patch("services.genesis.news_feed.load_settings")
    def test_news_fallback_is_not_empty(self, mock_settings: Mock, mock_client_cls: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="fmp-key", fmp_live_enabled=True)
        client = mock_client_cls.return_value
        client.get_market_news.return_value = []
        client.get_stock_news.return_value = []

        items = get_recent_market_news(["BNO"], limit=3)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["source"], "Genesis")
        self.assertIn("Genesis mantiene vigilancia", items[0]["title"])
        self.assertTrue(items[0]["is_important"])


if __name__ == "__main__":
    unittest.main()
