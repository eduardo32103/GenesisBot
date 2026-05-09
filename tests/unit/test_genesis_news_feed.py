from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from services.dashboard.get_news_snapshot import get_news_snapshot
from services.genesis.news_feed import get_news_source_status, get_recent_market_news


class GenesisNewsFeedTests(unittest.TestCase):
    @patch("services.genesis.news_feed.FmpClient")
    @patch("services.genesis.news_feed._fetch_public_rss_news")
    @patch("services.genesis.news_feed.load_settings")
    def test_recent_news_are_deduplicated_and_visual(self, mock_settings: Mock, mock_rss: Mock, mock_client_cls: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="fmp-key", fmp_live_enabled=True)
        mock_rss.return_value = []
        client = mock_client_cls.return_value
        client.get_market_news.return_value = [
            {
                "title": "Alphabet Inc. (GOOGL) Stock Price, News, Quote & History",
                "text": "Generic quote page.",
                "site": "Yahoo Finance",
                "publishedDate": "2026-05-05T11:00:00Z",
                "symbol": "GOOGL",
                "url": "https://example.com/quote",
            },
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
        self.assertNotIn("Stock Price", items[0]["title"])
        self.assertEqual(items[0]["original_title"], "NVIDIA rallies after AI demand update")
        self.assertIn("sube", items[0]["title"].casefold())
        self.assertEqual(items[0]["language"], "es")
        self.assertEqual(items[0]["tickers"], ["NVDA"])
        self.assertEqual(items[0]["impact"], "bullish")
        self.assertEqual(items[0]["image_url"], "https://example.com/nvda.jpg")
        self.assertEqual(items[0]["image_kind"], "real")
        self.assertEqual(items[0]["source"], "Market Source")
        self.assertTrue(items[0]["id"])
        self.assertTrue(items[0]["is_latest"])
        self.assertTrue(items[0]["is_important"])
        self.assertGreaterEqual(items[0]["recency_score"], 1)
        self.assertGreaterEqual(items[0]["relevance_score"], 1)
        self.assertIn("why_it_matters", items[0])
        self.assertIn("why_it_matters_es", items[0])
        self.assertIn("genesis_takeaway_es", items[0])
        self.assertIn("what_to_watch_es", items[0])
        self.assertIn("relative_time", items[0])
        self.assertIn("thumbnail_url", items[0])
        self.assertEqual(items[0]["asset_names_affected"], ["NVDA"])
        self.assertIn("tickers_affected", items[0])
        self.assertIn("watch_points", items[0])
        self.assertEqual(items[0]["sentiment"], items[0]["impact"])
        self.assertEqual(get_news_source_status()["fmp_market_news"]["status"], "ok")

    @patch("services.genesis.news_feed.FmpClient")
    @patch("services.genesis.news_feed._fetch_public_rss_news")
    @patch("services.genesis.news_feed.load_settings")
    def test_news_fallback_is_not_empty(self, mock_settings: Mock, mock_rss: Mock, mock_client_cls: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="fmp-key", fmp_live_enabled=True)
        mock_rss.return_value = []
        client = mock_client_cls.return_value
        client.get_market_news.return_value = []
        client.get_stock_news.return_value = []

        items = get_recent_market_news(["BNO"], limit=3)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["source"], "Genesis")
        self.assertIn("Genesis mantiene vigilancia", items[0]["title"])
        self.assertTrue(items[0]["is_important"])

    @patch("services.genesis.news_feed._fetch_og_image")
    @patch("services.genesis.news_feed._fetch_public_rss_news")
    @patch("services.genesis.news_feed.FmpClient")
    @patch("services.genesis.news_feed.load_settings")
    def test_rss_fallback_produces_recent_real_news(self, mock_settings: Mock, mock_client_cls: Mock, mock_rss: Mock, mock_og: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="", fmp_live_enabled=False)
        mock_client_cls.return_value.get_market_news.return_value = []
        mock_og.return_value = "https://example.com/oil.jpg"
        mock_rss.return_value = [
            {
                "title": "Brent crude rises as supply risk returns",
                "summary": "Oil traders watch supply risk.",
                "source": "Reuters",
                "publishedDate": "2026-05-06T12:00:00+00:00",
                "url": "https://example.com/oil",
                "image_url": "",
            }
        ]

        items = get_recent_market_news(["BZ=F"], limit=5, max_age_days=30)

        self.assertEqual(items[0]["source"], "Reuters")
        self.assertEqual(items[0]["category"], "commodity")
        self.assertIn("Brent", items[0]["title"])
        self.assertEqual(items[0]["image_url"], "https://example.com/oil.jpg")
        self.assertIn("BZ=F", items[0]["tickers"])
        self.assertIn("Brent Crude Oil", items[0]["genesis_takeaway_es"])
        self.assertIn("Brent Crude Oil", items[0]["what_to_watch_es"])
        self.assertNotIn("BZ=F", items[0]["genesis_takeaway_es"])
        self.assertNotIn("BZ=F", items[0]["what_to_watch_es"])
        self.assertTrue(items[0]["id"])

    @patch("services.dashboard.get_news_snapshot.get_recent_market_news")
    @patch("services.dashboard.get_news_snapshot.get_news_source_status")
    @patch("services.dashboard.get_news_snapshot._focus_tickers")
    def test_dashboard_news_snapshot_separates_important_and_latest(self, mock_focus: Mock, mock_status: Mock, mock_recent: Mock) -> None:
        mock_focus.return_value = ["NVDA"]
        mock_status.return_value = {"fmp_market_news": {"status": "ok"}}
        mock_recent.return_value = [
            {
                "id": "n1",
                "title": "NVDA sube por demanda de IA",
                "published_at": "2026-05-06T12:00:00+00:00",
                "tickers": ["NVDA"],
                "is_important": True,
            },
            {
                "id": "n2",
                "title": "Mercado mixto",
                "published_at": "2026-05-05T12:00:00+00:00",
                "tickers": [],
                "is_important": False,
            },
        ]

        payload = get_news_snapshot(limit=12)

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["items"][0]["id"], "n1")
        self.assertEqual(payload["important"][0]["id"], "n1")
        self.assertEqual(payload["latest"][0]["id"], "n1")
        self.assertEqual(payload["sections"]["mine"][0]["id"], "n1")
        self.assertEqual(payload["source_status"]["fmp_market_news"]["status"], "ok")

    @patch("services.dashboard.get_news_snapshot.get_recent_market_news")
    @patch("services.dashboard.get_news_snapshot.get_news_source_status")
    @patch("services.dashboard.get_news_snapshot._focus_tickers")
    def test_dashboard_news_snapshot_does_not_fake_important_section(self, mock_focus: Mock, mock_status: Mock, mock_recent: Mock) -> None:
        mock_focus.return_value = ["NVDA"]
        mock_status.return_value = {"fmp_market_news": {"status": "ok"}}
        mock_recent.return_value = [
            {
                "id": "n1",
                "title": "Mercado mixto",
                "published_at": "2026-05-05T12:00:00+00:00",
                "tickers": [],
                "is_important": False,
            }
        ]

        payload = get_news_snapshot(limit=12)

        self.assertEqual(payload["important"], [])
        self.assertEqual(payload["sections"]["important"], [])
        self.assertEqual(payload["latest"][0]["id"], "n1")


if __name__ == "__main__":
    unittest.main()
