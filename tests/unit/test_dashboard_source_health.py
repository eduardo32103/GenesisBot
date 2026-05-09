from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from services.dashboard.get_source_health import get_source_health


class DashboardSourceHealthTests(unittest.TestCase):
    @patch("services.dashboard.get_source_health.get_weather_answer")
    @patch("services.dashboard.get_source_health.PortfolioStore")
    @patch("services.dashboard.get_source_health.MemoryStore")
    @patch("services.dashboard.get_source_health.get_news_source_status")
    @patch("services.dashboard.get_source_health.get_recent_market_news")
    @patch("services.dashboard.get_source_health.FmpClient")
    @patch("services.dashboard.get_source_health.load_settings")
    def test_source_health_is_safe_and_reports_providers(
        self,
        mock_settings: Mock,
        mock_client_cls: Mock,
        mock_news: Mock,
        mock_news_status: Mock,
        mock_memory_cls: Mock,
        mock_store_cls: Mock,
        mock_weather: Mock,
    ) -> None:
        mock_settings.return_value = SimpleNamespace(
            fmp_api_key="super-secret-fmp",
            fmp_live_enabled=True,
            openai_api_key="super-secret-openai",
            genesis_llm_enabled=True,
            genesis_llm_model="gpt-5.5",
            genesis_vision_enabled=True,
            database_url="postgres://secret",
        )
        client = mock_client_cls.return_value
        client.get_quote.return_value = {"price": 500}
        client.get_historical_eod.return_value = [{"date": "2026-05-06", "close": 500}]
        client.get_smart_money_activity.return_value = [{"source": "Insider", "entity": "Director"}]
        client.get_last_error.return_value = ""
        mock_news.return_value = [{"id": "n1"}]
        mock_news_status.return_value = {
            "rss_news": {"status": "ok", "count": 3, "elapsed_ms": 20, "cache_hit": False, "last_error_safe": ""}
        }
        mock_memory = mock_memory_cls.return_value
        mock_memory.backend = "postgres"
        mock_memory.get_memory_summary.return_value = {"recent_events": []}
        mock_store = mock_store_cls.return_value
        mock_store.status.return_value = {"backend": "postgres", "durable": True}
        mock_weather.return_value = {"ok": True, "source": "open_meteo"}

        payload = get_source_health()
        serialized = str(payload)

        self.assertTrue(payload["ok"])
        self.assertTrue(payload["fmp"]["key_configured"])
        self.assertTrue(payload["fmp"]["quote_ok"])
        self.assertEqual(payload["fmp"]["news_count"], 1)
        self.assertTrue(payload["openai"]["key_configured"])
        self.assertEqual(payload["openai"]["model"], "gpt-5.5")
        self.assertTrue(payload["database"]["portfolio_store"]["durable"])
        self.assertIn("memory_collections", payload["database"])
        self.assertIn("asset_memory", payload["database"]["memory_collections"])
        self.assertTrue(payload["weather"]["open_meteo_ok"])
        self.assertTrue(payload["rss_news"]["enabled"])
        self.assertNotIn("super-secret-fmp", serialized)
        self.assertNotIn("super-secret-openai", serialized)


if __name__ == "__main__":
    unittest.main()
