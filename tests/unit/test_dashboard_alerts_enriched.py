from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from api import main as api_main
from services.dashboard import get_alerts_snapshot as alerts_module
from services.dashboard.get_alerts_snapshot import get_alerts_snapshot


class DashboardAlertsEnrichedTests(unittest.TestCase):
    @patch("services.dashboard.get_radar_snapshot.get_radar_snapshot")
    @patch("services.dashboard.get_alerts_snapshot._fetch_alerts_snapshot")
    @patch("services.dashboard.get_alerts_snapshot.load_settings")
    def test_derived_alerts_include_price_volume_and_detail_fields(
        self,
        mock_settings: Mock,
        mock_fetch_alerts: Mock,
        mock_radar_snapshot: Mock,
    ) -> None:
        mock_settings.return_value = SimpleNamespace(database_url="")
        mock_fetch_alerts.return_value = {
            "generated_at": "2026-05-06T12:00:00+00:00",
            "summary": {
                "total_recent": 0,
                "active_alerts": 0,
                "engine_summary": "Todavia no hay alertas recientes registradas.",
            },
            "items": [],
            "recent_alerts": [],
        }
        mock_radar_snapshot.return_value = {
            "items": [
                {
                    "ticker": "NVDA",
                    "current_price": 100.0,
                    "daily_change": 4.0,
                    "daily_change_pct": 4.0,
                    "volume": 2000,
                    "avg_volume": 1000,
                    "dayHigh": 101.0,
                    "dayLow": 90.0,
                }
            ]
        }

        payload = get_alerts_snapshot()
        alert = payload["items"][0]

        self.assertEqual(alert["ticker"], "NVDA")
        self.assertEqual(alert["price"], 100.0)
        self.assertEqual(alert["change"], 4.0)
        self.assertEqual(alert["change_pct"], 4.0)
        self.assertEqual(alert["volume"], 2000)
        self.assertEqual(alert["relative_volume"], 2.0)
        self.assertEqual(alert["dollar_volume"], 200000.0)
        self.assertEqual(alert["support"], 90.0)
        self.assertEqual(alert["resistance"], 101.0)
        self.assertEqual(alert["source"], "technical")
        self.assertIn("mini_series", alert)
        self.assertIn("genesis_reading", alert)
        self.assertEqual(alert["trend"], "alcista intradia")
        self.assertIn("volumen", alert["momentum"])
        self.assertIn("what_it_means", alert)
        self.assertIn("what_to_watch", alert)
        self.assertEqual(alert["title_es"], alert["title"])
        self.assertEqual(alert["summary_es"], alert["summary"])
        self.assertIn("genesis_reading_es", alert)
        self.assertIn("what_happened_es", alert)
        self.assertIn("why_it_matters_es", alert)
        self.assertIn("what_to_watch_es", alert)
        self.assertEqual(alert["affected_portfolio_assets"], ["NVDA"])
        self.assertEqual(alert["affected_watchlist_assets"], ["NVDA"])

    @patch("services.dashboard.get_radar_snapshot.get_radar_snapshot")
    @patch("services.dashboard.get_alerts_snapshot._fetch_alerts_snapshot")
    @patch("services.dashboard.get_alerts_snapshot.load_settings")
    def test_persisted_alerts_are_enriched_with_market_fields(
        self,
        mock_settings: Mock,
        mock_fetch_alerts: Mock,
        mock_radar_snapshot: Mock,
    ) -> None:
        mock_settings.return_value = SimpleNamespace(database_url="")
        mock_fetch_alerts.return_value = {
            "generated_at": "2026-05-06T12:00:00+00:00",
            "summary": {"total_recent": 1, "active_alerts": 1},
            "items": [
                {
                    "alert_id": "db-alert-nvda",
                    "ticker": "NVDA",
                    "title": "Ruptura en vigilancia",
                    "summary": "Alerta persistida.",
                    "source": "database",
                    "created_at": "2026-05-06T12:00:00+00:00",
                }
            ],
            "recent_alerts": [],
        }
        mock_radar_snapshot.return_value = {
            "items": [
                {
                    "ticker": "NVDA",
                    "current_price": 120.0,
                    "daily_change": 2.0,
                    "daily_change_pct": 1.7,
                    "volume": 5000,
                    "avg_volume": 2500,
                    "dayHigh": 121.0,
                    "dayLow": 115.0,
                }
            ]
        }

        payload = get_alerts_snapshot()
        alert = payload["items"][0]

        self.assertEqual(alert["id"], "db-alert-nvda")
        self.assertEqual(alert["price"], 120.0)
        self.assertEqual(alert["relative_volume"], 2.0)
        self.assertEqual(alert["dollar_volume"], 600000.0)
        self.assertEqual(alert["support"], 115.0)
        self.assertEqual(alert["resistance"], 121.0)
        self.assertIn("genesis_reading", alert)
        self.assertIn("genesis_reading_es", alert)
        self.assertEqual(alert["trend"], "alcista intradia")
        self.assertIn("what_it_means", alert)
        self.assertIn("what_to_watch", alert)
        self.assertEqual(alert["title_es"], "Ruptura en vigilancia")
        self.assertIn("what_happened_es", alert)

    @patch("services.dashboard.get_alerts_snapshot._radar_by_ticker")
    def test_zero_price_alert_uses_live_market_price(self, mock_radar_by_ticker: Mock) -> None:
        mock_radar_by_ticker.return_value = {
            "BIP": {
                "ticker": "BIP",
                "name": "Brookfield Infrastructure Partners L.P.",
                "current_price": 36.77,
                "daily_change": -0.2,
                "daily_change_pct": -0.54,
                "volume": 765_653,
                "avg_volume": 900_000,
            }
        }

        rows = alerts_module._enrich_alert_items(
            [
                {
                    "alert_id": "db-alert-bip",
                    "ticker": "BIP",
                    "price": 0,
                    "title": "BIP en vigilancia",
                    "summary": "Alerta persistida con precio viejo en cero.",
                    "source": "database",
                }
            ]
        )

        self.assertEqual(rows[0]["price"], 36.77)
        self.assertEqual(rows[0]["change_pct"], -0.54)
        self.assertGreater(rows[0]["dollar_volume"], 0)

    @patch("services.dashboard.get_alerts_snapshot._fetch_opportunity_quotes_bulk")
    @patch("services.dashboard.get_alerts_snapshot.load_settings")
    def test_external_market_opportunities_use_fmp_quotes_and_strategy(
        self,
        mock_settings: Mock,
        mock_bulk_quotes: Mock,
    ) -> None:
        alerts_module._OPPORTUNITY_CACHE["expires_at"] = 0
        alerts_module._OPPORTUNITY_CACHE["items"] = []
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=True)
        mock_bulk_quotes.return_value = {
            "NVDA": {
                "name": "NVIDIA Corporation",
                "price": 215.2,
                "change": 3.7,
                "changesPercentage": 1.75,
                "volume": 134_128_204,
                "dayLow": 211.0,
                "dayHigh": 218.0,
            }
        }

        rows = alerts_module._market_opportunity_alerts()

        self.assertEqual(len(rows), 1)
        alert = rows[0]
        self.assertEqual(alert["ticker"], "NVDA")
        self.assertEqual(alert["id"], "opportunity:NVDA")
        self.assertTrue(alert["is_opportunity"])
        self.assertEqual(alert["price"], 215.2)
        self.assertEqual(alert["volume"], 134_128_204)
        self.assertAlmostEqual(alert["dollar_volume"], 28_864_389_500.8)
        self.assertIn("strategy", alert)
        self.assertIn("decision", alert)
        self.assertIn(alert["decision"], {"buy_cautiously", "watch_confirmation", "wait_for_setup", "wait", "reduce_or_sell_risk"})
        self.assertIn("decision_label_es", alert)
        self.assertIn("validación", alert["strategy"]["name"])
        self.assertNotIn("Senal", alert["strategy"]["summary"])

    def test_crypto_alert_does_not_multiply_quote_volume_into_absurd_value(self) -> None:
        alert = alerts_module._technical_alert(
            "BTC-USD",
            "BTC-USD: volumen visible",
            "BTC-USD con actividad de mercado.",
            "technical_watch",
            1.2,
            "2026-05-09T12:00:00+00:00",
            {
                "price": 80_767.34,
                "change_pct": 0.4,
                "volume": 17_765_685_248,
                "avg_volume": 34_000_000_000,
                "support": 79_000,
                "resistance": 82_000,
            },
        )

        self.assertEqual(alert["dollar_volume"], 17_765_685_248)
        self.assertIn("$17.8B", alert["strategy"]["flow_context"])
        self.assertLess(alert["dollar_volume"], 1_000_000_000_000)

    @patch("api.main._market_search_for_proxy")
    def test_proxy_alerts_add_external_opportunities_for_non_wallet_assets(self, mock_search: Mock) -> None:
        def search(ticker: str) -> dict:
            if ticker == "NVDA":
                return {
                    "results": [
                        {
                            "ticker": "NVDA",
                            "name": "NVIDIA Corporation",
                            "current_price": 215.2,
                            "daily_change_pct": 1.75,
                            "volume": 134_128_204,
                            "day_low": 211.0,
                            "day_high": 218.0,
                        }
                    ]
                }
            return {"results": []}

        mock_search.side_effect = search
        payload = {
            "summary": {},
            "items": [{"id": "alert:mara", "ticker": "MARA", "price": 12.94, "volume": 47_962_656}],
            "recent_alerts": [],
        }

        api_main._massage_alerts_payload(payload)

        self.assertEqual(payload["items"][0]["id"], "opportunity:NVDA")
        self.assertTrue(payload["items"][0]["is_opportunity"])
        self.assertEqual(payload["items"][0]["ticker"], "NVDA")
        self.assertIn("strategy", payload["items"][0])
        self.assertIn("decision_label_es", payload["items"][0])
        self.assertEqual(payload["summary"]["opportunities"], 1)


if __name__ == "__main__":
    unittest.main()
