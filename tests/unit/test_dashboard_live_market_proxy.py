from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from api import main as api_main


class DashboardLiveMarketProxyTests(unittest.TestCase):
    def test_local_port_does_not_disable_live_proxy(self) -> None:
        env = {
            "PORT": "8765",
            "FMP_API_KEY": "",
            "FMP_LIVE_ENABLED": "",
            "OPENAI_API_KEY": "",
            "GENESIS_LLM_ENABLED": "",
            "RAILWAY_ENVIRONMENT": "",
            "RAILWAY_PROJECT_ID": "",
            "RAILWAY_SERVICE_ID": "",
            "RAILWAY_PUBLIC_DOMAIN": "",
            "RAILWAY_PRIVATE_DOMAIN": "",
            "GENESIS_DISABLE_PROD_PROXY": "",
        }
        with patch.dict("os.environ", env, clear=False):
            self.assertTrue(api_main._local_live_sources_missing())

    def test_railway_runtime_does_not_proxy_to_itself(self) -> None:
        env = {
            "PORT": "8765",
            "FMP_API_KEY": "",
            "FMP_LIVE_ENABLED": "",
            "OPENAI_API_KEY": "",
            "RAILWAY_ENVIRONMENT": "production",
            "GENESIS_DISABLE_PROD_PROXY": "",
        }
        with patch.dict("os.environ", env, clear=False):
            self.assertFalse(api_main._local_live_sources_missing())

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
    @patch("api.main.search_dashboard_market_ticker")
    def test_market_search_recomputes_zero_change_pct_from_previous_close(self, mock_local, mock_proxy) -> None:
        mock_local.return_value = {"ok": True, "results": []}
        mock_proxy.return_value = {
            "ok": True,
            "results": [
                {
                    "ticker": "NVDA",
                    "current_price": 215.2,
                    "previous_close": 211.497,
                    "daily_change": 3.703,
                    "daily_change_pct": 0.0,
                }
            ],
        }

        payload = api_main._search_market_with_live_fallback("NVDA")

        self.assertAlmostEqual(payload["results"][0]["daily_change_pct"], 1.75085, places=4)
        self.assertAlmostEqual(payload["results"][0]["change_pct"], 1.75085, places=4)

    def test_market_search_proxy_payload_recomputes_zero_change_pct(self) -> None:
        raw = json.dumps(
            {
                "ok": True,
                "results": [
                    {
                        "ticker": "NVDA",
                        "current_price": 215.2,
                        "previous_close": 211.497,
                        "daily_change": 3.703,
                        "daily_change_pct": 0.0,
                    }
                ],
            }
        ).encode("utf-8")

        payload = json.loads(api_main._massage_proxy_payload("/api/dashboard/market/search", raw).decode("utf-8"))

        self.assertAlmostEqual(payload["results"][0]["daily_change_pct"], 1.75085, places=4)

    def test_news_proxy_payload_keeps_filters_separate(self) -> None:
        raw = json.dumps(
            {
                "ok": True,
                "focus_tickers": ["NVDA"],
                "items": [
                    {"id": "important", "title": "NVDA confirma demanda", "tickers": ["NVDA"], "is_important": True, "published_at": "2026-05-09T10:00:00+00:00"},
                    {"id": "latest", "title": "Mercado general mixto", "tickers": [], "is_important": False, "published_at": "2026-05-09T11:00:00+00:00"},
                    {"id": "internal", "title": "Genesis mantiene vigilancia de mercado", "tickers": [], "is_important": True, "published_at": "2026-05-09T12:00:00+00:00"},
                ],
            }
        ).encode("utf-8")

        payload = json.loads(api_main._massage_proxy_payload("/api/dashboard/news", raw).decode("utf-8"))

        self.assertEqual([row["id"] for row in payload["important"]], ["important"])
        self.assertEqual([row["id"] for row in payload["sections"]["mine"]], ["important"])
        self.assertEqual([row["id"] for row in payload["sections"]["global"]], ["latest"])
        self.assertNotIn("internal", [row["id"] for row in payload["items"]])

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

    @patch("api.main._market_search_for_proxy")
    def test_genesis_whale_payload_gets_live_monitored_volume(self, mock_proxy) -> None:
        mock_proxy.return_value = {
            "ok": True,
            "results": [
                {
                    "ticker": "BNO",
                    "name": "United States Brent Oil Fund",
                    "current_price": 53.11,
                    "volume": 4_016_556,
                    "source": "datos_directos",
                }
            ],
        }
        payload = {
            "ok": True,
            "intent": "whale_activity",
            "response_type": "whale_flow",
            "whales": {
                "snapshot": {
                    "events": [
                        {
                            "ticker": "BNO",
                            "event_type": "smart_money_estimate",
                            "confirmed": False,
                            "source": "market_flow",
                        }
                    ]
                }
            },
            "structured": {"kind": "whale_flow", "events": []},
        }

        enriched = api_main._enrich_genesis_whale_payload(payload)
        row = enriched["structured"]["events"][0]

        self.assertEqual(row["price"], 53.11)
        self.assertEqual(row["volume"], 4_016_556)
        self.assertAlmostEqual(row["monitored_dollar_volume"], 213_319_289.16)
        self.assertEqual(enriched["structured"]["metrics"]["watched_volume"], 213_319_289.16)
        self.assertIn("BNO", enriched["answer"])
        self.assertIn("vigilados", enriched["answer"])

    @patch("api.main._market_search_for_proxy")
    def test_money_flow_proxy_payload_gets_live_quote_fields(self, mock_proxy) -> None:
        mock_proxy.return_value = {
            "ok": True,
            "results": [
                {
                    "ticker": "BNO",
                    "name": "United States Brent Oil Fund",
                    "current_price": 53.11,
                    "volume": 4_016_556,
                    "source": "datos_directos",
                }
            ],
        }
        raw = json.dumps(
            {
                "ok": True,
                "items": [
                    {
                        "ticker": "BNO",
                        "event_type": "smart_money_estimate",
                        "source": "market_flow",
                    }
                ],
            }
        ).encode("utf-8")

        payload = json.loads(api_main._massage_proxy_payload("/api/dashboard/money-flow/causal", raw).decode("utf-8"))
        row = payload["items"][0]

        self.assertEqual(row["price"], 53.11)
        self.assertEqual(row["volume"], 4_016_556)
        self.assertAlmostEqual(row["monitored_dollar_volume"], 213_319_289.16)
        self.assertEqual(row["source"], "datos_directos")

    def test_confirmed_asset_quote_replaces_stale_no_price_copy(self) -> None:
        payload = {
            "ok": True,
            "intent": "ticker_analysis",
            "response_type": "asset_analysis",
            "answer": "NVDA no tiene precio confirmado; Genesis evita lectura operativa hasta reconfirmar la fuente.",
            "quote": {
                "ticker": "NVDA",
                "current_price": 215.2,
                "daily_change": 3.7,
                "daily_change_pct": 0.0,
                "previous_close": 211.5,
                "source": "datos_directos",
                "message": "No tengo precio confirmado para ese activo.",
            },
            "structured": {
                "ticker": "NVDA",
                "confidence": 0.35,
                "thesis": "NVDA no tiene precio confirmado; Genesis evita lectura operativa hasta reconfirmar la fuente.",
                "price": {},
            },
        }

        enriched = api_main._enrich_genesis_asset_quote(payload)

        self.assertIn("precio confirmado", enriched["answer"])
        self.assertNotIn("no tiene precio confirmado", enriched["answer"].lower())
        self.assertNotIn("message", enriched["quote"])
        self.assertAlmostEqual(enriched["quote"]["daily_change_pct"], 1.7494, places=3)
        self.assertIn("precio confirmado", enriched["structured"]["thesis"])
        self.assertGreaterEqual(enriched["structured"]["confidence"], 0.82)


if __name__ == "__main__":
    unittest.main()
