from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from api.main import create_app
from services.genesis.market_format import format_market_number, market_class
from services.genesis.memory_store import MemoryStore
from services.genesis.price_truth import get_verified_market_quote, validate_price_sanity
from services.genesis.ticker_parser import extract_tickers_from_prompt
from services.genesis.tool_router import route_message


class GenesisTickerParserTests(unittest.TestCase):
    def test_extracts_real_tickers_without_verbs(self) -> None:
        cases = {
            "analiza nvda con graficas": ["NVDA"],
            "grafica btc-usd": ["BTC-USD"],
            "hazme una grafica de btc": ["BTC-USD"],
            "quiero ver meta": ["META"],
            "que opinas de bz=f": ["BZ=F"],
            "compara nflx contra nvda": ["NFLX", "NVDA"],
        }
        for prompt, expected in cases.items():
            with self.subTest(prompt=prompt):
                self.assertEqual(extract_tickers_from_prompt(prompt), expected)


class GenesisPriceTruthTests(unittest.TestCase):
    def test_bno_price_formats_without_scale_error(self) -> None:
        quote = get_verified_market_quote(
            "BNO",
            quote={"price": 57.27, "previousClose": 57.1, "change": 0.17, "changesPercentage": 0.3, "name": "United States Brent Oil Fund"},
            settings=SimpleNamespace(fmp_api_key="", fmp_live_enabled=False),
        )

        self.assertEqual(quote["current_price"], 57.27)
        self.assertEqual(quote["formatted_price"], "$57.27")
        self.assertTrue(quote["sanity"]["ok"])
        self.assertNotEqual(quote["formatted_price"], "$577.00")

    def test_price_sanity_guard_detects_suspicious_scale(self) -> None:
        sanity = validate_price_sanity("BNO", 577, 57.27)

        self.assertFalse(sanity["ok"])
        self.assertTrue(sanity["suspicious"])

    def test_market_color_classes(self) -> None:
        self.assertEqual(market_class(1.2), "up")
        self.assertEqual(market_class(-0.2), "down")
        self.assertEqual(market_class(0), "flat")
        self.assertEqual(market_class(None), "flat")
        self.assertEqual(format_market_number(57.27), "$57.27")


class GenesisMemoryStoreTests(unittest.TestCase):
    def test_sqlite_memory_persists_event_and_redacts_secret_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "memory.sqlite3"
            store = MemoryStore(database_url="", sqlite_path=path)
            store.save_event("ticker_analysis", {"ticker": "NVDA", "FMP_API_KEY": "SECRET"}, "test", "alta")

            fresh = MemoryStore(database_url="", sqlite_path=path)
            events = fresh.get_recent_events(10, "ticker_analysis")

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["payload"]["ticker"], "NVDA")
        rendered = json.dumps(events)
        self.assertNotIn("SECRET", rendered)
        self.assertNotIn("FMP_API_KEY", rendered)


class GenesisToolRouterTests(unittest.TestCase):
    def test_app_config_exposes_genesis_intelligence_endpoints(self) -> None:
        app_config = create_app()

        self.assertEqual(app_config["genesis_ask_endpoint"], "/api/genesis/ask")
        self.assertEqual(app_config["genesis_memory_recent_endpoint"], "/api/genesis/memory/recent")
        self.assertEqual(app_config["dashboard_chart_endpoint"], "/api/dashboard/chart?ticker={symbol}&range={range}")

    def test_greeting_is_human(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = route_message("hola", memory=store)

        self.assertEqual(payload["intent"], "greeting")
        self.assertIn("Genesis activo", payload["answer"])

    @patch("services.genesis.tool_router.get_verified_market_quote")
    def test_chart_request_uses_correct_ticker_and_verified_quote(self, mock_quote: Mock) -> None:
        mock_quote.return_value = {
            "ticker": "NVDA",
            "current_price": 905.25,
            "formatted_price": "$905.25",
            "daily_change": 12.4,
            "daily_change_pct": 1.39,
            "source_label": "Precio confirmado",
            "is_live": True,
            "source": "datos_directos",
            "previous_close": 892.85,
            "sanity": {"ok": True},
        }
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = route_message("analiza nvda con graficas", memory=store)

        self.assertEqual(payload["intent"], "chart")
        self.assertEqual(payload["chart"]["ticker"], "NVDA")
        mock_quote.assert_called_once_with("NVDA")
        self.assertIn("$905.25", payload["answer"])

    def test_weather_fallback_is_honest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = route_message("como esta el clima en Mazatlan", memory=store)

        self.assertEqual(payload["intent"], "weather")
        self.assertIn("No tengo proveedor de clima", payload["answer"])


if __name__ == "__main__":
    unittest.main()
