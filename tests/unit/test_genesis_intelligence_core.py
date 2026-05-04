from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from api.main import create_app
from integrations.fmp.client import FmpClient
from services.genesis.agent_router import AgentRouter
from services.genesis.chart_image_analysis import analyze_chart_image
from services.genesis.chart_agent import ChartAgent
from services.genesis.image_chart_agent import ImageChartAgent
from services.genesis.llm_orchestrator import LlmOrchestrator
from services.genesis.market_format import format_market_number, market_class
from services.genesis.memory_store import MemoryStore
from services.genesis.memory_agent import MemoryAgent
from services.genesis.price_agent import PriceAgent
from services.genesis.price_truth_service import PriceTruthService
from services.genesis.price_truth import get_verified_market_quote, validate_price_sanity
from services.genesis.response_composer import ResponseComposer
from services.genesis.returns_engine import calculate_returns, flatten_return_details
from services.genesis.technical_analysis import compute_technical_indicators
from services.genesis.technical_agent import TechnicalAgent
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
            "que hora es": [],
            "dame un resumen del dia": [],
            "oye genesis como se ve el mercado": [],
            "como estuvo el mercado el viernes pasado": [],
            "dame una grafica de bno": ["BNO"],
            "compara meta vs nvda": ["META", "NVDA"],
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
            store.save_message("default", "user", "analiza NVDA", {"OPENAI_API_KEY": "SECRET"})
            store.save_learned_context("asset_interest:NVDA", {"ticker": "NVDA"}, "test", "alta")
            store.track_entity("NVDA", "asset", {"reason": "test"})
            store.save_recent_topic("ticker_analysis", {"ticker": "NVDA"})

            fresh = MemoryStore(database_url="", sqlite_path=path)
            events = fresh.get_recent_events(10, "ticker_analysis")
            messages = fresh.get_recent_messages("default", 10)
            summary = fresh.get_memory_summary("NVDA")

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["payload"]["ticker"], "NVDA")
        self.assertEqual(messages[0]["content"], "analiza NVDA")
        self.assertEqual(summary["tracked_entities"][0]["ticker"], "NVDA")
        self.assertEqual(summary["learned_context"][0]["key"], "asset_interest:NVDA")
        rendered = json.dumps(events)
        self.assertNotIn("SECRET", rendered)
        self.assertNotIn("FMP_API_KEY", rendered)
        self.assertNotIn("OPENAI_API_KEY", json.dumps(messages))


class GenesisToolRouterTests(unittest.TestCase):
    def test_agent_router_classifies_general_intents_without_fake_tickers(self) -> None:
        router = AgentRouter()

        self.assertEqual(router.route("que hora es").intent, "time")
        self.assertEqual(router.route("dame un resumen del dia").intent, "daily_briefing")
        self.assertEqual(router.route("oye genesis como se ve el mercado").intent, "market_overview")
        self.assertEqual(router.route("como estuvo el mercado el viernes pasado").intent, "market_overview")
        self.assertEqual(router.route("que aprendiste de mis consultas recientes").intent, "memory_query")
        self.assertEqual(router.route("dame rsi y macd de nvda").intent, "technical_indicators")
        self.assertEqual(router.route("dame una grafica de nvda con sma 50").intent, "chart_request")

    def test_app_config_exposes_genesis_intelligence_endpoints(self) -> None:
        app_config = create_app()

        self.assertEqual(app_config["genesis_ask_endpoint"], "/api/genesis/ask")
        self.assertEqual(app_config["genesis_image_analysis_endpoint"], "/api/genesis/analyze-image")
        self.assertEqual(app_config["genesis_memory_recent_endpoint"], "/api/genesis/memory/recent")
        self.assertEqual(app_config["dashboard_chart_endpoint"], "/api/dashboard/chart?ticker={symbol}&range={range}")

    def test_greeting_is_human(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = route_message("hola", memory=store)

        self.assertEqual(payload["intent"], "greeting")
        self.assertIn("Hola", payload["answer"])

    def test_time_request_is_not_ticker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = route_message("que hora es", memory=store)

        self.assertEqual(payload["intent"], "time")
        self.assertEqual(payload["tickers"], [])
        self.assertIn("Son las", payload["answer"])

    @patch("services.genesis.price_agent.get_verified_market_quote")
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

        self.assertEqual(payload["intent"], "chart_request")
        self.assertEqual(payload["chart"]["ticker"], "NVDA")
        mock_quote.assert_called_once_with("NVDA")
        self.assertIn("$905.25", payload["answer"])

    def test_weather_fallback_is_honest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = route_message("como esta el clima en Mazatlan", memory=store)

        self.assertEqual(payload["intent"], "weather")
        self.assertIn("No tengo proveedor de clima", payload["answer"])

    def test_briefing_and_market_overview_do_not_use_fake_tickers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            price_agent = Mock()
            price_agent.quote.return_value = {"ticker": "SPY", "current_price": None}
            with patch("services.genesis.market_overview_agent.get_price_agent", return_value=price_agent):
                briefing = route_message("dame un resumen del dia", memory=store)
                overview = route_message("oye genesis como se ve el mercado", memory=store)
                friday = route_message("como estuvo el mercado el viernes pasado", memory=store)

        self.assertEqual(briefing["intent"], "daily_briefing")
        self.assertEqual(briefing["tickers"], [])
        self.assertEqual(overview["intent"], "market_overview")
        self.assertEqual(overview["tickers"], [])
        self.assertEqual(friday["intent"], "market_overview")
        self.assertEqual(friday["tickers"], [])

    def test_memory_query_uses_persistent_context_without_fake_ticker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            store.track_entity("NVDA", "asset", {"reason": "test"})
            payload = route_message("que aprendiste de mis consultas recientes", memory=store)

        self.assertEqual(payload["intent"], "memory_query")
        self.assertEqual(payload["tickers"], [])
        self.assertIn("NVDA", payload["answer"])

    def test_agent_modules_exist_as_internal_brain_components(self) -> None:
        self.assertIsInstance(PriceAgent(), PriceAgent)
        self.assertIsInstance(PriceTruthService(), PriceTruthService)
        self.assertIsInstance(ChartAgent(), ChartAgent)
        self.assertIsInstance(TechnicalAgent(), TechnicalAgent)
        self.assertIsInstance(ImageChartAgent(), ImageChartAgent)
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsInstance(MemoryAgent(MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")), MemoryAgent)
        self.assertIsInstance(ResponseComposer(), ResponseComposer)
        with patch("services.genesis.llm_orchestrator.load_settings", return_value=SimpleNamespace(genesis_llm_enabled=False, openai_api_key="", genesis_llm_model="test")):
            self.assertFalse(LlmOrchestrator().enabled())


class GenesisReturnsEngineTests(unittest.TestCase):
    def test_returns_are_calculated_from_first_and_last_close_by_range(self) -> None:
        points = [
            {"date": "2016-01-01", "close": 50},
            {"date": "2021-01-02", "close": 100},
            {"date": "2025-01-01", "close": 200},
            {"date": "2025-12-01", "close": 240},
            {"date": "2025-12-25", "close": 250},
            {"date": "2026-01-01", "close": 300},
        ]

        details = calculate_returns(points, [{"date": "2026-01-01 09:30:00", "close": 290}, {"date": "2026-01-01 16:00:00", "close": 300}])
        returns = flatten_return_details(details)

        self.assertEqual(returns["1D"], 3.4483)
        self.assertEqual(returns["1W"], 20.0)
        self.assertEqual(returns["1M"], 25.0)
        self.assertEqual(returns["1Y"], 50.0)
        self.assertEqual(returns["5Y"], 200.0)
        self.assertEqual(returns["MAX"], 500.0)
        self.assertEqual(details["MAX"]["first_close"], 50)
        self.assertEqual(details["MAX"]["last_close"], 300)

    def test_fmp_client_chooses_longest_history_for_unlimited_max(self) -> None:
        client = FmpClient("test-key")
        stable = [{"date": "2021-01-01", "close": 100}]
        legacy = [{"date": f"201{i}-01-01", "close": 50 + i} for i in range(7)]

        with patch.object(client, "_request_json", side_effect=[(200, stable), (200, {"historical": legacy})]):
            rows = client.get_historical_eod("NVDA", limit=None)

        self.assertEqual(rows, sorted(legacy, key=lambda item: item["date"], reverse=True))
        self.assertGreater(len(rows), len(stable))


class GenesisTechnicalAnalysisTests(unittest.TestCase):
    def test_indicators_include_rsi_macd_moving_averages_and_fibonacci(self) -> None:
        candles = [
            {
                "open": 100 + index,
                "high": 103 + index,
                "low": 98 + index,
                "close": 101 + index,
                "volume": 1000 + index * 10,
            }
            for index in range(240)
        ]

        indicators = compute_technical_indicators(candles)

        self.assertTrue(indicators["ok"])
        self.assertIsNotNone(indicators["rsi"])
        self.assertIsNotNone(indicators["macd"]["line"])
        self.assertIsNotNone(indicators["sma"]["200"])
        self.assertIsNotNone(indicators["ema"]["200"])
        self.assertIn("0.618", indicators["fibonacci"])
        self.assertEqual(indicators["golden_pocket"]["from"], indicators["fibonacci"]["0.65"])

    @patch("services.genesis.price_agent.get_verified_market_quote")
    @patch("services.genesis.technical_agent.get_asset_chart_series")
    def test_technical_request_returns_backend_indicators(self, mock_chart: Mock, mock_quote: Mock) -> None:
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
        mock_chart.return_value = {
            "ok": True,
            "ticker": "NVDA",
            "range": "1Y",
            "indicators": {
                "rsi": 62.4,
                "macd": {"line": 4.2},
                "support": 880,
                "resistance": 930,
                "golden_pocket": {"from": 890, "to": 900},
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = route_message("dame RSI, MACD y Fibonacci de NVDA", memory=store)

        self.assertEqual(payload["intent"], "technical_indicators")
        self.assertEqual(payload["technical"]["indicators"]["rsi"], 62.4)
        self.assertIn("Indicadores pedidos", payload["answer"])


class GenesisImageAnalysisTests(unittest.TestCase):
    @patch("services.genesis.chart_image_analysis.load_settings")
    def test_image_analysis_fallback_without_vision_provider(self, mock_settings: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(
            genesis_vision_enabled=False,
            genesis_llm_enabled=False,
            openai_api_key="",
        )
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = analyze_chart_image({"message": "analiza esta grafica de NVDA", "image": {}}, memory=store)

        self.assertEqual(payload["intent"], "image_chart_analysis")
        self.assertEqual(payload["status"], "vision_not_configured")
        self.assertEqual(payload["tickers"], ["NVDA"])
        self.assertIn("falta proveedor de vision", payload["answer"])


if __name__ == "__main__":
    unittest.main()
