from __future__ import annotations

import json
import os
import unittest
from unittest.mock import patch

from api.main import (
    _enrich_genesis_trade_decision,
    _is_asset_genesis_prompt,
    _is_comparison_genesis_prompt,
    _is_opportunity_genesis_prompt,
    _is_weather_genesis_prompt,
    _massage_proxy_payload,
    _prompt_tickers,
    _resolve_dashboard_host,
    _resolve_dashboard_port,
    create_app,
)
from services.dashboard.get_genesis_answer import get_genesis_answer, get_genesis_fallback_answer


class DashboardGenesisAssistantTests(unittest.TestCase):
    def test_web_app_exposes_genesis_endpoint_contract(self) -> None:
        app_config = create_app()

        self.assertEqual(
            app_config["genesis_endpoint"],
            "/api/dashboard/genesis?q={question}&context={context}&ticker={ticker}&panel_context={json}",
        )

    def test_dashboard_server_uses_railway_port_binding(self) -> None:
        with patch.dict(os.environ, {"PORT": "9101"}, clear=True):
            self.assertEqual(_resolve_dashboard_host(), "0.0.0.0")
            self.assertEqual(_resolve_dashboard_port(), 9101)

    def test_dashboard_server_defaults_to_localhost(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_resolve_dashboard_host(), "127.0.0.1")
            self.assertEqual(_resolve_dashboard_port(), 8000)

    def test_local_proxy_corrects_casual_prompt_before_ui(self) -> None:
        stale_payload = {
            "intent": "ticker_analysis",
            "response_type": "asset_analysis",
            "tickers": ["ESTAS"],
            "answer": "ESTAS no tiene precio confirmado.",
        }
        fixed = json.loads(
            _massage_proxy_payload(
                "/api/genesis/ask",
                json.dumps(stale_payload).encode("utf-8"),
                body={"message": "como estas", "context": "genesis"},
            ).decode("utf-8")
        )

        self.assertEqual(fixed["intent"], "greeting")
        self.assertEqual(fixed["response_type"], "general_assistant")
        self.assertEqual(fixed["tickers"], [])
        self.assertNotIn("ESTAS", json.dumps(fixed))

    @patch("api.main.ask_genesis")
    def test_local_proxy_corrects_whale_prompt_before_ui(self, mock_ask) -> None:
        mock_ask.return_value = {
            "ok": True,
            "intent": "whale_activity",
            "response_type": "whale_flow",
            "tickers": [],
            "answer": "Genesis resume flujo institucional sin inventar entidad.",
            "kind": "whale_flow",
        }
        stale_payload = {
            "intent": "ticker_analysis",
            "response_type": "asset_analysis",
            "tickers": ["ESTA"],
            "answer": "ESTA tiene precio confirmado.",
            "quote": {"ticker": "ESTA", "current_price": 71.77},
        }
        fixed = json.loads(
            _massage_proxy_payload(
                "/api/genesis/ask",
                json.dumps(stale_payload).encode("utf-8"),
                body={"message": "que esta pasando con las ballenas", "context": "genesis"},
            ).decode("utf-8")
        )

        self.assertEqual(fixed["intent"], "whale_activity")
        self.assertEqual(fixed["response_type"], "whale_flow")
        self.assertEqual(fixed["tickers"], [])
        self.assertNotIn("ESTA", json.dumps(fixed))

    @patch("api.main.ask_genesis")
    def test_local_proxy_corrects_news_prompt_before_ui(self, mock_ask) -> None:
        mock_ask.return_value = {
            "ok": True,
            "intent": "macro_news",
            "response_type": "news_brief",
            "tickers": [],
            "answer": "Genesis resume titulares reales sin convertir noticias en ticker.",
            "kind": "news_brief",
        }
        stale_payload = {
            "intent": "ticker_analysis",
            "response_type": "asset_analysis",
            "tickers": ["ENH"],
            "answer": "ENH tiene precio confirmado.",
            "quote": {"ticker": "ENH", "current_price": 92.98},
        }
        fixed = json.loads(
            _massage_proxy_payload(
                "/api/genesis/ask",
                json.dumps(stale_payload).encode("utf-8"),
                body={"message": "que esta pasando en noticias?", "context": "genesis"},
            ).decode("utf-8")
        )

        self.assertEqual(fixed["intent"], "macro_news")
        self.assertEqual(fixed["response_type"], "news_brief")
        self.assertEqual(fixed["tickers"], [])
        self.assertNotIn("ENH", json.dumps(fixed))

    def test_genesis_intent_router_does_not_treat_news_or_memory_as_asset(self) -> None:
        self.assertFalse(_is_asset_genesis_prompt("que esta pasando en noticias?"))
        self.assertFalse(_is_asset_genesis_prompt("que hicimos el viernes pasado?"))
        self.assertTrue(_is_comparison_genesis_prompt("compara nvda vs bno"))

    def test_genesis_intent_router_keeps_opportunities_out_of_fake_tickers(self) -> None:
        self.assertTrue(_is_opportunity_genesis_prompt("que oportunidades hay para comprar con cautela"))
        self.assertTrue(_is_opportunity_genesis_prompt("Genesis, caza buenos precios"))
        self.assertFalse(_is_asset_genesis_prompt("que compro hoy con buena validacion"))
        self.assertEqual(_prompt_tickers("que compro hoy con buena validacion"), [])

    @patch("api.main.get_dashboard_opportunities")
    def test_local_proxy_routes_opportunity_prompt_to_radar(self, mock_opportunities) -> None:
        mock_opportunities.return_value = {
            "ok": True,
            "items": [
                {
                    "ticker": "NVDA",
                    "asset_name": "NVIDIA Corporation",
                    "price": 215.2,
                    "opportunity_score": 82,
                    "decision": "buy_cautiously",
                    "decision_label_es": "Comprar con cautela",
                    "dollar_volume": 1200000000,
                    "genesis_reading_es": "NVDA queda en radar, no orden real.",
                }
            ],
            "summary": {"top_ticker": "NVDA", "top_score": 82},
            "source_status": {"provider_used": "unit"},
        }
        stale_payload = {
            "intent": "ticker_analysis",
            "response_type": "asset_analysis",
            "tickers": ["CAUTELA"],
            "answer": "CAUTELA no tiene precio confirmado.",
        }

        fixed = json.loads(
            _massage_proxy_payload(
                "/api/genesis/ask",
                json.dumps(stale_payload).encode("utf-8"),
                body={"message": "que hay para comprar con cautela?", "context": "genesis"},
            ).decode("utf-8")
        )

        self.assertEqual(fixed["intent"], "opportunities")
        self.assertEqual(fixed["response_type"], "opportunity_radar")
        self.assertEqual(fixed["tickers"], [])
        self.assertEqual(fixed["structured"]["kind"], "opportunity_radar")
        self.assertEqual(fixed["structured"]["title"], "Compra con cautela")
        self.assertEqual(fixed["structured"]["metrics"]["mode"], "cautious_buy")
        self.assertEqual(fixed["opportunities"][0]["ticker"], "NVDA")
        self.assertNotIn("CAUTELA", json.dumps(fixed))

    @patch("api.main.get_dashboard_opportunities")
    def test_local_proxy_differentiates_opportunity_modes(self, mock_opportunities) -> None:
        mock_opportunities.return_value = {
            "ok": True,
            "items": [],
            "summary": {},
            "source_status": {"provider_used": "unit"},
        }

        hunter = json.loads(
            _massage_proxy_payload(
                "/api/genesis/ask",
                json.dumps({"intent": "ticker_analysis", "response_type": "asset_analysis"}).encode("utf-8"),
                body={"message": "genesis, caza buenos precios", "context": "genesis"},
            ).decode("utf-8")
        )
        validation = json.loads(
            _massage_proxy_payload(
                "/api/genesis/ask",
                json.dumps({"intent": "ticker_analysis", "response_type": "asset_analysis"}).encode("utf-8"),
                body={"message": "que compro hoy con buena validacion", "context": "genesis"},
            ).decode("utf-8")
        )

        self.assertEqual(hunter["structured"]["title"], "Cazador de buenos precios")
        self.assertEqual(hunter["structured"]["metrics"]["mode"], "hunter")
        self.assertEqual(validation["structured"]["title"], "Validador de entradas")
        self.assertEqual(validation["structured"]["metrics"]["mode"], "validation")
        self.assertNotEqual(hunter["answer"], validation["answer"])

    @patch("services.dashboard.get_genesis_answer.get_opportunity_radar_snapshot")
    def test_service_routes_caza_buenos_precios_to_opportunities(self, mock_snapshot) -> None:
        mock_snapshot.return_value = {
            "ok": True,
            "items": [
                {
                    "ticker": "NVDA",
                    "opportunity_score": 78,
                    "decision_label_es": "Comprar con cautela",
                    "genesis_reading_es": "NVDA en radar paper.",
                    "what_to_watch_es": "Confirmar ruptura con volumen.",
                }
            ],
            "summary": {"top_ticker": "NVDA"},
        }

        payload = get_genesis_answer("genesis, caza buenos precios")

        self.assertEqual(payload["intent"], "opportunities")
        self.assertEqual(payload["context"]["tickers"], [])
        self.assertIn("Oportunidad principal", payload["answer"])

    @patch("api.main.get_weather_answer")
    def test_local_proxy_routes_weather_before_market_or_asset(self, mock_weather) -> None:
        mock_weather.return_value = {
            "ok": True,
            "intent": "weather",
            "city": "Los Mochis, Sinaloa, Mexico",
            "answer": "En Los Mochis esta despejado, cerca de 27.0 C. Fuente: Open-Meteo.",
            "source": "open_meteo",
            "condition": "despejado",
            "icon": "\u2600\ufe0f",
            "temperature": 27.0,
            "feels_like": 28.0,
            "min_temp": 22.0,
            "max_temp": 34.0,
            "precipitation_probability": 0,
            "wind_speed": 8.0,
            "updated_at": "2026-05-10T22:00",
        }
        stale_payload = {
            "ok": True,
            "intent": "market_overview",
            "response_type": "market_summary",
            "answer": "Panorama no concluyente",
        }

        fixed = json.loads(
            _massage_proxy_payload(
                "/api/genesis/ask",
                json.dumps(stale_payload).encode("utf-8"),
                body={"message": "como esta el clima en los mochis", "context": "genesis"},
            ).decode("utf-8")
        )

        self.assertEqual(fixed["intent"], "weather")
        self.assertEqual(fixed["response_type"], "weather")
        self.assertEqual(fixed["tickers"], [])
        self.assertIn("Los Mochis", fixed["weather"]["city"])
        self.assertNotIn("Panorama no concluyente", json.dumps(fixed))

    def test_weather_words_are_not_tickers(self) -> None:
        self.assertTrue(_is_weather_genesis_prompt("como esta el clima en los mochis"))
        self.assertFalse(_is_asset_genesis_prompt("como esta el clima en los mochis"))
        self.assertEqual(_prompt_tickers("como esta el clima en los mochis"), [])
        self.assertEqual(_prompt_tickers("hola genesis como estas"), [])

    @patch("api.main.get_dashboard_alerts")
    @patch("api.main.get_dashboard_news")
    @patch("api.main._market_search_for_proxy")
    def test_local_proxy_replaces_empty_market_overview_with_live_briefing(self, mock_search, mock_news, mock_alerts) -> None:
        prices = {
            "SPY": (737.62, 0.42, 58_000_000),
            "QQQ": (711.23, 0.61, 42_000_000),
            "BTC-USD": (80_354.12, -0.18, 30_000_000_000),
            "BZ=F": (53.11, -1.04, 4_016_556),
            "NVDA": (215.20, 1.75, 134_128_204),
        }

        def fake_search(query: str) -> dict:
            price, change_pct, volume = prices.get(query, (100.0, 0.0, 1_000_000))
            return {
                "results": [
                    {
                        "ticker": query,
                        "asset_name": query,
                        "current_price": price,
                        "daily_change_pct": change_pct,
                        "volume": volume,
                    }
                ],
                "provider_used": "test",
            }

        mock_search.side_effect = fake_search
        mock_news.return_value = {
            "important": [
                {"title_es": "Futuros de tecnologia suben antes de la apertura", "source": "test"},
            ],
            "latest": [],
        }
        mock_alerts.return_value = {
            "items": [
                {"ticker": "NVDA", "title_es": "NVDA lidera con volumen visible"},
            ]
        }
        stale_payload = {
            "ok": True,
            "intent": "market_overview",
            "response_type": "market_summary",
            "answer": "Panorama no concluyente: No hay evidencia reciente suficiente para elevar activos en prioridad",
            "structured": {"metrics": {"news": 0, "alerts": 0}},
        }

        fixed = json.loads(
            _massage_proxy_payload(
                "/api/genesis/ask",
                json.dumps(stale_payload).encode("utf-8"),
                body={"message": "como ves el mercado para manana?", "context": "genesis"},
            ).decode("utf-8")
        )

        self.assertEqual(fixed["intent"], "market_overview")
        self.assertEqual(fixed["response_type"], "market_summary")
        self.assertEqual(fixed["tickers"], [])
        self.assertIn("Lectura para manana", fixed["answer"])
        self.assertIn("SPY", fixed["answer"])
        self.assertNotIn("Panorama no concluyente", fixed["answer"])
        self.assertEqual(fixed["source_status"]["quote_count"], 5)
        self.assertTrue(fixed["structured"]["sections"])

    @patch("api.main._market_search_for_proxy")
    def test_local_proxy_corrects_comparison_prompt_before_asset(self, mock_search) -> None:
        def fake_search(query: str) -> dict:
            return {
                "results": [
                    {
                        "ticker": query,
                        "asset_name": query,
                        "current_price": 100 if query == "NVDA" else 50,
                        "daily_change_pct": 1.2 if query == "NVDA" else -0.5,
                        "volume": 2_000_000,
                    }
                ],
                "provider_used": "test",
            }

        mock_search.side_effect = fake_search
        stale_payload = {
            "intent": "ticker_analysis",
            "response_type": "asset_analysis",
            "tickers": ["NVDA"],
            "answer": "NVDA tiene precio confirmado.",
        }
        fixed = json.loads(
            _massage_proxy_payload(
                "/api/genesis/ask",
                json.dumps(stale_payload).encode("utf-8"),
                body={"message": "compara nvda vs bno", "context": "genesis"},
            ).decode("utf-8")
        )

        self.assertEqual(fixed["intent"], "comparison")
        self.assertEqual(fixed["response_type"], "comparison")
        self.assertEqual(fixed["tickers"], ["NVDA", "BNO"])

    def test_trade_question_gets_decision_contract_for_ui(self) -> None:
        payload = {
            "ok": True,
            "intent": "ticker_analysis",
            "response_type": "asset_analysis",
            "kind": "asset_analysis",
            "tickers": ["NVDA"],
            "answer": "NVDA tiene precio confirmado.",
            "quote": {
                "ticker": "NVDA",
                "current_price": 215.20,
                "daily_change": 3.70,
                "daily_change_pct": 1.75,
                "volume": 134128204,
                "day_low": 212.89,
                "day_high": 217.80,
                "source_label": "FMP / datos directos",
            },
            "structured": {
                "ticker": "NVDA",
                "confidence": 0.82,
                "levels": {"support": 212.89, "resistance": 217.80},
                "indicators": {"relative_volume": 1.12, "rsi": 55.0},
            },
        }

        fixed = _enrich_genesis_trade_decision(payload, "deberia comprar nvda?")

        self.assertIn("decision", fixed)
        self.assertIn("decision", fixed["structured"])
        self.assertIn("VEREDICTO", fixed["answer"])
        self.assertIn("Entrada condicional", fixed["answer"])
        self.assertTrue(fixed["structured"]["decision"]["not_real_order"])
        self.assertNotIn("compra segura", fixed["answer"].lower())
        self.assertNotIn("garantiza", fixed["answer"].lower())

    def test_genesis_answer_is_local_and_conservative(self) -> None:
        payload = get_genesis_answer("que esta pasando")

        self.assertEqual(payload["status"], "genesis_assistant_ready")
        self.assertEqual(payload["phase"], "6.5.E")
        self.assertIn(payload["intent"], {"overview", "system", "asset_priority", "money_flow", "alerts", "reliability"})
        self.assertTrue(payload["answer"])
        self.assertIn("summary", payload["blocks"])
        self.assertIn("executive_read", payload["blocks"])
        self.assertIn("main_signals", payload["blocks"])
        self.assertIn("risks", payload["blocks"])
        self.assertIn(payload["blocks"]["reliability"], {"alta", "media", "baja", "no concluyente"})
        self.assertTrue(payload["blocks"]["next_step"])
        self.assertIn("conservadora", payload["honesty_note"].lower())
        self.assertNotIn("institucionales comprando", payload["answer"].lower())

    def test_genesis_accepts_dashboard_context(self) -> None:
        payload = get_genesis_answer("que pasa", context="money_flow", ticker="BNO")

        self.assertEqual(payload["context"]["scope"], "money_flow")
        self.assertEqual(payload["context"]["ticker"], "BNO")
        self.assertEqual(payload["intent"], "money_flow")

    def test_genesis_accepts_unified_panel_context(self) -> None:
        payload = get_genesis_answer(
            "que tan confiable esta Genesis",
            context="general",
            panel_context={
                "active_view": "command-center",
                "scope": "reliability",
                "label": "Confiabilidad",
                "reliability": {"level": "BAJA", "decision": "usable con cautela"},
                "executive_queue": {"total": "3", "review_now": "1"},
            },
        )

        self.assertEqual(payload["intent"], "reliability")
        self.assertEqual(payload["context"]["active_view"], "command-center")
        self.assertEqual(payload["context"]["signals"]["reliability"]["level"], "BAJA")
        self.assertEqual(payload["source_status"]["panel_context"], "provided")
        self.assertEqual(payload["blocks"]["reliability"], "baja")

    def test_genesis_detects_ticker_from_question(self) -> None:
        payload = get_genesis_answer("que esta pasando con BNO", context="general")

        self.assertEqual(payload["context"]["scope"], "ticker")
        self.assertEqual(payload["context"]["ticker"], "BNO")
        self.assertEqual(payload["intent"], "asset_priority")
        self.assertIn("BNO", payload["answer"])

    @patch("services.dashboard.get_genesis_answer.get_asset_decision_packet")
    def test_genesis_uses_asset_decision_packet_when_available(self, mock_packet) -> None:
        mock_packet.return_value = {
            "ticker": "NVDA",
            "company_name": "NVIDIA",
            "price": 525.0,
            "technical_read": "Precio disponible y tendencia positiva.",
            "money_flow_read": "Sin ballena identificada.",
            "macro_read": "Sin contexto macro activo.",
            "news_read": "Sin noticias activas.",
            "supports": ["Precio confirmado por datos directos."],
            "risks": ["Falta volumen relativo."],
            "missing_evidence": ["volumen relativo"],
            "confidence": "media",
            "decision_label": "Esperar confirmacion",
            "decision_reason": "la tendencia ayuda, pero faltan confirmaciones completas.",
            "entry_condition": "Comprar con cautela solo si NVDA rompe resistencia cercana con volumen.",
            "action_plan": "Esperar confirmacion de volumen antes de operar. Genesis ahora no entraria todavia.",
            "invalidation": "Se invalida si pierde soporte.",
            "next_step": "Esperar confirmacion de volumen.",
            "scenarios": {"alcista": "sube si confirma volumen", "neutral": "vigilar", "bajista": "pierde soporte"},
            "source_status": {"fmp_live_ready": True, "money_flow_available": True, "macro_available": False},
        }

        payload = get_genesis_answer("Analiza NVDA con los datos disponibles", context="general")

        mock_packet.assert_called_once_with("NVDA")
        self.assertEqual(payload["intent"], "asset_priority")
        self.assertIn("NVDA", payload["answer"])
        self.assertIn("525.0", payload["answer"])
        self.assertIn("Esperar confirmacion", payload["blocks"]["decision"])
        self.assertIn("Genesis ahora", payload["blocks"]["next_step"])
        self.assertIn("invalida", payload["blocks"]["next_step"].lower())
        self.assertTrue(payload["compact_mode"])
        self.assertIn("Entrada condicional", payload["assistant_narrative"])
        self.assertIn("Invalidacion", payload["assistant_narrative"])
        self.assertIn("Plan de accion", payload["assistant_narrative"])
        self.assertEqual(payload["source_status"]["market_data"], "available")

    @patch("services.dashboard.get_genesis_answer.get_asset_decision_packet")
    def test_explicit_question_ticker_overrides_active_context(self, mock_packet) -> None:
        mock_packet.return_value = {
            "ticker": "NVDA",
            "company_name": "NVIDIA",
            "price": 525.0,
            "technical_read": "Datos directos disponibles.",
            "money_flow_read": "Sin ballena identificada.",
            "macro_read": "Sin contexto macro activo.",
            "supports": ["Precio confirmado."],
            "risks": ["Falta volumen."],
            "missing_evidence": ["volumen relativo"],
            "confidence": "media",
            "decision_label": "Vigilar",
            "next_step": "Vigilar continuidad.",
            "scenarios": {"alcista": "confirma", "neutral": "vigilar", "bajista": "pierde soporte"},
            "source_status": {"fmp_live_ready": True, "money_flow_available": True, "macro_available": False},
        }

        payload = get_genesis_answer(
            "analiza NVDA con datos directos",
            context="ticker",
            ticker="BNO",
            panel_context={"scope": "ticker", "ticker": "BNO", "active_view": "radar"},
        )

        mock_packet.assert_called_once_with("NVDA")
        self.assertEqual(payload["context"]["ticker"], "NVDA")
        self.assertIn("NVDA", payload["answer"])
        self.assertNotIn("BNO:", payload["answer"])

    @patch("services.dashboard.get_genesis_answer.get_asset_decision_packet")
    def test_operational_question_uses_explicit_ticker(self, mock_packet) -> None:
        mock_packet.return_value = {
            "ticker": "NVDA",
            "company_name": "NVIDIA",
            "price": 525.0,
            "technical_read": "Datos directos disponibles.",
            "money_flow_read": "Sin ballena identificada.",
            "macro_read": "Sin contexto macro activo.",
            "supports": ["Precio confirmado."],
            "risks": ["Falta confirmacion adicional."],
            "missing_evidence": ["volumen relativo"],
            "confidence": "media",
            "decision_label": "Vigilar",
            "next_step": "Validar volumen y zona de entrada antes de operar.",
            "source_status": {"fmp_live_ready": True, "quote": True},
        }

        payload = get_genesis_answer("es buena idea comprar NVDA?", context="general", ticker="BNO")

        mock_packet.assert_called_once_with("NVDA")
        self.assertEqual(payload["context"]["ticker"], "NVDA")
        self.assertIn("Vigilar", payload["blocks"]["decision"])
        self.assertIn("NVDA", payload["answer"])

    @patch("services.dashboard.get_genesis_answer.get_asset_decision_packet")
    def test_buy_question_has_decision_core_blocks_without_hype(self, mock_packet) -> None:
        mock_packet.return_value = {
            "ticker": "NVDA",
            "company_name": "NVIDIA",
            "price": 525.0,
            "percent_change": 1.6,
            "technical_read": "Precio actual: 525.0. Tendencia: positiva. Zona simple 30 dias: soporte 500.00, resistencia 525.00.",
            "money_flow_read": "Sin senal confiable de Dinero Grande.",
            "whale_read": "Sin ballena identificada con la fuente activa.",
            "macro_read": "Sin contexto macro/noticias activo.",
            "news_read": "Sin catalizador macro/noticias confirmado en esta lectura.",
            "supports": ["Precio actual confirmado por datos directos.", "Tendencia positiva en el historico corto."],
            "risks": ["Volumen relativo no confirmado.", "Sin ballena identificada con la fuente activa."],
            "missing_evidence": ["volumen relativo", "ballena identificada"],
            "confidence": "media",
            "verdict": "Esperar confirmacion",
            "decision_label": "Esperar confirmacion",
            "decision_reason": "la tendencia ayuda, pero faltan confirmaciones completas.",
            "entry_condition": "Comprar con cautela solo si NVDA recupera resistencia cercana a 525.00 con volumen.",
            "action_plan": "Esperar confirmacion de volumen antes de operar. Genesis ahora no entraria todavia.",
            "invalidation": "Se invalida si pierde soporte.",
            "scenarios": {
                "alcista": "NVDA mejora si rompe resistencia con volumen.",
                "neutral": "NVDA queda en espera si no confirma direccion.",
                "bajista": "NVDA se deteriora si pierde soporte.",
            },
            "source_status": {"fmp_live_ready": True, "quote_available": True, "money_flow_available": False},
        }

        payload = get_genesis_answer("es buena idea comprar NVDA?", context="general", ticker="BNO")
        rendered_text = " ".join(
            [
                payload["answer"],
                payload["blocks"]["decision"],
                payload["blocks"]["summary"],
                payload["blocks"]["executive_read"],
                payload["blocks"]["money_flow"],
                payload["blocks"]["macro_news"],
                payload["blocks"]["next_step"],
                payload["assistant_narrative"],
                " ".join(payload["blocks"]["main_signals"]),
                " ".join(payload["blocks"]["risks"]),
                " ".join(payload["blocks"]["scenarios"]),
            ]
        )

        mock_packet.assert_called_once_with("NVDA")
        self.assertEqual(payload["context"]["ticker"], "NVDA")
        self.assertIn("VEREDICTO", payload["answer"])
        self.assertTrue(payload["compact_mode"])
        self.assertIn("Entrada condicional", payload["assistant_narrative"])
        self.assertIn("Invalidacion", payload["assistant_narrative"])
        self.assertIn("Plan de accion", payload["assistant_narrative"])
        self.assertIn("Esperar confirmacion", payload["blocks"]["decision"])
        self.assertTrue(payload["blocks"]["main_signals"])
        self.assertTrue(payload["blocks"]["risks"])
        self.assertIn("Genesis ahora", payload["blocks"]["next_step"])
        self.assertIn("No hay ballena confirmada", payload["blocks"]["money_flow"])
        self.assertNotIn("compra segura", rendered_text.lower())
        self.assertNotIn("garantiza", rendered_text.lower())
        for forbidden in ("probability ready", "probability disabled", "causalidad probabilidad", "insufficient_confirmation"):
            self.assertNotIn(forbidden, rendered_text)

    @patch("services.dashboard.get_genesis_answer.get_dashboard_radar_ticker_drilldown")
    def test_genesis_comparison_uses_both_explicit_tickers(self, mock_drilldown) -> None:
        def fake_drilldown(ticker: str) -> dict:
            return {
                "found": True,
                "ticker": ticker,
                "current_price": 100.0 if ticker == "NVDA" else 60.0,
                "decision": "vigilar",
                "main_reason": f"{ticker} con ficha disponible.",
                "current_reliability": "media",
                "market_data": {"live_ready": True, "quote_available": True, "source": "datos_directos"},
            }

        mock_drilldown.side_effect = fake_drilldown
        payload = get_genesis_answer("compara NVDA contra BNO", context="general", ticker="BNO")

        self.assertEqual([call.args[0] for call in mock_drilldown.call_args_list], ["NVDA", "BNO"])
        self.assertEqual(payload["context"]["ticker"], "NVDA")
        self.assertIn("NVDA", payload["answer"])
        self.assertIn("BNO", payload["answer"])

    @patch("services.dashboard.get_genesis_answer.get_macro_activity_snapshot")
    def test_genesis_macro_is_honest_when_no_context_exists(self, mock_macro) -> None:
        mock_macro.return_value = {
            "macro": {"available": False},
            "meta": {"macro_source": "unavailable", "activity_source": "unavailable"},
        }

        payload = get_genesis_answer("que dice Mundo/Macro", context="general")

        self.assertEqual(payload["intent"], "macro")
        self.assertEqual(payload["context"]["ticker"], "")
        self.assertIn("Sin contexto macro activo", payload["answer"])
        self.assertEqual(payload["blocks"]["reliability"], "no concluyente")

    @patch("services.dashboard.get_genesis_answer.get_money_flow_jarvis_answer")
    def test_genesis_understands_dinero_grande_as_money_flow(self, mock_money_flow) -> None:
        mock_money_flow.return_value = {
            "answer": "No hay Money Flow concluyente. Ballenas identificadas: ninguna entidad confirmada.",
            "items": [],
            "source_status": {"detection_status": "ready", "causal_status": "ready"},
            "honesty_note": "Lectura conservadora.",
        }

        payload = get_genesis_answer("que esta viendo Dinero Grande ahora", context="general")

        self.assertEqual(payload["intent"], "money_flow")
        self.assertIn("No hay ballena confirmada", payload["answer"])
        self.assertIn("Genesis", payload["answer"])
        self.assertNotIn("Money Flow", payload["answer"])
        self.assertNotIn("probability disabled", payload["answer"])

    def test_genesis_does_not_treat_casual_chat_as_ticker(self) -> None:
        payload = get_genesis_answer("como estas", context="general")

        self.assertNotEqual(payload["context"]["ticker"], "ESTAS")
        self.assertEqual(payload["context"]["tickers"], [])
        self.assertNotIn("ESTAS", payload["answer"])

    def test_genesis_does_not_treat_market_question_as_acomo_ticker(self) -> None:
        payload = get_genesis_answer("oye como esta el mercado el dia de hoy", context="general")

        self.assertEqual(payload["intent"], "market_overview")
        self.assertEqual(payload["context"]["ticker"], "")
        self.assertEqual(payload["context"]["tickers"], [])
        self.assertNotIn("ACOMO", str(payload))

    def test_genesis_blocks_do_not_expose_internal_keys(self) -> None:
        payload = get_genesis_answer("que esta pasando con BNO", context="general")
        rendered_text = " ".join(
            [
                payload["answer"],
                payload["blocks"]["summary"],
                payload["blocks"]["executive_read"],
                payload["blocks"]["next_step"],
                " ".join(payload["blocks"]["main_signals"]),
                " ".join(payload["blocks"]["risks"]),
            ]
        )

        for forbidden in (
            "queue_source",
            "alerts_origin",
            "radar_drilldown_decision_layer",
            "health_status",
            "detection_ready_causality_disabled",
        ):
            self.assertNotIn(forbidden, rendered_text)

    def test_genesis_fallback_is_human_and_structured(self) -> None:
        payload = get_genesis_fallback_answer("que pasa con XYZ", context="ticker", ticker="XYZ")

        self.assertEqual(payload["phase"], "6.5.E")
        self.assertEqual(payload["status"], "genesis_assistant_ready")
        self.assertIn("No pude leer", payload["answer"])
        self.assertEqual(payload["blocks"]["reliability"], "no concluyente")
        self.assertIn("summary", payload["blocks"])
        self.assertIn("executive_read", payload["blocks"])
        self.assertIn("main_signals", payload["blocks"])
        self.assertIn("risks", payload["blocks"])
        self.assertIn("next_step", payload["blocks"])

        rendered_text = " ".join(
            [
                payload["answer"],
                payload["blocks"]["summary"],
                payload["blocks"]["executive_read"],
                payload["blocks"]["next_step"],
                " ".join(payload["blocks"]["main_signals"]),
                " ".join(payload["blocks"]["risks"]),
            ]
        )
        self.assertNotIn("Failed to fetch", rendered_text)
        self.assertNotIn("queue_source", rendered_text)


if __name__ == "__main__":
    unittest.main()
