from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from api.main import _resolve_dashboard_host, _resolve_dashboard_port, create_app
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
                " ".join(payload["blocks"]["main_signals"]),
                " ".join(payload["blocks"]["risks"]),
                " ".join(payload["blocks"]["scenarios"]),
            ]
        )

        mock_packet.assert_called_once_with("NVDA")
        self.assertEqual(payload["context"]["ticker"], "NVDA")
        self.assertIn("VEREDICTO", payload["answer"])
        self.assertIn("Esperar confirmacion", payload["blocks"]["decision"])
        self.assertTrue(payload["blocks"]["main_signals"])
        self.assertTrue(payload["blocks"]["risks"])
        self.assertIn("Genesis ahora", payload["blocks"]["next_step"])
        self.assertIn("Sin ballena identificada", payload["blocks"]["money_flow"])
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
        self.assertIn("Sin senal confiable de Dinero Grande", payload["answer"])
        self.assertNotIn("Money Flow", payload["answer"])
        self.assertNotIn("probability disabled", payload["answer"])

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
