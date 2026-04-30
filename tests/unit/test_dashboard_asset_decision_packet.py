from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from services.dashboard.get_asset_decision_packet import get_asset_decision_packet


class DashboardAssetDecisionPacketTests(unittest.TestCase):
    @patch("services.dashboard.get_asset_decision_packet.get_macro_activity_snapshot")
    @patch("services.dashboard.get_asset_decision_packet.get_money_flow_jarvis_answer")
    @patch("services.dashboard.get_asset_decision_packet.get_radar_snapshot")
    @patch("services.dashboard.get_asset_decision_packet.FmpClient")
    @patch("services.dashboard.get_asset_decision_packet.load_settings")
    def test_builds_conservative_packet_from_direct_data(
        self,
        mock_settings: Mock,
        mock_fmp_client: Mock,
        mock_radar: Mock,
        mock_money_flow: Mock,
        mock_macro: Mock,
    ) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=True)
        client = mock_fmp_client.return_value
        client.get_quote.return_value = {
            "price": 525.0,
            "change": 8.0,
            "changesPercentage": 1.6,
            "volume": 2000000,
            "avgVolume": 1000000,
            "name": "NVIDIA",
        }
        client.get_profile.return_value = {"companyName": "NVIDIA", "sector": "Technology", "industry": "Semiconductors"}
        client.get_stock_news.return_value = [{"title": "NVIDIA reports demand strength"}]
        client.get_historical_eod.return_value = [{"close": 525.0, "date": "2026-04-30"}, {"close": 500.0, "date": "2026-04-01"}]
        mock_radar.return_value = {"items": []}
        mock_money_flow.return_value = {
            "answer": "Flujo detectado, sin ballena identificada por insufficient_confirmation.",
            "items": [{"ticker": "NVDA", "flow_detected": True, "whale_identified": False}],
        }
        mock_macro.return_value = {
            "macro": {"available": True, "summary": "Macro estable.", "bias_label": "neutral", "confidence": 60},
        }

        packet = get_asset_decision_packet("nvda")

        self.assertEqual(packet["ticker"], "NVDA")
        self.assertEqual(packet["company_name"], "NVIDIA")
        self.assertEqual(packet["price"], 525.0)
        self.assertIn(packet["decision_label"], {"Comprar con cautela", "Esperar confirmacion", "Vigilar"})
        self.assertIn("ballena identificada", packet["whale_read"].lower())
        self.assertIn("confirmacion insuficiente", packet["money_flow_read"])
        self.assertNotIn("insufficient_confirmation", packet["money_flow_read"])
        self.assertTrue(packet["source_status"]["fmp_live_ready"])
        self.assertTrue(packet["source_status"]["quote_available"])
        self.assertFalse(packet["source_status"]["whale_identified"])

    @patch("services.dashboard.get_asset_decision_packet.FmpClient")
    @patch("services.dashboard.get_asset_decision_packet.load_settings")
    def test_does_not_call_fmp_when_live_is_disabled(self, mock_settings: Mock, mock_fmp_client: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=False)

        with patch("services.dashboard.get_asset_decision_packet.get_radar_snapshot", return_value={"items": []}):
            with patch("services.dashboard.get_asset_decision_packet.get_money_flow_jarvis_answer", return_value={"answer": "", "items": []}):
                with patch("services.dashboard.get_asset_decision_packet.get_macro_activity_snapshot", return_value={"macro": {"available": False}}):
                    packet = get_asset_decision_packet("BNO")

        self.assertEqual(packet["decision_label"], "No concluyente")
        self.assertFalse(packet["source_status"]["fmp_live_ready"])
        mock_fmp_client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
