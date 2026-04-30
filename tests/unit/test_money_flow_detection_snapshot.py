from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from services.dashboard.get_money_flow_detection_snapshot import get_money_flow_detection_snapshot


class MoneyFlowDetectionSnapshotTests(unittest.TestCase):
    @patch("services.dashboard.get_money_flow_detection_snapshot.get_macro_activity_snapshot")
    @patch("services.dashboard.get_money_flow_detection_snapshot.get_fmp_dependencies_snapshot")
    @patch("services.dashboard.get_money_flow_detection_snapshot._load_persisted_market_metrics")
    @patch("services.dashboard.get_money_flow_detection_snapshot.get_radar_snapshot")
    def test_detects_conservative_money_flow_signals_from_persisted_metrics(
        self,
        mock_radar: Mock,
        mock_market_metrics: Mock,
        mock_fmp: Mock,
        mock_macro: Mock,
    ) -> None:
        mock_market_metrics.return_value = {}
        mock_radar.return_value = {
            "summary": {"data_origin": "database"},
            "items": [
                {
                    "ticker": "NVDA",
                    "reference_price": 101.0,
                    "source": "live",
                    "origin": "database",
                    "updated_at": "2026-04-25T15:00:00+00:00",
                    "money_flow": {
                        "price_change_pct": 3.2,
                        "relative_volume": 2.1,
                        "volume_baseline": 1000000,
                        "breakout_reference": 100.0,
                        "sector_move_pct": 1.8,
                        "risk_proxy_move_pct": 1.3,
                        "source_group_move_pct": -0.8,
                        "target_group_move_pct": 1.6,
                        "timestamp": "2026-04-25T15:00:00+00:00",
                    },
                },
                {
                    "ticker": "BNO",
                    "reference_price": 60.0,
                    "source": "live",
                    "origin": "database",
                    "money_flow": {
                        "price_change_pct": -3.1,
                        "relative_volume": 2.0,
                        "volume_baseline": 800000,
                        "timestamp": "2026-04-25T15:00:00+00:00",
                    },
                },
                {
                    "ticker": "IAU",
                    "reference_price": 35.0,
                    "source": "live",
                    "origin": "database",
                    "money_flow": {
                        "price_change_pct": 0.2,
                        "relative_volume": 2.2,
                        "volume_baseline": 700000,
                        "timestamp": "2026-04-25T15:00:00+00:00",
                    },
                },
            ],
        }
        mock_fmp.return_value = {"meta": {"source": "runtime_snapshot"}}
        mock_macro.return_value = {
            "macro": {"available": True},
            "meta": {"macro_source": "runtime_snapshot"},
        }

        payload = get_money_flow_detection_snapshot()
        by_ticker = {item["ticker"]: item for item in payload["items"]}
        nvda_signals = {signal["type"]: signal for signal in by_ticker["NVDA"]["signals"]}
        bno_signals = {signal["type"]: signal for signal in by_ticker["BNO"]["signals"]}
        iau_signals = {signal["type"]: signal for signal in by_ticker["IAU"]["signals"]}

        self.assertEqual(payload["phase"], "5.2")
        self.assertFalse(payload["summary"]["causality_enabled"])
        self.assertFalse(payload["summary"]["institutional_claims_enabled"])
        self.assertFalse(payload["summary"]["fmp_live_queries_enabled"])
        self.assertTrue(nvda_signals["strong_inflow"]["detected"])
        self.assertTrue(nvda_signals["volume_breakout"]["detected"])
        self.assertTrue(nvda_signals["sector_pressure"]["detected"])
        self.assertTrue(nvda_signals["risk_on_risk_off"]["detected"])
        self.assertTrue(nvda_signals["rotation"]["detected"])
        self.assertTrue(bno_signals["strong_outflow"]["detected"])
        self.assertTrue(iau_signals["price_volume_divergence"]["detected"])
        self.assertEqual(by_ticker["NVDA"]["language_guardrail"], "No se afirma institucionalidad ni causalidad.")

    @patch("services.dashboard.get_money_flow_detection_snapshot.get_macro_activity_snapshot")
    @patch("services.dashboard.get_money_flow_detection_snapshot.get_fmp_dependencies_snapshot")
    @patch("services.dashboard.get_money_flow_detection_snapshot._load_persisted_market_metrics")
    @patch("services.dashboard.get_money_flow_detection_snapshot.get_radar_snapshot")
    def test_marks_insufficient_confirmation_when_inputs_are_missing(
        self,
        mock_radar: Mock,
        mock_market_metrics: Mock,
        mock_fmp: Mock,
        mock_macro: Mock,
    ) -> None:
        mock_market_metrics.return_value = {}
        mock_radar.return_value = {
            "summary": {"data_origin": "portfolio_fallback"},
            "items": [
                {
                    "ticker": "IXC",
                    "reference_price": 0.0,
                    "source": "unavailable",
                    "origin": "portfolio_fallback",
                    "updated_at": "",
                }
            ],
        }
        mock_fmp.return_value = {"meta": {"source": "unavailable"}}
        mock_macro.return_value = {
            "macro": {"available": False},
            "meta": {"macro_source": "unavailable"},
        }

        payload = get_money_flow_detection_snapshot()
        item = payload["items"][0]
        signals = {signal["type"]: signal for signal in item["signals"]}

        self.assertEqual(item["primary_signal"], "insufficient_confirmation")
        self.assertTrue(signals["insufficient_confirmation"]["detected"])
        self.assertIn("price_change_pct", signals["insufficient_confirmation"]["missing_inputs"])
        self.assertEqual(payload["summary"]["assets_insufficient_confirmation"], 1)

    @patch("services.dashboard.get_money_flow_detection_snapshot.get_macro_activity_snapshot")
    @patch("services.dashboard.get_money_flow_detection_snapshot.get_fmp_dependencies_snapshot")
    @patch("services.dashboard.get_money_flow_detection_snapshot._load_persisted_market_metrics")
    @patch("services.dashboard.get_money_flow_detection_snapshot.get_radar_snapshot")
    def test_uses_persisted_fmp_market_metrics_without_live_queries(
        self,
        mock_radar: Mock,
        mock_market_metrics: Mock,
        mock_fmp: Mock,
        mock_macro: Mock,
    ) -> None:
        mock_radar.return_value = {
            "summary": {"data_origin": "database"},
            "items": [
                {
                    "ticker": "MSFT",
                    "reference_price": 420.0,
                    "source": "cache",
                    "origin": "database",
                    "updated_at": "2026-04-25T15:00:00+00:00",
                }
            ],
        }
        mock_market_metrics.return_value = {
            "MSFT": {
                "price_change_pct": 3.0,
                "relative_volume": 2.0,
                "volume_baseline": 900000,
                "timestamp": "2026-04-25T15:00:00+00:00",
            }
        }
        mock_fmp.return_value = {"meta": {"source": "runtime_snapshot"}}
        mock_macro.return_value = {
            "macro": {"available": False},
            "meta": {"macro_source": "unavailable"},
        }

        payload = get_money_flow_detection_snapshot()
        item = payload["items"][0]
        signals = {signal["type"]: signal for signal in item["signals"]}

        self.assertTrue(signals["strong_inflow"]["detected"])
        self.assertEqual(payload["source_status"]["persisted_market_metrics_count"], 1)
        self.assertFalse(payload["summary"]["fmp_live_queries_enabled"])


if __name__ == "__main__":
    unittest.main()
