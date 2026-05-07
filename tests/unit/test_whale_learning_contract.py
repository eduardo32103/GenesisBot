from __future__ import annotations

import unittest

from services.genesis.whale_learning import _shape_estimate_event, _shape_event, _summary


class WhaleLearningContractTests(unittest.TestCase):
    def test_smart_money_estimate_uses_volume_without_confirmed_amount(self) -> None:
        event = _shape_estimate_event(
            "BTC-USD",
            {
                "current_price": 100_000,
                "volume": 2500,
                "avg_volume": 1000,
                "amount_usd": 9_000_000_000_000,
                "direction": "inflow accumulation",
                "timestamp": "2026-05-06T12:00:00+00:00",
                "source": "technical",
            },
        )

        self.assertEqual(event["event_type"], "smart_money_estimate")
        self.assertFalse(event["confirmed"])
        self.assertEqual(event["confirmed_amount_usd"], None)
        self.assertEqual(event["monitored_dollar_volume"], 250_000_000)
        self.assertEqual(event["relative_volume"], 2.5)
        self.assertEqual(event["estimated_flow_direction"], "inflow")
        self.assertTrue(event["amount_suspicious"])
        self.assertEqual(event["entity_name"], "")
        self.assertIn("No hay entidad confirmada", event["genesis_reading_es"])

    def test_confirmed_whale_requires_plausible_amount(self) -> None:
        event = _shape_event(
            "NVDA",
            "Fondo reportado",
            {
                "amount_usd": 50_000_000,
                "amount": 100_000,
                "price": 500,
                "current_price": 500,
                "volume": 2_000_000,
                "timestamp": "2026-05-06T12:00:00+00:00",
                "source": "fmp",
                "direction": "buy",
            },
        )

        self.assertEqual(event["event_type"], "whale_confirmed")
        self.assertTrue(event["confirmed"])
        self.assertEqual(event["entity_name"], "Fondo reportado")
        self.assertEqual(event["confirmed_amount_usd"], 50_000_000)
        self.assertEqual(event["price_used"], 500)
        self.assertIn("genesis_reading_es", event)

    def test_summary_separates_confirmed_value_from_monitored_volume(self) -> None:
        confirmed = _shape_event(
            "NVDA",
            "Fondo reportado",
            {"amount_usd": 50_000_000, "amount": 100_000, "price": 500, "volume": 2_000_000, "direction": "buy"},
        )
        estimated = _shape_estimate_event("BTC-USD", {"current_price": 100_000, "volume": 2500, "direction": "inflow"})

        summary = _summary([confirmed, estimated])

        self.assertEqual(summary["confirmed_value"], 50_000_000)
        self.assertEqual(summary["watched_volume"], 250_000_000)
        self.assertEqual(summary["confirmed_count"], 1)
        self.assertEqual(summary["estimated_count"], 1)


if __name__ == "__main__":
    unittest.main()
