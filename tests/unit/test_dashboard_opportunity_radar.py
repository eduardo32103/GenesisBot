from __future__ import annotations

import unittest
from types import SimpleNamespace

from services.dashboard import get_opportunity_radar as radar
from services.dashboard.get_opportunity_radar import get_opportunity_radar_snapshot


class FakeFmpClient:
    def get_market_movers(self, kind: str, limit: int = 20):
        if kind == "gainers":
            return [{"symbol": "NVDA"}, {"symbol": "MSFT"}]
        if kind == "actives":
            return [{"symbol": "AAPL"}]
        return [{"symbol": "BNO"}]

    def get_company_screener(self, limit: int = 30, min_market_cap: int = 1_000_000_000, exchange: str = "NASDAQ,NYSE"):
        return [{"symbol": "META"}, {"symbol": "AMD"}]

    def get_batch_quotes(self, symbols, symbol_map=None):
        rows = []
        for symbol in symbols:
            if symbol == "BTC-USD":
                rows.append(
                    {
                        "symbol": "BTCUSD",
                        "name": "Bitcoin",
                        "price": 80600,
                        "change": 250,
                        "changesPercentage": 0.31,
                        "volume": 30_000_000_000,
                    }
                )
                continue
            price = 215.20 if symbol == "NVDA" else 120.0
            rows.append(
                {
                    "symbol": symbol,
                    "name": f"{symbol} Corp",
                    "price": price,
                    "change": 3.7,
                    "changesPercentage": 1.75,
                    "volume": 134_000_000 if symbol == "NVDA" else 12_000_000,
                    "avgVolume": 90_000_000 if symbol == "NVDA" else 10_000_000,
                }
            )
        return rows

    def get_quote(self, ticker: str):
        return {}

    def get_historical_eod(self, ticker: str, limit: int = 90):
        base = 190 if ticker == "NVDA" else 100
        return [
            {
                "date": f"2026-05-{day:02d}",
                "open": base + day,
                "high": base + day + 4,
                "low": base + day - 3,
                "close": base + day + 1,
                "volume": 100_000_000,
            }
            for day in range(1, 31)
        ]

    def get_profile(self, ticker: str):
        return {"companyName": "NVIDIA Corporation" if ticker == "NVDA" else f"{ticker} Corp"}

    def get_stock_news(self, ticker: str, limit: int = 2):
        if ticker == "NVDA":
            return [{"title": "Nvidia AI demand lifts chip sector", "publishedDate": "2026-05-11"}]
        return []

    def get_smart_money_activity(self, ticker: str, limit: int = 2):
        return [{"source": "FMP institutional", "type": "volume"}] if ticker == "NVDA" else []

    def get_analyst_signal(self, ticker: str):
        if ticker == "NVDA":
            return {"targetMean": 260}
        return {}

    def get_earnings_calendar(self, from_date=None, to_date=None, limit: int = 100):
        return [{"symbol": "NVDA", "date": "2026-05-20"}]


class FakeMemoryStore:
    backend = "memory"

    def __init__(self):
        self.saved = []

    def get_tracked_entities(self, limit: int = 40):
        return [{"ticker": "NVDA"}, {"ticker": "BTC-USD"}]

    def save_signal_event(self, ticker, payload=None, source="signals", confidence="media"):
        self.saved.append(("signal", ticker, payload, source, confidence))
        return {"ok": True}

    def save_hypothesis(self, ticker, payload=None, source="genesis", confidence="media"):
        self.saved.append(("hypothesis", ticker, payload, source, confidence))
        return {"ok": True}

    def save_decision_note(self, ticker, verdict, payload=None, source="genesis", confidence="media"):
        self.saved.append(("decision", ticker, verdict, payload, source, confidence))
        return {"ok": True}

    def save_asset_memory(self, ticker, payload=None, source="genesis", confidence="media"):
        self.saved.append(("asset", ticker, payload, source, confidence))
        return {"ok": True}


class DashboardOpportunityRadarTests(unittest.TestCase):
    def setUp(self) -> None:
        radar._CACHE["expires_at"] = 0
        radar._CACHE["payload"] = None

    def test_radar_uses_fmp_signals_and_persists_learning_events(self) -> None:
        store = FakeMemoryStore()
        payload = get_opportunity_radar_snapshot(
            force_refresh=True,
            client=FakeFmpClient(),
            store=store,
            settings=SimpleNamespace(fmp_api_key="configured", fmp_live_enabled=True),
        )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["kind"], "opportunity_radar")
        self.assertGreaterEqual(len(payload["items"]), 1)
        top = payload["items"][0]
        self.assertIn("decision_label_es", top)
        self.assertIn("opportunity_score", top)
        self.assertLessEqual(top.get("dollar_volume") or 0, 1_000_000_000_000)
        self.assertTrue(payload["source_status"]["fmp"]["quote_ok"])
        self.assertTrue(any(row[0] == "signal" for row in store.saved))
        self.assertTrue(any(row[0] == "decision" for row in store.saved))

    def test_missing_fmp_key_returns_safe_empty_contract(self) -> None:
        class EmptyClient(FakeFmpClient):
            def get_market_movers(self, kind: str, limit: int = 20):
                return []

            def get_company_screener(self, limit: int = 30, min_market_cap: int = 1_000_000_000, exchange: str = "NASDAQ,NYSE"):
                return []

            def get_batch_quotes(self, symbols, symbol_map=None):
                return []

            def get_quote(self, ticker: str):
                return {}

        payload = get_opportunity_radar_snapshot(
            force_refresh=True,
            client=EmptyClient(),
            store=FakeMemoryStore(),
            settings=SimpleNamespace(fmp_api_key="", fmp_live_enabled=False),
        )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["items"], [])
        self.assertFalse(payload["source_status"]["fmp"]["key_configured"])
        self.assertFalse(payload["source_status"]["fmp"]["quote_ok"])


if __name__ == "__main__":
    unittest.main()
