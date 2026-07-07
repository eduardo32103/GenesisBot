from __future__ import annotations

import unittest
from types import SimpleNamespace

from services.mt5.mt5_crypto_symbol_discovery import build_crypto_symbol_discovery, build_crypto_symbol_readiness


class MT5CryptoSymbolDiscoveryTests(unittest.TestCase):
    def test_symbol_alias_discovery_accepts_broker_suffix(self) -> None:
        result = build_crypto_symbol_readiness("BTCUSD", [_symbol("BTCUSDm", currency_base="BTC", currency_profit="USD")])

        self.assertEqual(result["readiness_state"], "symbol_available_needs_runtime_context")
        self.assertEqual(result["requested_asset"], "BTCUSD")
        self.assertEqual(result["resolved_symbol"], "BTCUSDm")
        self.assertTrue(result["symbol_alias_used"])
        self.assertFalse(result["fallback_to_xau"])

    def test_symbol_alias_discovery_accepts_xbtusd_for_btc(self) -> None:
        result = build_crypto_symbol_readiness("BTCUSD", [_symbol("XBTUSD", currency_base="BTC", currency_profit="USD")])

        self.assertEqual(result["requested_asset"], "BTCUSD")
        self.assertEqual(result["resolved_symbol"], "XBTUSD")
        self.assertTrue(result["symbol_alias_used"])

    def test_symbol_alias_discovery_fails_closed_when_no_crypto_symbols(self) -> None:
        result = build_crypto_symbol_discovery([_symbol("EURUSD"), _symbol("XAUUSD.b", currency_base="XAU", currency_profit="USD")])

        self.assertEqual(result["label"], "NO_CRYPTO_SYMBOLS_AVAILABLE")
        self.assertEqual(result["btc_eth_symbol_count"], 0)
        self.assertEqual(result["resolutions"]["BTCUSD"]["readiness_state"], "blocked_symbol_not_available")
        self.assertFalse(result["resolutions"]["BTCUSD"]["entry_allowed_for_paper_test"])

    def test_crypto_readiness_reports_resolved_symbol(self) -> None:
        result = build_crypto_symbol_readiness("ETHUSD", [_symbol("ETHUSD.raw", currency_base="ETH", currency_profit="USD")])

        self.assertEqual(result["requested_asset"], "ETHUSD")
        self.assertEqual(result["resolved_symbol"], "ETHUSD.raw")
        self.assertEqual(result["broker_symbol"], "ETHUSD.raw")
        self.assertTrue(result["symbol_alias_used"])
        self.assertFalse(result["entry_allowed_for_paper_test"])

    def test_crypto_readiness_never_falls_back_to_xau(self) -> None:
        result = build_crypto_symbol_readiness("BTCUSD", [_symbol("XAUUSD.b", currency_base="XAU", currency_profit="USD")])

        self.assertEqual(result["readiness_state"], "blocked_symbol_not_available")
        self.assertEqual(result["resolved_symbol"], "")
        self.assertEqual(result["fallback_symbol"], "")
        self.assertFalse(result["fallback_to_xau"])
        self.assertFalse(result["entry_allowed_for_paper_test"])


def _symbol(
    name: str,
    *,
    path: str = "Crypto",
    visible: bool = True,
    trade_mode: int = 4,
    digits: int = 2,
    spread: int = 25,
    currency_base: str = "",
    currency_profit: str = "USD",
) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        path=path,
        visible=visible,
        trade_mode=trade_mode,
        digits=digits,
        spread=spread,
        currency_base=currency_base,
        currency_profit=currency_profit,
    )


if __name__ == "__main__":
    unittest.main()
