from __future__ import annotations

import json
import os
from typing import Any

from services.genesis.ticker_parser import normalize_ticker


DEFAULT_SYMBOL_MAP = {
    "BTC": "BTCUSD",
    "BTC-USD": "BTCUSD",
    "BTCUSD": "BTCUSD",
    "BTCUSDT": "BTCUSD",
    "ETH-USD": "ETHUSD",
    "ETHUSD": "ETHUSD",
    "NVDA": "NVDA",
    "MSFT": "MSFT",
    "AAPL": "AAPL",
    "SPY": "SPY",
    "VOO": "VOO",
    "QQQ": "QQQ",
    "XAUUSD": "XAUUSD",
    "IAU": "XAUUSD",
    "GLD": "XAUUSD",
    "SLV": "XAGUSD",
    "BNO": "USOIL",
    "BRENT": "UKOIL",
    "BZ=F": "UKOIL",
    "USO": "USOIL",
}


class MT5SymbolMapper:
    def __init__(self, *, symbol_map: dict[str, str] | None = None, allowed_symbols: list[str] | None = None) -> None:
        self.symbol_map = {**DEFAULT_SYMBOL_MAP, **_env_symbol_map(), **{str(k).upper(): str(v).upper() for k, v in (symbol_map or {}).items()}}
        self.allowed_symbols = {item.upper().strip() for item in (allowed_symbols if allowed_symbols is not None else _env_allowed_symbols()) if item}

    def map_symbol(self, symbol: str) -> dict[str, Any]:
        genesis_symbol = normalize_ticker(symbol)
        explicit_symbol = self.symbol_map.get(genesis_symbol) or self.symbol_map.get(genesis_symbol.replace("-", ""))
        mt5_symbol = explicit_symbol or genesis_symbol.replace("-", "")
        mapped = bool(explicit_symbol)
        allowed = mt5_symbol in self.allowed_symbols if self.allowed_symbols else True
        ok = bool(mt5_symbol) and mapped and allowed
        reason = "ok" if ok else "symbol_not_allowed" if mapped and not allowed else "symbol_not_mapped"
        return {
            "ok": ok,
            "genesis_symbol": genesis_symbol,
            "mt5_symbol": mt5_symbol,
            "mapped": mapped,
            "allowed": allowed,
            "reason": reason,
            "allowed_symbols": sorted(self.allowed_symbols),
        }


def _env_symbol_map() -> dict[str, str]:
    raw = os.getenv("MT5_SYMBOL_MAP_JSON", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(key).upper().strip(): str(value).upper().strip() for key, value in data.items() if key and value}


def _env_allowed_symbols() -> list[str]:
    raw = os.getenv("MT5_ALLOWED_SYMBOLS", "BTCUSD,NVDA,SPY,QQQ,XAUUSD").strip()
    return [item.strip().upper() for item in raw.split(",") if item.strip()]
