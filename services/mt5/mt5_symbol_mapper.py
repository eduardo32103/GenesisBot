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

BTC_PROXY_WARNING = "BTC en este broker puede ser ETF/proxy, validar instrumento antes de operar."


class MT5SymbolMapper:
    def __init__(self, *, symbol_map: dict[str, str] | None = None, allowed_symbols: list[str] | None = None) -> None:
        self.custom_symbol_map = {**_env_symbol_map(), **_clean_symbol_map(symbol_map or {})}
        self.default_symbol_map = dict(DEFAULT_SYMBOL_MAP)
        self.symbol_map = {**self.default_symbol_map, **self.custom_symbol_map}
        self.allowed_symbols = {item.upper().strip() for item in (allowed_symbols if allowed_symbols is not None else _env_allowed_symbols()) if item}

    def map_symbol(self, symbol: str) -> dict[str, Any]:
        raw_symbol = _clean_symbol(symbol)
        genesis_symbol = normalize_ticker(raw_symbol)
        candidates = _unique(
            [
                raw_symbol,
                genesis_symbol,
                raw_symbol.replace("-", ""),
                genesis_symbol.replace("-", ""),
            ]
        )
        custom_symbol = _first_map_value(self.custom_symbol_map, candidates)
        allowed_direct_symbol = _first_allowed_symbol(self.allowed_symbols, candidates)
        default_symbol = _first_map_value(self.default_symbol_map, candidates)
        mt5_symbol = custom_symbol or allowed_direct_symbol or default_symbol or genesis_symbol.replace("-", "")
        mapped = bool(custom_symbol or allowed_direct_symbol or default_symbol)
        allowed = mt5_symbol in self.allowed_symbols if self.allowed_symbols else True
        ok = bool(mt5_symbol) and mapped and allowed
        reason = "ok" if ok else "symbol_not_allowed" if mapped and not allowed else "symbol_not_mapped"
        warnings = [BTC_PROXY_WARNING] if genesis_symbol == "BTC-USD" and mt5_symbol == "BTC" else []
        return {
            "ok": ok,
            "raw_symbol": raw_symbol,
            "genesis_symbol": genesis_symbol,
            "mt5_symbol": mt5_symbol,
            "mapped": mapped,
            "allowed": allowed,
            "reason": reason,
            "warnings": warnings,
            "instrument_warning": warnings[0] if warnings else "",
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
    return _clean_symbol_map(data)


def _env_allowed_symbols() -> list[str]:
    raw = os.getenv("MT5_ALLOWED_SYMBOLS", "BTCUSD,NVDA,SPY,QQQ,XAUUSD").strip()
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def _clean_symbol_map(data: dict[str, str]) -> dict[str, str]:
    return {_clean_symbol(key): _clean_symbol(value) for key, value in data.items() if key and value}


def _clean_symbol(value: object) -> str:
    return str(value or "").upper().strip().replace("/", "-").rstrip(".,;:!?")


def _first_map_value(mapping: dict[str, str], candidates: list[str]) -> str:
    for candidate in candidates:
        value = mapping.get(candidate)
        if value:
            return value
    return ""


def _first_allowed_symbol(allowed_symbols: set[str], candidates: list[str]) -> str:
    if not allowed_symbols:
        return ""
    for candidate in candidates:
        if candidate in allowed_symbols:
            return candidate
    return ""


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    clean: list[str] = []
    for value in values:
        if value and value not in seen:
            clean.append(value)
            seen.add(value)
    return clean
