from __future__ import annotations

from typing import Any


BTC_PROXY_WARNING = "MT5 BTC parece ETF/proxy, no BTCUSD spot"


def resolve_instrument(value: dict[str, Any] | str | None) -> dict[str, Any]:
    payload = value if isinstance(value, dict) else {"symbol": value}
    original_symbol = _symbol(payload.get("symbol") or payload.get("ticker") or payload.get("original_symbol"))
    description = str(payload.get("symbol_description") or payload.get("description") or "").strip()
    path = str(payload.get("symbol_path") or payload.get("path") or "").strip()
    currency_base = _symbol(payload.get("currency_base"))
    currency_profit = _symbol(payload.get("currency_profit"))
    compact = original_symbol.replace("-", "").replace("/", "").rstrip(".")
    compact_alnum = "".join(char for char in compact if char.isalnum())
    compact_root = compact_alnum.rstrip("M")
    desc_l = description.casefold()

    if _looks_like_spot_btc(original_symbol, compact, compact_root, description, currency_base, currency_profit):
        return _payload(
            original_symbol=original_symbol,
            normalized_symbol="BTCUSD",
            underlying="BTC",
            instrument_type="crypto_spot",
            is_spot_crypto=True,
            description=description,
            path=path,
            currency_base=currency_base,
            currency_profit=currency_profit,
            warning="",
        )

    if _looks_like_spot_eth(original_symbol, compact_root, description, currency_base, currency_profit):
        return _payload(
            original_symbol=original_symbol,
            normalized_symbol="ETHUSD",
            underlying="ETH",
            instrument_type="crypto_spot",
            is_spot_crypto=True,
            description=description,
            path=path,
            currency_base=currency_base,
            currency_profit=currency_profit,
            warning="",
        )

    if _looks_like_xauusd(compact_alnum, compact_root, description, currency_base, currency_profit):
        return _payload(
            original_symbol=original_symbol,
            normalized_symbol="XAUUSD",
            underlying="XAU",
            instrument_type="metal_spot",
            is_spot_crypto=False,
            description=description,
            path=path,
            currency_base=currency_base,
            currency_profit=currency_profit,
            warning="",
        )

    if original_symbol == "BTC" or any(token in desc_l for token in ("grayscale", "trust", "etf", "fund", "mini trust")):
        return _payload(
            original_symbol=original_symbol or "BTC",
            normalized_symbol="BTC_PROXY",
            underlying="BTC",
            instrument_type="crypto_etf_proxy",
            is_spot_crypto=False,
            description=description,
            path=path,
            currency_base=currency_base,
            currency_profit=currency_profit,
            warning=BTC_PROXY_WARNING,
        )

    return _payload(
        original_symbol=original_symbol,
        normalized_symbol=original_symbol,
        underlying=original_symbol,
        instrument_type="unknown",
        is_spot_crypto=False,
        description=description,
        path=path,
        currency_base=currency_base,
        currency_profit=currency_profit,
        warning="",
    )


def normalize_mt5_symbol(value: dict[str, Any] | str | None) -> str:
    return str(resolve_instrument(value).get("normalized_symbol") or "").upper()


def symbol_aliases(value: dict[str, Any] | str | None) -> set[str]:
    info = resolve_instrument(value)
    normalized = str(info.get("normalized_symbol") or "").upper()
    original = str(info.get("original_symbol") or "").upper()
    if normalized == "BTCUSD":
        return {"BTCUSD", "BTCUSD.", "BTCUSDM", "BTCUSDm".upper(), "BTCUSDT", "BTC-USD", "XBTUSD"}
    if normalized == "ETHUSD":
        return {"ETHUSD", "ETHUSD.", "ETHUSDM", "ETHUSDm".upper(), "ETHUSDT", "ETH-USD"}
    if normalized == "XAUUSD":
        return {"XAUUSD", "XAUUSD.", "XAUUSD.B", "XAUUSD#", "XAUUSDM", "GOLD", "GOLD.B"}
    if normalized == "BTC_PROXY":
        return {"BTC", "BTC_PROXY"}
    return {item for item in {normalized, original} if item}


def enrich_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    clean = dict(payload or {})
    info = resolve_instrument(clean)
    clean.setdefault("original_symbol", info["original_symbol"])
    clean["normalized_symbol"] = info["normalized_symbol"]
    clean["instrument_type"] = info["instrument_type"]
    clean["is_spot_crypto"] = info["is_spot_crypto"]
    clean["underlying"] = info["underlying"]
    if info.get("warning"):
        clean["instrument_warning"] = info["warning"]
    return clean


def payload_matches_symbol(payload: dict[str, Any], symbol: str) -> bool:
    if not symbol:
        return True
    query_info = resolve_instrument(symbol)
    query_normalized = str(query_info.get("normalized_symbol") or "").upper()
    payload_info = resolve_instrument(
        {
            **(payload or {}),
            "symbol": payload.get("symbol") or payload.get("original_symbol") or payload.get("normalized_symbol"),
        }
    )
    stored_normalized = str(payload.get("normalized_symbol") or "").upper()
    payload_normalized = str(payload_info.get("normalized_symbol") or "").upper() if stored_normalized == "BTC" else stored_normalized or str(payload_info.get("normalized_symbol") or "").upper()
    return bool(query_normalized and payload_normalized == query_normalized)


def _looks_like_spot_btc(symbol: str, compact: str, compact_root: str, description: str, currency_base: str, currency_profit: str) -> bool:
    compact_alnum = "".join(char for char in compact if char.isalnum())
    if compact_alnum in {"BTCUSD", "BTCUSDT", "XBTUSD"} or compact_root == "BTCUSD" or compact_root.startswith("BTCUSD"):
        return True
    if currency_base == "BTC" and currency_profit in {"USD", "USDT"}:
        return True
    desc_l = description.casefold()
    return "bitcoin vs. usd" in desc_l or "bitcoin vs usd" in desc_l


def _looks_like_spot_eth(symbol: str, compact_root: str, description: str, currency_base: str, currency_profit: str) -> bool:
    if compact_root in {"ETHUSD", "ETHUSDT"} or compact_root.startswith("ETHUSD"):
        return True
    if currency_base == "ETH" and currency_profit in {"USD", "USDT"}:
        return True
    desc_l = description.casefold()
    return "ethereum vs. usd" in desc_l or "ethereum vs usd" in desc_l


def _looks_like_xauusd(compact_alnum: str, compact_root: str, description: str, currency_base: str, currency_profit: str) -> bool:
    if compact_alnum in {"XAUUSD", "GOLD"} or compact_root.startswith("XAUUSD") or compact_root.startswith("GOLD"):
        return True
    if currency_base in {"XAU", "GOLD"} and currency_profit == "USD":
        return True
    desc_l = description.casefold()
    return "gold" in desc_l and ("usd" in desc_l or "dollar" in desc_l)


def _payload(
    *,
    original_symbol: str,
    normalized_symbol: str,
    underlying: str,
    instrument_type: str,
    is_spot_crypto: bool,
    description: str,
    path: str,
    currency_base: str,
    currency_profit: str,
    warning: str,
) -> dict[str, Any]:
    return {
        "ok": True,
        "symbol": original_symbol,
        "original_symbol": original_symbol,
        "normalized_symbol": normalized_symbol,
        "underlying": underlying,
        "instrument_type": instrument_type,
        "is_spot_crypto": is_spot_crypto,
        "description": description,
        "path": path,
        "currency_base": currency_base,
        "currency_profit": currency_profit,
        "warning": warning,
        "symbol_aliases": sorted(symbol_aliases_from_normalized(normalized_symbol)),
    }


def symbol_aliases_from_normalized(normalized_symbol: str) -> set[str]:
    normalized = _symbol(normalized_symbol)
    if normalized == "BTCUSD":
        return {"BTCUSD", "BTCUSD.", "BTCUSDM", "BTCUSDT", "BTC-USD", "XBTUSD"}
    if normalized == "ETHUSD":
        return {"ETHUSD", "ETHUSD.", "ETHUSDM", "ETHUSDT", "ETH-USD"}
    if normalized == "XAUUSD":
        return {"XAUUSD", "XAUUSD.", "XAUUSD.B", "XAUUSD#", "XAUUSDM", "GOLD", "GOLD.B"}
    if normalized == "BTC_PROXY":
        return {"BTC", "BTC_PROXY"}
    return {normalized} if normalized else set()


def _symbol(value: object) -> str:
    return str(value or "").upper().strip().replace("/", "-").rstrip(";,!?")
