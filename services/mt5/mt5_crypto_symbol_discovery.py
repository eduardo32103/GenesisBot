from __future__ import annotations

from typing import Any

from services.mt5.instrument_resolver import resolve_instrument


CRYPTO_SYMBOL_DISCOVERY_VERSION = "2026-07-05.mt5_crypto_symbol_discovery.v1"
CRYPTO_SEARCH_TERMS = ("BTC", "XBT", "BITCOIN", "ETH", "ETHEREUM", "CRYPTO")
REQUESTED_CRYPTO_ASSETS = ("BTCUSD", "ETHUSD")


def discover_mt5_crypto_symbols(mt5_module: Any | None = None) -> dict[str, Any]:
    try:
        mt5 = mt5_module or _import_mt5()
    except ImportError as exc:
        return _unavailable("MetaTrader5_import_error", detail=str(exc))

    initialized = False
    try:
        initialized = bool(mt5.initialize())
        if not initialized:
            return _unavailable("mt5_initialize_failed", detail=_last_error_text(mt5))
        raw_symbols = mt5.symbols_get()
        if raw_symbols is None:
            return _unavailable("mt5_symbols_get_failed", detail=_last_error_text(mt5))
        return build_crypto_symbol_discovery(raw_symbols)
    finally:
        if initialized:
            try:
                mt5.shutdown()
            except Exception:
                pass


def build_crypto_symbol_discovery(symbols: list[Any] | tuple[Any, ...]) -> dict[str, Any]:
    records = filter_crypto_symbols(symbols)
    resolutions = {asset: build_crypto_symbol_readiness(asset, records) for asset in REQUESTED_CRYPTO_ASSETS}
    btc_eth_records = [row for row in records if row.get("normalized_symbol") in REQUESTED_CRYPTO_ASSETS]
    status = "crypto_symbols_discovered" if btc_eth_records else "no_crypto_symbols_available"
    return {
        "ok": True,
        "status": status,
        "discovery_version": CRYPTO_SYMBOL_DISCOVERY_VERSION,
        "search_terms": list(CRYPTO_SEARCH_TERMS),
        "symbols": records,
        "crypto_symbol_count": len(records),
        "btc_eth_symbol_count": len(btc_eth_records),
        "resolutions": resolutions,
        "label": "CRYPTO_SYMBOLS_DISCOVERED" if btc_eth_records else "NO_CRYPTO_SYMBOLS_AVAILABLE",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
    }


def filter_crypto_symbols(symbols: list[Any] | tuple[Any, ...]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol in symbols or []:
        record = symbol_info_to_record(symbol)
        haystack = _symbol_haystack(record)
        if not any(term in haystack for term in CRYPTO_SEARCH_TERMS):
            continue
        if _is_btc_eth_cross_pair(record):
            continue
        rows.append(record)
    return sorted(rows, key=lambda item: (str(item.get("normalized_symbol") or ""), str(item.get("name") or "")))


def build_crypto_symbol_readiness(requested_asset: str, symbols: list[Any] | tuple[Any, ...]) -> dict[str, Any]:
    requested = _requested_asset(requested_asset)
    records = [symbol_info_to_record(item) for item in symbols]
    candidates = [row for row in records if row.get("normalized_symbol") == requested]
    resolved = _choose_symbol(requested, candidates)
    if not resolved:
        return {
            "ok": True,
            "status": "crypto_symbol_not_available",
            "requested_asset": requested,
            "resolved_symbol": "",
            "symbol_alias_used": False,
            "readiness_state": "blocked_symbol_not_available",
            "entry_allowed_for_paper_test": False,
            "recommendation": "configure_mt5_crypto_symbol_alias",
            "candidate_symbols": [row.get("name") for row in candidates],
            "fallback_to_xau": False,
            "fallback_symbol": "",
            **_safety(),
        }
    resolved_name = str(resolved.get("name") or "").strip()
    return {
        "ok": True,
        "status": "crypto_symbol_available",
        "requested_asset": requested,
        "resolved_symbol": resolved_name,
        "broker_symbol": resolved_name,
        "symbol_alias_used": resolved_name.upper() != requested,
        "readiness_state": "symbol_available_needs_runtime_context",
        "entry_allowed_for_paper_test": False,
        "recommendation": "use_resolved_broker_symbol_then_validate_runtime_context",
        "candidate_symbols": [row.get("name") for row in candidates],
        "resolved_symbol_info": resolved,
        "fallback_to_xau": False,
        "fallback_symbol": "",
        **_safety(),
    }


def symbol_info_to_record(symbol: Any) -> dict[str, Any]:
    name = str(_field(symbol, "name") or "").strip()
    path = str(_field(symbol, "path") or "").strip()
    description = str(_field(symbol, "description") or "").strip()
    currency_base = str(_field(symbol, "currency_base") or "").upper().strip()
    currency_profit = str(_field(symbol, "currency_profit") or "").upper().strip()
    info = resolve_instrument(
        {
            "symbol": name,
            "description": description,
            "symbol_path": path,
            "currency_base": currency_base,
            "currency_profit": currency_profit,
        }
    )
    return {
        "name": name,
        "path": path,
        "description": description,
        "visible": bool(_field(symbol, "visible")),
        "trade_mode": _field(symbol, "trade_mode"),
        "digits": _field(symbol, "digits"),
        "spread": _field(symbol, "spread"),
        "currency_base": currency_base,
        "currency_profit": currency_profit,
        "normalized_symbol": info.get("normalized_symbol") or "",
        "instrument_type": info.get("instrument_type") or "",
        "is_spot_crypto": bool(info.get("is_spot_crypto")),
        "warning": info.get("warning") or "",
    }


def _choose_symbol(requested: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    if not candidates:
        return {}
    exact = [row for row in candidates if str(row.get("name") or "").upper() == requested]
    if exact:
        return _prefer_visible(exact)
    return _prefer_visible(candidates)


def _prefer_visible(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    visible = [row for row in candidates if row.get("visible")]
    pool = visible or candidates
    return sorted(pool, key=lambda row: (str(row.get("name") or "").upper(), str(row.get("path") or "").upper()))[0]


def _symbol_haystack(record: dict[str, Any]) -> str:
    values = [
        record.get("name"),
        record.get("path"),
        record.get("description"),
        record.get("currency_base"),
        record.get("currency_profit"),
    ]
    return " ".join(str(value or "").upper() for value in values)


def _is_btc_eth_cross_pair(record: dict[str, Any]) -> bool:
    compact = "".join(char for char in str(record.get("name") or "").upper() if char.isalnum())
    has_btc = "BTC" in compact or "XBT" in compact
    has_eth = "ETH" in compact
    return has_btc and has_eth


def _requested_asset(value: str) -> str:
    normalized = str(resolve_instrument(value).get("normalized_symbol") or "").upper().strip()
    return normalized if normalized in REQUESTED_CRYPTO_ASSETS else str(value or "").upper().strip()


def _field(symbol: Any, name: str) -> Any:
    if isinstance(symbol, dict):
        return symbol.get(name)
    return getattr(symbol, name, None)


def _last_error_text(mt5: Any) -> str:
    try:
        return str(mt5.last_error())
    except Exception:
        return ""


def _import_mt5() -> Any:
    import MetaTrader5 as mt5  # type: ignore

    return mt5


def _unavailable(reason: str, *, detail: str = "") -> dict[str, Any]:
    return {
        "ok": False,
        "status": "mt5_python_discovery_unavailable",
        "label": "MT5_PYTHON_DISCOVERY_UNAVAILABLE",
        "reason": reason,
        "detail": detail,
        "symbols": [],
        "resolutions": {asset: build_crypto_symbol_readiness(asset, []) for asset in REQUESTED_CRYPTO_ASSETS},
        **_safety(),
    }


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
    }
