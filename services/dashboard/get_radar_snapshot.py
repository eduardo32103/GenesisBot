from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.settings import load_settings
from services.dashboard.get_operational_health import _connect_database, _safe_iso

_ROOT_DIR = Path(__file__).resolve().parents[2]
_PORTFOLIO_FALLBACK_PATH = _ROOT_DIR / "portfolio.json"

_SOURCE_LABELS = {
    "live": "live",
    "cache": "cache",
    "contingency": "contingencia",
    "unavailable": "unavailable",
}


def _max_timestamp(values: list[str]) -> str:
    cleaned = [str(value or "").strip() for value in values if str(value or "").strip()]
    if not cleaned:
        return ""
    try:
        return max(cleaned)
    except Exception:
        return cleaned[-1]


def _infer_item_source(reference_price: float, explicit_source: str = "") -> str:
    normalized = str(explicit_source or "").strip().lower()
    if normalized in _SOURCE_LABELS:
        return normalized
    return "contingency" if reference_price > 0 else "unavailable"


def _source_note(source: str) -> str:
    if source == "live":
        return "Cotización verificada en vivo."
    if source == "cache":
        return "Última referencia disponible en caché."
    if source == "contingency":
        return "Última referencia persistida; no reemplaza una cotización en vivo."
    return "Sin referencia suficiente para mostrar precio."


def _signal_text(is_investment: bool, source: str) -> str:
    if is_investment and source != "unavailable":
        return "Posición abierta"
    if is_investment:
        return "Posición abierta sin referencia"
    if source != "unavailable":
        return "En radar con referencia"
    return "Solo vigilancia"


def _shape_item(
    ticker: str,
    *,
    is_investment: bool = False,
    amount_usd: float = 0.0,
    reference_price: float = 0.0,
    source: str = "",
    updated_at: str = "",
    origin: str = "",
) -> dict[str, Any]:
    normalized_source = _infer_item_source(reference_price, explicit_source=source)
    return {
        "ticker": str(ticker or "").strip().upper(),
        "is_investment": bool(is_investment),
        "amount_usd": float(amount_usd or 0.0),
        "reference_price": float(reference_price or 0.0),
        "source": normalized_source,
        "source_label": _SOURCE_LABELS.get(normalized_source, "unavailable"),
        "source_note": _source_note(normalized_source),
        "signal": _signal_text(bool(is_investment), normalized_source),
        "updated_at": _safe_iso(updated_at),
        "origin": origin or "unknown",
    }


def _fetch_wallet_rows(database_url: str, chat_id: str) -> list[dict[str, Any]]:
    if not database_url or not str(chat_id or "").strip().isdigit():
        return []

    conn = None
    try:
        conn = _connect_database(database_url)
        if not conn:
            return []

        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT ticker, is_investment, amount_usd, entry_price, timestamp
            FROM wallet
            WHERE user_id=%s
            ORDER BY ticker
            """,
            (int(chat_id),),
        )
        rows = cursor.fetchall() or []
        conn.commit()
        shaped = []
        for row in rows:
            shaped.append(
                _shape_item(
                    row[0],
                    is_investment=bool(row[1]),
                    amount_usd=float(row[2] or 0.0),
                    reference_price=float(row[3] or 0.0),
                    updated_at=row[4] or "",
                    origin="database",
                )
            )
        return shaped
    except Exception:
        return []
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _parse_portfolio_fallback() -> list[dict[str, Any]]:
    if not _PORTFOLIO_FALLBACK_PATH.exists():
        return []

    try:
        raw = json.loads(_PORTFOLIO_FALLBACK_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []

    timestamp = datetime.fromtimestamp(_PORTFOLIO_FALLBACK_PATH.stat().st_mtime, tz=timezone.utc).isoformat()
    items: list[dict[str, Any]] = []

    if isinstance(raw, dict) and isinstance(raw.get("portfolio"), dict):
        raw = raw["portfolio"]

    if isinstance(raw, dict):
        for ticker, value in raw.items():
            if isinstance(value, (int, float)):
                items.append(
                    _shape_item(
                        ticker,
                        reference_price=float(value),
                        updated_at=timestamp,
                        origin="portfolio_fallback",
                    )
                )
                continue

            if isinstance(value, dict):
                items.append(
                    _shape_item(
                        ticker,
                        is_investment=bool(value.get("is_investment", False)),
                        amount_usd=float(value.get("amount_usd", 0.0) or 0.0),
                        reference_price=float(value.get("entry_price", value.get("reference_price", 0.0)) or 0.0),
                        source=str(value.get("source", "")),
                        updated_at=value.get("timestamp") or timestamp,
                        origin="portfolio_fallback",
                    )
                )
        return sorted(items, key=lambda item: item["ticker"])

    if isinstance(raw, list):
        for ticker in raw:
            items.append(_shape_item(str(ticker), updated_at=timestamp, origin="portfolio_fallback"))
    return sorted(items, key=lambda item: item["ticker"])


def _build_snapshot_summary(items: list[dict[str, Any]], data_origin: str) -> dict[str, Any]:
    investment_count = sum(1 for item in items if item.get("is_investment"))
    with_reference = sum(1 for item in items if item.get("source") != "unavailable")
    unavailable_count = sum(1 for item in items if item.get("source") == "unavailable")
    last_update = _max_timestamp([str(item.get("updated_at") or "") for item in items])

    if not items:
        note = "No hay activos vigilados todavía."
    elif data_origin == "database":
        note = "Snapshot real construido desde la tabla wallet. Las referencias mostradas no implican precio live."
    elif data_origin == "portfolio_fallback":
        note = "Snapshot construido desde portfolio.json como contingencia local. No reemplaza un feed en vivo."
    else:
        note = "Snapshot sin fuente persistida disponible."

    return {
        "tracked_count": len(items),
        "investment_count": investment_count,
        "reference_count": with_reference,
        "unavailable_count": unavailable_count,
        "last_update": last_update,
        "data_origin": data_origin,
        "note": note,
    }


def get_radar_snapshot() -> dict[str, Any]:
    settings = load_settings()
    items = _fetch_wallet_rows(settings.database_url, settings.chat_id)
    data_origin = "database" if items else "none"

    if not items:
        items = _parse_portfolio_fallback()
        if items:
            data_origin = "portfolio_fallback"

    items = sorted(
        items,
        key=lambda item: (
            0 if item.get("is_investment") else 1,
            item.get("source") == "unavailable",
            item.get("ticker") or "",
        ),
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": _build_snapshot_summary(items, data_origin),
        "items": items,
    }
