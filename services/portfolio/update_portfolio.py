from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.portfolio.get_portfolio_snapshot import normalize_portfolio_positions

_ROOT_DIR = Path(__file__).resolve().parents[2]
_PORTFOLIO_PATH = _ROOT_DIR / "portfolio.json"
_TICKER_PATTERN = re.compile(r"^[A-Z0-9.\-=]{1,15}$")


def _normalize_ticker(value: object) -> str:
    return str(value or "").strip().upper()


def _coerce_positive_float(value: object) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    return numeric if numeric > 0 else 0.0


def _read_raw_portfolio(path: Path = _PORTFOLIO_PATH) -> dict[str, Any]:
    if not path.exists():
        return {"owner_id": "dashboard_web", "positions": []}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"owner_id": "dashboard_web", "positions": []}
    return raw if isinstance(raw, dict) else {"owner_id": "dashboard_web", "positions": raw if isinstance(raw, list) else []}


def _portfolio_positions_for_write(raw: dict[str, Any]) -> list[dict[str, Any]]:
    positions = []
    for position in normalize_portfolio_positions(raw):
        shaped = {
            "ticker": position["ticker"],
            "display_name": position.get("display_name") or position["ticker"],
        }
        units = _coerce_positive_float(position.get("units"))
        entry_price = _coerce_positive_float(position.get("entry_price"))
        reference_price = _coerce_positive_float(position.get("reference_price"))
        amount_usd = _coerce_positive_float(position.get("amount_usd"))
        if units > 0:
            shaped["units"] = units
        if entry_price > 0:
            shaped["entry_price"] = entry_price
        if amount_usd > 0 and not (units > 0 and entry_price > 0):
            shaped["amount_usd"] = amount_usd
        if reference_price > 0 and entry_price <= 0:
            shaped["reference_price"] = reference_price
        opened_at = str(position.get("opened_at") or "").strip()
        if opened_at:
            shaped["opened_at"] = opened_at
        raw_mode = _find_raw_mode(raw, position["ticker"])
        if raw_mode:
            shaped["mode"] = raw_mode
        positions.append(shaped)
    return positions


def _find_raw_mode(raw: dict[str, Any], ticker: str) -> str:
    normalized = _normalize_ticker(ticker)
    blocks = [raw.get("positions"), raw.get("portfolio"), raw]
    for block in blocks:
        if isinstance(block, list):
            for item in block:
                if isinstance(item, dict) and _normalize_ticker(item.get("ticker") or item.get("symbol")) == normalized:
                    return str(item.get("mode") or "").strip()
        if isinstance(block, dict):
            item = block.get(normalized) or block.get(normalized.lower())
            if isinstance(item, dict):
                return str(item.get("mode") or "").strip()
    return ""


def _write_positions(positions: list[dict[str, Any]], raw: dict[str, Any], path: Path = _PORTFOLIO_PATH) -> None:
    payload = {
        "owner_id": raw.get("owner_id", "dashboard_web"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "positions": sorted(positions, key=lambda item: item["ticker"]),
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def add_ticker_to_portfolio(ticker: str, *, path: Path = _PORTFOLIO_PATH) -> dict[str, Any]:
    normalized = _normalize_ticker(ticker)
    if not normalized or not _TICKER_PATTERN.match(normalized):
        return {"ok": False, "status": "invalid", "message": "Ticker no valido."}

    raw = _read_raw_portfolio(path)
    positions = _portfolio_positions_for_write(raw)
    if any(_normalize_ticker(position.get("ticker")) == normalized for position in positions):
        return {"ok": True, "status": "exists", "ticker": normalized, "message": "Este activo ya esta en tu cartera/watchlist."}

    positions.append({"ticker": normalized, "display_name": normalized})
    _write_positions(positions, raw, path)
    return {"ok": True, "status": "added", "ticker": normalized, "message": "Activo agregado."}


def simulate_paper_position(
    ticker: str,
    *,
    units: object,
    entry_price: object,
    path: Path = _PORTFOLIO_PATH,
) -> dict[str, Any]:
    normalized = _normalize_ticker(ticker)
    if not normalized or not _TICKER_PATTERN.match(normalized):
        return {"ok": False, "status": "invalid", "message": "Ticker no valido."}

    normalized_units = _coerce_positive_float(units)
    normalized_entry = _coerce_positive_float(entry_price)
    if normalized_units <= 0 or normalized_entry <= 0:
        return {"ok": False, "status": "invalid", "message": "Necesito unidades y precio de entrada mayores a cero."}

    raw = _read_raw_portfolio(path)
    positions = _portfolio_positions_for_write(raw)
    timestamp = datetime.now(timezone.utc).isoformat()
    updated = False
    for position in positions:
        if _normalize_ticker(position.get("ticker")) != normalized:
            continue
        position["units"] = normalized_units
        position["entry_price"] = normalized_entry
        position["mode"] = "paper"
        position["opened_at"] = timestamp
        updated = True
        break

    if not updated:
        positions.append(
            {
                "ticker": normalized,
                "display_name": normalized,
                "units": normalized_units,
                "entry_price": normalized_entry,
                "mode": "paper",
                "opened_at": timestamp,
            }
        )

    _write_positions(positions, raw, path)
    return {
        "ok": True,
        "status": "paper_position_saved",
        "ticker": normalized,
        "units": normalized_units,
        "entry_price": normalized_entry,
        "mode": "paper",
        "message": f"Compra simulada de {normalized} guardada.",
    }


def remove_watchlist_ticker(ticker: str, *, path: Path = _PORTFOLIO_PATH) -> dict[str, Any]:
    normalized = _normalize_ticker(ticker)
    if not normalized or not _TICKER_PATTERN.match(normalized):
        return {"ok": False, "status": "invalid", "message": "Ticker no valido."}

    raw = _read_raw_portfolio(path)
    positions = _portfolio_positions_for_write(raw)
    kept = []
    removed = False
    for position in positions:
        if _normalize_ticker(position.get("ticker")) != normalized:
            kept.append(position)
            continue
        if _coerce_positive_float(position.get("units")) > 0:
            kept.append(position)
            continue
        removed = True

    if not removed:
        return {"ok": False, "status": "not_found", "ticker": normalized, "message": "No encontre este activo en seguimiento."}

    _write_positions(kept, raw, path)
    return {"ok": True, "status": "removed", "ticker": normalized, "message": f"{normalized} quitado de seguimiento."}


def remove_paper_position(ticker: str, *, path: Path = _PORTFOLIO_PATH) -> dict[str, Any]:
    normalized = _normalize_ticker(ticker)
    if not normalized or not _TICKER_PATTERN.match(normalized):
        return {"ok": False, "status": "invalid", "message": "Ticker no valido."}

    raw = _read_raw_portfolio(path)
    positions = _portfolio_positions_for_write(raw)
    kept = []
    removed = False
    for position in positions:
        if _normalize_ticker(position.get("ticker")) != normalized:
            kept.append(position)
            continue
        if str(position.get("mode") or "").strip().lower() == "paper" or _coerce_positive_float(position.get("units")) > 0:
            removed = True
            continue
        kept.append(position)

    if not removed:
        return {"ok": False, "status": "not_found", "ticker": normalized, "message": "No encontre compra simulada para cerrar."}

    _write_positions(kept, raw, path)
    return {"ok": True, "status": "paper_removed", "ticker": normalized, "message": f"Compra simulada de {normalized} cerrada."}
