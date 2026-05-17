from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.mt5.mt5_order_model import sanitize_payload


def normalize_account_state(payload: dict[str, Any] | None) -> dict[str, Any]:
    body = sanitize_payload(payload or {})
    trade_mode = str(body.get("trade_mode") or body.get("account_trade_mode") or body.get("mode") or "").lower()
    is_demo = bool(body.get("is_demo")) or trade_mode == "demo" or "demo" in str(body.get("server") or "").lower()
    return {
        "account_id": str(body.get("account_id") or body.get("login") or body.get("account") or "")[:80],
        "server": str(body.get("server") or "")[:160],
        "currency": str(body.get("currency") or "USD")[:20],
        "balance": _to_float(body.get("balance")),
        "equity": _to_float(body.get("equity")),
        "margin": _to_float(body.get("margin")),
        "free_margin": _to_float(body.get("free_margin") or body.get("margin_free")),
        "open_trades": int(_to_float(body.get("open_trades") or body.get("positions")) or 0),
        "daily_loss_pct": _to_float(body.get("daily_loss_pct")) or 0.0,
        "is_demo": is_demo,
        "trade_mode": trade_mode or ("demo" if is_demo else "unknown"),
        "broker_touched": False,
        "secrets_stored": False,
        "raw_sanitized": body,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

