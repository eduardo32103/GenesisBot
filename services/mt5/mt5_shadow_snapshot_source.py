from __future__ import annotations

import hashlib
import json
from typing import Any

from app.settings import load_settings
from services.genesis.memory_store import MemoryStore, safe_postgres_db_fingerprint
from services.mt5.mt5_shadow_trading import MT5ShadowTrading


SNAPSHOT_SOURCE_NAME = "mt5_shadow_trading_latest_state"


def load_governor_shadow_snapshot(*, limit: int) -> dict[str, Any]:
    """Load latest-state shadow trades for governors with live DB source alignment."""
    clean_limit = max(1, int(limit or 100))
    database_url = str(getattr(load_settings(), "database_url", "") or "")
    live_db_required = bool(database_url)
    try:
        memory = MemoryStore(require_postgres=live_db_required, ensure_schema=False)
        snapshot = MT5ShadowTrading(memory=memory).snapshot(limit=clean_limit)
        source = _source_metadata(
            memory=memory,
            limit=clean_limit,
            live_db_required=live_db_required,
            database_url=database_url,
            snapshot=snapshot,
            status="ready",
            source_unavailable=False,
        )
        return {
            **snapshot,
            "source_unavailable": False,
            "shadow_snapshot_source": source,
            "snapshot_source": source,
        }
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        source = _source_metadata(
            memory=None,
            limit=clean_limit,
            live_db_required=live_db_required,
            database_url=database_url,
            snapshot={},
            status="shadow_snapshot_source_unavailable",
            source_unavailable=True,
            error_type=type(exc).__name__,
        )
        return {
            "ok": False,
            "status": "shadow_snapshot_source_unavailable",
            "source_unavailable": True,
            "error": type(exc).__name__,
            "closed_trades": [],
            "open_trades": [],
            "items": [],
            "shadow_snapshot_source": source,
            "snapshot_source": source,
            **_safety(),
        }


def _source_metadata(
    *,
    memory: Any | None,
    limit: int,
    live_db_required: bool,
    database_url: str,
    snapshot: dict[str, Any],
    status: str,
    source_unavailable: bool,
    error_type: str = "",
) -> dict[str, Any]:
    backend = str(getattr(memory, "backend", "") or "unavailable")
    resolver_used = str(getattr(memory, "resolver_used", "") or getattr(memory, "postgres_resolver_used", "") or "")
    db_fingerprint = str(getattr(memory, "db_fingerprint", "") or safe_postgres_db_fingerprint(database_url) or "")
    live_db_detected = bool(backend == "postgres" and db_fingerprint)
    source = {
        "source_name": SNAPSHOT_SOURCE_NAME,
        "snapshot_source": SNAPSHOT_SOURCE_NAME,
        "backend_type": backend,
        "live_db_required": bool(live_db_required),
        "live_db_detected": live_db_detected,
        "source_available": not source_unavailable,
        "source_unavailable": bool(source_unavailable),
        "status": status,
        "limit_used": limit,
        "open_shadow_trades_count": len(snapshot.get("open_trades") or []),
        "closed_shadow_trades_count": len(snapshot.get("closed_trades") or []),
        "snapshot_items_count": len(snapshot.get("items") or []),
        "resolver_used": resolver_used,
        "db_fingerprint": db_fingerprint,
        "connection_error_type": error_type,
        "source_fingerprint": "",
        **_safety(),
    }
    source["source_fingerprint"] = _fingerprint(source)
    return source


def _fingerprint(source: dict[str, Any]) -> str:
    payload = {
        "source_name": source.get("source_name") or "",
        "backend_type": source.get("backend_type") or "",
        "live_db_required": bool(source.get("live_db_required")),
        "live_db_detected": bool(source.get("live_db_detected")),
        "db_fingerprint": source.get("db_fingerprint") or "",
        "resolver_used": source.get("resolver_used") or "",
        "limit_used": int(source.get("limit_used") or 0),
        "open_shadow_trades_count": int(source.get("open_shadow_trades_count") or 0),
        "closed_shadow_trades_count": int(source.get("closed_shadow_trades_count") or 0),
        "snapshot_items_count": int(source.get("snapshot_items_count") or 0),
        "source_unavailable": bool(source.get("source_unavailable")),
        "connection_error_type": source.get("connection_error_type") or "",
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:24]


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
