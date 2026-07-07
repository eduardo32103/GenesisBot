from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from app.settings import load_settings
from services.genesis.memory_store import MemoryStore
from services.mt5.mt5_shadow_trading import MT5ShadowTrading, is_main_metric_trade


INSPECTOR_VERSION = "2026-07-07.mt5_legacy_shadow_inspector.v1"
CAPITAL_SNAPSHOT_LIMIT = 500
CAPITAL_EFFECTIVE_FETCH_LIMIT = 100
LEGACY_COLLECTION = "mt5_shadow_trades"
LEGACY_EVENT_TYPE = "mt5_shadow_trade"
LIVE_BACKEND = "postgres"
_CLOSED_STATUSES = {"win", "loss", "breakeven", "closed"}


def inspect_legacy_open_shadows(
    *,
    limit: int = CAPITAL_SNAPSHOT_LIMIT,
    status: str = "open",
    require_live_db: bool = False,
    redact_ids: bool = True,
    include_sensitive_ids: bool = False,
    memory: MemoryStore | Any | None = None,
) -> dict[str, Any]:
    """Read the same legacy MemoryStore source used by Capital Protection."""
    clean_limit = max(1, int(limit or CAPITAL_SNAPSHOT_LIMIT))
    clean_status = str(status or "open").casefold().strip()
    include_raw_ids = bool(include_sensitive_ids) or not bool(redact_ids)
    if require_live_db and include_raw_ids:
        return _source_unavailable(
            limit=clean_limit,
            status=clean_status,
            require_live_db=require_live_db,
            reason="include_sensitive_ids_not_allowed_with_require_live_db",
            recommendation="rerun_without_include_sensitive_ids_for_live_db",
        )
    if require_live_db and memory is None and not load_settings().database_url:
        return _source_unavailable(
            limit=clean_limit,
            status=clean_status,
            require_live_db=require_live_db,
            reason="source_unavailable_require_live_db",
        )

    try:
        active_memory = memory or MemoryStore(require_postgres=require_live_db, ensure_schema=False)
    except Exception:
        return _source_unavailable(
            limit=clean_limit,
            status=clean_status,
            require_live_db=require_live_db,
            reason="source_unavailable_require_live_db" if require_live_db else "inspector_source_unavailable_read_only",
        )

    backend = str(getattr(active_memory, "backend", "") or "unknown")
    database_url = str(getattr(active_memory, "database_url", "") or "")
    live_db_detected = bool(database_url and backend == LIVE_BACKEND)
    if require_live_db and not live_db_detected:
        return _source_unavailable(
            limit=clean_limit,
            status=clean_status,
            require_live_db=require_live_db,
            reason="source_unavailable_require_live_db",
            backend_type=backend,
            live_db_detected=live_db_detected,
        )

    try:
        shadow = MT5ShadowTrading(memory=active_memory)
        snapshot = shadow.snapshot(limit=clean_limit)
        raw_rows = _raw_legacy_rows(active_memory, limit=clean_limit)
    except Exception:
        return _source_unavailable(
            limit=clean_limit,
            status=clean_status,
            require_live_db=require_live_db,
            reason="inspector_source_unavailable_read_only",
            backend_type=backend,
            live_db_detected=live_db_detected,
        )
    close_index = _close_record_index(raw_rows)
    open_rows = _status_rows(snapshot.get("open_trades") or [], clean_status)
    rows = _status_rows(snapshot.get("items") or [], clean_status) if clean_status != "open" else open_rows
    records = [_record_summary(row, close_index=close_index, redact_ids=not include_raw_ids) for row in rows]
    count = len(records)
    source_matches = _source_matches_capital(
        backend=backend,
        status=clean_status,
        limit=clean_limit,
        require_live_db=require_live_db,
        live_db_detected=live_db_detected,
    )
    fingerprint = _source_fingerprint(
        backend=backend,
        live_db_detected=live_db_detected,
        status=clean_status,
        limit=clean_limit,
        count=count,
    )
    return {
        "ok": True,
        "status": "legacy_open_shadow_inspector_ready",
        "inspector_version": INSPECTOR_VERSION,
        "mode": "read_only_inspection",
        "source_name": "legacy_memory_store_mt5_shadow_trades",
        "backend_type": backend,
        "live_db_required": bool(require_live_db),
        "live_db_detected": live_db_detected,
        "source_matches_capital_protection": source_matches,
        "source_fingerprint": fingerprint,
        "query_description": _query_description(clean_limit),
        "limit_used": clean_limit,
        "capital_snapshot_limit": CAPITAL_SNAPSHOT_LIMIT,
        "effective_fetch_limit": min(clean_limit, CAPITAL_EFFECTIVE_FETCH_LIMIT),
        "status_filter": clean_status,
        "scope": {
            "symbol": "global",
            "timeframe": "global",
            "profile": "global",
            "session_id": "none",
            "broker_account": "none",
        },
        "open_shadow_trades_count": count,
        "records_sample": records[:25],
        "records_full_included": False,
        "symbols_included": _counts(records, "symbol"),
        "timeframes_included": _counts(records, "timeframe"),
        "profiles_included": _counts(records, "profile"),
        "oldest_open_at": _oldest(records),
        "newest_open_at": _newest(records),
        "matching_close_record_scope": "same_legacy_query_window_only",
        "has_any_matching_close_record": any(bool(row.get("has_matching_close_record")) for row in records),
        "recommendation": _recommendation(source_matches, require_live_db, live_db_detected, count),
        "read_only": True,
        "mutations_executed": False,
        "shadow_opened": False,
        "shadow_closed": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        **_safety(),
    }


def _raw_legacy_rows(memory: Any, *, limit: int) -> list[dict[str, Any]]:
    try:
        rows = memory.get_mt5_events(LEGACY_COLLECTION, None, limit=min(limit, CAPITAL_EFFECTIVE_FETCH_LIMIT))
    except Exception:
        return []
    return [row for row in rows if isinstance(row, dict)]


def _status_rows(rows: list[Any], status: str) -> list[dict[str, Any]]:
    clean: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not is_main_metric_trade(row, query_symbol=""):
            continue
        if status != "all" and str(row.get("status") or "").casefold().strip() != status:
            continue
        clean.append(dict(row))
    return clean


def _close_record_index(rows: list[dict[str, Any]]) -> set[str]:
    closed: set[str] = set()
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        trade_id = _trade_id(payload)
        status = str(payload.get("status") or "").casefold().strip()
        if trade_id and status in _CLOSED_STATUSES:
            closed.add(trade_id)
    return closed


def _record_summary(row: dict[str, Any], *, close_index: set[str], redact_ids: bool) -> dict[str, Any]:
    trade_id = _trade_id(row)
    opened_at = str(row.get("opened_at") or row.get("created_at") or "")
    return {
        "shadow_trade_id": _redact_id(trade_id) if redact_ids else trade_id,
        "shadow_trade_id_hash": _hash_id(trade_id),
        "symbol": _symbol(row.get("symbol") or row.get("normalized_symbol")),
        "timeframe": str(row.get("timeframe") or "").upper().strip(),
        "profile": str(row.get("strategy_profile") or row.get("profile") or row.get("family") or "").strip(),
        "status": str(row.get("status") or "").casefold().strip(),
        "opened_at": opened_at,
        "updated_at": str(row.get("updated_at") or ""),
        "created_at": str(row.get("created_at") or ""),
        "source": str(row.get("source") or ""),
        "has_matching_close_record": trade_id in close_index,
        "broker_touched": bool(row.get("broker_touched")),
        "order_executed": bool(row.get("order_executed")),
        "order_policy": str(row.get("order_policy") or "journal_only_no_broker"),
    }


def _source_matches_capital(
    *,
    backend: str,
    status: str,
    limit: int,
    require_live_db: bool,
    live_db_detected: bool,
) -> bool:
    if status != "open":
        return False
    if int(limit) != CAPITAL_SNAPSHOT_LIMIT:
        return False
    if require_live_db and not live_db_detected:
        return False
    return backend in {"postgres", "sqlite"}


def _source_unavailable(
    *,
    limit: int,
    status: str,
    require_live_db: bool,
    reason: str,
    backend_type: str = "unavailable",
    live_db_detected: bool = False,
    recommendation: str = "run_in_live_environment_with_database_url_before_cleanup",
) -> dict[str, Any]:
    return {
        "ok": False,
        "status": reason,
        "reason": reason,
        "inspector_version": INSPECTOR_VERSION,
        "mode": "read_only_inspection",
        "source_name": "legacy_memory_store_mt5_shadow_trades",
        "backend_type": backend_type,
        "live_db_required": bool(require_live_db),
        "live_db_detected": bool(live_db_detected),
        "source_matches_capital_protection": False,
        "source_fingerprint": "",
        "query_description": _query_description(limit),
        "limit_used": int(limit),
        "capital_snapshot_limit": CAPITAL_SNAPSHOT_LIMIT,
        "effective_fetch_limit": min(int(limit), CAPITAL_EFFECTIVE_FETCH_LIMIT),
        "status_filter": status,
        "scope": {
            "symbol": "global",
            "timeframe": "global",
            "profile": "global",
            "session_id": "none",
            "broker_account": "none",
        },
        "open_shadow_trades_count": 0,
        "records_sample": [],
        "records_full_included": False,
        "symbols_included": {},
        "timeframes_included": {},
        "profiles_included": {},
        "oldest_open_at": "",
        "newest_open_at": "",
        "recommendation": recommendation,
        "read_only": True,
        "mutations_executed": False,
        "shadow_opened": False,
        "shadow_closed": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        **_safety(),
    }


def _query_description(limit: int) -> str:
    effective_limit = min(int(limit), CAPITAL_EFFECTIVE_FETCH_LIMIT)
    return (
        "MT5ShadowTrading().snapshot(limit=500) -> "
        f"MemoryStore.get_mt5_events('{LEGACY_COLLECTION}', symbol=None, limit={effective_limit}) -> "
        "genesis_memory_events WHERE event_type='mt5_shadow_trade' ORDER BY id DESC"
    )


def _source_fingerprint(*, backend: str, live_db_detected: bool, status: str, limit: int, count: int) -> str:
    payload = {
        "source": "legacy_memory_store_mt5_shadow_trades",
        "backend": backend,
        "live_db_detected": bool(live_db_detected),
        "status": status,
        "limit": int(limit),
        "effective_fetch_limit": min(int(limit), CAPITAL_EFFECTIVE_FETCH_LIMIT),
        "scope": "global",
        "open_shadow_trades_count": int(count),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:24]


def _recommendation(source_matches: bool, require_live_db: bool, live_db_detected: bool, count: int) -> str:
    if require_live_db and not live_db_detected:
        return "run_in_live_environment_with_database_url_before_cleanup"
    if not source_matches:
        return "do_not_cleanup_until_source_matches_capital_protection"
    if count:
        return "compare_count_with_live_capital_before_apply_cleanup"
    return "legacy_source_matches_and_reports_zero_open"


def _counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "UNKNOWN")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _oldest(rows: list[dict[str, Any]]) -> str:
    timestamps = sorted(parsed for parsed in (_parse_time(row.get("opened_at")) for row in rows) if parsed)
    return timestamps[0].isoformat() if timestamps else ""


def _newest(rows: list[dict[str, Any]]) -> str:
    timestamps = sorted(parsed for parsed in (_parse_time(row.get("opened_at")) for row in rows) if parsed)
    return timestamps[-1].isoformat() if timestamps else ""


def _parse_time(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _trade_id(row: dict[str, Any]) -> str:
    return str(row.get("shadow_trade_id") or row.get("trade_id") or "").strip()


def _symbol(value: object) -> str:
    return str(value or "").upper().strip().replace(".B", "")


def _redact_id(value: str) -> str:
    if not value:
        return ""
    digest = _hash_id(value)
    return f"redacted:{digest[:12]}"


def _hash_id(value: str) -> str:
    if not value:
        return ""
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:16]


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
