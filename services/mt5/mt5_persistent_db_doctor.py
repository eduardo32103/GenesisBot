from __future__ import annotations

import os
import threading
import time
from typing import Any

from scripts.run_persistent_db_connection_diagnostics import run_connection_diagnostics
from scripts.run_persistent_intelligence_apply_schema import run_apply_schema
from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore


DB_DOCTOR_VERSION = "2026-06-11.mt5_persistent_db_doctor.v1"
_AUTO_HEAL_LOCK = threading.Lock()
_AUTO_HEAL_LAST_RUN = 0.0
_AUTO_HEAL_RESULT: dict[str, Any] = {}


def run_persistent_db_doctor(
    *,
    apply_schema: bool = False,
    repair: bool = False,
    wait_for_connection: bool = False,
    max_connect_attempts: int = 10,
    connect_backoff_seconds: float = 5.0,
    prefer_public_url: bool = True,
    use_public_url: bool = False,
    statement_timeout_ms: int = 30000,
    store: MT5PersistentIntelligenceStore | None = None,
) -> dict[str, Any]:
    active_store = store or MT5PersistentIntelligenceStore()
    healthcheck = active_store.healthcheck(write_test_event=False)
    diagnostics = run_connection_diagnostics(
        use_public_url=use_public_url,
        prefer_public_url=prefer_public_url,
        wait_for_connection=wait_for_connection,
        max_connect_attempts=max(1, int(max_connect_attempts or 1)),
        connect_backoff_seconds=connect_backoff_seconds,
        statement_timeout_ms=statement_timeout_ms,
    )
    missing_tables = list(healthcheck.get("missing_tables") or [])
    should_apply = bool((apply_schema or repair) and missing_tables)
    apply_result: dict[str, Any] = {"attempted": False, "applied": False}
    post_healthcheck: dict[str, Any] = {}
    if should_apply:
        apply_result = run_apply_schema(
            apply=True,
            include_rls=False,
            wait_for_connection=wait_for_connection,
            max_connect_attempts=max_connect_attempts,
            connect_backoff_seconds=connect_backoff_seconds,
            use_public_url=use_public_url,
            prefer_public_url=prefer_public_url,
            statement_timeout_ms=statement_timeout_ms,
        )
        apply_result["attempted"] = True
        post_healthcheck = MT5PersistentIntelligenceStore().healthcheck(write_test_event=False)
    effective = post_healthcheck or healthcheck
    recommendation = _recommendation(
        healthcheck=effective,
        diagnostics=diagnostics,
        apply_result=apply_result,
        repair_requested=bool(apply_schema or repair),
    )
    return {
        "ok": True,
        "status": "persistent_db_doctor_ready",
        "doctor_version": DB_DOCTOR_VERSION,
        "provider": effective.get("provider") or diagnostics.get("provider"),
        "db_available": bool(effective.get("db_available")),
        "db_degraded": bool(effective.get("db_degraded")),
        "tables_ready": bool(effective.get("tables_ready")),
        "missing_tables": list(effective.get("missing_tables") or missing_tables),
        "writes_frozen": bool(effective.get("writes_frozen")),
        "schema_missing_write_freeze": bool(effective.get("schema_missing_write_freeze")),
        "queue_depth": int(effective.get("queue_depth") or 0),
        "failed_writes": int(effective.get("failed_writes") or 0),
        "queued_writes": int(effective.get("queued_writes") or 0),
        "dropped_noncritical_writes": int(effective.get("dropped_noncritical_writes") or 0),
        "last_db_error_category": str(effective.get("last_db_error_category") or diagnostics.get("error_category") or ""),
        "auto_apply_schema_enabled": _auto_apply_enabled(),
        "apply_schema_requested": bool(apply_schema),
        "repair_requested": bool(repair),
        "diagnostics": _compact_diagnostics(diagnostics),
        "apply_result": _compact_apply_result(apply_result),
        "post_apply_healthcheck": _compact_healthcheck(post_healthcheck),
        "recommendation": recommendation,
        "decision": "NO_TRADE" if not bool(effective.get("tables_ready")) else "",
        "reason": "persistent_intelligence_schema_missing" if not bool(effective.get("tables_ready")) else "persistent_intelligence_ready",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "secrets_printed": False,
        **_safety(),
    }


def maybe_auto_apply_persistent_schema() -> dict[str, Any]:
    global _AUTO_HEAL_LAST_RUN, _AUTO_HEAL_RESULT
    if not _auto_apply_enabled():
        return {
            "ok": True,
            "status": "persistent_db_doctor_auto_apply_disabled",
            "auto_apply_schema_enabled": False,
            "attempted": False,
            "secrets_printed": False,
            **_safety(),
        }
    cooldown = _auto_apply_cooldown_seconds()
    now = time.monotonic()
    with _AUTO_HEAL_LOCK:
        if _AUTO_HEAL_LAST_RUN and now - _AUTO_HEAL_LAST_RUN < cooldown:
            return {
                "ok": True,
                "status": "persistent_db_doctor_auto_apply_cooldown",
                "auto_apply_schema_enabled": True,
                "attempted": False,
                "cooldown_seconds": cooldown,
                "last_result": _AUTO_HEAL_RESULT,
                "secrets_printed": False,
                **_safety(),
            }
        _AUTO_HEAL_LAST_RUN = now
    result = run_persistent_db_doctor(
        repair=True,
        wait_for_connection=False,
        max_connect_attempts=1,
        connect_backoff_seconds=1.0,
        prefer_public_url=True,
    )
    with _AUTO_HEAL_LOCK:
        _AUTO_HEAL_RESULT = _compact_doctor_result(result)
    return result


def reset_persistent_db_doctor_for_tests() -> None:
    global _AUTO_HEAL_LAST_RUN, _AUTO_HEAL_RESULT
    with _AUTO_HEAL_LOCK:
        _AUTO_HEAL_LAST_RUN = 0.0
        _AUTO_HEAL_RESULT = {}


def _recommendation(
    *,
    healthcheck: dict[str, Any],
    diagnostics: dict[str, Any],
    apply_result: dict[str, Any],
    repair_requested: bool,
) -> str:
    if healthcheck.get("tables_ready") and not healthcheck.get("db_degraded"):
        return "persistent_intelligence_ready"
    category = str(healthcheck.get("last_db_error_category") or diagnostics.get("error_category") or "")
    if category == "max_connections":
        return "restart_db_and_app_then_apply_schema"
    if apply_result.get("attempted") and not apply_result.get("applied"):
        return "apply_schema_failed_review_connection_diagnostics"
    if not diagnostics.get("can_connect") and str(diagnostics.get("error_category") or "") == "missing_database_env":
        return "repair_database_connection_env"
    if healthcheck.get("missing_tables"):
        return "apply_schema_sql" if not repair_requested else "retry_schema_apply_with_public_url_or_query_console"
    if not diagnostics.get("can_connect"):
        return "repair_database_connection_env"
    return "review_persistent_db_health"


def _compact_diagnostics(payload: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "provider",
        "connection_source",
        "DATABASE_URL_PRESENT",
        "DATABASE_PUBLIC_URL_PRESENT",
        "PGHOST_PRESENT",
        "PGUSER_PRESENT",
        "PGPASSWORD_PRESENT",
        "can_parse_url",
        "can_connect",
        "connect_attempts",
        "error_category",
        "error_message_sanitized",
        "secrets_printed",
        "broker_touched",
        "order_executed",
        "order_policy",
    )
    return {key: payload.get(key) for key in keys if key in payload}


def _compact_apply_result(payload: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "attempted",
        "provider",
        "db_available",
        "dry_run",
        "applied",
        "include_rls",
        "connection_source",
        "connect_attempts",
        "tables_ready",
        "missing_tables_after",
        "recommendation",
        "secrets_printed",
        "broker_touched",
        "order_executed",
        "order_policy",
    )
    return {key: payload.get(key) for key in keys if key in payload}


def _compact_healthcheck(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    keys = (
        "provider",
        "db_available",
        "db_degraded",
        "tables_ready",
        "missing_tables",
        "writes_frozen",
        "queue_depth",
        "failed_writes",
        "last_db_error_category",
        "recommendation",
        "secrets_printed",
        "broker_touched",
        "order_executed",
        "order_policy",
    )
    return {key: payload.get(key) for key in keys if key in payload}


def _compact_doctor_result(payload: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "provider",
        "db_available",
        "db_degraded",
        "tables_ready",
        "missing_tables",
        "writes_frozen",
        "recommendation",
        "secrets_printed",
        "broker_touched",
        "order_executed",
        "order_policy",
    )
    return {key: payload.get(key) for key in keys}


def _auto_apply_enabled() -> bool:
    return str(os.getenv("GENESIS_DB_AUTO_APPLY_SCHEMA") or "false").casefold().strip() in {"1", "true", "yes", "on"}


def _auto_apply_cooldown_seconds() -> float:
    try:
        return max(60.0, float(os.getenv("GENESIS_DB_AUTO_APPLY_SCHEMA_COOLDOWN_SEC") or 3600.0))
    except (TypeError, ValueError):
        return 3600.0


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
