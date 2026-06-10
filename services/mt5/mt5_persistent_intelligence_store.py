from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation_registry_status
from services.mt5.mt5_persistent_schema import (
    PERSISTENT_INTELLIGENCE_SCHEMA_VERSION,
    REQUIRED_TABLES,
    TABLE_PRIMARY_KEYS,
    persistent_schema_status,
)
from services.mt5.mt5_research_rejection_registry import research_rejection_registry_status


STORE_VERSION = "2026-06-10.mt5_persistent_intelligence_store.v1"
MAX_STRING_LENGTH = 500
MAX_JSON_BYTES = 24_000

_LAST_WRITE_AT = ""
_FAILED_WRITES = 0
_QUEUED_WRITES = 0


class SupabaseRestClient:
    def __init__(self, *, url: str = "", key: str = "", timeout_seconds: float = 5.0) -> None:
        self.url = str(url or "").rstrip("/")
        self.key = str(key or "")
        self.timeout_seconds = float(timeout_seconds or 5.0)
        self.url_configured = bool(self.url)
        self.key_configured = bool(self.key)

    @classmethod
    def from_env(cls) -> "SupabaseRestClient":
        url = (
            os.getenv("SUPABASE_URL")
            or os.getenv("SUPABASE_PROJECT_URL")
            or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
            or ""
        )
        key = (
            os.getenv("SUPABASE_SECRET_KEY")
            or _json_key(os.getenv("SUPABASE_SECRET_KEYS"), "default")
            or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            or os.getenv("SUPABASE_SERVICE_KEY")
            or ""
        )
        return cls(url=url, key=key, timeout_seconds=float(os.getenv("SUPABASE_REST_TIMEOUT_SECONDS") or 5.0))

    @property
    def available(self) -> bool:
        return bool(self.url and self.key)

    def table_ready(self, table: str) -> bool:
        if not self.available:
            return False
        self.select(table, params={"select": "*", "limit": "1"})
        return True

    def insert(self, table: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", table, payload=payload, prefer="return=minimal")

    def upsert(self, table: str, payload: dict[str, Any], *, on_conflict: tuple[str, ...]) -> dict[str, Any]:
        params = {"on_conflict": ",".join(on_conflict)} if on_conflict else {}
        return self._request("POST", table, params=params, payload=payload, prefer="resolution=merge-duplicates,return=minimal")

    def select(self, table: str, *, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
        result = self._request("GET", table, params=params or {})
        if isinstance(result, list):
            return [row for row in result if isinstance(row, dict)]
        return []

    def _request(
        self,
        method: str,
        table: str,
        *,
        params: dict[str, str] | None = None,
        payload: dict[str, Any] | None = None,
        prefer: str = "",
    ) -> Any:
        if not self.available:
            raise RuntimeError("supabase_not_configured")
        query = f"?{urlencode(params or {})}" if params else ""
        target = f"{self.url}/rest/v1/{table}{query}"
        data = None
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "apikey": self.key,
            "User-Agent": "GenesisPersistentIntelligence/1.0",
        }
        if _looks_like_legacy_jwt(self.key):
            headers["Authorization"] = f"Bearer {self.key}"
        if prefer:
            headers["Prefer"] = prefer
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        request = Request(target, data=data, headers=headers, method=method)
        with urlopen(request, timeout=self.timeout_seconds) as response:
            body = response.read()
        if not body:
            return {}
        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            return {}


class MT5PersistentIntelligenceStore:
    def __init__(self, *, client: Any | None = None) -> None:
        self.client = client if client is not None else SupabaseRestClient.from_env()

    def status(self) -> dict[str, Any]:
        return self.healthcheck(write_test_event=False)

    def healthcheck(self, *, write_test_event: bool = False) -> dict[str, Any]:
        env_status = _env_status(self.client)
        client_available = bool(getattr(self.client, "available", False))
        table_status: dict[str, bool] = {}
        table_errors: dict[str, str] = {}
        if client_available:
            for table in REQUIRED_TABLES:
                try:
                    table_status[table] = bool(self.client.table_ready(table))
                except Exception as exc:
                    table_status[table] = False
                    table_errors[table] = _safe_error(exc)
        else:
            table_status = {table: False for table in REQUIRED_TABLES}
        tables_ready = all(table_status.values()) if table_status else False
        db_available = client_available and not _connection_unavailable(table_errors)
        permission_checks = {
            "select": bool(db_available and any(table_status.values())),
            "insert": False,
            "upsert": False,
            "write_test_event_required": True,
        }
        test_write = {"attempted": False, "ok": False}
        if write_test_event and db_available and tables_ready:
            insert_result = self.record_decision_event(
                {
                    "symbol": "HEALTHCHECK",
                    "timeframe": "NA",
                    "decision": "NO_TRADE",
                    "reason": "persistent_intelligence_healthcheck",
                    "profile": "persistent_intelligence_healthcheck",
                    "risk_state": "diagnostic",
                    "risk_allowed": False,
                    "risk_reason": "healthcheck_only",
                    "broker_touched": False,
                    "order_executed": False,
                    "order_policy": "journal_only_no_broker",
                }
            )
            upsert_result = self.upsert_profile_state(
                {
                    "symbol": "HEALTHCHECK",
                    "timeframe": "NA",
                    "profile": "persistent_intelligence_healthcheck",
                    "status": "diagnostic",
                    "active": False,
                    "applies_to_paper_shadow": False,
                    "applies_to_real_trading": False,
                    "registry_source": "persistent_intelligence_healthcheck",
                }
            )
            permission_checks["insert"] = bool(insert_result.get("ok"))
            permission_checks["upsert"] = bool(upsert_result.get("ok"))
            test_write = {
                "attempted": True,
                "ok": bool(insert_result.get("ok") and upsert_result.get("ok")),
                "decision_event": insert_result,
                "profile_state_upsert": upsert_result,
            }
        recommendation = _healthcheck_recommendation(
            env_status=env_status,
            db_available=db_available,
            tables_ready=tables_ready,
            write_test_event=write_test_event,
            permission_checks=permission_checks,
        )
        return {
            "ok": True,
            "status": "persistent_intelligence_status_ready",
            "store_version": STORE_VERSION,
            "schema_version": PERSISTENT_INTELLIGENCE_SCHEMA_VERSION,
            "env": env_status,
            "db_available": db_available,
            "db_degraded": not (db_available and tables_ready and (not write_test_event or bool(test_write.get("ok")))),
            "tables_ready": tables_ready,
            "table_status": table_status,
            "table_errors": table_errors,
            "permission_checks": permission_checks,
            "last_write_at": _LAST_WRITE_AT,
            "failed_writes": _FAILED_WRITES,
            "queued_writes": _QUEUED_WRITES,
            "estimated_storage_mode": "supabase_rest" if db_available and tables_ready else "local_runtime_only",
            "test_write": test_write,
            "recommendation": recommendation,
            "critical_persistence_available": db_available and tables_ready,
            "decision": "NO_TRADE",
            "reason": "persistent_intelligence_db_degraded" if not (db_available and tables_ready) else "persistent_intelligence_ready",
            "schema": persistent_schema_status(),
            "secrets_printed": False,
            **_safety(),
        }

    def upsert_profile_state(self, payload: dict[str, Any]) -> dict[str, Any]:
        row = _sanitize_row(
            {
                "symbol": _symbol(payload.get("symbol")),
                "timeframe": _timeframe(payload.get("timeframe")),
                "profile": payload.get("profile"),
                "status": payload.get("status") or "observation_only",
                "active": bool(payload.get("active")),
                "applies_to_paper_shadow": bool(payload.get("applies_to_paper_shadow")),
                "applies_to_real_trading": False,
                "degradation_reason": payload.get("degradation_reason") or "",
                "registry_source": payload.get("registry_source") or payload.get("degradation_source") or "",
                "updated_at": payload.get("updated_at") or _now(),
            }
        )
        return self._safe_upsert("mt5_profile_state", row)

    def upsert_profile_performance(self, payload: dict[str, Any]) -> dict[str, Any]:
        row = _sanitize_row(
            {
                "symbol": _symbol(payload.get("symbol")),
                "timeframe": _timeframe(payload.get("timeframe")),
                "profile": payload.get("profile"),
                "trades_forward": _int(payload.get("trades_forward")),
                "wins": _int(payload.get("wins")),
                "losses": _int(payload.get("losses")),
                "win_rate": _float(payload.get("win_rate")),
                "profit_factor": _float(payload.get("profit_factor")),
                "expectancy": _float(payload.get("expectancy")),
                "max_drawdown": _float(payload.get("max_drawdown")),
                "consecutive_losses": _int(payload.get("consecutive_losses")),
                "recent_closed": _int(payload.get("recent_closed")),
                "recent_profit_factor": _float(payload.get("recent_profit_factor") or payload.get("recent_pf")),
                "recent_expectancy": _float(payload.get("recent_expectancy")),
                "updated_at": payload.get("updated_at") or _now(),
            }
        )
        return self._safe_upsert("mt5_profile_performance", row)

    def record_shadow_trade(self, payload: dict[str, Any]) -> dict[str, Any]:
        row = _sanitize_row(
            {
                "shadow_trade_id": payload.get("shadow_trade_id") or payload.get("trade_id"),
                "symbol": _symbol(payload.get("symbol")),
                "timeframe": _timeframe(payload.get("timeframe")),
                "profile": payload.get("profile") or payload.get("strategy_profile") or payload.get("filter_profile"),
                "side": str(payload.get("side") or payload.get("action") or "").lower(),
                "entry_price": _float(payload.get("entry_price") or payload.get("entry")),
                "exit_price": _float(payload.get("exit_price")),
                "pnl": _float(payload.get("pnl")),
                "pnl_pct": _float(payload.get("pnl_pct")),
                "r_multiple": _float(payload.get("r_multiple")),
                "status": payload.get("status") or payload.get("lifecycle_status") or "",
                "opened_at": payload.get("opened_at") or payload.get("created_at") or None,
                "closed_at": payload.get("closed_at") or None,
                "exit_reason": payload.get("exit_reason") or "",
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        )
        return self._safe_upsert("mt5_shadow_trades", row)

    def record_decision_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        row = _sanitize_row(
            {
                "timestamp": payload.get("timestamp") or payload.get("generated_at") or _now(),
                "symbol": _symbol(payload.get("symbol")),
                "timeframe": _timeframe(payload.get("timeframe")),
                "decision": payload.get("decision") or "NO_TRADE",
                "reason": payload.get("reason") or "",
                "profile": payload.get("profile") or payload.get("strategy_profile") or payload.get("paper_forward_candidate_profile") or "",
                "strategy_score": _float(payload.get("strategy_score") or payload.get("score")),
                "momentum_score": _float(payload.get("momentum_score")),
                "trend_score": _float(payload.get("trend_score")),
                "volatility_score": _float(payload.get("volatility_score")),
                "risk_state": payload.get("risk_state") or "",
                "risk_allowed": bool(payload.get("risk_allowed") if "risk_allowed" in payload else payload.get("risk_governor_allowed")),
                "risk_reason": payload.get("risk_reason") or payload.get("risk_governor_reason") or "",
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        )
        return self._safe_insert("mt5_decision_events", row)

    def record_risk_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        row = _sanitize_row(
            {
                "timestamp": payload.get("timestamp") or _now(),
                "symbol": _symbol(payload.get("symbol")),
                "timeframe": _timeframe(payload.get("timeframe")),
                "risk_state": payload.get("risk_state") or "",
                "allowed": bool(payload.get("allowed")),
                "reason": payload.get("reason") or "",
                "circuit_breaker": payload.get("circuit_breaker") or "",
                "consecutive_losses": _int(payload.get("consecutive_losses")),
                "drawdown": _float(payload.get("drawdown") or payload.get("max_drawdown")),
                "open_shadow_count": _int(payload.get("open_shadow_count") or payload.get("open_shadow_trades")),
                "recommended_action": payload.get("recommended_action") or "",
            }
        )
        return self._safe_insert("mt5_risk_events", row)

    def record_candidate_rotation_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        row = _sanitize_row(
            {
                "run_id": payload.get("run_id") or f"rotation-{uuid4().hex[:12]}",
                "timestamp": payload.get("timestamp") or _now(),
                "recommendation": payload.get("recommendation") or "",
                "recommended_candidate": _compact_json(payload.get("recommended_candidate") or {}),
                "candidate_activated": False,
                "paper_forward_onboarding_started": False,
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        )
        return self._safe_upsert("mt5_candidate_rotation_runs", row)

    def record_adaptive_governor_state(self, payload: dict[str, Any]) -> dict[str, Any]:
        row = _sanitize_row(
            {
                "timestamp": payload.get("timestamp") or payload.get("generated_at") or _now(),
                "global_state": payload.get("global_state") or "",
                "recommended_next_action": payload.get("recommended_next_action") or "",
                "active_profiles": _compact_json(payload.get("active_profiles") or []),
                "paused_profiles": _compact_json(payload.get("paused_profiles") or []),
                "degraded_profiles": _compact_json(payload.get("degraded_profiles") or []),
                "circuit_breakers": _compact_json(payload.get("circuit_breakers") or []),
                "open_shadow_trades": _int(payload.get("open_shadow_trades")),
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        )
        return self._safe_insert("mt5_adaptive_governor_state", row)

    def record_research_lesson(self, payload: dict[str, Any]) -> dict[str, Any]:
        row = _sanitize_row(
            {
                "timestamp": payload.get("timestamp") or _now(),
                "family": payload.get("family") or "",
                "symbol": _symbol(payload.get("symbol")),
                "timeframe": _timeframe(payload.get("timeframe")),
                "lesson_type": payload.get("lesson_type") or "",
                "failure_pattern": payload.get("failure_pattern") or "",
                "summary": payload.get("summary") or "",
                "avoid_next": _compact_json(payload.get("avoid_next") or []),
                "recommended_next_research_phase": payload.get("recommended_next_research_phase") or "",
            }
        )
        return self._safe_insert("mt5_research_lessons", row)

    def get_degraded_profiles(self) -> dict[str, Any]:
        result = self._safe_select("mt5_degradation_registry", params={"select": "*", "limit": "500"})
        rows = result.get("rows") or []
        if rows:
            return {"ok": True, "source": "supabase", "degraded_profiles": rows, **_safety()}
        fallback = forward_profile_degradation_registry_status()
        return {
            "ok": True,
            "source": "local_forward_profile_degradation_registry",
            "db_degraded": True,
            "degraded_profiles": fallback.get("degraded_profiles") or [],
            **_safety(),
        }

    def get_rejected_research_families(self) -> dict[str, Any]:
        result = self._safe_select("mt5_research_rejection_registry", params={"select": "*", "limit": "500"})
        rows = result.get("rows") or []
        if rows:
            return {"ok": True, "source": "supabase", "research_rejections": rows, **_safety()}
        fallback = research_rejection_registry_status()
        return {
            "ok": True,
            "source": "local_research_rejection_registry",
            "db_degraded": True,
            "research_rejections": fallback.get("research_rejections") or [],
            **_safety(),
        }

    def get_adaptive_governor_state(self) -> dict[str, Any]:
        result = self._safe_select(
            "mt5_adaptive_governor_state",
            params={"select": "*", "order": "timestamp.desc", "limit": "1"},
        )
        rows = result.get("rows") or []
        return {
            "ok": True,
            "db_degraded": bool(result.get("db_degraded")),
            "state": rows[0] if rows else {},
            "source": "supabase" if rows else "none",
            **_safety(),
        }

    def compact_old_decision_events(
        self,
        *,
        older_than_days: int = 30,
        limit: int = 1000,
        dry_run: bool = True,
        confirm_delete_detail: bool = False,
    ) -> dict[str, Any]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=int(older_than_days or 30))).isoformat()
        result = self._safe_select(
            "mt5_decision_events",
            params={"select": "timestamp,symbol,timeframe,decision,reason,profile,risk_state", "timestamp": f"lt.{cutoff}", "limit": str(int(limit or 1000))},
        )
        rows = [row for row in result.get("rows") or [] if isinstance(row, dict)]
        summary = _decision_summary(rows)
        write_result = {"ok": True, "skipped": True}
        if rows and not dry_run:
            write_result = self.record_research_lesson(
                {
                    "family": "decision_events",
                    "lesson_type": "decision_event_compaction_summary",
                    "failure_pattern": "historical_decision_distribution",
                    "summary": json.dumps(summary, ensure_ascii=True, sort_keys=True)[:MAX_STRING_LENGTH],
                    "avoid_next": [],
                    "recommended_next_research_phase": "continue_research",
                }
            )
        return {
            "ok": True,
            "status": "persistent_intelligence_compaction_ready",
            "dry_run": bool(dry_run),
            "older_than_days": int(older_than_days or 30),
            "retention_plan": {
                "decision_events": "keep_recent_detail_and_summarize_historical_events",
                "risk_events": "keep_critical_events_complete",
                "shadow_trades": "keep_all_compact_trade_rows",
                "research_lessons": "keep_all",
                "raw_ticks": "do_not_store",
                "ohlc_csv": "do_not_store",
            },
            "rows_scanned": len(rows),
            "rows_summarized": len(rows),
            "rows_deleted": 0,
            "delete_detail_confirmed": bool(confirm_delete_detail),
            "critical_data_deleted": False,
            "summary": summary,
            "write_result": write_result,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def _safe_insert(self, table: str, row: dict[str, Any]) -> dict[str, Any]:
        if not self._available():
            return _write_unavailable(table, "supabase_not_configured")
        started = time.monotonic()
        try:
            self.client.insert(table, row)
            return _write_ok(table, started)
        except (HTTPError, URLError, TimeoutError, OSError, RuntimeError) as exc:
            return _write_failed(table, exc, started)

    def _safe_upsert(self, table: str, row: dict[str, Any]) -> dict[str, Any]:
        if not self._available():
            return _write_unavailable(table, "supabase_not_configured")
        started = time.monotonic()
        try:
            self.client.upsert(table, row, on_conflict=TABLE_PRIMARY_KEYS.get(table, ()))
            return _write_ok(table, started)
        except (HTTPError, URLError, TimeoutError, OSError, RuntimeError) as exc:
            return _write_failed(table, exc, started)

    def _safe_select(self, table: str, *, params: dict[str, str]) -> dict[str, Any]:
        if not self._available():
            return {"ok": False, "db_degraded": True, "rows": [], "reason": "supabase_not_configured", **_safety()}
        try:
            rows = self.client.select(table, params=params)
            return {"ok": True, "db_degraded": False, "rows": rows, **_safety()}
        except (HTTPError, URLError, TimeoutError, OSError, RuntimeError) as exc:
            return {"ok": False, "db_degraded": True, "rows": [], "reason": _safe_error(exc), **_safety()}

    def _available(self) -> bool:
        return bool(getattr(self.client, "available", False))


def persistent_intelligence_status(*, write_test_event: bool = False) -> dict[str, Any]:
    return MT5PersistentIntelligenceStore().healthcheck(write_test_event=write_test_event)


def compact_old_decision_events(
    *,
    older_than_days: int = 30,
    limit: int = 1000,
    dry_run: bool = True,
    confirm_delete_detail: bool = False,
) -> dict[str, Any]:
    return MT5PersistentIntelligenceStore().compact_old_decision_events(
        older_than_days=older_than_days,
        limit=limit,
        dry_run=dry_run,
        confirm_delete_detail=confirm_delete_detail,
    )


def _env_status(client: Any) -> dict[str, Any]:
    url_configured = bool(getattr(client, "url_configured", False))
    key_configured = bool(getattr(client, "key_configured", False))
    if not hasattr(client, "url_configured") and getattr(client, "available", False):
        url_configured = True
    if not hasattr(client, "key_configured") and getattr(client, "available", False):
        key_configured = True
    return {
        "supabase_url_present": url_configured,
        "supabase_secret_key_present": key_configured,
        "supabase_env_ready": bool(url_configured and key_configured),
        "secret_values_printed": False,
    }


def _healthcheck_recommendation(
    *,
    env_status: dict[str, Any],
    db_available: bool,
    tables_ready: bool,
    write_test_event: bool,
    permission_checks: dict[str, Any],
) -> str:
    if not env_status.get("supabase_env_ready"):
        return "configure_supabase_env"
    if not db_available:
        return "verify_supabase_connection"
    if not tables_ready:
        return "apply_schema_sql"
    if write_test_event and not (permission_checks.get("insert") and permission_checks.get("upsert")):
        return "verify_supabase_insert_upsert_permissions"
    return "persistent_intelligence_ready"


def _connection_unavailable(table_errors: dict[str, str]) -> bool:
    if not table_errors:
        return False
    text = " ".join(table_errors.values()).casefold()
    connection_markers = (
        "timed out",
        "timeout",
        "connection refused",
        "name or service not known",
        "nodename nor servname",
        "network is unreachable",
        "temporary failure in name resolution",
        "401",
        "unauthorized",
        "invalid api key",
        "invalid jwt",
    )
    return any(marker in text for marker in connection_markers)


def _write_ok(table: str, started: float) -> dict[str, Any]:
    global _LAST_WRITE_AT
    _LAST_WRITE_AT = _now()
    return {
        "ok": True,
        "table": table,
        "db_degraded": False,
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }


def _write_unavailable(table: str, reason: str) -> dict[str, Any]:
    global _QUEUED_WRITES
    _QUEUED_WRITES += 1
    return {"ok": False, "table": table, "db_degraded": True, "queued": True, "reason": reason, **_safety()}


def _write_failed(table: str, exc: Exception, started: float) -> dict[str, Any]:
    global _FAILED_WRITES
    _FAILED_WRITES += 1
    return {
        "ok": False,
        "table": table,
        "db_degraded": True,
        "queued": True,
        "reason": _safe_error(exc),
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }


def _sanitize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _sanitize_value(value) for key, value in row.items()}


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, str):
        return value[:MAX_STRING_LENGTH]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, dict)):
        return _compact_json(value)
    return str(value)[:MAX_STRING_LENGTH]


def _compact_json(value: Any) -> Any:
    try:
        text = json.dumps(value, ensure_ascii=True, sort_keys=True)
    except (TypeError, ValueError):
        return {}
    if len(text.encode("utf-8")) <= MAX_JSON_BYTES:
        return value
    if isinstance(value, list):
        compacted = value[:20]
        return {"compacted": True, "original_count": len(value), "items": compacted}
    if isinstance(value, dict):
        compacted = {key: value[key] for key in list(value.keys())[:40]}
        compacted["compacted"] = True
        return compacted
    return str(value)[:MAX_STRING_LENGTH]


def _decision_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_decision: dict[str, int] = {}
    by_reason: dict[str, int] = {}
    for row in rows:
        decision = str(row.get("decision") or "NO_TRADE")
        reason = str(row.get("reason") or "")
        by_decision[decision] = by_decision.get(decision, 0) + 1
        by_reason[reason] = by_reason.get(reason, 0) + 1
    return {
        "total_rows": len(rows),
        "by_decision": dict(sorted(by_decision.items())),
        "top_reasons": dict(sorted(by_reason.items(), key=lambda item: item[1], reverse=True)[:10]),
    }


def _json_key(raw: str | None, key: str) -> str:
    if not raw:
        return ""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return ""
    if not isinstance(payload, dict):
        return ""
    return str(payload.get(key) or "")


def _looks_like_legacy_jwt(value: str) -> bool:
    return str(value or "").count(".") >= 2 and not str(value or "").startswith("sb_")


def _safe_error(exc: object) -> str:
    text = str(exc or exc.__class__.__name__)
    for value in (os.getenv("SUPABASE_SECRET_KEY"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"), os.getenv("SUPABASE_SERVICE_KEY")):
        if value:
            text = text.replace(value, "[redacted]")
    return text[:500]


def _symbol(value: object) -> str:
    return str(value or "").upper().strip().replace(".B", "")


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _int(value: object) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
