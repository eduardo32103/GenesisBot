from __future__ import annotations

import json
import os
import re
import ssl
import time
from datetime import datetime, timedelta, timezone
from importlib.util import find_spec
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urlencode, urlparse
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
_POSTGRES_DRIVER_NAME = "pg8000"
_JSONB_COLUMNS = {
    "recommended_candidate",
    "active_profiles",
    "paused_profiles",
    "degraded_profiles",
    "circuit_breakers",
    "avoid_next",
}
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_LAST_WRITE_AT = ""
_FAILED_WRITES = 0
_QUEUED_WRITES = 0


class UnavailableDbClient:
    provider = "none"
    available = False
    url_configured = False
    key_configured = False
    database_url_configured = False
    driver_available = True
    unavailable_reason = "database_env_not_configured"

    def table_ready(self, table: str) -> bool:
        return False

    def insert(self, table: str, payload: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError(self.unavailable_reason)

    def upsert(self, table: str, payload: dict[str, Any], *, on_conflict: tuple[str, ...]) -> dict[str, Any]:
        raise RuntimeError(self.unavailable_reason)

    def select(self, table: str, *, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
        return []


class SupabaseRestClient:
    provider = "supabase_rest"

    def __init__(self, *, url: str = "", key: str = "", timeout_seconds: float = 5.0) -> None:
        self.url = str(url or "").rstrip("/")
        self.key = str(key or "")
        self.timeout_seconds = float(timeout_seconds or 5.0)
        self.url_configured = bool(self.url)
        self.key_configured = bool(self.key)
        self.database_url_configured = False
        self.driver_available = True

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


class RailwayPostgresClient:
    provider = "railway_postgres"
    url_configured = False
    key_configured = False

    def __init__(
        self,
        *,
        database_url: str = "",
        timeout_seconds: float = 5.0,
        driver_available: bool | None = None,
    ) -> None:
        self.database_url = str(database_url or "")
        self.timeout_seconds = float(timeout_seconds or 5.0)
        self.database_url_configured = bool(self.database_url)
        self.driver_available = _postgres_driver_available() if driver_available is None else bool(driver_available)

    @classmethod
    def from_env(cls) -> "RailwayPostgresClient":
        return cls(
            database_url=os.getenv("DATABASE_URL") or "",
            timeout_seconds=float(os.getenv("POSTGRES_CONNECT_TIMEOUT_SECONDS") or 5.0),
        )

    @property
    def available(self) -> bool:
        return bool(self.database_url_configured and self.driver_available)

    def table_ready(self, table: str) -> bool:
        _validate_table(table)
        rows = self.select(
            "information_schema.tables",
            params={
                "select": "table_name",
                "table_schema": "eq.public",
                "table_name": f"eq.{table}",
                "limit": "1",
            },
        )
        return bool(rows)

    def insert(self, table: str, payload: dict[str, Any]) -> dict[str, Any]:
        _validate_table(table)
        row = dict(payload or {})
        if not row:
            raise RuntimeError("empty_payload")
        columns = [_validate_column(column) for column in row.keys()]
        values = [_postgres_value(column, row[column]) for column in columns]
        placeholders = [_postgres_placeholder(column) for column in columns]
        sql_text = f"insert into public.{table} ({', '.join(columns)}) values ({', '.join(placeholders)})"
        self._execute(sql_text, values)
        return {"ok": True}

    def upsert(self, table: str, payload: dict[str, Any], *, on_conflict: tuple[str, ...]) -> dict[str, Any]:
        _validate_table(table)
        row = dict(payload or {})
        if not row:
            raise RuntimeError("empty_payload")
        columns = [_validate_column(column) for column in row.keys()]
        conflict_columns = [_validate_column(column) for column in (on_conflict or ())]
        values = [_postgres_value(column, row[column]) for column in columns]
        placeholders = [_postgres_placeholder(column) for column in columns]
        sql_text = f"insert into public.{table} ({', '.join(columns)}) values ({', '.join(placeholders)})"
        if conflict_columns:
            update_columns = [column for column in columns if column not in conflict_columns]
            if update_columns:
                assignments = ", ".join(f"{column}=excluded.{column}" for column in update_columns)
                sql_text += f" on conflict ({', '.join(conflict_columns)}) do update set {assignments}"
            else:
                sql_text += f" on conflict ({', '.join(conflict_columns)}) do nothing"
        self._execute(sql_text, values)
        return {"ok": True}

    def select(self, table: str, *, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
        params = params or {}
        schema_prefix = "" if table == "information_schema.tables" else "public."
        if table != "information_schema.tables":
            _validate_table(table)
        columns = _postgres_select_columns(params.get("select") or "*")
        where_clauses: list[str] = []
        values: list[Any] = []
        for key, raw_value in params.items():
            if key in {"select", "order", "limit"}:
                continue
            column = _validate_column(key)
            operator, value = _postgres_filter(raw_value)
            where_clauses.append(f"{column} {operator} %s")
            values.append(value)
        sql_text = f"select {columns} from {schema_prefix}{table}"
        if where_clauses:
            sql_text += " where " + " and ".join(where_clauses)
        order = _postgres_order_clause(params.get("order") or "")
        if order:
            sql_text += f" order by {order}"
        limit = _postgres_limit(params.get("limit"))
        if limit:
            sql_text += " limit %s"
            values.append(limit)
        return self._fetch(sql_text, values)

    def _execute(self, sql_text: str, values: list[Any]) -> None:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            cursor.execute(sql_text, tuple(values))
            connection.commit()
        finally:
            try:
                connection.close()
            except Exception:
                pass

    def _fetch(self, sql_text: str, values: list[Any]) -> list[dict[str, Any]]:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            cursor.execute(sql_text, tuple(values))
            columns = [str(column[0]) for column in (cursor.description or [])]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            try:
                connection.close()
            except Exception:
                pass

    def _connect(self) -> Any:
        if not self.database_url_configured:
            raise RuntimeError("database_url_not_configured")
        if not self.driver_available:
            raise RuntimeError("postgres_driver_missing")
        import pg8000.dbapi

        parsed = urlparse(self.database_url)
        query = parse_qs(parsed.query or "")
        sslmode = str((query.get("sslmode") or [""])[0]).casefold()
        ssl_context = ssl.create_default_context() if sslmode in {"require", "verify-ca", "verify-full"} else None
        return pg8000.dbapi.connect(
            user=unquote(parsed.username or ""),
            password=unquote(parsed.password or "") if parsed.password else None,
            host=parsed.hostname or "localhost",
            port=int(parsed.port or 5432),
            database=(parsed.path or "/").lstrip("/") or None,
            timeout=self.timeout_seconds,
            ssl_context=ssl_context,
            application_name="GenesisPersistentIntelligence",
        )


class MT5PersistentIntelligenceStore:
    def __init__(self, *, client: Any | None = None) -> None:
        self.client = client if client is not None else detect_persistent_db_client()

    def status(self) -> dict[str, Any]:
        return self.healthcheck(write_test_event=False)

    def recent_events(self, *, limit: int = 10) -> dict[str, Any]:
        safe_limit = max(1, min(int(limit or 10), 50))
        decisions = self._safe_select(
            "mt5_decision_events",
            params={
                "select": "timestamp,symbol,timeframe,decision,reason,profile,risk_state,risk_allowed,risk_reason,broker_touched,order_executed,order_policy",
                "order": "timestamp.desc",
                "limit": str(safe_limit),
            },
        )
        risk_events = self._safe_select(
            "mt5_risk_events",
            params={
                "select": "timestamp,symbol,timeframe,risk_state,allowed,reason,circuit_breaker,open_shadow_count,recommended_action",
                "order": "timestamp.desc",
                "limit": str(safe_limit),
            },
        )
        shadow_events = self._safe_select(
            "mt5_shadow_trades",
            params={
                "select": "shadow_trade_id,symbol,timeframe,profile,side,status,opened_at,closed_at,pnl,r_multiple,exit_reason,broker_touched,order_executed,order_policy",
                "order": "opened_at.desc",
                "limit": str(safe_limit),
            },
        )
        research_lessons = self._safe_select(
            "mt5_research_lessons",
            params={
                "select": "timestamp,family,symbol,timeframe,lesson_type,failure_pattern,summary,recommended_next_research_phase",
                "order": "timestamp.desc",
                "limit": str(safe_limit),
            },
        )
        degraded = any(
            bool(result.get("db_degraded"))
            for result in (decisions, risk_events, shadow_events, research_lessons)
        )
        return {
            "ok": True,
            "status": "persistent_intelligence_recent_events_ready",
            "provider": _client_provider(self.client),
            "limit": safe_limit,
            "recent_decisions": _safety_rows(decisions.get("rows") or []),
            "recent_risk_events": _safety_rows(risk_events.get("rows") or []),
            "recent_shadow_events": _safety_rows(shadow_events.get("rows") or []),
            "recent_research_lessons": _safety_rows(research_lessons.get("rows") or []),
            "db_degraded": degraded,
            "failed_writes": _FAILED_WRITES,
            "queued_writes": _QUEUED_WRITES,
            "secrets_printed": False,
            **_safety(),
        }

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
        missing_tables = [table for table, ready in table_status.items() if not ready]
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
            "provider": _client_provider(self.client),
            "env": env_status,
            "db_available": db_available,
            "db_degraded": not (db_available and tables_ready and (not write_test_event or bool(test_write.get("ok")))),
            "tables_ready": tables_ready,
            "table_count": len(REQUIRED_TABLES),
            "missing_tables": missing_tables,
            "table_status": table_status,
            "table_errors": table_errors,
            "permission_checks": permission_checks,
            "last_write_at": _LAST_WRITE_AT,
            "failed_writes": _FAILED_WRITES,
            "queued_writes": _QUEUED_WRITES,
            "estimated_storage_mode": _client_provider(self.client) if db_available and tables_ready else "local_runtime_only",
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
            return {"ok": True, "source": _client_provider(self.client), "degraded_profiles": rows, **_safety()}
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
            return {"ok": True, "source": _client_provider(self.client), "research_rejections": rows, **_safety()}
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
            "provider": _client_provider(self.client),
            "db_degraded": bool(result.get("db_degraded")),
            "state": rows[0] if rows else {},
            "source": _client_provider(self.client) if rows else "none",
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
            "provider": _client_provider(self.client),
            "db_degraded": bool(result.get("db_degraded")),
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
            return _write_unavailable(table, _client_unavailable_reason(self.client))
        started = time.monotonic()
        try:
            self.client.insert(table, row)
            return _write_ok(table, started)
        except Exception as exc:
            return _write_failed(table, exc, started)

    def _safe_upsert(self, table: str, row: dict[str, Any]) -> dict[str, Any]:
        if not self._available():
            return _write_unavailable(table, _client_unavailable_reason(self.client))
        started = time.monotonic()
        try:
            self.client.upsert(table, row, on_conflict=TABLE_PRIMARY_KEYS.get(table, ()))
            return _write_ok(table, started)
        except Exception as exc:
            return _write_failed(table, exc, started)

    def _safe_select(self, table: str, *, params: dict[str, str]) -> dict[str, Any]:
        if not self._available():
            return {"ok": False, "db_degraded": True, "rows": [], "reason": _client_unavailable_reason(self.client), **_safety()}
        try:
            rows = self.client.select(table, params=params)
            return {"ok": True, "db_degraded": False, "rows": rows, **_safety()}
        except Exception as exc:
            return {"ok": False, "db_degraded": True, "rows": [], "reason": _safe_error(exc), **_safety()}

    def _available(self) -> bool:
        return bool(getattr(self.client, "available", False))


def detect_persistent_db_client() -> Any:
    forced_provider = str(os.getenv("PERSISTENT_DB_PROVIDER") or "").casefold().strip()
    database_url = os.getenv("DATABASE_URL") or ""
    supabase_url = os.getenv("SUPABASE_URL") or os.getenv("SUPABASE_PROJECT_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL") or ""
    supabase_key = (
        os.getenv("SUPABASE_SECRET_KEY")
        or _json_key(os.getenv("SUPABASE_SECRET_KEYS"), "default")
        or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or ""
    )
    if forced_provider == "supabase_rest":
        return SupabaseRestClient.from_env() if supabase_url and supabase_key else UnavailableDbClient()
    if forced_provider in {"railway_postgres", "postgres", "database_url"}:
        return RailwayPostgresClient.from_env() if database_url else UnavailableDbClient()
    if database_url:
        return RailwayPostgresClient.from_env()
    if supabase_url and supabase_key:
        return SupabaseRestClient.from_env()
    return UnavailableDbClient()


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


def persistent_intelligence_recent_events(*, limit: int = 10) -> dict[str, Any]:
    return MT5PersistentIntelligenceStore().recent_events(limit=limit)


def persist_decision_event(
    payload: dict[str, Any],
    *,
    critical: bool = False,
    store: MT5PersistentIntelligenceStore | None = None,
) -> dict[str, Any]:
    result = (store or MT5PersistentIntelligenceStore()).record_decision_event(payload)
    return _runtime_persistence_result("decision_event", result, critical=critical)


def persist_risk_event(
    payload: dict[str, Any],
    *,
    critical: bool = False,
    store: MT5PersistentIntelligenceStore | None = None,
) -> dict[str, Any]:
    result = (store or MT5PersistentIntelligenceStore()).record_risk_event(payload)
    return _runtime_persistence_result("risk_event", result, critical=critical)


def persist_shadow_trade(
    payload: dict[str, Any],
    *,
    critical: bool = False,
    store: MT5PersistentIntelligenceStore | None = None,
) -> dict[str, Any]:
    result = (store or MT5PersistentIntelligenceStore()).record_shadow_trade(payload)
    return _runtime_persistence_result("shadow_trade", result, critical=critical)


def persist_adaptive_governor_state(
    payload: dict[str, Any],
    *,
    critical: bool = False,
    store: MT5PersistentIntelligenceStore | None = None,
) -> dict[str, Any]:
    result = (store or MT5PersistentIntelligenceStore()).record_adaptive_governor_state(payload)
    return _runtime_persistence_result("adaptive_governor_state", result, critical=critical)


def persist_research_lesson(
    payload: dict[str, Any],
    *,
    critical: bool = False,
    store: MT5PersistentIntelligenceStore | None = None,
) -> dict[str, Any]:
    result = (store or MT5PersistentIntelligenceStore()).record_research_lesson(payload)
    return _runtime_persistence_result("research_lesson", result, critical=critical)


def persist_candidate_rotation_run(
    payload: dict[str, Any],
    *,
    critical: bool = False,
    store: MT5PersistentIntelligenceStore | None = None,
) -> dict[str, Any]:
    result = (store or MT5PersistentIntelligenceStore()).record_candidate_rotation_run(payload)
    return _runtime_persistence_result("candidate_rotation_run", result, critical=critical)


def _reset_persistent_intelligence_counters_for_tests() -> None:
    global _LAST_WRITE_AT, _FAILED_WRITES, _QUEUED_WRITES
    _LAST_WRITE_AT = ""
    _FAILED_WRITES = 0
    _QUEUED_WRITES = 0


def _env_status(client: Any) -> dict[str, Any]:
    provider = _client_provider(client)
    url_configured = bool(getattr(client, "url_configured", False))
    key_configured = bool(getattr(client, "key_configured", False))
    database_url_configured = bool(getattr(client, "database_url_configured", False))
    driver_available = bool(getattr(client, "driver_available", True))
    if not hasattr(client, "url_configured") and getattr(client, "available", False):
        url_configured = True
    if not hasattr(client, "key_configured") and getattr(client, "available", False):
        key_configured = True
    supabase_ready = bool(url_configured and key_configured)
    postgres_ready = bool(database_url_configured and driver_available)
    return {
        "provider": provider,
        "database_url_present": database_url_configured,
        "postgres_driver": _POSTGRES_DRIVER_NAME,
        "postgres_driver_available": driver_available,
        "supabase_url_present": url_configured,
        "supabase_secret_key_present": key_configured,
        "supabase_env_ready": supabase_ready,
        "database_env_ready": bool(supabase_ready or postgres_ready),
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
    if env_status.get("provider") == "railway_postgres" and not env_status.get("postgres_driver_available"):
        return "install_postgres_driver"
    if not env_status.get("database_env_ready"):
        return "configure_database_env"
    if not db_available:
        return "verify_database_connection"
    if not tables_ready:
        return "apply_schema_sql"
    if write_test_event and not (permission_checks.get("insert") and permission_checks.get("upsert")):
        return "verify_supabase_insert_upsert_permissions"
    return "persistent_intelligence_ready"


def _client_provider(client: Any) -> str:
    provider = str(getattr(client, "provider", "") or "").strip()
    return provider or ("configured_client" if getattr(client, "available", False) else "none")


def _client_unavailable_reason(client: Any) -> str:
    reason = str(getattr(client, "unavailable_reason", "") or "").strip()
    if reason:
        return reason
    if _client_provider(client) == "railway_postgres" and not bool(getattr(client, "driver_available", True)):
        return "postgres_driver_missing"
    return "database_not_configured"


def _postgres_driver_available() -> bool:
    return find_spec(_POSTGRES_DRIVER_NAME) is not None


def _validate_table(table: str) -> str:
    table = str(table or "").strip()
    if table not in REQUIRED_TABLES:
        raise RuntimeError("unsupported_table")
    return table


def _validate_column(column: str) -> str:
    column = str(column or "").strip()
    if not _IDENTIFIER_RE.match(column):
        raise RuntimeError("unsupported_column")
    return column


def _postgres_select_columns(raw: str) -> str:
    if str(raw or "").strip() == "*":
        return "*"
    columns = [_validate_column(column.strip()) for column in str(raw or "").split(",") if column.strip()]
    return ", ".join(columns) if columns else "*"


def _postgres_filter(raw_value: object) -> tuple[str, Any]:
    text = str(raw_value or "")
    if "." not in text:
        return "=", text
    op, value = text.split(".", 1)
    operators = {"eq": "=", "lt": "<", "lte": "<=", "gt": ">", "gte": ">=", "neq": "<>"}
    return operators.get(op, "="), value


def _postgres_order_clause(raw: str) -> str:
    if not raw:
        return ""
    parts = str(raw).split(".")
    column = _validate_column(parts[0])
    direction = "desc" if len(parts) > 1 and str(parts[1]).casefold() == "desc" else "asc"
    return f"{column} {direction}"


def _postgres_limit(raw: object) -> int:
    try:
        value = int(raw or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, min(value, 500))


def _postgres_placeholder(column: str) -> str:
    return "cast(%s as jsonb)" if column in _JSONB_COLUMNS else "%s"


def _postgres_value(column: str, value: Any) -> Any:
    if column in _JSONB_COLUMNS:
        try:
            return json.dumps(value if value is not None else ([] if column != "recommended_candidate" else {}), ensure_ascii=True, sort_keys=True)
        except (TypeError, ValueError):
            return "{}" if column == "recommended_candidate" else "[]"
    return value


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
        "could not connect",
        "connection timed out",
        "server closed the connection",
        "postgres_driver_missing",
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
    global _FAILED_WRITES, _QUEUED_WRITES
    _FAILED_WRITES += 1
    _QUEUED_WRITES += 1
    return {
        "ok": False,
        "table": table,
        "db_degraded": True,
        "queued": True,
        "reason": _safe_error(exc),
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }


def _runtime_persistence_result(event_type: str, write_result: dict[str, Any], *, critical: bool) -> dict[str, Any]:
    ok = bool(write_result.get("ok"))
    critical_failed = bool(critical and not ok)
    return {
        "ok": ok,
        "event_type": event_type,
        "write": write_result,
        "db_degraded": bool(write_result.get("db_degraded")),
        "queued": bool(write_result.get("queued")),
        "failed_writes": _FAILED_WRITES,
        "queued_writes": _QUEUED_WRITES,
        "critical": bool(critical),
        "critical_persistence_failed": critical_failed,
        "decision": "NO_TRADE" if critical_failed else "",
        "reason": "persistent_intelligence_db_degraded" if critical_failed else "",
        "secrets_printed": False,
        **_safety(),
    }


def _safety_rows(rows: list[Any]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        clean = _sanitize_row(row)
        clean["broker_touched"] = False
        clean["order_executed"] = False
        clean.setdefault("order_policy", "journal_only_no_broker")
        compact.append(clean)
    return compact


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
    for value in (
        os.getenv("DATABASE_URL"),
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_PROJECT_URL"),
        os.getenv("NEXT_PUBLIC_SUPABASE_URL"),
        os.getenv("SUPABASE_SECRET_KEY"),
        os.getenv("SUPABASE_SECRET_KEYS"),
        os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
        os.getenv("SUPABASE_SERVICE_KEY"),
    ):
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
