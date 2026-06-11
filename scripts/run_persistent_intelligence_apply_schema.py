from __future__ import annotations

import argparse
import json
import os
import re
import ssl
import sys
import time
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_persistent_schema import (  # noqa: E402
    REQUIRED_TABLES,
    get_persistent_intelligence_schema_sql,
)


class SchemaConnectionError(RuntimeError):
    def __init__(self, original: Exception, *, attempts: int) -> None:
        super().__init__(str(original))
        self.original = original
        self.attempts = attempts


class SchemaApplyError(RuntimeError):
    def __init__(
        self,
        original: Exception,
        *,
        statement_index: int,
        statement_kind: str,
        applied_count: int,
        redaction_values: tuple[str, ...] = (),
    ) -> None:
        super().__init__(str(original))
        self.original = original
        self.statement_index = int(statement_index)
        self.statement_kind = str(statement_kind or "unknown")
        self.applied_count = int(applied_count)
        self.error_category = _statement_error_category(self.statement_kind, original)
        self.sanitized_error = _safe_error(original, extra_secrets=redaction_values)


def run_apply_schema(
    *,
    apply: bool = False,
    include_rls: bool = False,
    database_url: str | None = None,
    connect_factory: Callable[[str], Any] | None = None,
    wait_for_connection: bool = False,
    max_connect_attempts: int = 10,
    connect_backoff_seconds: float = 5.0,
    use_public_url: bool = False,
    prefer_public_url: bool = False,
    statement_timeout_ms: int = 30000,
    verbose_sanitized: bool = False,
) -> dict[str, Any]:
    env_presence = _database_env_presence()
    target = _resolve_connection_target(
        database_url=database_url,
        use_public_url=use_public_url,
        prefer_public_url=prefer_public_url,
    )
    result: dict[str, Any] = {
        "ok": True,
        "provider": "railway_postgres" if target else "none",
        "selected_provider": "railway_postgres" if target else "none",
        "db_available": False,
        "can_connect": False,
        "schema_sql_ready": False,
        "dry_run": not bool(apply),
        "applied": False,
        "include_rls": bool(include_rls),
        "verbose_sanitized": bool(verbose_sanitized),
        "use_public_url": bool(use_public_url),
        "prefer_public_url": bool(prefer_public_url),
        "statement_timeout_ms": int(statement_timeout_ms or 30000),
        "connect_attempts": 0,
        "wait_for_connection": bool(wait_for_connection),
        "max_connect_attempts": int(max_connect_attempts or 1),
        "connect_backoff_seconds": float(connect_backoff_seconds or 0),
        "connection_source": target.get("source") if target else "none",
        "env_presence": env_presence,
        **env_presence,
        "tables_before": [],
        "missing_tables_before": list(REQUIRED_TABLES),
        "tables_after": [],
        "missing_tables_after": list(REQUIRED_TABLES),
        "tables_ready": False,
        "statement_count": 0,
        "statements_applied": 0,
        "statements_failed": 0,
        "first_failed_statement_index": 0,
        "first_failed_statement_kind": "",
        "first_failed_error_sanitized": "",
        "apply_failed_reason": "",
        "error_category": "",
        "recommendation": "configure_database_env" if not target else "dry_run_review_schema_then_apply",
        "secrets_printed": False,
        **_safety(),
    }
    sql = get_persistent_intelligence_schema_sql(include_rls=include_rls)
    result["schema_sql_ready"] = _schema_sql_is_safe(sql)
    statements = _sql_statements(sql)
    result["statement_count"] = len(statements)
    if not target or not result["schema_sql_ready"]:
        if not result["schema_sql_ready"]:
            result["ok"] = False
            result["recommendation"] = "repair_schema_sql_before_apply"
        return result

    connection = None
    try:
        connection, attempts = _connect_with_attempts(
            target,
            connect_factory=connect_factory,
            wait_for_connection=wait_for_connection,
            max_connect_attempts=max_connect_attempts,
            connect_backoff_seconds=connect_backoff_seconds,
            statement_timeout_ms=statement_timeout_ms,
        )
        result["connect_attempts"] = attempts
        result["db_available"] = True
        result["can_connect"] = True
        if connect_factory is None:
            _set_statement_timeout(connection, statement_timeout_ms)
        before = _list_ready_tables(connection)
        result["tables_before"] = before
        result["missing_tables_before"] = _missing_tables(before)
        if apply:
            apply_stats = _execute_schema(connection, statements, redaction_values=_target_redaction_values(target))
            result.update(apply_stats)
            result["applied"] = True
        after = _list_ready_tables(connection)
        result["tables_after"] = after
        result["missing_tables_after"] = _missing_tables(after)
        result["tables_ready"] = not result["missing_tables_after"]
        if result["tables_ready"]:
            result["recommendation"] = "persistent_intelligence_ready"
        elif apply:
            result["recommendation"] = "verify_schema_apply_permissions"
            result["apply_failed_reason"] = "tables_missing_after_schema_apply"
        else:
            result["recommendation"] = "run_apply_schema_with_apply"
        return result
    except SchemaApplyError as exc:
        result["ok"] = False
        result["db_available"] = True
        result["can_connect"] = True
        result["applied"] = False
        result["statements_applied"] = exc.applied_count
        result["statements_failed"] = 1
        result["first_failed_statement_index"] = exc.statement_index
        result["first_failed_statement_kind"] = exc.statement_kind
        result["first_failed_error_sanitized"] = exc.sanitized_error
        result["error_category"] = exc.error_category
        result["reason"] = exc.sanitized_error
        result["apply_failed_reason"] = exc.sanitized_error
        result["recommendation"] = "review_pgcrypto_extension_permissions" if exc.error_category == "extension_permission_error" else "repair_schema_statement_or_permissions"
        if connection is not None:
            try:
                after = _list_ready_tables(connection)
                result["tables_after"] = after
                result["missing_tables_after"] = _missing_tables(after)
                result["tables_ready"] = not result["missing_tables_after"]
            except Exception:
                pass
        return result
    except Exception as exc:
        connected_before_error = bool(connection is not None or result.get("db_available"))
        result["ok"] = False
        result["db_available"] = connected_before_error
        result["can_connect"] = connected_before_error
        result["error_category"] = _error_category(exc)
        result["reason"] = _safe_error(exc, extra_secrets=_target_redaction_values(target))
        result["apply_failed_reason"] = _safe_error(exc, extra_secrets=_target_redaction_values(target)) if connected_before_error and apply else ""
        result["recommendation"] = "review_database_table_probe_or_permissions" if connected_before_error else "verify_database_connection"
        result["connect_attempts"] = max(1, int(getattr(exc, "attempts", None) or result.get("connect_attempts") or 0))
        return result
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_apply_schema(
        apply=args.apply,
        include_rls=args.include_rls,
        wait_for_connection=args.wait_for_connection,
        max_connect_attempts=args.max_connect_attempts,
        connect_backoff_seconds=args.connect_backoff_seconds,
        use_public_url=args.use_public_url,
        prefer_public_url=args.prefer_public_url,
        statement_timeout_ms=args.statement_timeout_ms,
        verbose_sanitized=args.verbose_sanitized,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok", True) else 1


def _execute_schema(connection: Any, sql_or_statements: str | list[str], *, redaction_values: tuple[str, ...] = ()) -> dict[str, int]:
    statements = _sql_statements(sql_or_statements) if isinstance(sql_or_statements, str) else list(sql_or_statements)
    cursor = connection.cursor()
    applied = 0
    for index, statement in enumerate(statements, start=1):
        statement_kind = _statement_kind(statement)
        try:
            cursor.execute(statement)
            connection.commit()
            applied += 1
        except Exception as exc:
            try:
                connection.rollback()
            except Exception:
                pass
            raise SchemaApplyError(
                exc,
                statement_index=index,
                statement_kind=statement_kind,
                applied_count=applied,
                redaction_values=redaction_values,
            ) from exc
    return {
        "statement_count": len(statements),
        "statements_applied": applied,
        "statements_failed": 0,
    }


def _connect_with_attempts(
    target: dict[str, str],
    *,
    connect_factory: Callable[[str], Any] | None,
    wait_for_connection: bool,
    max_connect_attempts: int,
    connect_backoff_seconds: float,
    statement_timeout_ms: int,
) -> tuple[Any, int]:
    attempts = max(1, int(max_connect_attempts or 1)) if wait_for_connection else 1
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            connection = connect_factory(target.get("connect_arg") or "") if connect_factory else _connect_target(target, statement_timeout_ms=statement_timeout_ms)
            return connection, attempt
        except Exception as exc:
            last_error = exc
            if not wait_for_connection or attempt >= attempts:
                break
            time.sleep(max(0.1, float(connect_backoff_seconds or 0.1)))
    if last_error is not None:
        raise SchemaConnectionError(last_error, attempts=attempts) from last_error
    raise RuntimeError("database_connection_unavailable")


def _resolve_connection_target(
    *,
    database_url: str | None,
    use_public_url: bool,
    prefer_public_url: bool,
) -> dict[str, str]:
    explicit = str(database_url or "").strip()
    if explicit:
        return {"type": "url", "source": "explicit_database_url", "connect_arg": explicit, "url": explicit}

    private_url = str(os.getenv("DATABASE_URL") or "").strip()
    public_url = str(os.getenv("DATABASE_PUBLIC_URL") or "").strip()
    if use_public_url:
        return {"type": "url", "source": "DATABASE_PUBLIC_URL", "connect_arg": public_url, "url": public_url} if public_url else {}
    if prefer_public_url and public_url:
        return {"type": "url", "source": "DATABASE_PUBLIC_URL", "connect_arg": public_url, "url": public_url}
    if private_url:
        return {"type": "url", "source": "DATABASE_URL", "connect_arg": private_url, "url": private_url}
    if public_url:
        return {"type": "url", "source": "DATABASE_PUBLIC_URL", "connect_arg": public_url, "url": public_url}
    if _pg_env_configured():
        return {"type": "pg_env", "source": "PGHOST", "connect_arg": "PGHOST_ENV"}
    return {}


def _database_env_presence() -> dict[str, bool]:
    return {
        "DATABASE_URL_PRESENT": bool(os.getenv("DATABASE_URL")),
        "DATABASE_PUBLIC_URL_PRESENT": bool(os.getenv("DATABASE_PUBLIC_URL")),
        "PGHOST_PRESENT": bool(os.getenv("PGHOST")),
        "PGPORT_PRESENT": bool(os.getenv("PGPORT")),
        "PGDATABASE_PRESENT": bool(os.getenv("PGDATABASE")),
        "PGUSER_PRESENT": bool(os.getenv("PGUSER")),
        "PGPASSWORD_PRESENT": bool(os.getenv("PGPASSWORD")),
    }


def _pg_env_configured() -> bool:
    return bool(os.getenv("PGHOST") and os.getenv("PGUSER") and os.getenv("PGDATABASE"))


def _connect_target(target: dict[str, str], *, statement_timeout_ms: int) -> Any:
    del statement_timeout_ms
    if target.get("type") == "pg_env":
        return _connect_pg8000_from_env()
    return _connect_pg8000(str(target.get("url") or ""))


def _target_redaction_values(target: dict[str, str]) -> tuple[str, ...]:
    values: list[str] = []
    for key in ("url", "connect_arg"):
        value = str(target.get(key) or "")
        if value and value not in values and value != "PGHOST_ENV":
            values.append(value)
        if value.startswith(("postgres://", "postgresql://")):
            parsed = urlparse(value)
            if parsed.username:
                values.append(unquote(parsed.username))
            if parsed.password:
                values.append(unquote(parsed.password))
    return tuple(value for value in values if value)


def _set_statement_timeout(connection: Any, statement_timeout_ms: int) -> None:
    try:
        cursor = connection.cursor()
        timeout_ms = max(1000, int(statement_timeout_ms or 30000))
        cursor.execute(f"set statement_timeout to {timeout_ms}")
        connection.commit()
    except Exception:
        try:
            connection.rollback()
        except Exception:
            pass


def _list_ready_tables(connection: Any) -> list[str]:
    cursor = connection.cursor()
    cursor.execute(
        """
        select table_name
        from information_schema.tables
        where table_schema = 'public'
          and table_name in (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        tuple(REQUIRED_TABLES),
    )
    return sorted(str(row[0]) for row in cursor.fetchall())


def _missing_tables(ready_tables: list[str]) -> list[str]:
    ready = set(ready_tables)
    return [table for table in REQUIRED_TABLES if table not in ready]


def _schema_sql_is_safe(sql: str) -> bool:
    lowered = f" {sql.casefold()} "
    forbidden = (" drop ", " drop\n", " truncate ", " delete from ")
    return bool(sql.strip()) and not any(token in lowered for token in forbidden)


def _sql_statements(sql: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    quote: str | None = None
    dollar_tag = ""
    i = 0
    while i < len(sql):
        char = sql[i]
        next_char = sql[i + 1] if i + 1 < len(sql) else ""
        if dollar_tag:
            if sql.startswith(dollar_tag, i):
                buffer.append(dollar_tag)
                i += len(dollar_tag)
                dollar_tag = ""
                continue
            buffer.append(char)
            i += 1
            continue
        if quote == "'":
            buffer.append(char)
            if char == "'" and next_char == "'":
                buffer.append(next_char)
                i += 2
                continue
            if char == "'":
                quote = None
            i += 1
            continue
        if quote == '"':
            buffer.append(char)
            if char == '"' and next_char == '"':
                buffer.append(next_char)
                i += 2
                continue
            if char == '"':
                quote = None
            i += 1
            continue
        if char == "-" and next_char == "-":
            i = sql.find("\n", i + 2)
            if i == -1:
                break
            buffer.append("\n")
            i += 1
            continue
        if char == "/" and next_char == "*":
            end = sql.find("*/", i + 2)
            i = len(sql) if end == -1 else end + 2
            buffer.append(" ")
            continue
        if char == "'":
            quote = "'"
            buffer.append(char)
            i += 1
            continue
        if char == '"':
            quote = '"'
            buffer.append(char)
            i += 1
            continue
        if char == "$":
            match = re.match(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$", sql[i:])
            if match:
                dollar_tag = match.group(0)
                buffer.append(dollar_tag)
                i += len(dollar_tag)
                continue
        if char == ";":
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            i += 1
            continue
        buffer.append(char)
        i += 1
    statement = "".join(buffer).strip()
    if statement:
        statements.append(statement)
    return statements


def _statement_kind(statement: str) -> str:
    normalized = re.sub(r"\s+", " ", statement.strip().casefold())
    if normalized.startswith("create extension"):
        return "create_extension"
    if normalized.startswith("create table"):
        return "create_table"
    if normalized.startswith("create index"):
        return "create_index"
    if normalized.startswith("alter table"):
        return "alter_table"
    if normalized.startswith("set "):
        return "set"
    return (normalized.split(" ", 1)[0] if normalized else "unknown") or "unknown"


def _statement_error_category(statement_kind: str, exc: object) -> str:
    text = str(exc or "").casefold()
    if statement_kind == "create_extension" and any(token in text for token in ("permission", "denied", "privilege", "not owner")):
        return "extension_permission_error"
    if statement_kind == "create_extension":
        return "extension_apply_error"
    return _error_category(exc)


def _connect_pg8000(database_url: str) -> Any:
    import pg8000.dbapi

    parsed = urlparse(database_url)
    query = parse_qs(parsed.query or "")
    sslmode = str((query.get("sslmode") or [""])[0]).casefold()
    ssl_context = ssl.create_default_context() if sslmode in {"require", "verify-ca", "verify-full"} else None
    return pg8000.dbapi.connect(
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or "") if parsed.password else None,
        host=parsed.hostname or "localhost",
        port=int(parsed.port or 5432),
        database=(parsed.path or "/").lstrip("/") or None,
        timeout=float(os.getenv("PERSISTENT_DB_CONNECT_TIMEOUT_SEC") or 5.0),
        ssl_context=ssl_context,
        application_name="GenesisPersistentSchemaApply",
    )


def _connect_pg8000_from_env() -> Any:
    import pg8000.dbapi

    sslmode = str(os.getenv("PGSSLMODE") or os.getenv("PGSSL") or "").casefold()
    ssl_context = ssl.create_default_context() if sslmode in {"require", "verify-ca", "verify-full", "true", "1"} else None
    return pg8000.dbapi.connect(
        user=os.getenv("PGUSER") or "",
        password=os.getenv("PGPASSWORD") or None,
        host=os.getenv("PGHOST") or "localhost",
        port=int(os.getenv("PGPORT") or 5432),
        database=os.getenv("PGDATABASE") or None,
        timeout=float(os.getenv("PERSISTENT_DB_CONNECT_TIMEOUT_SEC") or 5.0),
        ssl_context=ssl_context,
        application_name="GenesisPersistentSchemaApply",
    )


def _human_summary(result: dict[str, Any]) -> str:
    return "\n".join(
        [
            "MT5 Persistent Intelligence Apply Schema",
            f"provider={result.get('provider')}",
            f"selected_provider={result.get('selected_provider')}",
            f"db_available={result.get('db_available')}",
            f"can_connect={result.get('can_connect')}",
            f"schema_sql_ready={result.get('schema_sql_ready')}",
            f"dry_run={result.get('dry_run')}",
            f"applied={result.get('applied')}",
            f"include_rls={result.get('include_rls')}",
            f"verbose_sanitized={result.get('verbose_sanitized')}",
            f"use_public_url={result.get('use_public_url')}",
            f"prefer_public_url={result.get('prefer_public_url')}",
            f"connection_source={result.get('connection_source')}",
            f"DATABASE_URL_PRESENT={result.get('DATABASE_URL_PRESENT')}",
            f"DATABASE_PUBLIC_URL_PRESENT={result.get('DATABASE_PUBLIC_URL_PRESENT')}",
            f"PGHOST_PRESENT={result.get('PGHOST_PRESENT')}",
            f"connect_attempts={result.get('connect_attempts')}",
            f"statement_count={result.get('statement_count')}",
            f"statements_applied={result.get('statements_applied')}",
            f"statements_failed={result.get('statements_failed')}",
            f"first_failed_statement_index={result.get('first_failed_statement_index')}",
            f"first_failed_statement_kind={result.get('first_failed_statement_kind')}",
            f"first_failed_error_sanitized={result.get('first_failed_error_sanitized')}",
            f"apply_failed_reason={result.get('apply_failed_reason')}",
            f"error_category={result.get('error_category')}",
            f"tables_before={','.join(result.get('tables_before') or [])}",
            f"missing_tables_before={','.join(result.get('missing_tables_before') or [])}",
            f"tables_after={','.join(result.get('tables_after') or [])}",
            f"missing_tables_after={','.join(result.get('missing_tables_after') or [])}",
            f"tables_ready={result.get('tables_ready')}",
            f"recommendation={result.get('recommendation')}",
            f"secrets_printed={result.get('secrets_printed')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
        ]
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare or apply the Genesis MT5 Persistent Intelligence schema safely.")
    parser.add_argument("--apply", action="store_true", help="Apply the idempotent schema. Default is dry-run.")
    parser.add_argument("--no-rls", dest="include_rls", action="store_false", help="Do not include RLS statements. Default.")
    parser.add_argument("--include-rls", dest="include_rls", action="store_true", help="Include optional RLS statements.")
    parser.add_argument("--wait-for-connection", action="store_true", help="Retry transient connection failures before giving up.")
    parser.add_argument("--max-connect-attempts", type=int, default=10)
    parser.add_argument("--connect-backoff-seconds", type=float, default=5.0)
    parser.add_argument("--use-public-url", action="store_true", help="Use DATABASE_PUBLIC_URL only.")
    parser.add_argument("--prefer-public-url", action="store_true", help="Prefer DATABASE_PUBLIC_URL when present, otherwise fall back.")
    parser.add_argument("--statement-timeout-ms", type=int, default=30000)
    parser.add_argument("--verbose-sanitized", action="store_true", help="Show detailed sanitized apply diagnostics without SQL or secrets.")
    parser.add_argument("--json", action="store_true")
    parser.set_defaults(include_rls=False)
    return parser.parse_args(argv)


def _safe_error(exc: object, *, extra_secrets: tuple[str, ...] = ()) -> str:
    text = str(exc or exc.__class__.__name__)
    for value in (
        os.getenv("DATABASE_URL"),
        os.getenv("DATABASE_PUBLIC_URL"),
        os.getenv("PGPASSWORD"),
        *extra_secrets,
    ):
        if value:
            text = text.replace(value, "[redacted]")
    parsed_words = []
    for value in (os.getenv("DATABASE_URL"), os.getenv("DATABASE_PUBLIC_URL")):
        if value:
            parsed = urlparse(value)
            if parsed.password:
                parsed_words.append(unquote(parsed.password))
            if parsed.username:
                parsed_words.append(unquote(parsed.username))
    for value in parsed_words:
        if value:
            text = text.replace(value, "[redacted]")
    return text[:500]


def _error_category(exc: object) -> str:
    text = str(exc or "").casefold()
    if "max clients" in text or "too many clients" in text or "pool_size" in text:
        return "max_connections"
    if "timeout" in text or "timed out" in text:
        return "timeout"
    if "permission" in text or "denied" in text:
        return "permission"
    return "connection_or_schema_error"


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


if __name__ == "__main__":
    raise SystemExit(main())
