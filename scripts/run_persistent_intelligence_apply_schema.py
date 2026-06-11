from __future__ import annotations

import argparse
import json
import os
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


def run_apply_schema(
    *,
    apply: bool = False,
    include_rls: bool = False,
    database_url: str | None = None,
    connect_factory: Callable[[str], Any] | None = None,
    wait_for_connection: bool = False,
    max_connect_attempts: int = 10,
    connect_backoff_seconds: float = 5.0,
) -> dict[str, Any]:
    db_url = str(database_url if database_url is not None else os.getenv("DATABASE_URL") or "")
    result: dict[str, Any] = {
        "ok": True,
        "provider": "railway_postgres" if db_url else "none",
        "db_available": False,
        "schema_sql_ready": False,
        "dry_run": not bool(apply),
        "applied": False,
        "include_rls": bool(include_rls),
        "connect_attempts": 0,
        "wait_for_connection": bool(wait_for_connection),
        "max_connect_attempts": int(max_connect_attempts or 1),
        "connect_backoff_seconds": float(connect_backoff_seconds or 0),
        "tables_before": [],
        "missing_tables_before": list(REQUIRED_TABLES),
        "tables_after": [],
        "missing_tables_after": list(REQUIRED_TABLES),
        "tables_ready": False,
        "recommendation": "configure_database_env" if not db_url else "dry_run_review_schema_then_apply",
        "secrets_printed": False,
        **_safety(),
    }
    sql = get_persistent_intelligence_schema_sql(include_rls=include_rls)
    result["schema_sql_ready"] = _schema_sql_is_safe(sql)
    if not db_url or not result["schema_sql_ready"]:
        if not result["schema_sql_ready"]:
            result["ok"] = False
            result["recommendation"] = "repair_schema_sql_before_apply"
        return result

    connection = None
    try:
        connection, attempts = _connect_with_attempts(
            db_url,
            connect_factory=connect_factory,
            wait_for_connection=wait_for_connection,
            max_connect_attempts=max_connect_attempts,
            connect_backoff_seconds=connect_backoff_seconds,
        )
        result["connect_attempts"] = attempts
        result["db_available"] = True
        before = _list_ready_tables(connection)
        result["tables_before"] = before
        result["missing_tables_before"] = _missing_tables(before)
        if apply:
            _execute_schema(connection, sql)
            result["applied"] = True
        after = _list_ready_tables(connection)
        result["tables_after"] = after
        result["missing_tables_after"] = _missing_tables(after)
        result["tables_ready"] = not result["missing_tables_after"]
        if result["tables_ready"]:
            result["recommendation"] = "persistent_intelligence_ready"
        elif apply:
            result["recommendation"] = "verify_schema_apply_permissions"
        else:
            result["recommendation"] = "run_apply_schema_with_apply"
        return result
    except Exception as exc:
        result["ok"] = False
        result["db_available"] = False
        result["error_category"] = _error_category(exc)
        result["reason"] = _safe_error(exc)
        result["recommendation"] = "verify_database_connection"
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
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok", True) else 1


def _execute_schema(connection: Any, sql: str) -> None:
    cursor = connection.cursor()
    try:
        for statement in _sql_statements(sql):
            cursor.execute(statement)
        connection.commit()
    except Exception:
        try:
            connection.rollback()
        except Exception:
            pass
        raise


def _connect_with_attempts(
    database_url: str,
    *,
    connect_factory: Callable[[str], Any] | None,
    wait_for_connection: bool,
    max_connect_attempts: int,
    connect_backoff_seconds: float,
) -> tuple[Any, int]:
    attempts = max(1, int(max_connect_attempts or 1)) if wait_for_connection else 1
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            connection = connect_factory(database_url) if connect_factory else _connect_pg8000(database_url)
            return connection, attempt
        except Exception as exc:
            last_error = exc
            if not wait_for_connection or attempt >= attempts:
                break
            time.sleep(max(0.1, float(connect_backoff_seconds or 0.1)))
    if last_error is not None:
        raise SchemaConnectionError(last_error, attempts=attempts) from last_error
    raise RuntimeError("database_connection_unavailable")


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
    return [part.strip() for part in sql.split(";") if part.strip()]


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


def _human_summary(result: dict[str, Any]) -> str:
    return "\n".join(
        [
            "MT5 Persistent Intelligence Apply Schema",
            f"provider={result.get('provider')}",
            f"db_available={result.get('db_available')}",
            f"schema_sql_ready={result.get('schema_sql_ready')}",
            f"dry_run={result.get('dry_run')}",
            f"applied={result.get('applied')}",
            f"include_rls={result.get('include_rls')}",
            f"connect_attempts={result.get('connect_attempts')}",
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
    parser.add_argument("--json", action="store_true")
    parser.set_defaults(include_rls=False)
    return parser.parse_args(argv)


def _safe_error(exc: object) -> str:
    text = str(exc or exc.__class__.__name__)
    for value in (os.getenv("DATABASE_URL"),):
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
