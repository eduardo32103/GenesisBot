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
    use_public_url: bool = False,
    prefer_public_url: bool = False,
    statement_timeout_ms: int = 30000,
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
        "db_available": False,
        "schema_sql_ready": False,
        "dry_run": not bool(apply),
        "applied": False,
        "include_rls": bool(include_rls),
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
        "recommendation": "configure_database_env" if not target else "dry_run_review_schema_then_apply",
        "secrets_printed": False,
        **_safety(),
    }
    sql = get_persistent_intelligence_schema_sql(include_rls=include_rls)
    result["schema_sql_ready"] = _schema_sql_is_safe(sql)
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
        if connect_factory is None:
            _set_statement_timeout(connection, statement_timeout_ms)
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
        use_public_url=args.use_public_url,
        prefer_public_url=args.prefer_public_url,
        statement_timeout_ms=args.statement_timeout_ms,
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


def _set_statement_timeout(connection: Any, statement_timeout_ms: int) -> None:
    try:
        cursor = connection.cursor()
        cursor.execute("set statement_timeout to %s", (max(1000, int(statement_timeout_ms or 30000)),))
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
            f"db_available={result.get('db_available')}",
            f"schema_sql_ready={result.get('schema_sql_ready')}",
            f"dry_run={result.get('dry_run')}",
            f"applied={result.get('applied')}",
            f"include_rls={result.get('include_rls')}",
            f"use_public_url={result.get('use_public_url')}",
            f"prefer_public_url={result.get('prefer_public_url')}",
            f"connection_source={result.get('connection_source')}",
            f"DATABASE_URL_PRESENT={result.get('DATABASE_URL_PRESENT')}",
            f"DATABASE_PUBLIC_URL_PRESENT={result.get('DATABASE_PUBLIC_URL_PRESENT')}",
            f"PGHOST_PRESENT={result.get('PGHOST_PRESENT')}",
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
    parser.add_argument("--use-public-url", action="store_true", help="Use DATABASE_PUBLIC_URL only.")
    parser.add_argument("--prefer-public-url", action="store_true", help="Prefer DATABASE_PUBLIC_URL when present, otherwise fall back.")
    parser.add_argument("--statement-timeout-ms", type=int, default=30000)
    parser.add_argument("--json", action="store_true")
    parser.set_defaults(include_rls=False)
    return parser.parse_args(argv)


def _safe_error(exc: object) -> str:
    text = str(exc or exc.__class__.__name__)
    for value in (
        os.getenv("DATABASE_URL"),
        os.getenv("DATABASE_PUBLIC_URL"),
        os.getenv("PGPASSWORD"),
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
