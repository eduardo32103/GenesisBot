from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_persistent_intelligence_apply_schema import (  # noqa: E402
    _connect_with_attempts,
    _database_env_presence,
    _error_category,
    _resolve_connection_target,
    _safe_error,
)


def run_connection_diagnostics(
    *,
    use_public_url: bool = False,
    prefer_public_url: bool = False,
    wait_for_connection: bool = False,
    max_connect_attempts: int = 1,
    connect_backoff_seconds: float = 2.0,
    statement_timeout_ms: int = 30000,
    connect_factory: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    target = _resolve_connection_target(
        database_url=None,
        use_public_url=use_public_url,
        prefer_public_url=prefer_public_url,
    )
    env_presence = _database_env_presence()
    result: dict[str, Any] = {
        "ok": True,
        "provider": "railway_postgres" if target else "none",
        "env_presence": env_presence,
        **env_presence,
        "connection_source": target.get("source") if target else "none",
        "can_parse_url": _can_parse_url(target),
        "can_connect": False,
        "error_category": "",
        "error_message_sanitized": "",
        "connect_attempts": 0,
        "use_public_url": bool(use_public_url),
        "prefer_public_url": bool(prefer_public_url),
        "secrets_printed": False,
        **_safety(),
    }
    if not target:
        result["error_category"] = "missing_database_env"
        result["error_message_sanitized"] = "database_connection_env_not_configured"
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
        result["can_connect"] = True
        return result
    except Exception as exc:
        result["ok"] = False
        result["connect_attempts"] = max(1, int(getattr(exc, "attempts", None) or 0))
        result["error_category"] = _error_category(exc)
        result["error_message_sanitized"] = _safe_error(exc)
        return result
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_connection_diagnostics(
        use_public_url=args.use_public_url,
        prefer_public_url=args.prefer_public_url,
        wait_for_connection=args.wait_for_connection,
        max_connect_attempts=args.max_connect_attempts,
        connect_backoff_seconds=args.connect_backoff_seconds,
        statement_timeout_ms=args.statement_timeout_ms,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0


def _can_parse_url(target: dict[str, str]) -> bool:
    if not target:
        return False
    if target.get("type") == "pg_env":
        return True
    parsed = urlparse(str(target.get("url") or ""))
    return bool(parsed.scheme and parsed.hostname)


def _human_summary(result: dict[str, Any]) -> str:
    return "\n".join(
        [
            "MT5 Persistent DB Connection Diagnostics",
            f"provider={result.get('provider')}",
            f"connection_source={result.get('connection_source')}",
            f"DATABASE_URL_PRESENT={result.get('DATABASE_URL_PRESENT')}",
            f"DATABASE_PUBLIC_URL_PRESENT={result.get('DATABASE_PUBLIC_URL_PRESENT')}",
            f"PGHOST_PRESENT={result.get('PGHOST_PRESENT')}",
            f"PGUSER_PRESENT={result.get('PGUSER_PRESENT')}",
            f"PGPASSWORD_PRESENT={result.get('PGPASSWORD_PRESENT')}",
            f"can_parse_url={result.get('can_parse_url')}",
            f"can_connect={result.get('can_connect')}",
            f"connect_attempts={result.get('connect_attempts')}",
            f"error_category={result.get('error_category')}",
            f"error_message_sanitized={result.get('error_message_sanitized')}",
            f"secrets_printed={result.get('secrets_printed')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
        ]
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose Genesis Persistent DB connectivity without printing secrets.")
    parser.add_argument("--use-public-url", action="store_true")
    parser.add_argument("--prefer-public-url", action="store_true")
    parser.add_argument("--wait-for-connection", action="store_true")
    parser.add_argument("--max-connect-attempts", type=int, default=1)
    parser.add_argument("--connect-backoff-seconds", type=float, default=2.0)
    parser.add_argument("--statement-timeout-ms", type=int, default=30000)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


if __name__ == "__main__":
    raise SystemExit(main())
