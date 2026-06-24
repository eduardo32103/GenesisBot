from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class DatabaseHealthResult:
    ok: bool
    version: str = ""
    error: str = ""


def probe_database_version(get_connection: Callable[[], Any]) -> DatabaseHealthResult:
    try:
        conn = get_connection()
        if not conn:
            return DatabaseHealthResult(ok=False, error="conn es None")

        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        row = cursor.fetchone()
        version = str(row[0]) if row else ""
        return DatabaseHealthResult(ok=True, version=version)
    except Exception as exc:
        return DatabaseHealthResult(ok=False, error=str(exc))
