from __future__ import annotations

import time
from dataclasses import dataclass
from threading import Lock
from typing import Any


DEGRADED_SECONDS = 60.0
QUERY_TIMEOUT_MS = 1500


@dataclass
class MT5DBCircuitBreaker:
    degraded_until: float = 0.0
    last_error: str = ""
    last_error_at: str = ""
    last_duration_ms: int = 0
    tripped_count: int = 0


_STATE = MT5DBCircuitBreaker()
_LOCK = Lock()


def is_db_degraded() -> bool:
    with _LOCK:
        return time.monotonic() < _STATE.degraded_until


def record_db_success(duration_ms: int = 0) -> None:
    with _LOCK:
        _STATE.last_duration_ms = int(duration_ms or 0)


def record_db_error(error: object, *, duration_ms: int = 0) -> None:
    message = _error_message(error)
    if not _is_timeout_error(message, duration_ms):
        return
    with _LOCK:
        _STATE.degraded_until = time.monotonic() + DEGRADED_SECONDS
        _STATE.last_error = message[:500]
        _STATE.last_error_at = _now()
        _STATE.last_duration_ms = int(duration_ms or 0)
        _STATE.tripped_count += 1


def reset_db_circuit_breaker() -> None:
    with _LOCK:
        _STATE.degraded_until = 0.0
        _STATE.last_error = ""
        _STATE.last_error_at = ""
        _STATE.last_duration_ms = 0
        _STATE.tripped_count = 0


def status_payload() -> dict[str, Any]:
    with _LOCK:
        remaining = max(0, int(round(_STATE.degraded_until - time.monotonic())))
        return {
            "db_degraded": remaining > 0,
            "db_degraded_seconds_remaining": remaining,
            "db_last_error": _STATE.last_error,
            "db_last_error_at": _STATE.last_error_at,
            "db_last_duration_ms": _STATE.last_duration_ms,
            "db_tripped_count": _STATE.tripped_count,
        }


def _is_timeout_error(message: str, duration_ms: int) -> bool:
    text = message.casefold()
    if duration_ms and duration_ms > QUERY_TIMEOUT_MS:
        return True
    return any(
        token in text
        for token in (
            "57014",
            "statement timeout",
            "canceling statement due to statement timeout",
            "timeout",
            "timed out",
            "databaseerror",
            "pg8000",
        )
    )


def _error_message(error: object) -> str:
    try:
        return str(error or "")
    except Exception:
        return error.__class__.__name__


def _now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()
