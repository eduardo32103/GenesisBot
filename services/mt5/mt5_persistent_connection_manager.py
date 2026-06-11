from __future__ import annotations

import hashlib
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable


DEFAULT_POOL_MAX_SIZE = 1
DEFAULT_CONNECT_TIMEOUT_SECONDS = 5.0
DEFAULT_WRITE_TIMEOUT_SECONDS = 5.0
DEFAULT_QUEUE_MAX_SIZE = 100
DEFAULT_DEDUPE_WINDOW_SECONDS = 60.0


class PersistentDbBackpressureError(RuntimeError):
    def __init__(self, reason: str, *, error_category: str = "backpressure") -> None:
        super().__init__(reason)
        self.error_category = error_category


class PersistentWriteBackpressure:
    def __init__(
        self,
        *,
        pool_max_size: int = DEFAULT_POOL_MAX_SIZE,
        write_timeout_seconds: float = DEFAULT_WRITE_TIMEOUT_SECONDS,
        queue_max_size: int = DEFAULT_QUEUE_MAX_SIZE,
        dedupe_window_seconds: float = DEFAULT_DEDUPE_WINDOW_SECONDS,
    ) -> None:
        self.pool_max_size = max(1, int(pool_max_size or DEFAULT_POOL_MAX_SIZE))
        self.write_timeout_seconds = max(0.05, float(write_timeout_seconds or DEFAULT_WRITE_TIMEOUT_SECONDS))
        self.queue_max_size = max(0, int(queue_max_size if queue_max_size is not None else DEFAULT_QUEUE_MAX_SIZE))
        self.dedupe_window_seconds = max(1.0, float(dedupe_window_seconds or DEFAULT_DEDUPE_WINDOW_SECONDS))
        self._write_slots = threading.BoundedSemaphore(self.pool_max_size)
        self._lock = threading.Lock()
        self._queue: deque[dict[str, Any]] = deque()
        self._dedupe_seen: dict[tuple[Any, ...], float] = {}
        self._in_use = 0
        self.failed_writes = 0
        self.queued_writes = 0
        self.dropped_noncritical_writes = 0
        self.suppressed_duplicate_events = 0
        self.last_db_error_category = ""
        self.last_db_error = ""
        self.last_db_error_at = ""
        self._last_db_error_monotonic = 0.0
        self._backoff_until = 0.0
        self._backoff_seconds = 0.0

    @classmethod
    def from_env(cls) -> "PersistentWriteBackpressure":
        return cls(
            pool_max_size=_env_int("PERSISTENT_DB_POOL_MAX_SIZE", DEFAULT_POOL_MAX_SIZE),
            write_timeout_seconds=_env_float("PERSISTENT_DB_WRITE_TIMEOUT_SEC", DEFAULT_WRITE_TIMEOUT_SECONDS),
            queue_max_size=_env_int("PERSISTENT_DB_QUEUE_MAX_SIZE", DEFAULT_QUEUE_MAX_SIZE),
            dedupe_window_seconds=_env_float("PERSISTENT_DB_DEDUPE_WINDOW_SEC", DEFAULT_DEDUPE_WINDOW_SECONDS),
        )

    def reset(self) -> None:
        with self._lock:
            self._queue.clear()
            self._dedupe_seen.clear()
            self._in_use = 0
            self.failed_writes = 0
            self.queued_writes = 0
            self.dropped_noncritical_writes = 0
            self.suppressed_duplicate_events = 0
            self.last_db_error_category = ""
            self.last_db_error = ""
            self.last_db_error_at = ""
            self._last_db_error_monotonic = 0.0
            self._backoff_until = 0.0
            self._backoff_seconds = 0.0
        self._write_slots = threading.BoundedSemaphore(self.pool_max_size)

    def begin_write(self, table: str, row: dict[str, Any], *, critical: bool = False) -> dict[str, Any]:
        now = time.monotonic()
        if not critical and self._is_duplicate(table, row, now):
            with self._lock:
                self.suppressed_duplicate_events += 1
            return {
                "short_circuit": True,
                "result": {
                    "ok": True,
                    "table": table,
                    "db_degraded": False,
                    "queued": False,
                    "skipped": True,
                    "suppressed_duplicate": True,
                    "reason": "duplicate_event_coalesced",
                    **_safety(),
                },
            }
        if self.backoff_active():
            return {
                "short_circuit": True,
                "result": self.queue_or_drop(
                    table,
                    row,
                    critical=critical,
                    reason="persistent_db_backoff_active",
                    error_category=self.last_db_error_category or "backoff",
                ),
            }
        if not self._write_slots.acquire(timeout=self.write_timeout_seconds):
            return {
                "short_circuit": True,
                "result": self.queue_or_drop(
                    table,
                    row,
                    critical=critical,
                    reason="persistent_db_write_semaphore_timeout",
                    error_category="pool_exhausted",
                ),
            }
        token = object()
        with self._lock:
            self._in_use += 1
        return {"short_circuit": False, "token": token}

    def end_write(self, token: object | None = None) -> None:
        del token
        with self._lock:
            self._in_use = max(0, self._in_use - 1)
        try:
            self._write_slots.release()
        except ValueError:
            pass

    def record_unavailable(self, table: str, row: dict[str, Any], *, critical: bool, reason: str) -> dict[str, Any]:
        return self.queue_or_drop(table, row, critical=critical, reason=reason, error_category=classify_db_error(reason))

    def record_failure(
        self,
        table: str,
        row: dict[str, Any],
        *,
        critical: bool,
        reason: str,
        duration_ms: int,
    ) -> dict[str, Any]:
        category = classify_db_error(reason)
        if category in {"missing_schema", "missing_table"}:
            result = self.record_schema_missing_freeze(table, row, critical=critical)
            result["duration_ms"] = duration_ms
            return result
        with self._lock:
            self.failed_writes += 1
            self._record_error_locked(category, reason)
            if category in {"max_connections", "pool_exhausted"}:
                self._backoff_seconds = min(60.0, max(2.0, self._backoff_seconds * 2.0 or 2.0))
                self._backoff_until = time.monotonic() + self._backoff_seconds
        result = self.queue_or_drop(table, row, critical=critical, reason=reason, error_category=category)
        result["duration_ms"] = duration_ms
        return result

    def activate_schema_missing_freeze(self, *, reason: str = "schema_missing_write_freeze") -> dict[str, Any]:
        with self._lock:
            cleared = len(self._queue)
            self._queue.clear()
            self._record_error_locked("missing_schema", str(reason or "schema_missing_write_freeze"))
            self._backoff_until = 0.0
            self._backoff_seconds = 0.0
            queue_depth = len(self._queue)
        return {
            "ok": True,
            "schema_missing_write_freeze": True,
            "cleared_queued_writes": cleared,
            "queue_depth": queue_depth,
            "queue_max_size": self.queue_max_size,
            **_safety(),
        }

    def record_schema_missing_freeze(self, table: str, row: dict[str, Any], *, critical: bool) -> dict[str, Any]:
        del row
        with self._lock:
            self._record_error_locked("missing_schema", "schema_missing_write_freeze")
            if not critical:
                self.dropped_noncritical_writes += 1
            queue_depth = len(self._queue)
        return {
            "ok": False,
            "table": table,
            "db_degraded": True,
            "queued": False,
            "schema_missing_write_freeze": True,
            "reason": "schema_missing_write_freeze",
            "error_category": "missing_schema",
            "queue_depth": queue_depth,
            "queue_max_size": self.queue_max_size,
            **_safety(),
        }

    def clear_schema_missing(self) -> None:
        with self._lock:
            if self.last_db_error_category == "missing_schema":
                self.last_db_error_category = ""
                self.last_db_error = ""
                self.last_db_error_at = ""
                self._last_db_error_monotonic = 0.0

    def queue_or_drop(
        self,
        table: str,
        row: dict[str, Any],
        *,
        critical: bool,
        reason: str,
        error_category: str,
    ) -> dict[str, Any]:
        with self._lock:
            self._record_error_locked(error_category or self.last_db_error_category, str(reason or ""))
            if self.queue_max_size <= 0 or len(self._queue) >= self.queue_max_size:
                if critical:
                    return {
                        "ok": False,
                        "table": table,
                        "db_degraded": True,
                        "queued": False,
                        "queue_full": True,
                        "reason": str(reason or "persistent_db_queue_full")[:500],
                        "error_category": error_category,
                        "queue_depth": len(self._queue),
                        "queue_max_size": self.queue_max_size,
                        **_safety(),
                    }
                self.dropped_noncritical_writes += 1
                return {
                    "ok": False,
                    "table": table,
                    "db_degraded": True,
                    "queued": False,
                    "dropped_noncritical_write": True,
                    "reason": str(reason or "persistent_db_queue_full")[:500],
                    "error_category": error_category,
                    "queue_depth": len(self._queue),
                    "queue_max_size": self.queue_max_size,
                    **_safety(),
                }
            self._queue.append(
                {
                    "table": table,
                    "critical": bool(critical),
                    "reason": str(reason or "")[:500],
                    "error_category": error_category,
                    "queued_at": _now_monotonic(),
                    "row": _compact_queued_row(row),
                }
            )
            self.queued_writes += 1
            return {
                "ok": False,
                "table": table,
                "db_degraded": True,
                "queued": True,
                "reason": str(reason or "")[:500],
                "error_category": error_category,
                "queue_depth": len(self._queue),
                "queue_max_size": self.queue_max_size,
                **_safety(),
            }

    def backoff_active(self) -> bool:
        return time.monotonic() < self._backoff_until

    def status(self, *, pool_status: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            queue_depth = len(self._queue)
            in_use = self._in_use
            failed = self.failed_writes
            queued = self.queued_writes
            dropped = self.dropped_noncritical_writes
            suppressed = self.suppressed_duplicate_events
            category = self.last_db_error_category
            last_error_at = self.last_db_error_at
            last_error_age = max(0.0, time.monotonic() - self._last_db_error_monotonic) if self._last_db_error_monotonic else None
            backoff_remaining = max(0.0, self._backoff_until - time.monotonic())
        pool = dict(pool_status or {})
        return {
            "pool_enabled": True,
            "pool_max_size": int(pool.get("pool_max_size") or self.pool_max_size),
            "pool_in_use": int(pool.get("pool_in_use") if "pool_in_use" in pool else in_use),
            "pool_idle": int(pool.get("pool_idle") or 0),
            "queue_depth": queue_depth,
            "queue_max_size": self.queue_max_size,
            "failed_writes": failed,
            "queued_writes": queued,
            "dropped_noncritical_writes": dropped,
            "suppressed_duplicate_events": suppressed,
            "last_db_error_category": category,
            "last_db_error_at": last_error_at,
            "last_db_error_age_seconds": round(last_error_age, 3) if last_error_age is not None else None,
            "backoff_active": backoff_remaining > 0,
            "backoff_remaining_seconds": round(backoff_remaining, 3),
            **_safety(),
        }

    def _record_error_locked(self, category: str, reason: str) -> None:
        self.last_db_error_category = str(category or "").strip()
        self.last_db_error = str(reason or "")[:500]
        self.last_db_error_at = datetime.now(timezone.utc).isoformat()
        self._last_db_error_monotonic = time.monotonic()

    def _is_duplicate(self, table: str, row: dict[str, Any], now: float) -> bool:
        key = _dedupe_key(table, row)
        if key is None:
            return False
        with self._lock:
            cutoff = now - self.dedupe_window_seconds
            for existing_key, seen_at in list(self._dedupe_seen.items()):
                if seen_at < cutoff:
                    self._dedupe_seen.pop(existing_key, None)
            seen_at = self._dedupe_seen.get(key)
            self._dedupe_seen[key] = now
            return bool(seen_at and seen_at >= cutoff)


class PooledPostgresConnectionManager:
    def __init__(
        self,
        *,
        connection_factory: Callable[[], Any],
        pool_max_size: int = DEFAULT_POOL_MAX_SIZE,
        connect_timeout_seconds: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
    ) -> None:
        self.connection_factory = connection_factory
        self.pool_max_size = max(1, int(pool_max_size or DEFAULT_POOL_MAX_SIZE))
        self.connect_timeout_seconds = max(0.05, float(connect_timeout_seconds or DEFAULT_CONNECT_TIMEOUT_SECONDS))
        self._slots = threading.BoundedSemaphore(self.pool_max_size)
        self._lock = threading.Lock()
        self._idle: list[Any] = []
        self._in_use = 0

    def with_connection(self, operation: Callable[[Any], Any]) -> Any:
        if persistent_write_backpressure().backoff_active():
            raise PersistentDbBackpressureError("persistent_db_backoff_active", error_category="backoff")
        if not self._slots.acquire(timeout=self.connect_timeout_seconds):
            raise PersistentDbBackpressureError("persistent_db_connection_pool_exhausted", error_category="pool_exhausted")
        connection = None
        reusable = False
        with self._lock:
            self._in_use += 1
            if self._idle:
                connection = self._idle.pop()
        try:
            if connection is None:
                connection = self.connection_factory()
            result = operation(connection)
            reusable = True
            return result
        except Exception:
            self._close(connection)
            raise
        finally:
            with self._lock:
                self._in_use = max(0, self._in_use - 1)
                if reusable and connection is not None and len(self._idle) < self.pool_max_size:
                    self._idle.append(connection)
                elif connection is not None and not reusable:
                    pass
                elif connection is not None:
                    self._close(connection)
            try:
                self._slots.release()
            except ValueError:
                pass

    def close_all(self) -> None:
        with self._lock:
            idle = list(self._idle)
            self._idle.clear()
        for connection in idle:
            self._close(connection)

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "pool_enabled": True,
                "pool_max_size": self.pool_max_size,
                "pool_in_use": self._in_use,
                "pool_idle": len(self._idle),
                **_safety(),
            }

    @staticmethod
    def _close(connection: Any) -> None:
        try:
            connection.close()
        except Exception:
            pass


_BACKPRESSURE: PersistentWriteBackpressure | None = None
_POSTGRES_POOLS: dict[str, PooledPostgresConnectionManager] = {}
_POSTGRES_POOLS_LOCK = threading.Lock()


def persistent_write_backpressure() -> PersistentWriteBackpressure:
    global _BACKPRESSURE
    if _BACKPRESSURE is None:
        _BACKPRESSURE = PersistentWriteBackpressure.from_env()
    return _BACKPRESSURE


def get_postgres_connection_manager(
    *,
    database_url: str,
    connection_factory: Callable[[], Any],
    pool_max_size: int,
    connect_timeout_seconds: float,
) -> PooledPostgresConnectionManager:
    key = hashlib.sha256(str(database_url or "").encode("utf-8")).hexdigest()
    with _POSTGRES_POOLS_LOCK:
        existing = _POSTGRES_POOLS.get(key)
        if existing is not None and existing.pool_max_size == max(1, int(pool_max_size or DEFAULT_POOL_MAX_SIZE)):
            return existing
        manager = PooledPostgresConnectionManager(
            connection_factory=connection_factory,
            pool_max_size=pool_max_size,
            connect_timeout_seconds=connect_timeout_seconds,
        )
        _POSTGRES_POOLS[key] = manager
        return manager


def close_all_persistent_postgres_pools() -> None:
    with _POSTGRES_POOLS_LOCK:
        pools = list(_POSTGRES_POOLS.values())
        _POSTGRES_POOLS.clear()
    for pool in pools:
        pool.close_all()


def reset_persistent_connection_state_for_tests() -> None:
    global _BACKPRESSURE
    close_all_persistent_postgres_pools()
    _BACKPRESSURE = PersistentWriteBackpressure.from_env()


def classify_db_error(error: object) -> str:
    text = str(error or "").casefold()
    max_connection_markers = (
        "max clients",
        "too many clients",
        "too_many_connections",
        "remaining connection slots",
        "max_connections",
        "pool_size",
        "pool exhausted",
        "connection pool exhausted",
    )
    timeout_markers = ("timeout", "timed out", "deadline exceeded")
    auth_markers = ("401", "403", "unauthorized", "invalid api key", "invalid jwt", "permission denied")
    connection_markers = ("connection refused", "could not connect", "server closed the connection", "network is unreachable")
    if any(marker in text for marker in max_connection_markers):
        return "max_connections"
    if any(marker in text for marker in timeout_markers):
        return "timeout"
    if any(marker in text for marker in auth_markers):
        return "auth_or_permission"
    if any(marker in text for marker in connection_markers):
        return "connection_unavailable"
    if "relation does not exist" in text or "undefined table" in text or "missing_schema" in text:
        return "missing_schema"
    if "database_env_not_configured" in text or "not_configured" in text:
        return "not_configured"
    return "write_failed"


def _dedupe_key(table: str, row: dict[str, Any]) -> tuple[Any, ...] | None:
    if table == "mt5_decision_events" and str(row.get("decision") or "").upper() == "NO_TRADE":
        return (
            table,
            row.get("symbol") or "",
            row.get("timeframe") or "",
            row.get("decision") or "",
            row.get("reason") or "",
            row.get("profile") or "",
        )
    if table == "mt5_research_lessons":
        return (
            table,
            row.get("family") or "",
            row.get("symbol") or "",
            row.get("timeframe") or "",
            row.get("lesson_type") or "",
            row.get("failure_pattern") or "",
        )
    if table == "mt5_adaptive_governor_state":
        return (
            table,
            row.get("global_state") or "",
            row.get("recommended_next_action") or "",
            row.get("open_shadow_trades") or 0,
        )
    return None


def _compact_queued_row(row: dict[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key in list(row.keys())[:20]:
        value = row[key]
        if isinstance(value, str):
            compact[key] = value[:200]
        elif isinstance(value, (int, float, bool)) or value is None:
            compact[key] = value
        else:
            compact[key] = "[compact_payload]"
    return compact


def _env_int(name: str, default: int) -> int:
    try:
        return max(0, int(os.getenv(name) or default))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name) or default)
    except (TypeError, ValueError):
        return default


def _now_monotonic() -> float:
    return time.monotonic()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
