from __future__ import annotations

import os
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from services.genesis.memory_store import MemoryStore
from services.mt5 import mt5_db_circuit_breaker as circuit


DEFAULT_MAX_SIZE = 5000


@dataclass
class MT5IngestStats:
    enqueued: int = 0
    dropped: int = 0
    flushed: int = 0
    failed_flushes: int = 0
    dead_letter: int = 0
    last_enqueue_at: str = ""
    last_drop_at: str = ""
    last_flush_at: str = ""
    last_successful_flush_at: str = ""
    last_failed_flush_at: str = ""
    last_error: str = ""


class MT5IngestQueue:
    def __init__(self, *, max_size: int | None = None, memory_factory: Callable[[], MemoryStore] | None = None) -> None:
        self.max_size = int(max_size or _int_env("MT5_INGEST_QUEUE_MAX", DEFAULT_MAX_SIZE))
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=max(1, self.max_size))
        self._stats = MT5IngestStats()
        self._lock = threading.Lock()
        self._worker_started = False
        self._memory_factory = memory_factory or MemoryStore
        self._dead_letter: list[dict[str, Any]] = []

    def enqueue(self, collection: str, symbol: str, payload: dict[str, Any]) -> dict[str, Any]:
        self._start_worker()
        event = {
            "collection": str(collection or "mt5_journal"),
            "symbol": str(symbol or payload.get("symbol") or "MT5").upper().strip(),
            "payload": dict(payload or {}),
        }
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            with self._lock:
                self._stats.dropped += 1
                self._stats.last_drop_at = _now()
            return {"queued": False, "dropped": True, "warning": "ingest_queue_full", **self.status()}
        with self._lock:
            self._stats.enqueued += 1
            self._stats.last_enqueue_at = _now()
        return {"queued": True, "dropped": False, **self.status()}

    def status(self) -> dict[str, Any]:
        with self._lock:
            stats = {
                "queue_depth": self._queue.qsize(),
                "queue_max_size": self.max_size,
                "dropped_events": self._stats.dropped,
                "enqueued_events": self._stats.enqueued,
                "flushed_events": self._stats.flushed,
                "failed_flushes": self._stats.failed_flushes,
                "dead_letter_count": self._stats.dead_letter,
                "last_enqueue_at": self._stats.last_enqueue_at,
                "last_drop_at": self._stats.last_drop_at,
                "last_flush_at": self._stats.last_flush_at,
                "last_successful_flush_at": self._stats.last_successful_flush_at,
                "last_failed_flush_at": self._stats.last_failed_flush_at,
                "last_ingest_error": self._stats.last_error,
            }
        return stats

    def _start_worker(self) -> None:
        if self._worker_started:
            return
        with self._lock:
            if self._worker_started:
                return
            self._worker_started = True
            thread = threading.Thread(target=self._worker, name="mt5-ingest-queue", daemon=True)
            thread.start()

    def _worker(self) -> None:
        while True:
            event = self._queue.get()
            try:
                started = time.monotonic()
                if circuit.is_db_degraded():
                    time.sleep(0.25)
                    self._requeue_or_dead_letter(event, "db_degraded")
                    continue
                self._flush_event(event, started=started)
            except Exception as exc:
                self._record_failed_event(event, exc)
            finally:
                self._queue.task_done()

    def flush_once_for_tests(self, *, limit: int = 25) -> None:
        for _ in range(max(1, int(limit or 1))):
            try:
                event = self._queue.get_nowait()
            except queue.Empty:
                return
            try:
                self._flush_event(event, started=time.monotonic())
            except Exception as exc:
                self._record_failed_event(event, exc)
            finally:
                self._queue.task_done()

    def dead_letters_for_tests(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._dead_letter)

    def _flush_event(self, event: dict[str, Any], *, started: float) -> None:
        memory = self._memory_factory()
        try:
            memory.save_mt5_event(event["collection"], event["symbol"], event["payload"])
        except Exception:
            self._rollback_memory(memory)
            raise
        duration_ms = int(round((time.monotonic() - started) * 1000))
        circuit.record_db_success(duration_ms)
        timestamp = _now()
        with self._lock:
            self._stats.flushed += 1
            self._stats.last_flush_at = timestamp
            self._stats.last_successful_flush_at = timestamp

    def _record_failed_event(self, event: dict[str, Any], exc: Exception) -> None:
        circuit.record_db_error(exc)
        message = str(exc)[:500]
        timestamp = _now()
        dead_letter = {**event, "error": message, "failed_at": timestamp}
        with self._lock:
            self._stats.failed_flushes += 1
            self._stats.dead_letter += 1
            self._stats.last_error = message
            self._stats.last_failed_flush_at = timestamp
            self._dead_letter.append(dead_letter)
            self._dead_letter = self._dead_letter[-100:]

    def _requeue_or_dead_letter(self, event: dict[str, Any], reason: str) -> None:
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            self._record_failed_event(event, RuntimeError(reason))
            with self._lock:
                self._stats.dropped += 1
                self._stats.last_drop_at = _now()

    @staticmethod
    def _rollback_memory(memory: MemoryStore) -> None:
        conn = getattr(memory, "_pg", None)
        if conn is None:
            return
        try:
            conn.rollback()
        except Exception:
            try:
                conn.close()
            except Exception:
                pass


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


_GLOBAL_QUEUE = MT5IngestQueue()


def enqueue_mt5_event(collection: str, symbol: str, payload: dict[str, Any]) -> dict[str, Any]:
    return _GLOBAL_QUEUE.enqueue(collection, symbol, payload)


def ingest_status() -> dict[str, Any]:
    return _GLOBAL_QUEUE.status()
