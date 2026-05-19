from __future__ import annotations

import os
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from services.genesis.memory_store import MemoryStore
from services.mt5 import mt5_db_circuit_breaker as circuit


DEFAULT_MAX_SIZE = 5000


@dataclass
class MT5IngestStats:
    enqueued: int = 0
    dropped: int = 0
    flushed: int = 0
    failed_flushes: int = 0
    last_enqueue_at: str = ""
    last_drop_at: str = ""
    last_flush_at: str = ""
    last_error: str = ""


class MT5IngestQueue:
    def __init__(self, *, max_size: int | None = None) -> None:
        self.max_size = int(max_size or _int_env("MT5_INGEST_QUEUE_MAX", DEFAULT_MAX_SIZE))
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=max(1, self.max_size))
        self._stats = MT5IngestStats()
        self._lock = threading.Lock()
        self._worker_started = False

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
                "last_enqueue_at": self._stats.last_enqueue_at,
                "last_drop_at": self._stats.last_drop_at,
                "last_flush_at": self._stats.last_flush_at,
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
                    self._queue.put_nowait(event)
                    continue
                memory = MemoryStore()
                memory.save_mt5_event(event["collection"], event["symbol"], event["payload"])
                duration_ms = int(round((time.monotonic() - started) * 1000))
                circuit.record_db_success(duration_ms)
                with self._lock:
                    self._stats.flushed += 1
                    self._stats.last_flush_at = _now()
            except queue.Full:
                with self._lock:
                    self._stats.dropped += 1
                    self._stats.last_drop_at = _now()
            except Exception as exc:
                circuit.record_db_error(exc)
                with self._lock:
                    self._stats.failed_flushes += 1
                    self._stats.last_error = str(exc)[:500]
            finally:
                self._queue.task_done()


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
