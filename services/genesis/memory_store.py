from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from app.settings import load_settings

_ROOT_DIR = Path(__file__).resolve().parents[2]
_DEFAULT_SQLITE_PATH = _ROOT_DIR / ".genesis_memory.sqlite3"
_SECRET_KEY_PARTS = ("api", "key", "secret", "token", "password", "credential")


class MemoryStore:
    def __init__(self, database_url: str | None = None, sqlite_path: str | Path | None = None) -> None:
        settings = load_settings()
        self.database_url = database_url if database_url is not None else settings.database_url
        self.sqlite_path = Path(sqlite_path or _DEFAULT_SQLITE_PATH)
        self.backend = "sqlite"
        self._pg = None
        if self.database_url:
            try:
                self._pg = self._connect_postgres(self.database_url)
                self.backend = "postgres"
            except Exception:
                self._pg = None
                self.backend = "sqlite"
        self._ensure_schema()

    def save_event(self, event_type: str, payload: dict[str, Any] | None = None, source: str = "genesis", confidence: str | float = "media") -> dict[str, Any]:
        payload = _sanitize(payload or {})
        event = {
            "event_type": str(event_type or "event").strip()[:80],
            "payload": payload,
            "source": str(source or "genesis").strip()[:80],
            "confidence": str(confidence or "media").strip()[:40],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if self.backend == "postgres" and self._pg is not None:
            self._pg_execute(
                "INSERT INTO genesis_memory_events (event_type, payload_json, source, confidence, created_at) VALUES (%s, %s, %s, %s, %s)",
                (event["event_type"], json.dumps(payload), event["source"], event["confidence"], event["created_at"]),
            )
        else:
            with closing(self._sqlite()) as conn:
                conn.execute(
                    "INSERT INTO genesis_memory_events (event_type, payload_json, source, confidence, created_at) VALUES (?, ?, ?, ?, ?)",
                    (event["event_type"], json.dumps(payload), event["source"], event["confidence"], event["created_at"]),
                )
                conn.commit()
        return event

    def get_recent_events(self, limit: int = 20, event_type: str | None = None) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 20), 200))
        if self.backend == "postgres" and self._pg is not None:
            if event_type:
                rows = self._pg_fetch(
                    "SELECT event_type, payload_json, source, confidence, created_at FROM genesis_memory_events WHERE event_type = %s ORDER BY id DESC LIMIT %s",
                    (event_type, safe_limit),
                )
            else:
                rows = self._pg_fetch(
                    "SELECT event_type, payload_json, source, confidence, created_at FROM genesis_memory_events ORDER BY id DESC LIMIT %s",
                    (safe_limit,),
                )
        else:
            with closing(self._sqlite()) as conn:
                if event_type:
                    rows = conn.execute(
                        "SELECT event_type, payload_json, source, confidence, created_at FROM genesis_memory_events WHERE event_type = ? ORDER BY id DESC LIMIT ?",
                        (event_type, safe_limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT event_type, payload_json, source, confidence, created_at FROM genesis_memory_events ORDER BY id DESC LIMIT ?",
                        (safe_limit,),
                    ).fetchall()
        return [_event_row(row) for row in rows]

    def save_user_preference(self, key: str, value: Any) -> None:
        clean_key = str(key or "").strip()[:120]
        if not clean_key:
            return
        payload = json.dumps(_sanitize(value))
        if self.backend == "postgres" and self._pg is not None:
            self._pg_execute(
                """
                INSERT INTO genesis_user_preferences (pref_key, value_json, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (pref_key) DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = EXCLUDED.updated_at
                """,
                (clean_key, payload, datetime.now(timezone.utc).isoformat()),
            )
        else:
            with closing(self._sqlite()) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO genesis_user_preferences (pref_key, value_json, updated_at) VALUES (?, ?, ?)",
                    (clean_key, payload, datetime.now(timezone.utc).isoformat()),
                )
                conn.commit()

    def get_user_preferences(self) -> dict[str, Any]:
        if self.backend == "postgres" and self._pg is not None:
            rows = self._pg_fetch("SELECT pref_key, value_json FROM genesis_user_preferences", ())
        else:
            with closing(self._sqlite()) as conn:
                rows = conn.execute("SELECT pref_key, value_json FROM genesis_user_preferences").fetchall()
        return {str(row[0]): _loads(row[1]) for row in rows}

    def save_market_observation(self, ticker: str, observation: str) -> None:
        self.save_event("market_observation", {"ticker": str(ticker or "").upper(), "observation": observation}, "market", "media")

    def get_market_memory(self, ticker: str) -> list[dict[str, Any]]:
        normalized = str(ticker or "").upper()
        return [event for event in self.get_recent_events(100, "market_observation") if str(event.get("payload", {}).get("ticker") or "").upper() == normalized]

    def save_whale_event(self, ticker: str, entity: str = "", action: str = "", amount: object = None, date: str = "", confidence: str | float = "media") -> None:
        self.save_event(
            "whale_event",
            {"ticker": str(ticker or "").upper(), "entity": entity, "action": action, "amount": amount, "date": date},
            "whales",
            confidence,
        )

    def get_whale_memory(self, ticker: str | None = None) -> list[dict[str, Any]]:
        events = self.get_recent_events(100, "whale_event")
        if ticker:
            normalized = str(ticker or "").upper()
            return [event for event in events if str(event.get("payload", {}).get("ticker") or "").upper() == normalized]
        return events

    def save_genesis_conversation_summary(self, summary: str) -> None:
        self.save_event("conversation_summary", {"summary": summary}, "genesis", "media")

    def get_relevant_memory(self, query: str) -> list[dict[str, Any]]:
        text = str(query or "").casefold()
        events = self.get_recent_events(80)
        if not text:
            return events[:10]
        return [
            event
            for event in events
            if text in json.dumps(event.get("payload", {}), ensure_ascii=False).casefold()
            or text in str(event.get("event_type", "")).casefold()
        ][:10]

    def _ensure_schema(self) -> None:
        if self.backend == "postgres" and self._pg is not None:
            self._pg_execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_memory_events (
                    id SERIAL PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    source TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """,
                (),
            )
            self._pg_execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_user_preferences (
                    pref_key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """,
                (),
            )
            return
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._sqlite()) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_memory_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    source TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_user_preferences (
                    pref_key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def _sqlite(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.sqlite_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _connect_postgres(self, database_url: str):
        import pg8000.dbapi

        parsed = urlparse(database_url)
        return pg8000.dbapi.connect(
            user=parsed.username,
            password=parsed.password,
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=(parsed.path or "/").lstrip("/"),
            ssl_context=True if parsed.scheme.endswith("+ssl") else None,
        )

    def _pg_execute(self, sql: str, params: tuple) -> None:
        cursor = self._pg.cursor()
        cursor.execute(sql, params)
        self._pg.commit()
        cursor.close()

    def _pg_fetch(self, sql: str, params: tuple) -> list:
        cursor = self._pg.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        cursor.close()
        return rows


def _event_row(row: Any) -> dict[str, Any]:
    return {
        "event_type": row[0],
        "payload": _loads(row[1]),
        "source": row[2],
        "confidence": row[3],
        "created_at": row[4],
    }


def _loads(value: Any) -> Any:
    try:
        return json.loads(value)
    except Exception:
        return {}


def _sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        clean: dict[str, Any] = {}
        for key, raw in value.items():
            key_text = str(key or "")
            if any(part in key_text.casefold() for part in _SECRET_KEY_PARTS):
                continue
            clean[key_text[:120]] = _sanitize(raw)
        return clean
    if isinstance(value, list):
        return [_sanitize(item) for item in value[:100]]
    if isinstance(value, (str, int, float, bool)) or value is None:
        text = str(value)
        if any(part in text.casefold() for part in ("apikey=", "fmp_api_key", "openai_api_key")):
            return "[redacted]"
        return value
    return str(value)
