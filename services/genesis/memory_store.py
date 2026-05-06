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

    def save_message(
        self,
        conversation_id: str,
        role: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        message = {
            "conversation_id": str(conversation_id or "default").strip()[:120] or "default",
            "role": str(role or "assistant").strip()[:40],
            "content": str(content or "").strip()[:4000],
            "metadata": _sanitize(metadata or {}),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if not message["content"]:
            return message
        self.save_conversation(message["conversation_id"], message["content"][:120] if message["role"] == "user" else "")
        if self.backend == "postgres" and self._pg is not None:
            self._pg_execute(
                "INSERT INTO genesis_messages (conversation_id, role, content, metadata_json, created_at) VALUES (%s, %s, %s, %s, %s)",
                (
                    message["conversation_id"],
                    message["role"],
                    message["content"],
                    json.dumps(message["metadata"]),
                    message["created_at"],
                ),
            )
        else:
            with closing(self._sqlite()) as conn:
                conn.execute(
                    "INSERT INTO genesis_messages (conversation_id, role, content, metadata_json, created_at) VALUES (?, ?, ?, ?, ?)",
                    (
                        message["conversation_id"],
                        message["role"],
                        message["content"],
                        json.dumps(message["metadata"]),
                        message["created_at"],
                    ),
                )
                conn.commit()
        return message

    def get_recent_messages(self, conversation_id: str = "default", limit: int = 20) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 20), 100))
        clean_id = str(conversation_id or "default").strip()[:120] or "default"
        if self.backend == "postgres" and self._pg is not None:
            rows = self._pg_fetch(
                "SELECT conversation_id, role, content, metadata_json, created_at FROM genesis_messages WHERE conversation_id = %s ORDER BY id DESC LIMIT %s",
                (clean_id, safe_limit),
            )
        else:
            with closing(self._sqlite()) as conn:
                rows = conn.execute(
                    "SELECT conversation_id, role, content, metadata_json, created_at FROM genesis_messages WHERE conversation_id = ? ORDER BY id DESC LIMIT ?",
                    (clean_id, safe_limit),
                ).fetchall()
        return [_message_row(row) for row in reversed(rows)]

    def list_conversations(self, limit: int = 20) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 20), 100))
        if self.backend == "postgres" and self._pg is not None:
            rows = self._pg_fetch(
                "SELECT conversation_id, summary, updated_at FROM genesis_conversations ORDER BY updated_at DESC LIMIT %s",
                (safe_limit,),
            )
        else:
            with closing(self._sqlite()) as conn:
                rows = conn.execute(
                    "SELECT conversation_id, summary, updated_at FROM genesis_conversations ORDER BY updated_at DESC LIMIT ?",
                    (safe_limit,),
                ).fetchall()
        return [_conversation_row(row) for row in rows]

    def save_conversation(self, conversation_id: str = "default", summary: str = "") -> None:
        clean_id = str(conversation_id or "default").strip()[:120] or "default"
        payload = str(summary or "").strip()[:2000]
        now = datetime.now(timezone.utc).isoformat()
        if self.backend == "postgres" and self._pg is not None:
            self._pg_execute(
                """
                INSERT INTO genesis_conversations (conversation_id, summary, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (conversation_id) DO UPDATE SET
                    summary = CASE WHEN EXCLUDED.summary = '' THEN genesis_conversations.summary ELSE EXCLUDED.summary END,
                    updated_at = EXCLUDED.updated_at
                """,
                (clean_id, payload, now),
            )
        else:
            with closing(self._sqlite()) as conn:
                existing = conn.execute("SELECT summary FROM genesis_conversations WHERE conversation_id = ?", (clean_id,)).fetchone()
                summary_to_store = payload or (existing[0] if existing else "")
                conn.execute(
                    "INSERT OR REPLACE INTO genesis_conversations (conversation_id, summary, updated_at) VALUES (?, ?, ?)",
                    (clean_id, summary_to_store, now),
                )
                conn.commit()

    def save_learned_context(self, key: str, value: Any, source: str = "genesis", confidence: str | float = "media") -> None:
        clean_key = str(key or "").strip()[:160]
        if not clean_key:
            return
        now = datetime.now(timezone.utc).isoformat()
        payload = json.dumps(_sanitize(value))
        if self.backend == "postgres" and self._pg is not None:
            self._pg_execute(
                """
                INSERT INTO genesis_learned_context (context_key, value_json, source, confidence, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (context_key) DO UPDATE SET
                    value_json = EXCLUDED.value_json,
                    source = EXCLUDED.source,
                    confidence = EXCLUDED.confidence,
                    updated_at = EXCLUDED.updated_at
                """,
                (clean_key, payload, str(source or "genesis")[:80], str(confidence or "media")[:40], now),
            )
        else:
            with closing(self._sqlite()) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO genesis_learned_context (context_key, value_json, source, confidence, updated_at) VALUES (?, ?, ?, ?, ?)",
                    (clean_key, payload, str(source or "genesis")[:80], str(confidence or "media")[:40], now),
                )
                conn.commit()

    def get_learned_context(self, limit: int = 20) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 20), 100))
        if self.backend == "postgres" and self._pg is not None:
            rows = self._pg_fetch(
                "SELECT context_key, value_json, source, confidence, updated_at FROM genesis_learned_context ORDER BY updated_at DESC LIMIT %s",
                (safe_limit,),
            )
        else:
            with closing(self._sqlite()) as conn:
                rows = conn.execute(
                    "SELECT context_key, value_json, source, confidence, updated_at FROM genesis_learned_context ORDER BY updated_at DESC LIMIT ?",
                    (safe_limit,),
                ).fetchall()
        return [_learned_row(row) for row in rows]

    def track_entity(self, ticker: str, entity_type: str = "asset", context: dict[str, Any] | None = None) -> None:
        normalized = str(ticker or "").strip().upper()[:40]
        if not normalized:
            return
        now = datetime.now(timezone.utc).isoformat()
        payload = json.dumps(_sanitize(context or {}))
        if self.backend == "postgres" and self._pg is not None:
            self._pg_execute(
                """
                INSERT INTO genesis_tracked_entities (ticker, entity_type, context_json, last_seen_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (ticker) DO UPDATE SET
                    entity_type = EXCLUDED.entity_type,
                    context_json = EXCLUDED.context_json,
                    last_seen_at = EXCLUDED.last_seen_at
                """,
                (normalized, str(entity_type or "asset")[:60], payload, now),
            )
        else:
            with closing(self._sqlite()) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO genesis_tracked_entities (ticker, entity_type, context_json, last_seen_at) VALUES (?, ?, ?, ?)",
                    (normalized, str(entity_type or "asset")[:60], payload, now),
                )
                conn.commit()

    def get_tracked_entities(self, limit: int = 20) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 20), 100))
        if self.backend == "postgres" and self._pg is not None:
            rows = self._pg_fetch(
                "SELECT ticker, entity_type, context_json, last_seen_at FROM genesis_tracked_entities ORDER BY last_seen_at DESC LIMIT %s",
                (safe_limit,),
            )
        else:
            with closing(self._sqlite()) as conn:
                rows = conn.execute(
                    "SELECT ticker, entity_type, context_json, last_seen_at FROM genesis_tracked_entities ORDER BY last_seen_at DESC LIMIT ?",
                    (safe_limit,),
                ).fetchall()
        return [_tracked_row(row) for row in rows]

    def save_recent_topic(self, topic: str, context: dict[str, Any] | None = None) -> None:
        clean_topic = str(topic or "").strip()[:160]
        if not clean_topic:
            return
        now = datetime.now(timezone.utc).isoformat()
        payload = json.dumps(_sanitize(context or {}))
        if self.backend == "postgres" and self._pg is not None:
            self._pg_execute(
                "INSERT INTO genesis_recent_topics (topic, context_json, created_at) VALUES (%s, %s, %s)",
                (clean_topic, payload, now),
            )
        else:
            with closing(self._sqlite()) as conn:
                conn.execute(
                    "INSERT INTO genesis_recent_topics (topic, context_json, created_at) VALUES (?, ?, ?)",
                    (clean_topic, payload, now),
                )
                conn.commit()

    def get_recent_topics(self, limit: int = 10) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 10), 50))
        if self.backend == "postgres" and self._pg is not None:
            rows = self._pg_fetch(
                "SELECT topic, context_json, created_at FROM genesis_recent_topics ORDER BY id DESC LIMIT %s",
                (safe_limit,),
            )
        else:
            with closing(self._sqlite()) as conn:
                rows = conn.execute(
                    "SELECT topic, context_json, created_at FROM genesis_recent_topics ORDER BY id DESC LIMIT ?",
                    (safe_limit,),
                ).fetchall()
        return [_topic_row(row) for row in rows]

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
        payload = {"ticker": str(ticker or "").upper(), "observation": observation}
        self.save_event("market_observation", payload, "market", "media")
        self._save_market_observation_row(payload["ticker"], observation, "market", "media")

    def get_market_memory(self, ticker: str) -> list[dict[str, Any]]:
        normalized = str(ticker or "").upper()
        rows = self._get_market_observation_rows(normalized)
        if rows:
            return rows
        return [event for event in self.get_recent_events(100, "market_observation") if str(event.get("payload", {}).get("ticker") or "").upper() == normalized]

    def save_whale_event(
        self,
        ticker: str,
        entity: str = "",
        action: str = "",
        amount: object = None,
        date: str = "",
        confidence: str | float = "media",
        event: dict[str, Any] | None = None,
    ) -> None:
        base = {
            "ticker": str(ticker or "").upper(),
            "asset_name": "",
            "asset_type": "",
            "event_type": "whale_confirmed" if entity else "smart_money_estimate",
            "entity_name": entity,
            "entity_type": "",
            "entity": entity,
            "action": action,
            "units": None,
            "price": None,
            "amount": amount,
            "amount_usd": amount if isinstance(amount, (int, float)) else None,
            "current_price": None,
            "estimated_value": amount if isinstance(amount, (int, float)) else None,
            "date": date,
            "source": "whales",
            "confidence": confidence,
            "evidence": {},
            "genesis_reading": "",
        }
        payload = {**base, **_sanitize(event or {})}
        payload["ticker"] = str(payload.get("ticker") or ticker or "").upper()
        payload["entity_name"] = payload.get("entity_name") or payload.get("entity") or entity
        payload["entity"] = payload.get("entity_name") or ""
        payload["action"] = payload.get("action") or action
        payload["date"] = payload.get("date") or date
        self.save_event(
            "whale_event",
            payload,
            str(payload.get("source") or "whales"),
            payload.get("confidence") or confidence,
        )
        self._save_whale_event_row(payload, str(payload.get("source") or "whales"), payload.get("confidence") or confidence)

    def get_whale_memory(self, ticker: str | None = None) -> list[dict[str, Any]]:
        events = self._get_whale_event_rows(ticker)
        if not events:
            events = self.get_recent_events(100, "whale_event")
        if ticker:
            normalized = str(ticker or "").upper()
            return [event for event in events if str(event.get("payload", {}).get("ticker") or event.get("ticker") or "").upper() == normalized]
        return events

    def save_alert_event(self, ticker: str, alert_type: str, payload: dict[str, Any] | None = None, confidence: str | float = "media") -> None:
        clean_payload = {"ticker": str(ticker or "").upper(), "alert_type": alert_type, **_sanitize(payload or {})}
        self.save_event("alert_event", clean_payload, "alerts", confidence)
        self._save_alert_event_row(clean_payload, "alerts", confidence)

    def get_alert_memory(self, ticker: str | None = None) -> list[dict[str, Any]]:
        return self._get_alert_event_rows(ticker)

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

    def get_memory_summary(self, query: str = "") -> dict[str, Any]:
        return {
            "backend": self.backend,
            "durable": self.backend == "postgres",
            "recent_events": self.get_recent_events(8),
            "recent_messages": self.get_recent_messages(limit=8),
            "learned_context": self.get_learned_context(8),
            "tracked_entities": self.get_tracked_entities(8),
            "recent_topics": self.get_recent_topics(8),
            "market_observations": self._get_market_observation_rows("", limit=8),
            "whale_events": self._get_whale_event_rows(None, limit=8),
            "alert_events": self._get_alert_event_rows(None, limit=8),
            "relevant": self.get_relevant_memory(query) if query else [],
        }

    def _save_market_observation_row(self, ticker: str, observation: str, source: str, confidence: str | float) -> None:
        now = datetime.now(timezone.utc).isoformat()
        payload = json.dumps(_sanitize({"observation": observation}))
        if self.backend == "postgres" and self._pg is not None:
            self._pg_execute(
                "INSERT INTO genesis_market_observations (ticker, observation_json, source, confidence, created_at) VALUES (%s, %s, %s, %s, %s)",
                (str(ticker or "").upper()[:40], payload, str(source or "market")[:80], str(confidence or "media")[:40], now),
            )
        else:
            with closing(self._sqlite()) as conn:
                conn.execute(
                    "INSERT INTO genesis_market_observations (ticker, observation_json, source, confidence, created_at) VALUES (?, ?, ?, ?, ?)",
                    (str(ticker or "").upper()[:40], payload, str(source or "market")[:80], str(confidence or "media")[:40], now),
                )
                conn.commit()

    def _get_market_observation_rows(self, ticker: str = "", limit: int = 50) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 50), 200))
        normalized = str(ticker or "").upper()
        if self.backend == "postgres" and self._pg is not None:
            if normalized:
                rows = self._pg_fetch(
                    "SELECT ticker, observation_json, source, confidence, created_at FROM genesis_market_observations WHERE ticker = %s ORDER BY id DESC LIMIT %s",
                    (normalized, safe_limit),
                )
            else:
                rows = self._pg_fetch(
                    "SELECT ticker, observation_json, source, confidence, created_at FROM genesis_market_observations ORDER BY id DESC LIMIT %s",
                    (safe_limit,),
                )
        else:
            with closing(self._sqlite()) as conn:
                if normalized:
                    rows = conn.execute(
                        "SELECT ticker, observation_json, source, confidence, created_at FROM genesis_market_observations WHERE ticker = ? ORDER BY id DESC LIMIT ?",
                        (normalized, safe_limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT ticker, observation_json, source, confidence, created_at FROM genesis_market_observations ORDER BY id DESC LIMIT ?",
                        (safe_limit,),
                    ).fetchall()
        return [_domain_row("market_observation", row) for row in rows]

    def _save_whale_event_row(self, payload: dict[str, Any], source: str, confidence: str | float) -> None:
        now = datetime.now(timezone.utc).isoformat()
        clean_payload = _sanitize(payload)
        if self.backend == "postgres" and self._pg is not None:
            self._pg_execute(
                "INSERT INTO genesis_whale_events (ticker, entity, action, amount_json, event_date, source, confidence, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    str(clean_payload.get("ticker") or "").upper()[:40],
                    str(clean_payload.get("entity_name") or clean_payload.get("entity") or "")[:200],
                    str(clean_payload.get("action") or "")[:120],
                    json.dumps(clean_payload),
                    str(clean_payload.get("date") or "")[:80],
                    str(source or "whales")[:80],
                    str(confidence or "media")[:40],
                    now,
                ),
            )
        else:
            with closing(self._sqlite()) as conn:
                conn.execute(
                    "INSERT INTO genesis_whale_events (ticker, entity, action, amount_json, event_date, source, confidence, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        str(clean_payload.get("ticker") or "").upper()[:40],
                        str(clean_payload.get("entity_name") or clean_payload.get("entity") or "")[:200],
                        str(clean_payload.get("action") or "")[:120],
                        json.dumps(clean_payload),
                        str(clean_payload.get("date") or "")[:80],
                        str(source or "whales")[:80],
                        str(confidence or "media")[:40],
                        now,
                    ),
                )
                conn.commit()

    def _get_whale_event_rows(self, ticker: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 50), 200))
        normalized = str(ticker or "").upper()
        if self.backend == "postgres" and self._pg is not None:
            if normalized:
                rows = self._pg_fetch(
                    "SELECT ticker, entity, action, amount_json, event_date, source, confidence, created_at FROM genesis_whale_events WHERE ticker = %s ORDER BY id DESC LIMIT %s",
                    (normalized, safe_limit),
                )
            else:
                rows = self._pg_fetch(
                    "SELECT ticker, entity, action, amount_json, event_date, source, confidence, created_at FROM genesis_whale_events ORDER BY id DESC LIMIT %s",
                    (safe_limit,),
                )
        else:
            with closing(self._sqlite()) as conn:
                if normalized:
                    rows = conn.execute(
                        "SELECT ticker, entity, action, amount_json, event_date, source, confidence, created_at FROM genesis_whale_events WHERE ticker = ? ORDER BY id DESC LIMIT ?",
                        (normalized, safe_limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT ticker, entity, action, amount_json, event_date, source, confidence, created_at FROM genesis_whale_events ORDER BY id DESC LIMIT ?",
                        (safe_limit,),
                    ).fetchall()
        return [_whale_row(row) for row in rows]

    def _save_alert_event_row(self, payload: dict[str, Any], source: str, confidence: str | float) -> None:
        now = datetime.now(timezone.utc).isoformat()
        clean_payload = _sanitize(payload)
        if self.backend == "postgres" and self._pg is not None:
            self._pg_execute(
                "INSERT INTO genesis_alert_events (ticker, alert_type, payload_json, source, confidence, created_at) VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    str(clean_payload.get("ticker") or "").upper()[:40],
                    str(clean_payload.get("alert_type") or "alert")[:120],
                    json.dumps(clean_payload),
                    str(source or "alerts")[:80],
                    str(confidence or "media")[:40],
                    now,
                ),
            )
        else:
            with closing(self._sqlite()) as conn:
                conn.execute(
                    "INSERT INTO genesis_alert_events (ticker, alert_type, payload_json, source, confidence, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        str(clean_payload.get("ticker") or "").upper()[:40],
                        str(clean_payload.get("alert_type") or "alert")[:120],
                        json.dumps(clean_payload),
                        str(source or "alerts")[:80],
                        str(confidence or "media")[:40],
                        now,
                    ),
                )
                conn.commit()

    def _get_alert_event_rows(self, ticker: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 50), 200))
        normalized = str(ticker or "").upper()
        if self.backend == "postgres" and self._pg is not None:
            if normalized:
                rows = self._pg_fetch(
                    "SELECT ticker, alert_type, payload_json, source, confidence, created_at FROM genesis_alert_events WHERE ticker = %s ORDER BY id DESC LIMIT %s",
                    (normalized, safe_limit),
                )
            else:
                rows = self._pg_fetch(
                    "SELECT ticker, alert_type, payload_json, source, confidence, created_at FROM genesis_alert_events ORDER BY id DESC LIMIT %s",
                    (safe_limit,),
                )
        else:
            with closing(self._sqlite()) as conn:
                if normalized:
                    rows = conn.execute(
                        "SELECT ticker, alert_type, payload_json, source, confidence, created_at FROM genesis_alert_events WHERE ticker = ? ORDER BY id DESC LIMIT ?",
                        (normalized, safe_limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT ticker, alert_type, payload_json, source, confidence, created_at FROM genesis_alert_events ORDER BY id DESC LIMIT ?",
                        (safe_limit,),
                    ).fetchall()
        return [_alert_row(row) for row in rows]

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
            self._pg_execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_conversations (
                    conversation_id TEXT PRIMARY KEY,
                    summary TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """,
                (),
            )
            self._pg_execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_messages (
                    id SERIAL PRIMARY KEY,
                    conversation_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """,
                (),
            )
            self._pg_execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_learned_context (
                    context_key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    source TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """,
                (),
            )
            self._pg_execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_tracked_entities (
                    ticker TEXT PRIMARY KEY,
                    entity_type TEXT NOT NULL,
                    context_json TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL
                )
                """,
                (),
            )
            self._pg_execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_recent_topics (
                    id SERIAL PRIMARY KEY,
                    topic TEXT NOT NULL,
                    context_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """,
                (),
            )
            self._pg_execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_market_observations (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    observation_json TEXT NOT NULL,
                    source TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """,
                (),
            )
            self._pg_execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_whale_events (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    entity TEXT NOT NULL,
                    action TEXT NOT NULL,
                    amount_json TEXT NOT NULL,
                    event_date TEXT NOT NULL,
                    source TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """,
                (),
            )
            self._pg_execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_alert_events (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    alert_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    source TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    created_at TEXT NOT NULL
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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_conversations (
                    conversation_id TEXT PRIMARY KEY,
                    summary TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_learned_context (
                    context_key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    source TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_tracked_entities (
                    ticker TEXT PRIMARY KEY,
                    entity_type TEXT NOT NULL,
                    context_json TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_recent_topics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    context_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_market_observations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    observation_json TEXT NOT NULL,
                    source TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_whale_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    entity TEXT NOT NULL,
                    action TEXT NOT NULL,
                    amount_json TEXT NOT NULL,
                    event_date TEXT NOT NULL,
                    source TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS genesis_alert_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    alert_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    source TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    created_at TEXT NOT NULL
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


def _message_row(row: Any) -> dict[str, Any]:
    return {
        "conversation_id": row[0],
        "role": row[1],
        "content": row[2],
        "metadata": _loads(row[3]),
        "created_at": row[4],
    }


def _conversation_row(row: Any) -> dict[str, Any]:
    return {
        "conversation_id": row[0],
        "summary": row[1],
        "updated_at": row[2],
    }


def _learned_row(row: Any) -> dict[str, Any]:
    return {
        "key": row[0],
        "value": _loads(row[1]),
        "source": row[2],
        "confidence": row[3],
        "updated_at": row[4],
    }


def _tracked_row(row: Any) -> dict[str, Any]:
    return {
        "ticker": row[0],
        "entity_type": row[1],
        "context": _loads(row[2]),
        "last_seen_at": row[3],
    }


def _topic_row(row: Any) -> dict[str, Any]:
    return {
        "topic": row[0],
        "context": _loads(row[1]),
        "created_at": row[2],
    }


def _domain_row(event_type: str, row: Any) -> dict[str, Any]:
    payload = _loads(row[1])
    if not isinstance(payload, dict):
        payload = {}
    payload["ticker"] = row[0]
    return {
        "event_type": event_type,
        "ticker": row[0],
        "payload": payload,
        "source": row[2],
        "confidence": row[3],
        "created_at": row[4],
    }


def _whale_row(row: Any) -> dict[str, Any]:
    stored = _loads(row[3])
    if isinstance(stored, dict):
        payload = stored
        payload.setdefault("ticker", row[0])
        payload.setdefault("entity", row[1])
        payload.setdefault("entity_name", row[1])
        payload.setdefault("action", row[2])
        payload.setdefault("date", row[4])
    else:
        payload = {
            "ticker": row[0],
            "entity": row[1],
            "entity_name": row[1],
            "action": row[2],
            "amount": stored,
            "amount_usd": stored if isinstance(stored, (int, float)) else None,
            "estimated_value": stored if isinstance(stored, (int, float)) else None,
            "date": row[4],
            "event_type": "whale_confirmed" if row[1] else "smart_money_estimate",
        }
    payload = {
        **payload,
        "source": payload.get("source") or row[5],
        "confidence": payload.get("confidence") or row[6],
        "created_at": payload.get("created_at") or row[7],
    }
    return {
        "event_type": payload.get("event_type") or "whale_event",
        "ticker": row[0],
        "payload": payload,
        "source": row[5],
        "confidence": row[6],
        "created_at": row[7],
    }


def _alert_row(row: Any) -> dict[str, Any]:
    payload = _loads(row[2])
    if not isinstance(payload, dict):
        payload = {}
    payload["ticker"] = row[0]
    payload["alert_type"] = row[1]
    return {
        "event_type": "alert_event",
        "ticker": row[0],
        "payload": payload,
        "source": row[3],
        "confidence": row[4],
        "created_at": row[5],
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
