from __future__ import annotations

import json
import sqlite3
import hashlib
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from app.settings import load_settings
from services.mt5.instrument_resolver import enrich_payload, normalize_mt5_symbol, payload_matches_symbol, symbol_aliases

_ROOT_DIR = Path(__file__).resolve().parents[2]
_DEFAULT_SQLITE_PATH = _ROOT_DIR / ".genesis_memory.sqlite3"
_SECRET_KEY_PARTS = ("api", "key", "secret", "token", "password", "credential")
_TRACKED_ENTITY_BLOCKLIST = {
    "BALLNEA",
    "BALLNEAS",
    "BALENA",
    "BALENAS",
    "BALLENA",
    "BALLENAS",
    "SMART",
    "MONEY",
    "FLOW",
    "FLUJO",
    "INSTITUCIONAL",
    "INSTITUCIONALES",
}
_LEARNING_TABLES = {
    "asset_memory": "genesis_asset_memory",
    "signal_events": "genesis_signal_events",
    "news_events": "genesis_news_events",
    "decision_notes": "genesis_decision_notes",
    "hypothesis_log": "genesis_hypothesis_log",
    "outcome_tracking": "genesis_outcome_tracking",
}


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
        if not normalized or _is_tracked_entity_noise(normalized):
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
        return [item for item in (_tracked_row(row) for row in rows) if not _is_tracked_entity_noise(item.get("ticker"))]

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

    def save_asset_memory(
        self,
        ticker: str,
        payload: dict[str, Any] | None = None,
        source: str = "genesis",
        confidence: str | float = "media",
    ) -> dict[str, Any]:
        return self._save_learning_row("asset_memory", ticker, payload or {}, source, confidence, "asset_memory")

    def get_asset_memory(self, ticker: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        return self._get_learning_rows("asset_memory", ticker, limit)

    def save_signal_event(
        self,
        ticker: str,
        payload: dict[str, Any] | None = None,
        source: str = "signals",
        confidence: str | float = "media",
    ) -> dict[str, Any]:
        return self._save_learning_row("signal_events", ticker, payload or {}, source, confidence, "signal_event")

    def get_signal_events(self, ticker: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        return self._get_learning_rows("signal_events", ticker, limit)

    def save_news_event(
        self,
        ticker: str,
        payload: dict[str, Any] | None = None,
        source: str = "news",
        confidence: str | float = "media",
    ) -> dict[str, Any]:
        return self._save_learning_row("news_events", ticker, payload or {}, source, confidence, "news_event")

    def get_news_events(self, ticker: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        return self._get_learning_rows("news_events", ticker, limit)

    def save_decision_note(
        self,
        ticker: str,
        verdict: str,
        payload: dict[str, Any] | None = None,
        source: str = "genesis",
        confidence: str | float = "media",
    ) -> dict[str, Any]:
        data = {"verdict": verdict, **_sanitize(payload or {})}
        return self._save_learning_row("decision_notes", ticker, data, source, confidence, "decision_note")

    def get_decision_notes(self, ticker: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        return self._get_learning_rows("decision_notes", ticker, limit)

    def save_hypothesis(
        self,
        ticker: str,
        payload: dict[str, Any] | None = None,
        source: str = "genesis",
        confidence: str | float = "media",
    ) -> dict[str, Any]:
        return self._save_learning_row("hypothesis_log", ticker, payload or {}, source, confidence, "hypothesis")

    def get_hypotheses(self, ticker: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        return self._get_learning_rows("hypothesis_log", ticker, limit)

    def save_outcome_tracking(
        self,
        ticker: str,
        payload: dict[str, Any] | None = None,
        source: str = "genesis",
        confidence: str | float = "media",
    ) -> dict[str, Any]:
        return self._save_learning_row("outcome_tracking", ticker, payload or {}, source, confidence, "outcome_tracking")

    def get_outcome_tracking(self, ticker: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        return self._get_learning_rows("outcome_tracking", ticker, limit)

    def save_hedge_event(
        self,
        ticker: str,
        payload: dict[str, Any] | None = None,
        source: str = "hedge_engine",
        confidence: str | float = "media",
    ) -> dict[str, Any]:
        clean = {"ticker": str(ticker or "").upper(), "collection": "hedge_events", **_sanitize(payload or {})}
        return self.save_event("hedge_event", clean, source, confidence)

    def get_hedge_events(self, ticker: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        normalized = str(ticker or "").upper().strip()
        rows = self.get_recent_events(limit, "hedge_event")
        if normalized:
            return [row for row in rows if str(row.get("payload", {}).get("ticker") or "").upper() == normalized]
        return rows

    def save_strategy_profile_result(
        self,
        ticker: str,
        payload: dict[str, Any] | None = None,
        source: str = "strategy_research_lab",
        confidence: str | float = "media",
    ) -> dict[str, Any]:
        clean = {"ticker": str(ticker or "").upper(), "collection": "strategy_profile_results", **_sanitize(payload or {})}
        return self.save_event("strategy_profile_result", clean, source, confidence)

    def get_strategy_profile_results(self, ticker: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        normalized = str(ticker or "").upper().strip()
        rows = self.get_recent_events(limit, "strategy_profile_result")
        if normalized:
            return [row for row in rows if str(row.get("payload", {}).get("ticker") or "").upper() == normalized]
        return rows

    def save_asset_strategy_recommendation(
        self,
        ticker: str,
        payload: dict[str, Any] | None = None,
        source: str = "strategy_research_lab",
        confidence: str | float = "media",
    ) -> dict[str, Any]:
        clean = {"ticker": str(ticker or "").upper(), "collection": "asset_strategy_recommendations", **_sanitize(payload or {})}
        return self.save_event("asset_strategy_recommendation", clean, source, confidence)

    def get_asset_strategy_recommendations(self, ticker: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        normalized = str(ticker or "").upper().strip()
        rows = self.get_recent_events(limit, "asset_strategy_recommendation")
        if normalized:
            return [row for row in rows if str(row.get("payload", {}).get("ticker") or "").upper() == normalized]
        return rows

    def save_backtest_run(
        self,
        ticker: str,
        payload: dict[str, Any] | None = None,
        source: str = "strategy_research_lab",
        confidence: str | float = "media",
    ) -> dict[str, Any]:
        clean = {"ticker": str(ticker or "").upper(), "collection": "backtest_runs", **_sanitize(payload or {})}
        return self.save_event("backtest_run", clean, source, confidence)

    def get_backtest_runs(self, ticker: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        normalized = str(ticker or "").upper().strip()
        rows = self.get_recent_events(limit, "backtest_run")
        if normalized:
            return [row for row in rows if str(row.get("payload", {}).get("ticker") or "").upper() == normalized]
        return rows

    def save_no_edge_decision(
        self,
        ticker: str,
        payload: dict[str, Any] | None = None,
        source: str = "strategy_research_lab",
        confidence: str | float = "media",
    ) -> dict[str, Any]:
        clean = {"ticker": str(ticker or "").upper(), "collection": "no_edge_decisions", **_sanitize(payload or {})}
        return self.save_event("no_edge_decision", clean, source, confidence)

    def get_no_edge_decisions(self, ticker: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        normalized = str(ticker or "").upper().strip()
        fetch_limit = max(limit * 5, 100) if normalized else limit
        rows = self.get_recent_events(fetch_limit, "no_edge_decision")
        if normalized:
            return [row for row in rows if str(row.get("payload", {}).get("ticker") or "").upper() == normalized][:limit]
        return rows

    def save_btc_edge_result(
        self,
        ticker: str,
        payload: dict[str, Any] | None = None,
        source: str = "strategy_research_lab",
        confidence: str | float = "media",
    ) -> dict[str, Any]:
        clean = {"ticker": str(ticker or "").upper(), "collection": "btc_edge_results", **_sanitize(payload or {})}
        return self.save_event("btc_edge_result", clean, source, confidence)

    def get_btc_edge_results(self, ticker: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        normalized = str(ticker or "").upper().strip()
        fetch_limit = max(limit * 5, 100) if normalized else limit
        rows = self.get_recent_events(fetch_limit, "btc_edge_result")
        if normalized:
            return [row for row in rows if str(row.get("payload", {}).get("ticker") or "").upper() == normalized][:limit]
        return rows

    def save_btc_backtest_result(
        self,
        ticker: str,
        payload: dict[str, Any] | None = None,
        source: str = "strategy_research_lab",
        confidence: str | float = "media",
    ) -> dict[str, Any]:
        clean = {"ticker": str(ticker or "").upper(), "collection": "btc_backtest_results", **_sanitize(payload or {})}
        return self.save_event("btc_backtest_result", clean, source, confidence)

    def get_btc_backtest_results(self, ticker: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        normalized = str(ticker or "").upper().strip()
        fetch_limit = max(limit * 5, 100) if normalized else limit
        rows = self.get_recent_events(fetch_limit, "btc_backtest_result")
        if normalized:
            return [row for row in rows if str(row.get("payload", {}).get("ticker") or "").upper() == normalized][:limit]
        return rows

    def save_mt5_event(
        self,
        collection: str,
        symbol: str,
        payload: dict[str, Any] | None = None,
        source: str = "mt5_bridge",
        confidence: str | float = "media",
    ) -> dict[str, Any]:
        clean_collection = str(collection or "mt5_journal").strip()[:80]
        raw_payload = _sanitize(payload or {})
        clean_symbol = str(symbol or raw_payload.get("symbol") or raw_payload.get("ticker") or "").upper().strip()
        enriched = enrich_payload({**raw_payload, "symbol": clean_symbol})
        clean = {
            **enriched,
            "symbol": clean_symbol,
            "original_symbol": str(enriched.get("original_symbol") or raw_payload.get("original_symbol") or raw_payload.get("symbol") or clean_symbol).upper().strip(),
            "normalized_symbol": str(enriched.get("normalized_symbol") or clean_symbol).upper().strip(),
            "collection": clean_collection,
            "broker_touched": False,
            "order_executed": False,
        }
        return self.save_event(_mt5_event_type(clean_collection), clean, source, confidence)

    def get_mt5_events(self, collection: str | None = None, symbol: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        event_type = _mt5_event_type(collection) if collection else None
        normalized = str(symbol or "").upper().strip()
        aliases = _mt5_symbol_aliases(normalized)
        fetch_limit = max(limit * 12, 500) if normalized else limit
        rows = self.get_recent_events(fetch_limit, event_type)
        if normalized:
            return [
                row
                for row in rows
                if _mt5_payload_symbol_match(row.get("payload") if isinstance(row.get("payload"), dict) else {}, aliases)
            ][:limit]
        return rows[:limit]

    def get_asset_learning_summary(self, ticker: str, limit: int = 12) -> dict[str, Any]:
        normalized = str(ticker or "").upper().strip()[:40]
        if not normalized:
            return {
                "ticker": "",
                "asset_memory": [],
                "signals": [],
                "alerts": [],
                "whales": [],
                "news": [],
                "decisions": [],
                "hypotheses": [],
                "outcomes": [],
                "patterns": [],
                "summary_lines": ["Aún no hay activo claro para consultar memoria."],
            }
        asset_memory = self.get_asset_memory(normalized, limit)
        signals = self.get_signal_events(normalized, limit)
        alerts = self.get_alert_memory(normalized)[:limit]
        whales = self.get_whale_memory(normalized)[:limit]
        news = self.get_news_events(normalized, limit)
        decisions = self.get_decision_notes(normalized, limit)
        hypotheses = self.get_hypotheses(normalized, limit)
        outcomes = self.get_outcome_tracking(normalized, limit)
        patterns = [
            row
            for row in self.get_learned_context(limit * 3)
            if normalized in str(row.get("key") or "").upper()
            or normalized in json.dumps(row.get("value") or {}, ensure_ascii=False).upper()
        ][:limit]
        summary_lines = _asset_summary_lines(normalized, asset_memory, signals, alerts, whales, news, decisions, hypotheses, outcomes, patterns)
        return {
            "ticker": normalized,
            "asset_memory": asset_memory,
            "signals": signals,
            "alerts": alerts,
            "whales": whales,
            "news": news,
            "decisions": decisions,
            "hypotheses": hypotheses,
            "outcomes": outcomes,
            "patterns": patterns,
            "summary_lines": summary_lines,
            "counts": {
                "asset_memory": len(asset_memory),
                "signals": len(signals),
                "alerts": len(alerts),
                "whales": len(whales),
                "news": len(news),
                "decisions": len(decisions),
                "hypotheses": len(hypotheses),
                "outcomes": len(outcomes),
            },
        }

    def save_genesis_conversation_summary(self, summary: str) -> None:
        self.save_event("conversation_summary", {"summary": summary}, "genesis", "media")

    def get_relevant_memory(self, query: str) -> list[dict[str, Any]]:
        text = str(query or "").casefold()
        events = self.get_recent_events(80)
        domain_events = (
            self.get_asset_memory(limit=20)
            + self.get_signal_events(limit=20)
            + self.get_news_events(limit=20)
            + self.get_decision_notes(limit=20)
            + self.get_hypotheses(limit=20)
            + self.get_outcome_tracking(limit=20)
        )
        if not text:
            return (domain_events + events)[:10]
        return [
            event
            for event in (domain_events + events)
            if text in json.dumps(event.get("payload", {}), ensure_ascii=False).casefold()
            or text in str(event.get("event_type", "")).casefold()
            or text in str(event.get("ticker", "")).casefold()
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
            "asset_memory": self.get_asset_memory(limit=8),
            "signal_events": self.get_signal_events(limit=8),
            "news_events": self.get_news_events(limit=8),
            "decision_notes": self.get_decision_notes(limit=8),
            "hypothesis_log": self.get_hypotheses(limit=8),
            "outcome_tracking": self.get_outcome_tracking(limit=8),
            "relevant": self.get_relevant_memory(query) if query else [],
        }

    def _save_learning_row(
        self,
        table_key: str,
        ticker: str,
        payload: dict[str, Any],
        source: str,
        confidence: str | float,
        default_event_type: str,
    ) -> dict[str, Any]:
        table = _learning_table_name(table_key)
        clean_payload = _sanitize(payload or {})
        normalized = str(ticker or clean_payload.get("ticker") or "").upper().strip()[:40]
        now = datetime.now(timezone.utc).isoformat()
        record = {
            "id": str(clean_payload.get("id") or clean_payload.get("event_id") or "")[:180],
            "event_type": str(clean_payload.get("event_type") or default_event_type)[:80],
            "ticker": normalized,
            "asset_name": str(clean_payload.get("asset_name") or clean_payload.get("name") or "")[:240],
            "timestamp": str(clean_payload.get("timestamp") or clean_payload.get("created_at") or clean_payload.get("published_at") or now)[:80],
            "source": str(clean_payload.get("source") or source or "genesis")[:80],
            "confidence": str(clean_payload.get("confidence") or confidence or "media")[:40],
            "raw_data_sanitized": clean_payload.get("raw_data_sanitized") or clean_payload.get("raw_data") or {},
            "genesis_reading": str(clean_payload.get("genesis_reading") or clean_payload.get("genesis_reading_es") or clean_payload.get("summary") or clean_payload.get("thesis") or "")[:4000],
            "expected_direction": str(clean_payload.get("expected_direction") or clean_payload.get("direction") or clean_payload.get("direction_estimate") or "")[:80],
            "expected_impact": str(clean_payload.get("expected_impact") or clean_payload.get("impact") or "")[:120],
            "actual_outcome_1h": clean_payload.get("actual_outcome_1h"),
            "actual_outcome_24h": clean_payload.get("actual_outcome_24h"),
            "actual_outcome_7d": clean_payload.get("actual_outcome_7d"),
            "status": str(clean_payload.get("status") or "watching")[:40],
            "created_at": str(clean_payload.get("created_at") or now)[:80],
            "updated_at": now,
        }
        record["payload"] = {**clean_payload, **{key: value for key, value in record.items() if key != "payload"}}
        record["event_id"] = record["id"] or _stable_learning_id(table_key, record["payload"])
        params = (
            record["event_id"],
            record["event_type"],
            record["ticker"],
            record["asset_name"],
            record["source"],
            record["confidence"],
            record["expected_direction"],
            record["expected_impact"],
            record["status"],
            json.dumps(record["payload"]),
            record["created_at"],
            record["updated_at"],
        )
        if self.backend == "postgres" and self._pg is not None:
            self._pg_execute(
                f"""
                INSERT INTO {table} (event_id, event_type, ticker, asset_name, source, confidence, expected_direction, expected_impact, status, payload_json, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO UPDATE SET
                    event_type = EXCLUDED.event_type,
                    ticker = EXCLUDED.ticker,
                    asset_name = EXCLUDED.asset_name,
                    source = EXCLUDED.source,
                    confidence = EXCLUDED.confidence,
                    expected_direction = EXCLUDED.expected_direction,
                    expected_impact = EXCLUDED.expected_impact,
                    status = EXCLUDED.status,
                    payload_json = EXCLUDED.payload_json,
                    updated_at = EXCLUDED.updated_at
                """,
                params,
            )
        else:
            with closing(self._sqlite()) as conn:
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO {table} (event_id, event_type, ticker, asset_name, source, confidence, expected_direction, expected_impact, status, payload_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    params,
                )
                conn.commit()
        return {key: value for key, value in record.items() if key != "id"}

    def _get_learning_rows(self, table_key: str, ticker: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        table = _learning_table_name(table_key)
        safe_limit = max(1, min(int(limit or 30), 200))
        normalized = str(ticker or "").upper().strip()[:40]
        if self.backend == "postgres" and self._pg is not None:
            if normalized:
                rows = self._pg_fetch(
                    f"SELECT event_id, event_type, ticker, asset_name, source, confidence, expected_direction, expected_impact, status, payload_json, created_at, updated_at FROM {table} WHERE ticker = %s ORDER BY updated_at DESC LIMIT %s",
                    (normalized, safe_limit),
                )
            else:
                rows = self._pg_fetch(
                    f"SELECT event_id, event_type, ticker, asset_name, source, confidence, expected_direction, expected_impact, status, payload_json, created_at, updated_at FROM {table} ORDER BY updated_at DESC LIMIT %s",
                    (safe_limit,),
                )
        else:
            with closing(self._sqlite()) as conn:
                if normalized:
                    rows = conn.execute(
                        f"SELECT event_id, event_type, ticker, asset_name, source, confidence, expected_direction, expected_impact, status, payload_json, created_at, updated_at FROM {table} WHERE ticker = ? ORDER BY updated_at DESC LIMIT ?",
                        (normalized, safe_limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        f"SELECT event_id, event_type, ticker, asset_name, source, confidence, expected_direction, expected_impact, status, payload_json, created_at, updated_at FROM {table} ORDER BY updated_at DESC LIMIT ?",
                        (safe_limit,),
                    ).fetchall()
        return [_learning_row(table_key, row) for row in rows]

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
            for table in _LEARNING_TABLES.values():
                self._pg_execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        event_id TEXT PRIMARY KEY,
                        event_type TEXT NOT NULL,
                        ticker TEXT NOT NULL,
                        asset_name TEXT NOT NULL,
                        source TEXT NOT NULL,
                        confidence TEXT NOT NULL,
                        expected_direction TEXT NOT NULL,
                        expected_impact TEXT NOT NULL,
                        status TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
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
            for table in _LEARNING_TABLES.values():
                conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        event_id TEXT PRIMARY KEY,
                        event_type TEXT NOT NULL,
                        ticker TEXT NOT NULL,
                        asset_name TEXT NOT NULL,
                        source TEXT NOT NULL,
                        confidence TEXT NOT NULL,
                        expected_direction TEXT NOT NULL,
                        expected_impact TEXT NOT NULL,
                        status TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
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


def _learning_table_name(table_key: str) -> str:
    try:
        return _LEARNING_TABLES[table_key]
    except KeyError as exc:
        raise ValueError(f"Unknown learning table: {table_key}") from exc


def _stable_learning_id(table_key: str, payload: dict[str, Any]) -> str:
    basis = {
        "table": table_key,
        "event_type": payload.get("event_type"),
        "ticker": payload.get("ticker"),
        "source": payload.get("source"),
        "timestamp": payload.get("timestamp") or payload.get("published_at") or payload.get("date") or payload.get("created_at"),
        "title": payload.get("title") or payload.get("title_es") or payload.get("verdict") or payload.get("summary"),
        "price": payload.get("price_at_decision") or payload.get("current_price") or payload.get("price"),
    }
    digest = hashlib.sha1(json.dumps(_sanitize(basis), sort_keys=True, ensure_ascii=True).encode("utf-8")).hexdigest()
    return f"{table_key}:{digest[:24]}"


def _learning_row(table_key: str, row: Any) -> dict[str, Any]:
    payload = _loads(row[9])
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("event_id", row[0])
    payload.setdefault("event_type", row[1])
    payload.setdefault("ticker", row[2])
    payload.setdefault("asset_name", row[3])
    payload.setdefault("source", row[4])
    payload.setdefault("confidence", row[5])
    payload.setdefault("expected_direction", row[6])
    payload.setdefault("expected_impact", row[7])
    payload.setdefault("status", row[8])
    payload.setdefault("created_at", row[10])
    payload.setdefault("updated_at", row[11])
    return {
        "collection": table_key,
        "event_id": row[0],
        "event_type": row[1],
        "ticker": row[2],
        "asset_name": row[3],
        "source": row[4],
        "confidence": row[5],
        "expected_direction": row[6],
        "expected_impact": row[7],
        "status": row[8],
        "payload": payload,
        "created_at": row[10],
        "updated_at": row[11],
    }


def _asset_summary_lines(
    ticker: str,
    asset_memory: list[dict[str, Any]],
    signals: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
    whales: list[dict[str, Any]],
    news: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
    hypotheses: list[dict[str, Any]],
    outcomes: list[dict[str, Any]],
    patterns: list[dict[str, Any]],
) -> list[str]:
    lines: list[str] = []
    if decisions:
        verdict = decisions[0].get("payload", {}).get("verdict") or decisions[0].get("payload", {}).get("decision")
        price = decisions[0].get("payload", {}).get("price_at_decision")
        price_text = f" en {price}" if price is not None else ""
        lines.append(f"Última decisión sobre {ticker}: {verdict or 'vigilar'}{price_text}.")
    if signals or alerts:
        lines.append(f"Tengo {len(signals) + len(alerts)} señales/alertas recientes guardadas para {ticker}.")
    if news:
        title = news[0].get("payload", {}).get("title_es") or news[0].get("payload", {}).get("title")
        lines.append(f"Noticia relevante mas reciente: {title or 'catalizador guardado'}.")
    if whales:
        event = whales[0].get("payload", {})
        event_type = event.get("event_type") or whales[0].get("event_type")
        if event_type == "whale_confirmed":
            lines.append(f"Hay una lectura de ballena confirmada guardada para {ticker}; revisa fuente y monto antes de actuar.")
        else:
            lines.append(f"Hay flujo smart money estimado guardado para {ticker}; no lo trato como compra confirmada.")
    if hypotheses:
        hypothesis = hypotheses[0].get("payload", {}).get("hypothesis") or hypotheses[0].get("payload", {}).get("genesis_reading")
        hypothesis = _clean_spanish_text(str(hypothesis or ""))
        lines.append(f"Hipótesis activa: {hypothesis or 'vigilar confirmación de precio y volumen'}.")
    if outcomes:
        status = outcomes[0].get("payload", {}).get("status") or outcomes[0].get("status")
        lines.append(f"Seguimiento de resultado: {status or 'abierto'}.")
    if patterns:
        lines.append(f"Genesis encontró {len(patterns)} patrones o contextos aprendidos relacionados con {ticker}.")
    if asset_memory and not lines:
        lines.append(f"{ticker} ya tiene memoria de analisis, pero faltan resultados cerrados para afirmar un patron.")
    if not lines:
        lines.append(f"Aún no tengo suficiente historial de {ticker}; desde ahora guardo tesis, señales, noticias y outcomes.")
    return lines[:6]


def _clean_spanish_text(value: str) -> str:
    return (
        str(value or "")
        .replace("confirmacion", "confirmación")
        .replace("direccion", "dirección")
        .replace("senal", "señal")
        .replace("senales", "señales")
        .replace("patron", "patrón")
    )


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


def _is_tracked_entity_noise(ticker: object) -> bool:
    return str(ticker or "").strip().upper() in _TRACKED_ENTITY_BLOCKLIST


def _mt5_event_type(collection: str | None) -> str:
    mapping = {
        "mt5_signals": "mt5_signal",
        "mt5_decisions": "mt5_decision",
        "mt5_order_requests": "mt5_order_request",
        "mt5_order_results": "mt5_order_result",
        "mt5_backtest_runs": "mt5_backtest_run",
        "mt5_forward_tests": "mt5_forward_test",
        "mt5_ticks": "mt5_tick",
        "mt5_shadow_trades": "mt5_shadow_trade",
        "mt5_signal_outcomes": "mt5_signal_outcome",
        "mt5_forward_metrics": "mt5_forward_metric",
        "mt5_no_trade_outcomes": "mt5_no_trade_outcome",
        "mt5_no_trade_evaluations": "mt5_no_trade_evaluation",
        "mt5_hedge_outcomes": "mt5_hedge_outcome",
        "mt5_risk_blocks": "mt5_risk_block",
        "mt5_journal": "mt5_journal",
        "mt5_account_sync": "mt5_account_sync",
        "mt5_replay_runs": "mt5_replay_run",
        "mt5_replay_shadow_trades": "mt5_replay_shadow_trade",
        "mt5_trade_memory": "mt5_trade_memory",
        "mt5_trade_lessons": "mt5_trade_lesson",
        "mt5_strategy_profile_stats": "mt5_strategy_profile_stat",
        "mt5_adaptive_state": "mt5_adaptive_state",
        "mt5_adaptive_recommendations": "mt5_adaptive_recommendation",
        "mt5_learning_runs": "mt5_learning_run",
    }
    clean = str(collection or "mt5_journal").strip()
    return mapping.get(clean, clean.rstrip("s") or "mt5_event")


def _mt5_normalized_symbol(symbol: object) -> str:
    return normalize_mt5_symbol(symbol)


def _mt5_symbol_aliases(symbol: object) -> set[str]:
    return symbol_aliases(symbol)


def _mt5_payload_symbol_match(payload: dict[str, Any], aliases: set[str]) -> bool:
    if not aliases:
        return True
    return any(payload_matches_symbol(payload, alias) for alias in aliases)


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
