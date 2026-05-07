from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.settings import load_settings
from services.dashboard.get_operational_health import _connect_database
from services.portfolio.get_portfolio_snapshot import normalize_portfolio_positions

_ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_PORTFOLIO_PATH = _ROOT_DIR / "portfolio.json"
DEFAULT_OWNER_ID = "dashboard_web"


class PortfolioStore:
    def __init__(
        self,
        *,
        owner_id: str = DEFAULT_OWNER_ID,
        database_url: str | None = None,
        fallback_path: Path | None = None,
    ) -> None:
        settings = load_settings()
        self.owner_id = str(owner_id or DEFAULT_OWNER_ID).strip() or DEFAULT_OWNER_ID
        self.database_url = database_url if database_url is not None else settings.database_url
        self.fallback_path = Path(fallback_path or DEFAULT_PORTFOLIO_PATH)
        self.backend = "json"
        self._conn = None
        if self.database_url:
            try:
                self._conn = _connect_database(self.database_url)
                if self._conn is not None:
                    self.backend = "postgres"
                    self._ensure_schema()
                    self._migrate_json_if_empty()
            except Exception:
                self._conn = None
                self.backend = "json"

    @property
    def durable(self) -> bool:
        return self.backend == "postgres" and self._conn is not None

    def status(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "durable": self.durable,
            "fallback_path": str(self.fallback_path),
        }

    def close(self) -> None:
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass

    def read_raw(self) -> dict[str, Any]:
        if self.durable:
            try:
                cursor = self._conn.cursor()
                cursor.execute(
                    """
                    SELECT payload_json, updated_at
                    FROM genesis_portfolio_positions
                    WHERE owner_id = %s
                    ORDER BY ticker
                    """,
                    (self.owner_id,),
                )
                rows = cursor.fetchall() or []
                cursor.close()
                positions = [_loads(row[0]) for row in rows]
                updated = max([str(row[1] or "") for row in rows if row and row[1]], default="")
                return {"owner_id": self.owner_id, "updated_at": updated, "positions": positions}
            except Exception:
                return self._read_json()
        return self._read_json()

    def write_positions(self, positions: list[dict[str, Any]], raw: dict[str, Any] | None = None) -> None:
        payload = {
            "owner_id": (raw or {}).get("owner_id", self.owner_id),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "positions": sorted(positions, key=lambda item: str(item.get("ticker") or "")),
        }
        if self.durable:
            self._write_postgres(payload)
            return
        self.fallback_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    def _write_postgres(self, payload: dict[str, Any]) -> None:
        positions = [item for item in payload.get("positions") or [] if isinstance(item, dict) and item.get("ticker")]
        tickers = [str(item.get("ticker") or "").strip().upper() for item in positions]
        cursor = self._conn.cursor()
        if tickers:
            placeholders = ", ".join(["%s"] * len(tickers))
            cursor.execute(
                f"DELETE FROM genesis_portfolio_positions WHERE owner_id = %s AND ticker NOT IN ({placeholders})",
                tuple([self.owner_id, *tickers]),
            )
        else:
            cursor.execute("DELETE FROM genesis_portfolio_positions WHERE owner_id = %s", (self.owner_id,))

        for item in positions:
            ticker = str(item.get("ticker") or "").strip().upper()
            clean = {**item, "ticker": ticker}
            cursor.execute(
                """
                INSERT INTO genesis_portfolio_positions (owner_id, ticker, payload_json, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (owner_id, ticker) DO UPDATE SET
                    payload_json = EXCLUDED.payload_json,
                    updated_at = EXCLUDED.updated_at
                """,
                (self.owner_id, ticker, json.dumps(clean, ensure_ascii=False), payload["updated_at"]),
            )
        self._conn.commit()
        cursor.close()

    def _ensure_schema(self) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS genesis_portfolio_positions (
                owner_id TEXT NOT NULL,
                ticker TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (owner_id, ticker)
            )
            """,
            (),
        )
        self._conn.commit()
        cursor.close()

    def _migrate_json_if_empty(self) -> None:
        if not self.fallback_path.exists():
            return
        cursor = self._conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM genesis_portfolio_positions WHERE owner_id = %s", (self.owner_id,))
        row = cursor.fetchone()
        cursor.close()
        if row and int(row[0] or 0) > 0:
            return
        raw = self._read_json()
        positions = _positions_for_store(raw)
        if positions:
            self.write_positions(positions, raw)

    def _read_json(self) -> dict[str, Any]:
        if not self.fallback_path.exists():
            return {"owner_id": self.owner_id, "positions": []}
        try:
            raw = json.loads(self.fallback_path.read_text(encoding="utf-8"))
        except Exception:
            return {"owner_id": self.owner_id, "positions": []}
        return raw if isinstance(raw, dict) else {"owner_id": self.owner_id, "positions": raw if isinstance(raw, list) else []}


def _loads(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        loaded = json.loads(str(value or "{}"))
    except Exception:
        loaded = {}
    return loaded if isinstance(loaded, dict) else {}


def _positions_for_store(raw: dict[str, Any]) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    for position in normalize_portfolio_positions(raw):
        shaped = {
            "ticker": position["ticker"],
            "display_name": position.get("display_name") or position["ticker"],
        }
        if position.get("watchlist"):
            shaped["watchlist"] = True
        if position.get("removed_watchlist"):
            shaped["removed_watchlist"] = True
        for key in ("units", "entry_price", "amount_usd", "reference_price", "mode", "opened_at"):
            value = position.get(key)
            if value not in (None, "", 0, 0.0):
                shaped[key] = value
        positions.append(shaped)
    return positions
