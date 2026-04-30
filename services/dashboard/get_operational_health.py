from __future__ import annotations

import json
import os
import ssl
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pg8000.dbapi

from app.settings import load_settings

_ROOT_DIR = Path(__file__).resolve().parents[2]
_PORTFOLIO_FALLBACK_PATH = _ROOT_DIR / "portfolio.json"
_BOT_LOCK_NAME = "telegram_leader"


def _safe_iso(raw_value: Any) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return ""
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).isoformat()
    except Exception:
        return text


def _heartbeat_age_seconds(raw_iso: str) -> int | None:
    if not raw_iso:
        return None
    try:
        dt = datetime.fromisoformat(raw_iso.replace("Z", "+00:00"))
        return max(int((datetime.now(timezone.utc) - dt).total_seconds()), 0)
    except Exception:
        return None


def _connect_database(database_url: str):
    if not database_url:
        return None

    parsed = urllib.parse.urlparse(database_url)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return pg8000.dbapi.connect(
        user=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port or 6543,
        database=(parsed.path or "/")[1:],
        ssl_context=ctx,
        timeout=8,
    )


def _fetch_runtime_snapshot(database_url: str) -> dict[str, Any]:
    conn = None
    try:
        conn = _connect_database(database_url)
        if not conn:
            return {}

        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT instance_id, hostname, pid, stage, notes, last_heartbeat, claimed_at
            FROM runtime_locks
            WHERE lock_name=%s
            """,
            (_BOT_LOCK_NAME,),
        )
        row = cursor.fetchone()
        conn.commit()
        if not row:
            return {}

        return {
            "instance_id": row[0],
            "hostname": row[1],
            "pid": row[2],
            "stage": row[3],
            "notes": row[4],
            "last_heartbeat": _safe_iso(row[5]),
            "claimed_at": _safe_iso(row[6]),
        }
    except Exception:
        return {}
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _fetch_radar_size(database_url: str) -> int:
    conn = None
    try:
        conn = _connect_database(database_url)
        if not conn:
            return 0

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM wallet")
        row = cursor.fetchone()
        conn.commit()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _fallback_radar_size() -> int:
    if not _PORTFOLIO_FALLBACK_PATH.exists():
        return 0
    try:
        raw = json.loads(_PORTFOLIO_FALLBACK_PATH.read_text(encoding="utf-8"))
    except Exception:
        return 0

    if isinstance(raw, list):
        return len(raw)
    if isinstance(raw, dict):
        if isinstance(raw.get("tickers"), list):
            return len(raw["tickers"])
        if isinstance(raw.get("positions"), list):
            return len(raw["positions"])
        return len(raw)
    return 0


def _system_state(stage: str, bot_configured: bool, has_database: bool) -> tuple[str, str]:
    normalized = str(stage or "").strip().lower()
    if not bot_configured:
        return "degraded", "Faltan credenciales de Telegram en el entorno."
    if normalized.startswith("boot") or normalized in {"grace", "restore", "smc_warmup", "geo_warmup"}:
        return "booting", "El runtime sigue completando el warmup inicial."
    if normalized in {"esperando_lock", "force_takeover", "lock_lost"}:
        return "degraded", "El bot está resolviendo liderazgo o takeover de Telegram."
    if normalized in {"polling", "processing_update"}:
        return "online", "El bot está operativo y procesando tráfico normal."
    if has_database:
        return "online", "El baseline está operativo con persistencia activa."
    return "limited", "El bot puede operar, pero sin confirmación completa de persistencia."


def _provider_note(has_fmp_key: bool) -> tuple[str, bool]:
    if not has_fmp_key:
        return "FMP_API_KEY no está configurada. El sistema queda degradado para datos de mercado.", True
    return "Estado detallado del proveedor se integrará en Fase 3.5. Este módulo solo expone salud operativa base.", False


def get_operational_health() -> dict[str, Any]:
    settings = load_settings()
    runtime = _fetch_runtime_snapshot(settings.database_url)
    radar_size = _fetch_radar_size(settings.database_url) or _fallback_radar_size()
    stage = runtime.get("stage") or "unknown"
    system_status, system_note = _system_state(stage, settings.has_telegram, settings.has_database)
    provider_note, provider_degraded = _provider_note(bool(settings.fmp_api_key))
    last_update = runtime.get("last_heartbeat") or runtime.get("claimed_at") or ""
    heartbeat_age = _heartbeat_age_seconds(last_update)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "system": {
            "status": system_status,
            "summary": system_note,
            "last_update": last_update,
        },
        "bot": {
            "configured": settings.has_telegram,
            "leader": runtime.get("instance_id") or "sin registro",
            "hostname": runtime.get("hostname") or os.getenv("HOSTNAME", "local"),
            "pid": runtime.get("pid") or "n/a",
            "boot_stage": stage,
            "heartbeat_age_seconds": heartbeat_age,
            "runtime_note": runtime.get("notes") or "",
        },
        "radar": {
            "size": radar_size,
        },
        "provider": {
            "fmp_key_configured": bool(settings.fmp_api_key),
            "degraded": provider_degraded,
            "note": provider_note,
        },
    }
