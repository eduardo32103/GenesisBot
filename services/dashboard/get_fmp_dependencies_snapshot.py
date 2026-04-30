from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.settings import load_settings

_ROOT_DIR = Path(__file__).resolve().parents[2]
_FMP_SNAPSHOT_PATH = _ROOT_DIR / "infra" / "runtime" / "fmp_snapshot.json"
_USAGE_KINDS = ("quote", "eod", "intraday", "news")
_USAGE_FIELDS = ("fetch", "ok", "cache_hit", "throttle", "quota", "access", "no_data", "upstream", "no_key", "bytes")


def _redact_fmp_secret_text(value: Any, api_key: str = "") -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if api_key:
        text = text.replace(api_key, "[credencial_oculta]")
    text = re.sub(r"([?&])apikey=[^&\s]+", r"\1credencial_oculta", text, flags=re.IGNORECASE)
    text = re.sub(r"\bapikey\s*=\s*[^&\s]+", "credencial_oculta", text, flags=re.IGNORECASE)
    return text


def _provider_note_for_settings(settings: Any) -> str:
    if getattr(settings, "fmp_api_key", "") and getattr(settings, "fmp_live_enabled", False):
        return "Datos directos activos para consultas manuales. Sin lectura guardada todavia."
    if getattr(settings, "fmp_api_key", ""):
        return "Fuente de mercado configurada. Datos directos apagados por FMP_LIVE_ENABLED."
    return "No hay lectura de mercado persistida todavia. El dashboard no consulta la fuente por su cuenta."


def _security_status() -> dict[str, bool]:
    return {
        "secret_exposed": False,
        "apikey_param_exposed": False,
    }


def _empty_usage_bucket() -> dict[str, int]:
    return {field: 0 for field in _USAGE_FIELDS}


def _empty_snapshot(note: str, *, key_configured: bool, live_enabled: bool) -> dict[str, Any]:
    usage = {kind: _empty_usage_bucket() for kind in _USAGE_KINDS}
    live_ready = bool(key_configured and live_enabled)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "provider": {
            "status": "OK" if live_ready else "DEGRADED",
            "degraded": not live_ready,
            "key_configured": bool(key_configured),
            "live_enabled": bool(live_enabled),
            "live_ready": live_ready,
            "note": note,
            "summary_window_seconds": 0,
        },
        "usage": usage,
        "signals": {
            "cache_hit": 0,
            "throttle": 0,
            "cooldown_active": 0,
            "quota": 0,
            "access": 0,
            "cooldown_breakdown": {
                "active": 0,
                "quota": 0,
                "access": 0,
                "upstream": 0,
                "no_key": 0,
            },
        },
        "last_incident": {},
        "meta": {
            "source": "unavailable",
            "snapshot_path": str(_FMP_SNAPSHOT_PATH),
        },
        "security": _security_status(),
    }


def _normalize_usage(raw_usage: Any) -> dict[str, dict[str, int]]:
    normalized: dict[str, dict[str, int]] = {}
    source = raw_usage if isinstance(raw_usage, dict) else {}
    for kind in _USAGE_KINDS:
        values = source.get(kind) if isinstance(source, dict) else {}
        bucket = _empty_usage_bucket()
        if isinstance(values, dict):
            for field in _USAGE_FIELDS:
                try:
                    bucket[field] = int(values.get(field, 0) or 0)
                except Exception:
                    bucket[field] = 0
        normalized[kind] = bucket
    return normalized


def _normalize_signals(raw_signals: Any, usage: dict[str, dict[str, int]]) -> dict[str, Any]:
    signals = raw_signals if isinstance(raw_signals, dict) else {}
    cooldown_breakdown = signals.get("cooldown_breakdown") if isinstance(signals.get("cooldown_breakdown"), dict) else {}
    return {
        "cache_hit": int(signals.get("cache_hit", sum(bucket["cache_hit"] for bucket in usage.values())) or 0),
        "throttle": int(signals.get("throttle", sum(bucket["throttle"] for bucket in usage.values())) or 0),
        "cooldown_active": int(signals.get("cooldown_active", cooldown_breakdown.get("active", 0)) or 0),
        "quota": int(signals.get("quota", sum(bucket["quota"] for bucket in usage.values())) or 0),
        "access": int(signals.get("access", sum(bucket["access"] for bucket in usage.values())) or 0),
        "cooldown_breakdown": {
            "active": int(cooldown_breakdown.get("active", 0) or 0),
            "quota": int(cooldown_breakdown.get("quota", 0) or 0),
            "access": int(cooldown_breakdown.get("access", 0) or 0),
            "upstream": int(cooldown_breakdown.get("upstream", 0) or 0),
            "no_key": int(cooldown_breakdown.get("no_key", 0) or 0),
        },
    }


def _normalize_last_incident(raw_incident: Any, api_key: str = "") -> dict[str, Any]:
    incident = raw_incident if isinstance(raw_incident, dict) else {}
    return {
        "category": _redact_fmp_secret_text(incident.get("category"), api_key).upper(),
        "ticker": str(incident.get("ticker") or "").strip().upper(),
        "status_code": incident.get("status_code"),
        "detail": _redact_fmp_secret_text(incident.get("detail"), api_key),
        "updated_at": str(incident.get("updated_at") or "").strip(),
    }


def get_fmp_dependencies_snapshot() -> dict[str, Any]:
    settings = load_settings()
    if not _FMP_SNAPSHOT_PATH.exists():
        return _empty_snapshot(
            _provider_note_for_settings(settings),
            key_configured=bool(settings.fmp_api_key),
            live_enabled=bool(settings.fmp_live_enabled),
        )

    try:
        raw = json.loads(_FMP_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        return _empty_snapshot(
            f"No pude leer la lectura de mercado persistida: {exc}",
            key_configured=bool(settings.fmp_api_key),
            live_enabled=bool(settings.fmp_live_enabled),
        )

    usage = _normalize_usage(raw.get("usage"))
    signals = _normalize_signals(raw.get("signals"), usage)
    provider = raw.get("provider") if isinstance(raw.get("provider"), dict) else {}
    last_incident = _normalize_last_incident(raw.get("last_incident"), settings.fmp_api_key)
    generated_at = str(raw.get("generated_at") or "").strip() or datetime.now(timezone.utc).isoformat()

    return {
        "generated_at": generated_at,
        "provider": {
            "status": str(provider.get("status") or "DEGRADED").strip().upper() or "DEGRADED",
            "degraded": bool(provider.get("degraded", True)),
            "key_configured": bool(settings.fmp_api_key),
            "live_enabled": bool(settings.fmp_live_enabled),
            "live_ready": bool(settings.fmp_api_key and settings.fmp_live_enabled),
            "note": _redact_fmp_secret_text(provider.get("note") or "Sin nota persistida del proveedor.", settings.fmp_api_key),
            "summary_window_seconds": int(provider.get("summary_window_seconds", 0) or 0),
        },
        "usage": usage,
        "signals": signals,
        "last_incident": last_incident,
        "meta": {
            "source": _redact_fmp_secret_text((raw.get("meta") or {}).get("source") or "runtime_snapshot", settings.fmp_api_key) or "runtime_snapshot",
            "snapshot_path": _redact_fmp_secret_text((raw.get("meta") or {}).get("snapshot_path") or _FMP_SNAPSHOT_PATH, settings.fmp_api_key),
        },
        "security": _security_status(),
    }
