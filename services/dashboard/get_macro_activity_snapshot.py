from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT_DIR = Path(__file__).resolve().parents[2]
_MACRO_SNAPSHOT_PATH = _ROOT_DIR / "infra" / "runtime" / "macro_snapshot.json"
_ACTIVITY_SNAPSHOT_PATH = _ROOT_DIR / "infra" / "runtime" / "activity_snapshot.json"


def _empty_macro_snapshot(note: str) -> dict[str, Any]:
    return {
        "available": False,
        "note": note,
        "last_update": "",
        "sentiment": {
            "raw": 0.0,
            "label": "Neutral",
            "icon": "N/D",
            "bull_pct": 50,
            "bear_pct": 50,
        },
        "bias_label": "macro mixto",
        "confidence": 0,
        "summary": "Sin contexto macro persistido todavía.",
        "dominant_risk": "",
        "high_risk_tickers": [],
        "sensitive_tickers": [],
        "headlines": [],
    }


def _empty_activity_snapshot(note: str) -> dict[str, Any]:
    return {
        "note": note,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "items": [],
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _normalize_macro(raw: Any) -> dict[str, Any]:
    macro = raw if isinstance(raw, dict) else {}
    sentiment = macro.get("sentiment") if isinstance(macro.get("sentiment"), dict) else {}
    headlines = macro.get("headlines") if isinstance(macro.get("headlines"), list) else []
    return {
        "available": bool(macro.get("available", False)),
        "note": str(macro.get("note") or "Sin snapshot macro disponible.").strip(),
        "last_update": str(macro.get("last_update") or "").strip(),
        "sentiment": {
            "raw": float(sentiment.get("raw", 0.0) or 0.0),
            "label": str(sentiment.get("label") or "Neutral").strip(),
            "icon": str(sentiment.get("icon") or "N/D").strip(),
            "bull_pct": int(sentiment.get("bull_pct", 50) or 50),
            "bear_pct": int(sentiment.get("bear_pct", 50) or 50),
        },
        "bias_label": str(macro.get("bias_label") or "macro mixto").strip(),
        "confidence": int(macro.get("confidence", 0) or 0),
        "summary": str(macro.get("summary") or "Sin contexto macro persistido todavía.").strip(),
        "dominant_risk": str(macro.get("dominant_risk") or "").strip(),
        "high_risk_tickers": [str(item or "").strip().upper() for item in macro.get("high_risk_tickers", []) if str(item or "").strip()],
        "sensitive_tickers": [str(item or "").strip().upper() for item in macro.get("sensitive_tickers", []) if str(item or "").strip()],
        "headlines": [
            {
                "title": str(item.get("title") or "").strip(),
                "source": str(item.get("source") or "").strip(),
                "published_at": str(item.get("published_at") or "").strip(),
                "impact_summary": str(item.get("impact_summary") or "").strip(),
            }
            for item in headlines[:4]
            if isinstance(item, dict) and str(item.get("title") or "").strip()
        ],
    }


def _normalize_activity(raw: Any) -> dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    normalized_items = []
    for item in items[:12]:
        if not isinstance(item, dict):
            continue
        normalized_items.append(
            {
                "event": str(item.get("event") or "").strip().upper(),
                "level": str(item.get("level") or "info").strip().lower(),
                "occurred_at": str(item.get("occurred_at") or "").strip(),
                "summary": str(item.get("summary") or "Sin detalle adicional.").strip(),
                "fields": item.get("fields") if isinstance(item.get("fields"), dict) else {},
            }
        )
    return {
        "note": str(payload.get("note") or "Sin actividad operativa persistida todavía.").strip(),
        "generated_at": str(payload.get("generated_at") or "").strip(),
        "items": normalized_items,
    }


def get_macro_activity_snapshot() -> dict[str, Any]:
    macro_payload = _read_json(_MACRO_SNAPSHOT_PATH) if _MACRO_SNAPSHOT_PATH.exists() else {}
    activity_payload = _read_json(_ACTIVITY_SNAPSHOT_PATH) if _ACTIVITY_SNAPSHOT_PATH.exists() else {}

    macro = _normalize_macro((macro_payload or {}).get("macro"))
    if not macro_payload:
        macro = _empty_macro_snapshot("Todavía no hay snapshot macro persistido desde el runtime.")

    activity = _normalize_activity(activity_payload)
    if not activity_payload:
        activity = _empty_activity_snapshot("Todavía no hay actividad operativa persistida desde el runtime.")

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "macro": macro,
        "activity": activity,
        "meta": {
            "macro_source": "runtime_snapshot" if macro_payload else "unavailable",
            "activity_source": "runtime_snapshot" if activity_payload else "unavailable",
            "macro_path": str(_MACRO_SNAPSHOT_PATH),
            "activity_path": str(_ACTIVITY_SNAPSHOT_PATH),
        },
    }
