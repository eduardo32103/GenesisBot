from __future__ import annotations

import base64
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.settings import load_settings
from services.genesis.memory_store import MemoryStore
from services.genesis.ticker_parser import extract_tickers_from_prompt

_ROOT_DIR = Path(__file__).resolve().parents[2]
_UPLOAD_DIR = _ROOT_DIR / ".genesis_uploads"
_DATA_URL_PATTERN = re.compile(r"^data:(?P<mime>image/[a-zA-Z0-9.+-]+);base64,(?P<data>.+)$", re.DOTALL)


def analyze_chart_image(payload: dict[str, Any], memory: MemoryStore | None = None) -> dict[str, Any]:
    settings = load_settings()
    store = memory or MemoryStore()
    message = str(payload.get("message") or "").strip()
    image = payload.get("image") if isinstance(payload.get("image"), dict) else {}
    saved = _save_image(image)
    tickers = extract_tickers_from_prompt(message)
    configured = bool(settings.genesis_vision_enabled and settings.genesis_llm_enabled and settings.openai_api_key)

    event_payload = {
        "message": message[:240],
        "tickers": tickers,
        "image_saved": bool(saved.get("path")),
        "mime": saved.get("mime") or image.get("type") or "",
        "vision_configured": configured,
    }
    store.save_event("chart_image_analysis", event_payload, "vision", "media" if configured else "baja")

    if not configured:
        return {
            "ok": True,
            "intent": "image_chart_analysis",
            "status": "vision_not_configured",
            "answer": "Recibi la imagen, pero falta proveedor de vision configurado.",
            "tickers": tickers,
            "image": {"stored": bool(saved.get("path")), "mime": saved.get("mime") or image.get("type") or ""},
            "next_step": "Configura GENESIS_VISION_ENABLED=true, GENESIS_LLM_ENABLED=true y OPENAI_API_KEY para analisis visual.",
        }

    return {
        "ok": True,
        "intent": "image_chart_analysis",
        "status": "vision_ready",
        "answer": "Recibi la imagen. Vision esta configurada; Genesis debe combinar la lectura visual con precio FMP confirmado antes de opinar.",
        "tickers": tickers,
        "image": {"stored": bool(saved.get("path")), "mime": saved.get("mime") or image.get("type") or ""},
        "vision_policy": "No se usan precios de la imagen como verdad; se reconfirman con FMP o snapshot.",
    }


def _save_image(image: dict[str, Any]) -> dict[str, str]:
    data_url = str(image.get("data_url") or image.get("dataUrl") or "").strip()
    if not data_url:
        return {}
    match = _DATA_URL_PATTERN.match(data_url)
    if not match:
        return {}
    mime = match.group("mime")
    extension = ".png" if "png" in mime else ".jpg" if "jpeg" in mime or "jpg" in mime else ".webp" if "webp" in mime else ".img"
    try:
        raw = base64.b64decode(match.group("data"), validate=True)
    except Exception:
        return {}
    if not raw or len(raw) > 5_000_000:
        return {}
    _UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"chart-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}{extension}"
    path = _UPLOAD_DIR / filename
    path.write_bytes(raw)
    return {"path": str(path), "mime": mime}
