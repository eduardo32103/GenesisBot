from __future__ import annotations

import json
import re
import urllib.request
from typing import Any

from app.settings import load_settings
from services.genesis.memory_store import MemoryStore
from services.genesis.ticker_parser import extract_tickers_from_prompt

_DATA_URL_PATTERN = re.compile(r"^data:(?P<mime>image/[a-zA-Z0-9.+-]+);base64,(?P<data>.+)$", re.DOTALL)


def analyze_chart_image(payload: dict[str, Any], memory: MemoryStore | None = None) -> dict[str, Any]:
    settings = load_settings()
    store = memory or MemoryStore()
    message = str(payload.get("message") or "").strip()
    image = payload.get("image") if isinstance(payload.get("image"), dict) else {}
    saved = _image_metadata(image)
    tickers = extract_tickers_from_prompt(message)
    configured = bool(settings.genesis_vision_enabled and settings.genesis_llm_enabled and settings.openai_api_key)

    event_payload = {
        "message": message[:240],
        "tickers": tickers,
        "image_received": bool(saved.get("mime")),
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
            "image": {"stored": False, "mime": saved.get("mime") or image.get("type") or ""},
            "next_step": "Configura GENESIS_VISION_ENABLED=true, GENESIS_LLM_ENABLED=true y OPENAI_API_KEY para analisis visual.",
        }

    if not saved.get("data_url"):
        return {
            "ok": True,
            "intent": "image_chart_analysis",
            "status": "image_missing",
            "answer": "No recibi una imagen valida. Adjunta un screenshot de la grafica y lo analizo con vision.",
            "tickers": tickers,
            "image": {"stored": False, "mime": saved.get("mime") or image.get("type") or ""},
        }
    try:
        answer = _call_openai_vision(settings.openai_api_key, settings.genesis_llm_model, saved["data_url"], message, tickers)
    except Exception:
        answer = "Recibi la imagen, pero el proveedor de vision no respondio. No voy a inventar una lectura visual."
    return {
        "ok": True,
        "intent": "image_chart_analysis",
        "status": "vision_ready",
        "answer": answer,
        "tickers": tickers,
        "image": {"stored": False, "mime": saved.get("mime") or image.get("type") or ""},
        "vision_policy": "No se usan precios de la imagen como verdad; se reconfirman con FMP o snapshot.",
    }


def _image_metadata(image: dict[str, Any]) -> dict[str, str]:
    data_url = str(image.get("data_url") or image.get("dataUrl") or "").strip()
    if not data_url:
        return {}
    match = _DATA_URL_PATTERN.match(data_url)
    if not match:
        return {}
    mime = match.group("mime")
    if not match.group("data") or len(match.group("data")) > 7_000_000:
        return {}
    return {"data_url": data_url, "mime": mime}


def _call_openai_vision(api_key: str, model: str, data_url: str, message: str, tickers: list[str]) -> str:
    prompt = (
        "Analiza esta imagen de grafica financiera en espanol. "
        "Describe solo lo visible: tendencia, soportes/resistencias, volumen si se distingue, RSI/MACD/Fibonacci si aparecen, "
        "momentum, riesgo, entrada condicional e invalidacion. No inventes precios ni retornos; si no se ve algo, dilo."
    )
    body = {
        "model": model or "gpt-5.5",
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": json.dumps({"prompt": prompt, "user_message": message, "tickers_detected": tickers}, ensure_ascii=False)},
                    {"type": "input_image", "image_url": data_url},
                ],
            }
        ],
    }
    request = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(body).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if isinstance(payload.get("output_text"), str):
        return payload["output_text"].strip()
    fragments: list[str] = []
    for item in payload.get("output", []) if isinstance(payload, dict) else []:
        for content in item.get("content", []) if isinstance(item, dict) else []:
            if content.get("type") in {"output_text", "text"} and content.get("text"):
                fragments.append(str(content["text"]))
    return "\n".join(fragments).strip() or "Recibi la imagen. Vision respondio sin texto util."
