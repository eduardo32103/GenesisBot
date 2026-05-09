from __future__ import annotations

import json
import os
import re
import urllib.request
from urllib.error import HTTPError, URLError
from typing import Any

from app.settings import load_settings
from services.genesis.memory_store import MemoryStore
from services.genesis.ticker_parser import extract_tickers_from_prompt

_DATA_URL_PATTERN = re.compile(r"^data:(?P<mime>image/[a-zA-Z0-9.+-]+);base64,(?P<data>.+)$", re.DOTALL)


def analyze_chart_image(payload: dict[str, Any], memory: MemoryStore | None = None) -> dict[str, Any]:
    payload = _normalize_image_payload(payload)
    settings = load_settings()
    store = memory or MemoryStore()
    message = str(payload.get("message") or payload.get("question") or "").strip()
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
        answer = (
            "Recibí la imagen, pero falta activar visión para leerla de verdad. "
            "Genesis no va a inventar velas: activa GENESIS_VISION_ENABLED, GENESIS_LLM_ENABLED y OPENAI_API_KEY."
        )
        return {
            "ok": True,
            "intent": "image_chart_analysis",
            "response_type": "chart_analysis",
            "status": "vision_not_configured",
            "answer": answer,
            "tickers": tickers,
            "image": {"stored": False, "mime": saved.get("mime") or image.get("type") or ""},
            "structured": _structured_chart_image_answer(answer, tickers, "vision_not_configured"),
            "next_step": "Configura GENESIS_VISION_ENABLED=true, GENESIS_LLM_ENABLED=true y OPENAI_API_KEY para analisis visual.",
        }

    if not saved.get("data_url"):
        answer = "No recibí una imagen válida. Adjunta un screenshot de la gráfica y la leo con visión."
        return {
            "ok": True,
            "intent": "image_chart_analysis",
            "response_type": "chart_analysis",
            "status": "image_missing",
            "answer": answer,
            "tickers": tickers,
            "image": {"stored": False, "mime": saved.get("mime") or image.get("type") or ""},
            "structured": _structured_chart_image_answer(answer, tickers, "image_missing"),
        }
    source_status: dict[str, Any] = {"provider": "openai_vision", "status": "ok", "cache_hit": False}
    try:
        vision = _call_openai_vision(settings.openai_api_key, settings.genesis_llm_model, saved["data_url"], message, tickers)
        answer = _clean_vision_text(vision["answer"])
        source_status["model"] = vision["model"]
    except Exception as exc:
        source_status.update({"status": "unavailable", "last_error_safe": _safe_error(exc)})
        answer = (
            "Recibí la imagen, pero el proveedor de visión no respondió a tiempo. "
            "No voy a inventar la lectura visual: reintenta con la gráfica completa o revisa source-health."
        )
    return {
        "ok": True,
        "intent": "image_chart_analysis",
        "response_type": "chart_analysis",
        "status": "vision_ready" if source_status["status"] == "ok" else "vision_unavailable",
        "answer": answer,
        "tickers": tickers,
        "image": {"stored": False, "mime": saved.get("mime") or image.get("type") or ""},
        "structured": _structured_chart_image_answer(answer, tickers, source_status["status"]),
        "source_status": source_status,
        "vision_policy": "No se usan precios de la imagen como verdad; se reconfirman con FMP o snapshot.",
    }


def _normalize_image_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {}
    normalized = dict(payload)
    message = str(
        normalized.get("message")
        or normalized.get("question")
        or normalized.get("prompt")
        or normalized.get("text")
        or ""
    ).strip()
    if not message:
        message = "Analiza esta grafica financiera."
    normalized["message"] = message
    image = normalized.get("image") if isinstance(normalized.get("image"), dict) else {}
    image = dict(image)
    data_url = str(
        image.get("data_url")
        or image.get("dataUrl")
        or normalized.get("image_data")
        or normalized.get("imageData")
        or normalized.get("data_url")
        or normalized.get("dataUrl")
        or ""
    ).strip()
    raw_base64 = str(normalized.get("image_base64") or normalized.get("base64") or "").strip()
    mime = str(image.get("type") or normalized.get("mime_type") or normalized.get("mime") or "image/png").strip() or "image/png"
    if raw_base64 and not data_url:
        data_url = f"data:{mime};base64,{raw_base64}"
    if data_url.startswith("data:") and ";base64," in data_url:
        mime = data_url.split(";base64,", 1)[0].replace("data:", "", 1) or mime
    image["data_url"] = data_url
    image["type"] = mime
    normalized["image"] = image
    return normalized


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


def _call_openai_vision(api_key: str, model: str, data_url: str, message: str, tickers: list[str]) -> dict[str, str]:
    prompt = (
        "Analiza esta imagen de grafica financiera en espanol claro y util. "
        "Devuelve una lectura breve para un trader: lectura rapida, tendencia visible, niveles, volumen si se distingue, "
        "RSI/MACD/Fibonacci si aparecen, riesgo, invalidacion y que vigilar. "
        "No inventes precios ni retornos; si no se ve algo, dilo con precision."
    )
    last_error: Exception | None = None
    for candidate in _vision_model_candidates(model):
        body = {
            "model": candidate,
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": json.dumps({"prompt": prompt, "user_message": message, "tickers_detected": tickers}, ensure_ascii=False)},
                        {"type": "input_image", "image_url": data_url, "detail": "high"},
                    ],
                }
            ],
            "max_output_tokens": 900,
        }
        request = urllib.request.Request(
            "https://api.openai.com/v1/responses",
            data=json.dumps(body).encode("utf-8"),
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=int(os.getenv("GENESIS_VISION_TIMEOUT", "45"))) as response:
                payload = json.loads(response.read().decode("utf-8"))
            text = _extract_openai_text(payload)
            if text:
                return {"answer": text, "model": candidate}
            last_error = RuntimeError("vision_without_text")
        except HTTPError as exc:
            last_error = RuntimeError(_http_error_summary(exc))
            continue
        except (TimeoutError, URLError, OSError) as exc:
            last_error = exc
            continue
    raise RuntimeError(_safe_error(last_error) if last_error else "vision_unavailable")


def _extract_openai_text(payload: dict[str, Any]) -> str:
    if isinstance(payload.get("output_text"), str):
        return payload["output_text"].strip()
    fragments: list[str] = []
    for item in payload.get("output", []) if isinstance(payload, dict) else []:
        for content in item.get("content", []) if isinstance(item, dict) else []:
            if content.get("type") in {"output_text", "text"} and content.get("text"):
                fragments.append(str(content["text"]))
    return "\n".join(fragments).strip()


def _vision_model_candidates(model: str) -> list[str]:
    configured = [os.getenv("GENESIS_VISION_MODEL", "").strip(), str(model or "").strip(), "gpt-4.1", "gpt-4o"]
    candidates: list[str] = []
    for candidate in configured:
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    return candidates or ["gpt-4.1"]


def _clean_vision_text(value: str) -> str:
    text = re.sub(r"[*#`_]+", "", str(value or "")).strip()
    return text or "La imagen llego, pero vision no devolvio texto util."


def _structured_chart_image_answer(answer: str, tickers: list[str], status: str) -> dict[str, Any]:
    sentences = [part.strip(" -\n\t") for part in re.split(r"(?<=[.!?])\s+|\n+", answer or "") if part.strip()]
    return {
        "kind": "chart_image_analysis",
        "title": "Analisis visual de grafica",
        "ticker": tickers[0] if tickers else "",
        "status": status,
        "confidence": 0.78 if status == "ok" else 0.42,
        "summary": sentences[0] if sentences else "Genesis recibio la imagen y espera lectura visual.",
        "sections": [
            {"title": "Lectura rapida", "bullets": sentences[:2]},
            {"title": "Que vigilar", "bullets": sentences[2:5]},
        ],
    }


def _http_error_summary(exc: HTTPError) -> str:
    try:
        body = exc.read(600).decode("utf-8", errors="ignore")
    except Exception:
        body = ""
    return f"openai_http_{exc.code}: {body[:220]}"


def _safe_error(exc: object) -> str:
    text = str(exc or "vision_unavailable")
    text = re.sub(r"sk-[A-Za-z0-9_-]+", "sk-***", text)
    return text[:260]
