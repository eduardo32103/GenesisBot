from __future__ import annotations

import os
import re


def detect_weather_request(message: str) -> bool:
    text = str(message or "").casefold()
    return any(token in text for token in ("clima", "temperatura", "llueve", "lluvia", "weather"))


def extract_city(message: str) -> str:
    text = str(message or "").strip()
    match = re.search(r"(?:en|de)\s+([A-Za-zÁÉÍÓÚÜÑáéíóúüñ .'-]{2,60})", text)
    return " ".join((match.group(1) if match else "").split()).strip(" ?.")


def get_weather_answer(message: str) -> dict:
    city = extract_city(message)
    if not os.getenv("GENESIS_WEATHER_PROVIDER") and not os.getenv("OPENWEATHER_API_KEY"):
        return {
            "ok": False,
            "intent": "weather",
            "city": city,
            "answer": "No tengo proveedor de clima configurado todavia.",
            "source": "weather_unconfigured",
        }
    return {
        "ok": False,
        "intent": "weather",
        "city": city,
        "answer": "Proveedor de clima pendiente de conexion segura.",
        "source": "weather_pending",
    }

