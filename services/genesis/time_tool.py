from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.settings import load_settings

_TIME_WORDS = ("hora", "horario", "que hora", "time")
_DATE_WORDS = ("fecha", "que fecha", "dia es", "date")


def detect_time_request(message: str) -> bool:
    text = str(message or "").casefold()
    return any(word in text for word in _TIME_WORDS)


def detect_date_request(message: str) -> bool:
    text = str(message or "").casefold()
    return any(word in text for word in _DATE_WORDS)


def _now() -> tuple[str, datetime]:
    settings = load_settings()
    timezone = settings.timezone or "America/Los_Angeles"
    try:
        return timezone, datetime.now(ZoneInfo(timezone))
    except Exception:
        timezone = "America/Los_Angeles"
        return timezone, datetime.now(ZoneInfo(timezone))


def get_time_answer() -> dict[str, str]:
    timezone, now = _now()
    return {
        "timezone": timezone,
        "iso": now.isoformat(),
        "answer": f"Son las {now.strftime('%H:%M')} en {timezone}.",
    }


def get_date_answer() -> dict[str, str]:
    timezone, now = _now()
    return {
        "timezone": timezone,
        "iso": now.isoformat(),
        "date": now.date().isoformat(),
        "answer": f"Hoy es {now.strftime('%d/%m/%Y')} en {timezone}.",
    }
