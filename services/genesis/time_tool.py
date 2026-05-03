from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.settings import load_settings

_TIME_WORDS = ("hora", "horario", "que hora", "qué hora", "time")


def detect_time_request(message: str) -> bool:
    text = str(message or "").casefold()
    return any(word in text for word in _TIME_WORDS)


def get_time_answer() -> dict[str, str]:
    settings = load_settings()
    timezone = settings.timezone or "America/Los_Angeles"
    try:
        now = datetime.now(ZoneInfo(timezone))
    except Exception:
        timezone = "America/Los_Angeles"
        now = datetime.now(ZoneInfo(timezone))
    return {
        "timezone": timezone,
        "iso": now.isoformat(),
        "answer": f"Son las {now.strftime('%H:%M')} en {timezone}.",
    }
