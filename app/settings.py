from __future__ import annotations

import os
from dataclasses import dataclass


def _clean_ascii_secret(value: str) -> str:
    return "".join(ch for ch in str(value or "") if ord(ch) < 128).strip()


def mask_secret(value: str, keep: int = 4) -> str:
    raw = str(value or "")
    if not raw:
        return ""
    if len(raw) <= keep:
        return "*" * len(raw)
    return f"{'*' * (len(raw) - keep)}{raw[-keep:]}"


@dataclass(frozen=True)
class Settings:
    env: str
    telegram_token: str
    chat_id: str
    backup_chat_id: str
    fmp_api_key: str
    openai_api_key: str
    gemini_api_key: str
    database_url: str
    supabase_url: str
    supabase_key: str
    redis_url: str
    timezone: str

    @property
    def has_telegram(self) -> bool:
        return bool(self.telegram_token and self.chat_id)

    @property
    def has_database(self) -> bool:
        return bool(self.database_url)

    def describe(self) -> dict[str, str | bool]:
        return {
            "env": self.env,
            "telegram": self.has_telegram,
            "database": self.has_database,
            "fmp_key": bool(self.fmp_api_key),
            "openai_key": bool(self.openai_api_key),
            "gemini_key": bool(self.gemini_api_key),
            "redis": bool(self.redis_url),
            "timezone": self.timezone,
        }


def load_settings() -> Settings:
    chat_id = os.getenv("CHAT_ID", "").strip()
    backup_chat_id = os.getenv("BACKUP_CHAT_ID", chat_id).strip() or chat_id
    return Settings(
        env=os.getenv("GENESIS_ENV", "development").strip() or "development",
        telegram_token=os.getenv("TELEGRAM_TOKEN", "").strip(),
        chat_id=chat_id,
        backup_chat_id=backup_chat_id,
        fmp_api_key=_clean_ascii_secret(os.getenv("FMP_API_KEY", "")),
        openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
        gemini_api_key=os.getenv("GEMINI_API_KEY", "").strip(),
        database_url=os.getenv("DATABASE_URL", "").strip(),
        supabase_url=os.getenv("SUPABASE_URL", "").strip(),
        supabase_key=os.getenv("SUPABASE_KEY", "").strip(),
        redis_url=os.getenv("REDIS_URL", "").strip(),
        timezone=os.getenv("GENESIS_TIMEZONE", "America/Los_Angeles").strip() or "America/Los_Angeles",
    )


def validate_runtime_settings(settings: Settings, require_telegram: bool = True) -> None:
    if require_telegram and not settings.has_telegram:
        raise RuntimeError("Faltan TELEGRAM_TOKEN o CHAT_ID para iniciar el runtime de Telegram.")
