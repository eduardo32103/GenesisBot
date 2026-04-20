from __future__ import annotations

from dataclasses import dataclass

from app.settings import Settings


@dataclass
class Container:
    settings: Settings

    def summary(self) -> dict[str, bool]:
        return {
            "telegram": self.settings.has_telegram,
            "database": self.settings.has_database,
            "redis": bool(self.settings.redis_url),
        }
