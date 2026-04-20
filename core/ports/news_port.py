from __future__ import annotations

from typing import Protocol


class NewsPort(Protocol):
    def get_market_news(self, tickers: list[str] | None = None) -> list[dict]:
        ...
