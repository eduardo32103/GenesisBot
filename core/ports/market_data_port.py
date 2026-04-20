from __future__ import annotations

from typing import Protocol


class MarketDataPort(Protocol):
    def get_quote(self, ticker: str) -> dict:
        ...

    def get_candles(self, ticker: str, timeframe: str) -> list[dict]:
        ...
