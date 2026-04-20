from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Signal:
    ticker: str
    direction: str
    timeframe: str
    entry_price: float
    score: float
