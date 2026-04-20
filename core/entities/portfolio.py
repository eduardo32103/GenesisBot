from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Portfolio:
    owner_id: str
    tickers: list[str] = field(default_factory=list)
