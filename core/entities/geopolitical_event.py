from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class GeopoliticalEvent:
    source: str
    title: str
    summary: str
    impacted_tickers: list[str] = field(default_factory=list)
    severity: float = 0.0
