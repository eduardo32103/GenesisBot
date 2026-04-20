from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class AnalysisReport:
    ticker: str
    timeframe: str
    summary: str
    orientation: str
    confidence: float
    key_levels: dict[str, float] = field(default_factory=dict)
