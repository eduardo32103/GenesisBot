from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Alert:
    alert_type: str
    ticker: str
    title: str
    body: str
    score: float = 0.0
    confidence: float = 0.0
