from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AlertPolicy:
    minimum_score: float = 70.0
    minimum_confidence: float = 0.65

    def allows(self, score: float, confidence: float) -> bool:
        return score >= self.minimum_score and confidence >= self.minimum_confidence
