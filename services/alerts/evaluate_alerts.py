from __future__ import annotations

from dataclasses import dataclass

from core.entities.alert import Alert
from core.policies.alert_policy import AlertPolicy


@dataclass
class AlertCandidate:
    ticker: str
    score: float
    confidence: float
    title: str
    body: str


def evaluate_candidate(candidate: AlertCandidate, policy: AlertPolicy | None = None) -> Alert | None:
    active_policy = policy or AlertPolicy()
    if not active_policy.allows(candidate.score, candidate.confidence):
        return None
    return Alert(
        alert_type="market",
        ticker=candidate.ticker,
        title=candidate.title,
        body=candidate.body,
        score=candidate.score,
        confidence=candidate.confidence,
    )
