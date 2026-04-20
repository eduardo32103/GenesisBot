from __future__ import annotations


def classify_risk(score: float) -> str:
    if score >= 80:
        return "low"
    if score >= 60:
        return "medium"
    return "high"
