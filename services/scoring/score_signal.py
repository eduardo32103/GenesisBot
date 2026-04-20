from __future__ import annotations


def score_signal(factors: dict[str, float]) -> float:
    if not factors:
        return 0.0
    return round(sum(factors.values()) / len(factors), 2)
