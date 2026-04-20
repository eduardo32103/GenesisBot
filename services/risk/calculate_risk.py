from __future__ import annotations

from core.policies.risk_policy import classify_risk


def calculate_asset_risk(score: float) -> dict[str, str]:
    return {"risk": classify_risk(score)}
