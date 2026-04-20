from __future__ import annotations


def validate_signal_outcome(entry_price: float, current_price: float) -> dict[str, float]:
    if entry_price == 0:
        return {"return_pct": 0.0}
    return {"return_pct": round(((current_price - entry_price) / entry_price) * 100, 2)}
