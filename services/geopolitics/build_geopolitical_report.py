from __future__ import annotations


def build_geopolitical_report(headlines: list[dict], wallet: list[str]) -> dict:
    return {
        "status": "pending_migration",
        "headline_count": len(headlines),
        "wallet_size": len(wallet),
    }
