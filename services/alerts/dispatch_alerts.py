from __future__ import annotations


def dispatch_alert(payload: dict) -> dict[str, str]:
    return {"status": "pending_migration", "channel": payload.get("channel", "telegram")}
