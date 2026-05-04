from __future__ import annotations


class AlertsAgent:
    def summary(self) -> dict:
        try:
            from services.dashboard.get_alerts_snapshot import get_alerts_snapshot

            payload = get_alerts_snapshot()
        except Exception:
            payload = {"items": []}
        items = payload.get("items") if isinstance(payload, dict) else []
        items = items if isinstance(items, list) else []
        return {
            "intent": "alerts",
            "answer": f"Alertas activas: {len(items)}. Genesis no eleva una alerta sin evidencia.",
            "items": items[:20],
        }


def get_alerts_agent() -> AlertsAgent:
    return AlertsAgent()
