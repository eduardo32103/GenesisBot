from __future__ import annotations

from services.dashboard.get_radar_snapshot import get_radar_snapshot


class TrackingAgent:
    def summary(self) -> dict:
        snapshot = get_radar_snapshot()
        items = [item for item in snapshot.get("items", []) if item.get("watchlist")]
        return {
            "intent": "tracking_summary",
            "answer": f"Seguimiento tiene {len(items)} activos vigilados con datos directos cuando estan disponibles.",
            "items": items,
        }


def get_tracking_agent() -> TrackingAgent:
    return TrackingAgent()
