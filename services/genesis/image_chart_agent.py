from __future__ import annotations

from typing import Any

from services.genesis.chart_image_analysis import analyze_chart_image
from services.genesis.memory_store import MemoryStore


class ImageChartAgent:
    def analyze(self, payload: dict[str, Any], memory: MemoryStore | None = None) -> dict[str, Any]:
        return analyze_chart_image(payload, memory=memory)


def get_image_chart_agent() -> ImageChartAgent:
    return ImageChartAgent()
