from __future__ import annotations

from services.genesis.image_chart_agent import ImageChartAgent, get_image_chart_agent

ImageAnalysisAgent = ImageChartAgent


def get_image_analysis_agent() -> ImageAnalysisAgent:
    return get_image_chart_agent()
