from __future__ import annotations

from services.genesis.weather_tool import get_weather_answer


class WeatherAgent:
    def answer(self, message: str) -> dict:
        return get_weather_answer(message)


def get_weather_agent() -> WeatherAgent:
    return WeatherAgent()
