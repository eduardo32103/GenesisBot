from __future__ import annotations

from typing import Any


class ResponseComposer:
    def general(self) -> str:
        return "Puedo ayudarte con mercado, cartera, seguimiento, ballenas, alertas, clima o una grafica. Dime el activo o el tema que quieres revisar."

    def greeting(self) -> str:
        return "Genesis activo. Que quieres revisar hoy?"

    def no_confirmed_price(self, ticker: str) -> str:
        return f"{ticker}: no tengo precio confirmado para ese activo."

    def compact(self, parts: list[Any]) -> str:
        return " ".join(str(part).strip() for part in parts if str(part or "").strip())


def get_response_composer() -> ResponseComposer:
    return ResponseComposer()
