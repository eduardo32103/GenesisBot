from __future__ import annotations

from typing import Any


class ResponseComposer:
    def general(self) -> str:
        return "Te sigo. Puedo revisar mercado, cartera, seguimiento, ballenas, alertas, clima o una grafica. Si falta una fuente, te lo digo sin inventar datos."

    def greeting(self) -> str:
        return "Hola. Que quieres revisar hoy?"

    def no_confirmed_price(self, ticker: str) -> str:
        return f"{ticker}: no tengo precio confirmado para ese activo."

    def compact(self, parts: list[Any]) -> str:
        return " ".join(str(part).strip() for part in parts if str(part or "").strip())


def get_response_composer() -> ResponseComposer:
    return ResponseComposer()
