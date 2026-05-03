from __future__ import annotations

from typing import Any

from services.genesis.tool_router import route_message


def ask_genesis(question: str = "", context: str = "general", ticker: str = "", panel_context: Any | None = None) -> dict[str, Any]:
    return route_message(question, context=context, ticker=ticker, panel_context=panel_context)

