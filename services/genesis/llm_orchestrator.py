from __future__ import annotations

import json
import logging
import urllib.request
from typing import Any

from app.settings import load_settings

_LOGGER = logging.getLogger("genesis.llm_orchestrator")


class LlmOrchestrator:
    def __init__(self) -> None:
        self.settings = load_settings()

    def enabled(self) -> bool:
        return bool(self.settings.genesis_llm_enabled and self.settings.openai_api_key)

    def compose(self, prompt: str, verified_context: dict[str, Any], fallback: str) -> dict[str, Any]:
        if not self.enabled():
            return {"used_llm": False, "answer": fallback, "reason": "llm_disabled"}
        safe_context = _sanitize_context(verified_context)
        try:
            answer = self._call_openai(prompt, safe_context)
        except Exception:
            _LOGGER.warning("Genesis LLM fallback activated", exc_info=True)
            return {"used_llm": False, "answer": fallback, "reason": "llm_error"}
        return {"used_llm": True, "answer": answer or fallback, "reason": "ok"}

    def _call_openai(self, prompt: str, verified_context: dict[str, Any]) -> str:
        body = {
            "model": self.settings.genesis_llm_model,
            "input": [
                {
                    "role": "system",
                    "content": (
                        "Eres Genesis, asistente financiero. Responde en espanol natural. "
                        "No inventes precios, retornos ni datos; usa solo el contexto verificado."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps({"prompt": prompt, "verified_context": verified_context}, ensure_ascii=False),
                },
            ],
        }
        request = urllib.request.Request(
            "https://api.openai.com/v1/responses",
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.settings.openai_api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=12) as response:
            payload = json.loads(response.read().decode("utf-8"))
        text = payload.get("output_text")
        if isinstance(text, str):
            return text.strip()
        fragments: list[str] = []
        for item in payload.get("output", []) if isinstance(payload, dict) else []:
            for content in item.get("content", []) if isinstance(item, dict) else []:
                if content.get("type") in {"output_text", "text"} and content.get("text"):
                    fragments.append(str(content["text"]))
        return "\n".join(fragments).strip()


def _sanitize_context(value: Any) -> Any:
    if isinstance(value, dict):
        clean = {}
        for key, raw in value.items():
            key_text = str(key)
            if any(part in key_text.casefold() for part in ("api", "key", "secret", "token", "password")):
                continue
            clean[key_text] = _sanitize_context(raw)
        return clean
    if isinstance(value, list):
        return [_sanitize_context(item) for item in value[:40]]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def get_llm_orchestrator() -> LlmOrchestrator:
    return LlmOrchestrator()
