from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def build_openai_client(api_key: str) -> Any | None:
    if not api_key:
        return None
    from openai import OpenAI

    return OpenAI(api_key=api_key)


@dataclass
class OpenAiClient:
    api_key: str
    _client: Any | None = field(default=None, init=False, repr=False)

    @property
    def client(self) -> Any | None:
        if self._client is None:
            self._client = build_openai_client(self.api_key)
        return self._client

    def complete(self, prompt: str, model: str = "gpt-4o", max_tokens: int = 700) -> str:
        client = self.client
        if client is None:
            raise RuntimeError("OPENAI_API_KEY no configurada.")
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
        )
        return response.choices[0].message.content.strip()
