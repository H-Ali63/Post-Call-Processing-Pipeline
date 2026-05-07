from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx

from src.config import settings


class RateLimitExceeded(Exception):
    def __init__(self, message: str, retry_after_seconds: int = 60):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


@dataclass(frozen=True)
class LLMResponse:
    payload: Dict[str, Any]
    provider: str
    model: str


class LLMClient:
    async def analyze_call(self, prompt: str) -> Dict[str, Any]:
        """
        Provider boundary for post-call analysis.

        The local assessment runs without real API keys, so the default path is
        deterministic. A real provider implementation should preserve the 429
        handling contract by raising RateLimitExceeded with Retry-After.
        """
        if settings.LLM_API_KEY == "sk-mock-key-for-assessment":
            return {
                "call_stage": "unknown",
                "entities": {},
                "summary": "Mock analysis result",
                "usage": {"total_tokens": 1500},
            }

        # Minimal provider hook. Production code would set the exact endpoint
        # and payload for the configured provider/model.
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {settings.LLM_API_KEY}"},
                    json={
                        "model": settings.LLM_MODEL,
                        "messages": [{"role": "user", "content": prompt}],
                        "response_format": {"type": "json_object"},
                    },
                )
        except httpx.HTTPError as exc:
            raise RuntimeError(f"LLM provider request failed: {exc}") from exc

        if response.status_code == 429:
            retry_after = _parse_retry_after(response.headers.get("retry-after"))
            raise RateLimitExceeded("LLM provider rate limit exceeded", retry_after)

        response.raise_for_status()
        data = response.json()
        content = data["choices"][0]["message"]["content"]
        usage = data.get("usage", {})
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            parsed = {
                "call_stage": "unknown",
                "entities": {"raw_content": content},
                "summary": content[:500],
            }
        parsed["usage"] = {"total_tokens": usage.get("total_tokens", 0)}
        return parsed


def _parse_retry_after(value: Optional[str]) -> int:
    if not value:
        return 60
    try:
        return max(int(value), 1)
    except ValueError:
        return 60


llm_client = LLMClient()
