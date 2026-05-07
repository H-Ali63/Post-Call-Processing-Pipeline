from __future__ import annotations

from typing import Any, Mapping

from src.config import settings


REDACTED = "[REDACTED]"


def redact_pii(value: Any) -> Any:
    if isinstance(value, Mapping):
        redacted = {}
        for key, item in value.items():
            if str(key).lower() in {field.lower() for field in settings.PII_LOG_FIELDS}:
                redacted[key] = REDACTED
            else:
                redacted[key] = redact_pii(item)
        return redacted

    if isinstance(value, list):
        return [redact_pii(item) for item in value]

    return value
