import hashlib
import hmac

from fastapi import HTTPException, Request

from src.config import settings


async def validate_webhook_signature(request: Request) -> None:
    """Validate an HMAC-SHA256 webhook signature when a secret is configured."""
    if not settings.WEBHOOK_SIGNING_SECRET:
        return

    supplied = request.headers.get(settings.WEBHOOK_SIGNATURE_HEADER)
    if not supplied:
        raise HTTPException(status_code=401, detail="Missing webhook signature")

    body = await request.body()
    expected = hmac.new(
        settings.WEBHOOK_SIGNING_SECRET.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()

    normalized = supplied.removeprefix("sha256=").strip()
    if not hmac.compare_digest(normalized, expected):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")
