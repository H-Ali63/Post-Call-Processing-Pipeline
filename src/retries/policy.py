from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from src.config import settings


@dataclass(frozen=True)
class RetryDecision:
    should_retry: bool
    next_run_at: Optional[datetime]
    delay_seconds: int


class ExponentialBackoffPolicy:
    def __init__(
        self,
        base_seconds: int = settings.WORKFLOW_BASE_RETRY_SECONDS,
        max_seconds: int = settings.WORKFLOW_MAX_RETRY_SECONDS,
    ):
        self.base_seconds = base_seconds
        self.max_seconds = max_seconds

    def next_delay(self, attempt: int, retry_after_seconds: Optional[int] = None) -> int:
        if retry_after_seconds is not None and retry_after_seconds > 0:
            return min(retry_after_seconds, self.max_seconds)

        exponent = max(attempt - 1, 0)
        return min(self.base_seconds * (2 ** exponent), self.max_seconds)

    def decide(
        self,
        attempt: int,
        max_attempts: int,
        now: Optional[datetime] = None,
        retry_after_seconds: Optional[int] = None,
    ) -> RetryDecision:
        now = now or datetime.utcnow()
        if attempt >= max_attempts:
            return RetryDecision(False, None, 0)

        delay = self.next_delay(attempt, retry_after_seconds)
        return RetryDecision(True, now + timedelta(seconds=delay), delay)
