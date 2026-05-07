"""
Gradual capacity signal for the dialler.

The old implementation froze an agent for 30 minutes at 90% RPM. The LLM
scheduler now performs hard admission control before provider calls, so the
dialler only needs a soft signal: keep calling, slow down briefly, or pause for
a few seconds while the backlog drains.
"""

import logging
from dataclasses import dataclass

from src.config import settings
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CapacitySignal:
    allowed: bool
    usage_ratio: float
    suggested_delay_seconds: int
    reason: str


class PostCallCircuitBreaker:
    async def check_capacity(self, agent_id: str) -> bool:
        signal = await self.capacity_signal(agent_id)
        return signal.allowed

    async def capacity_signal(self, agent_id: str) -> CapacitySignal:
        current_rpm = int(await redis_client.get("llm:postcall:rpm") or 0)
        current_tpm = int(await redis_client.get("llm:postcall:tpm") or 0)

        rpm_ratio = (
            current_rpm / settings.LLM_REQUESTS_PER_MINUTE
            if settings.LLM_REQUESTS_PER_MINUTE
            else 1
        )
        tpm_ratio = (
            current_tpm / settings.LLM_TOKENS_PER_MINUTE
            if settings.LLM_TOKENS_PER_MINUTE
            else 1
        )
        usage_ratio = max(rpm_ratio, tpm_ratio)

        if usage_ratio >= 1.0:
            delay = 15
            allowed = False
            reason = "rate_limit_exhausted"
        elif usage_ratio >= 0.90:
            delay = 5
            allowed = True
            reason = "slow_down"
        elif usage_ratio >= 0.75:
            delay = 1
            allowed = True
            reason = "warm"
        else:
            delay = 0
            allowed = True
            reason = "healthy"

        logger.info(
            "dialler_capacity_signal",
            extra={
                "agent_id": agent_id,
                "usage_ratio": round(usage_ratio, 3),
                "suggested_delay_seconds": delay,
                "reason": reason,
            },
        )
        return CapacitySignal(allowed, usage_ratio, delay, reason)

    async def record_postcall_start(self, estimated_tokens: int = 0):
        await redis_client.incr("llm:postcall:rpm")
        if estimated_tokens:
            await redis_client.incrby("llm:postcall:tpm", estimated_tokens)
        await redis_client.expire("llm:postcall:rpm", 60)
        await redis_client.expire("llm:postcall:tpm", 60)

    async def record_postcall_end(self):
        await redis_client.decr("llm:postcall:rpm")


circuit_breaker = PostCallCircuitBreaker()
