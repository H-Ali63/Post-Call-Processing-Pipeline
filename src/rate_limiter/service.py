from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from src.models.workflow import JobPriority
from src.repositories.token_usage import ReservationResult, token_usage_repository


@dataclass(frozen=True)
class Admission:
    admitted: bool
    defer_seconds: int = 0
    reason: str = ""
    customer_budget_tokens: int = 0
    global_remaining_tokens: int = 0


class LLMRateLimiter:
    async def admit(
        self,
        session: AsyncSession,
        *,
        customer_id: UUID,
        estimated_tokens: int,
        priority: JobPriority,
        customer_budget_tokens: int | None = None,
    ) -> Admission:
        reservation: ReservationResult = await token_usage_repository.reserve(
            session,
            customer_id=customer_id,
            estimated_tokens=estimated_tokens,
            allow_overflow=priority == JobPriority.HIGH,
            customer_budget_tokens=customer_budget_tokens,
        )
        return Admission(
            admitted=reservation.allowed,
            defer_seconds=reservation.defer_seconds,
            reason=reservation.reason,
            customer_budget_tokens=reservation.customer_budget_tokens,
            global_remaining_tokens=reservation.global_remaining_tokens,
        )

    async def record_usage(
        self,
        session: AsyncSession,
        *,
        customer_id: UUID,
        actual_tokens: int,
        customer_budget_tokens: int | None = None,
    ) -> None:
        await token_usage_repository.record_usage(
            session,
            customer_id=customer_id,
            actual_tokens=actual_tokens,
            customer_budget_tokens=customer_budget_tokens,
        )


llm_rate_limiter = LLMRateLimiter()
