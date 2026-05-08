from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.models.workflow import CustomerTokenUsage, JobPriority
from src.rate_limiter.budget import (
    BudgetState,
    budget_admission_policy,
)


GLOBAL_RATE_LIMIT_ID = UUID("00000000-0000-0000-0000-000000000000")


@dataclass(frozen=True)
class ReservationResult:
    allowed: bool
    defer_seconds: int = 0
    reason: str = ""
    customer_budget_tokens: int = 0
    global_remaining_tokens: int = 0


class TokenUsageRepository:
    def minute_window(self, now: Optional[datetime] = None) -> datetime:
        now = now or datetime.utcnow()
        return now.replace(second=0, microsecond=0)

    async def reserve(
        self,
        session: AsyncSession,
        *,
        customer_id: UUID,
        estimated_tokens: int,
        allow_overflow: bool = False,
        now: Optional[datetime] = None,
        customer_budget_tokens: Optional[int] = None,
    ) -> ReservationResult:
        now = now or datetime.utcnow()
        window_start = self.minute_window(now)
        customer_budget = (
            customer_budget_tokens or settings.CUSTOMER_DEFAULT_TOKENS_PER_MINUTE
        )
        # Store the platform-wide bucket as a synthetic customer. It keeps the
        # accounting path the same for global and customer limits.
        global_usage = await self._get_or_create_usage(
            session,
            customer_id=GLOBAL_RATE_LIMIT_ID,
            window_start=window_start,
            budget_tokens=settings.LLM_TOKENS_PER_MINUTE,
        )
        customer_usage = await self._get_or_create_usage(
            session,
            customer_id=customer_id,
            window_start=window_start,
            budget_tokens=customer_budget,
        )

        decision = budget_admission_policy.decide(
            global_state=BudgetState(
                token_limit=settings.LLM_TOKENS_PER_MINUTE,
                request_limit=settings.LLM_REQUESTS_PER_MINUTE,
                reserved_tokens=global_usage.reserved_tokens,
                reserved_requests=global_usage.requests_reserved,
            ),
            customer_state=BudgetState(
                token_limit=customer_budget,
                request_limit=settings.LLM_REQUESTS_PER_MINUTE,
                reserved_tokens=customer_usage.reserved_tokens,
                reserved_requests=customer_usage.requests_reserved,
            ),
            estimated_tokens=estimated_tokens,
            priority=JobPriority.HIGH if allow_overflow else JobPriority.MEDIUM,
        )

        if not decision.allowed:
            return ReservationResult(
                allowed=False,
                defer_seconds=self._seconds_to_next_window(now),
                reason=decision.reason,
                customer_budget_tokens=customer_budget,
                global_remaining_tokens=max(
                    settings.LLM_TOKENS_PER_MINUTE - global_usage.reserved_tokens, 0
                ),
            )

        # Reserve before the provider call. Actual usage is written later from
        # the provider's usage block, but this is the gate that prevents 429s.
        global_usage.reserved_tokens += estimated_tokens
        global_usage.requests_reserved += 1
        customer_usage.reserved_tokens += estimated_tokens
        customer_usage.requests_reserved += 1
        await session.flush()

        return ReservationResult(
            allowed=True,
            reason=decision.reason,
            customer_budget_tokens=customer_budget,
            global_remaining_tokens=max(
                settings.LLM_TOKENS_PER_MINUTE - global_usage.reserved_tokens, 0
            ),
        )

    async def record_usage(
        self,
        session: AsyncSession,
        *,
        customer_id: UUID,
        actual_tokens: int,
        now: Optional[datetime] = None,
        customer_budget_tokens: Optional[int] = None,
    ) -> None:
        window_start = self.minute_window(now)
        customer_budget = (
            customer_budget_tokens or settings.CUSTOMER_DEFAULT_TOKENS_PER_MINUTE
        )
        global_usage = await self._get_or_create_usage(
            session,
            customer_id=GLOBAL_RATE_LIMIT_ID,
            window_start=window_start,
            budget_tokens=settings.LLM_TOKENS_PER_MINUTE,
        )
        customer_usage = await self._get_or_create_usage(
            session,
            customer_id=customer_id,
            window_start=window_start,
            budget_tokens=customer_budget,
        )

        global_usage.used_tokens += actual_tokens
        global_usage.requests_used += 1
        customer_usage.used_tokens += actual_tokens
        customer_usage.requests_used += 1
        await session.flush()

    async def _get_or_create_usage(
        self,
        session: AsyncSession,
        *,
        customer_id: UUID,
        window_start: datetime,
        budget_tokens: int,
    ) -> CustomerTokenUsage:
        result = await session.execute(
            select(CustomerTokenUsage)
            .where(
                CustomerTokenUsage.customer_id == customer_id,
                CustomerTokenUsage.window_start == window_start,
            )
            .with_for_update()
        )
        usage = result.scalar_one_or_none()
        if usage is not None:
            return usage

        usage = CustomerTokenUsage(
            customer_id=customer_id,
            window_start=window_start,
            budget_tokens=budget_tokens,
            reserved_tokens=0,
            used_tokens=0,
            requests_reserved=0,
            requests_used=0,
        )
        session.add(usage)
        await session.flush()
        return usage

    def _seconds_to_next_window(self, now: datetime) -> int:
        next_window = self.minute_window(now) + timedelta(minutes=1, seconds=1)
        return max(int((next_window - now).total_seconds()), 1)


token_usage_repository = TokenUsageRepository()
