from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional
from uuid import UUID

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.models.workflow import (
    DeadLetterJob,
    InteractionJob,
    JobPriority,
    JobStatus,
    JobType,
)


class InteractionJobRepository:
    async def get(self, session: AsyncSession, job_id: UUID) -> Optional[InteractionJob]:
        result = await session.execute(
            select(InteractionJob).where(InteractionJob.id == job_id)
        )
        return result.scalar_one_or_none()

    async def get_by_idempotency_key(
        self, session: AsyncSession, idempotency_key: str
    ) -> Optional[InteractionJob]:
        result = await session.execute(
            select(InteractionJob).where(
                InteractionJob.idempotency_key == idempotency_key
            )
        )
        return result.scalar_one_or_none()

    async def create_if_absent(
        self,
        session: AsyncSession,
        *,
        interaction_id: UUID,
        session_id: UUID,
        lead_id: UUID,
        campaign_id: UUID,
        customer_id: UUID,
        job_type: JobType,
        priority: JobPriority,
        idempotency_key: str,
        payload: Dict[str, Any],
        token_estimate: int = 0,
        max_attempts: Optional[int] = None,
        run_after: Optional[datetime] = None,
    ) -> InteractionJob:
        existing = await self.get_by_idempotency_key(session, idempotency_key)
        if existing is not None:
            # Webhooks and Celery retries can both repeat. The idempotency key
            # makes the repeat boring instead of dangerous.
            return existing

        job = InteractionJob(
            interaction_id=interaction_id,
            session_id=session_id,
            lead_id=lead_id,
            campaign_id=campaign_id,
            customer_id=customer_id,
            job_type=job_type,
            status=JobStatus.PENDING,
            priority=priority,
            idempotency_key=idempotency_key,
            payload=payload,
            token_estimate=token_estimate,
            max_attempts=max_attempts or settings.WORKFLOW_MAX_ATTEMPTS,
            run_after=run_after or datetime.utcnow(),
        )
        session.add(job)
        await session.flush()
        return job

    async def claim_ready_jobs(
        self,
        session: AsyncSession,
        *,
        worker_id: str,
        job_types: Iterable[JobType],
        limit: int,
    ) -> List[InteractionJob]:
        now = datetime.utcnow()
        priority_order = case(
            (InteractionJob.priority == JobPriority.HIGH, 0),
            (InteractionJob.priority == JobPriority.MEDIUM, 1),
            (InteractionJob.priority == JobPriority.LOW, 2),
            else_=3,
        )

        # SKIP LOCKED lets a pool of workers share the table without waiting on
        # each other. Rows already claimed by one worker are invisible to the next.
        result = await session.execute(
            select(InteractionJob)
            .where(
                InteractionJob.job_type.in_(list(job_types)),
                InteractionJob.status.in_([JobStatus.PENDING, JobStatus.DEFERRED]),
                InteractionJob.run_after <= now,
            )
            .order_by(priority_order, InteractionJob.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        jobs = list(result.scalars().all())
        for job in jobs:
            job.status = JobStatus.RUNNING
            job.attempts += 1
            job.locked_by = worker_id
            job.locked_at = now
            job.started_at = job.started_at or now

        await session.flush()
        return jobs

    async def recover_stale_running_jobs(
        self, session: AsyncSession, *, worker_id: str
    ) -> int:
        cutoff = datetime.utcnow() - timedelta(seconds=settings.JOB_LOCK_TIMEOUT_SECONDS)
        # If a worker dies after claiming a row, the database still has the
        # lock timestamp. Move the row back to DEFERRED and let another worker
        # pick it up on the next pass.
        result = await session.execute(
            select(InteractionJob).where(
                InteractionJob.status == JobStatus.RUNNING,
                InteractionJob.locked_at < cutoff,
            )
        )
        jobs = list(result.scalars().all())
        for job in jobs:
            job.status = JobStatus.DEFERRED
            job.locked_by = None
            job.locked_at = None
            job.run_after = datetime.utcnow()
            job.last_error_code = "WORKER_LOCK_TIMEOUT"
            job.last_error_message = f"Recovered stale lock by {worker_id}"

        await session.flush()
        return len(jobs)

    async def mark_succeeded(
        self,
        session: AsyncSession,
        job: InteractionJob,
        *,
        result: Optional[Dict[str, Any]] = None,
        tokens_used: int = 0,
    ) -> None:
        job.status = JobStatus.SUCCEEDED
        job.result = result or {}
        job.tokens_used = tokens_used
        job.completed_at = datetime.utcnow()
        job.locked_by = None
        job.locked_at = None
        await session.flush()

    async def defer(
        self,
        session: AsyncSession,
        job: InteractionJob,
        *,
        run_after: datetime,
        error_code: str,
        error_message: str,
    ) -> None:
        job.status = JobStatus.DEFERRED
        job.run_after = run_after
        job.locked_by = None
        job.locked_at = None
        job.last_error_code = error_code
        job.last_error_message = error_message
        await session.flush()

    async def dead_letter(
        self,
        session: AsyncSession,
        job: InteractionJob,
        *,
        stage: str,
        error_code: str,
        error_message: str,
    ) -> DeadLetterJob:
        job.status = JobStatus.DEAD_LETTERED
        job.completed_at = datetime.utcnow()
        job.locked_by = None
        job.locked_at = None
        job.last_error_code = error_code
        job.last_error_message = error_message

        dead = DeadLetterJob(
            original_job_id=job.id,
            interaction_id=job.interaction_id,
            customer_id=job.customer_id,
            campaign_id=job.campaign_id,
            job_type=job.job_type.value,
            stage=stage,
            payload=job.payload or {},
            attempts=job.attempts,
            last_error_code=error_code,
            last_error_message=error_message,
        )
        session.add(dead)
        await session.flush()
        return dead

    async def queue_depth_by_status(
        self, session: AsyncSession, statuses: Iterable[JobStatus]
    ) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for status in statuses:
            result = await session.execute(
                select(func.count()).select_from(InteractionJob).where(
                    InteractionJob.status == status
                )
            )
            counts[status.value] = int(result.scalar_one())
        return counts


job_repository = InteractionJobRepository()
