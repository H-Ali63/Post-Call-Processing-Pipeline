from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.logging.audit import AuditContext, audit_logger
from src.models.workflow import InteractionJob
from src.repositories.interactions import interaction_repository
from src.repositories.jobs import job_repository
from src.retries.policy import ExponentialBackoffPolicy
from src.metrics import registry as metrics


class JobFailureHandler:
    def __init__(self, retry_policy: Optional[ExponentialBackoffPolicy] = None):
        self._retry_policy = retry_policy or ExponentialBackoffPolicy()

    async def handle(
        self,
        session: AsyncSession,
        job: InteractionJob,
        *,
        stage: str,
        error_code: str,
        error_message: str,
        retry_after_seconds: Optional[int] = None,
        retryable: bool = True,
    ) -> None:
        decision = (
            self._retry_policy.decide(
                attempt=job.attempts,
                max_attempts=job.max_attempts,
                now=datetime.utcnow(),
                retry_after_seconds=retry_after_seconds,
            )
            if retryable
            else None
        )

        await interaction_repository.append_error(
            session,
            job.interaction_id,
            stage=stage,
            error_code=error_code,
            error_message=error_message,
        )

        if decision and decision.should_retry and decision.next_run_at is not None:
            # The row stays in the main queue with a future run_after. That keeps
            # retry visibility in one place and avoids a second retry system.
            await job_repository.defer(
                session,
                job,
                run_after=decision.next_run_at,
                error_code=error_code,
                error_message=error_message,
            )
            metrics.workflow_retries_total.labels(
                job_type=job.job_type.value, error_code=error_code
            ).inc()
            await audit_logger.emit(
                session,
                AuditContext.from_job(job, stage=stage),
                "deferred",
                "job_retry_scheduled",
                error_code=error_code,
                metadata={
                    "next_run_at": decision.next_run_at.isoformat(),
                    "delay_seconds": decision.delay_seconds,
                    "error_message": error_message,
                },
            )
            return

        await job_repository.dead_letter(
            session,
            job,
            stage=stage,
            error_code=error_code,
            error_message=error_message,
        )
        metrics.workflow_dead_letters_total.labels(
            job_type=job.job_type.value, error_code=error_code
        ).inc()
        await audit_logger.emit(
            session,
            AuditContext.from_job(job, stage=stage),
            "dead_lettered",
            "job_moved_to_dead_letter",
            error_code=error_code,
            metadata={"error_message": error_message},
        )


job_failure_handler = JobFailureHandler()
