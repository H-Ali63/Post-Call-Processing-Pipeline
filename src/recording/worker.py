from __future__ import annotations

import socket
from dataclasses import dataclass
from uuid import UUID

from src.config import settings
from src.logging.audit import AuditContext, audit_logger
from src.metrics import registry as metrics
from src.models.workflow import JobStatus, JobType
from src.recording.service import (
    RecordingNotReady,
    RecordingUnavailable,
    recording_service,
)
from src.repositories.interactions import interaction_repository
from src.repositories.jobs import job_repository
from src.utils.db import async_session_factory
from src.workers.failure_handler import job_failure_handler


@dataclass(frozen=True)
class ClaimedRecordingJob:
    id: UUID
    interaction_id: UUID
    attempts: int
    payload: dict


class RecordingWorkerService:
    def __init__(self, worker_id: str | None = None):
        self.worker_id = worker_id or f"recording-worker-{socket.gethostname()}"

    async def run_once(self, *, limit: int = settings.JOB_CLAIM_BATCH_SIZE) -> int:
        snapshots = await self._claim_jobs(limit=limit)
        for snapshot in snapshots:
            await self._process(snapshot)
        return len(snapshots)

    async def _claim_jobs(self, *, limit: int) -> list[ClaimedRecordingJob]:
        async with async_session_factory() as session:
            async with session.begin():
                jobs = await job_repository.claim_ready_jobs(
                    session,
                    worker_id=self.worker_id,
                    job_types=[JobType.RECORDING_FETCH],
                    limit=limit,
                )
                return [
                    ClaimedRecordingJob(
                        job.id, job.interaction_id, job.attempts, job.payload or {}
                    )
                    for job in jobs
                ]

    async def _process(self, snapshot: ClaimedRecordingJob) -> None:
        payload = snapshot.payload
        try:
            result = await recording_service.poll_once(
                interaction_id=str(snapshot.interaction_id),
                call_sid=str(payload.get("call_sid") or ""),
                exotel_account_id=str(payload.get("exotel_account_id") or ""),
            )
        except RecordingNotReady as exc:
            metrics.recording_attempts_total.labels(status="not_ready").inc()
            await self._handle_failure(
                snapshot,
                "RECORDING_NOT_READY",
                str(exc),
                retry_after_seconds=self._next_recording_delay(snapshot),
                retryable=True,
            )
            return
        except RecordingUnavailable as exc:
            metrics.recording_attempts_total.labels(status="unavailable").inc()
            await self._handle_failure(
                snapshot,
                "RECORDING_UNAVAILABLE",
                str(exc),
                retryable=False,
            )
            return
        except Exception as exc:
            metrics.recording_attempts_total.labels(status="error").inc()
            await self._handle_failure(
                snapshot,
                exc.__class__.__name__,
                str(exc),
                retryable=True,
            )
            return

        async with async_session_factory() as session:
            async with session.begin():
                job = await job_repository.get(session, snapshot.id)
                if job is None or job.status != JobStatus.RUNNING:
                    return
                await interaction_repository.update_recording_key(
                    session, snapshot.interaction_id, result.s3_key
                )
                await job_repository.mark_succeeded(
                    session,
                    job,
                    result={
                        "s3_key": result.s3_key,
                        "recording_url_present": bool(result.recording_url),
                    },
                )
                metrics.recording_attempts_total.labels(status="uploaded").inc()
                metrics.workflow_jobs_total.labels(
                    job_type=job.job_type.value, status="succeeded"
                ).inc()
                await audit_logger.emit(
                    session,
                    AuditContext.from_job(job, stage="recording_worker"),
                    "succeeded",
                    "recording_uploaded",
                    metadata={"s3_key": result.s3_key},
                )

    async def _handle_failure(
        self,
        snapshot: ClaimedRecordingJob,
        error_code: str,
        error_message: str,
        *,
        retry_after_seconds: int | None = None,
        retryable: bool = True,
    ) -> None:
        async with async_session_factory() as session:
            async with session.begin():
                job = await job_repository.get(session, snapshot.id)
                if job is None:
                    return
                await job_failure_handler.handle(
                    session,
                    job,
                    stage="recording_worker",
                    error_code=error_code,
                    error_message=error_message,
                    retry_after_seconds=retry_after_seconds,
                    retryable=retryable,
                )

    def _next_recording_delay(self, snapshot: ClaimedRecordingJob) -> int:
        return min(
            settings.RECORDING_INITIAL_RETRY_SECONDS
            * (2 ** max(snapshot.attempts - 1, 0)),
            settings.RECORDING_MAX_RETRY_SECONDS,
        )


recording_worker_service = RecordingWorkerService()
