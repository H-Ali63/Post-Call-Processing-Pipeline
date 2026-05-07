from __future__ import annotations

import socket
from dataclasses import dataclass
from uuid import UUID

from src.config import settings
from src.logging.audit import AuditContext, audit_logger
from src.metrics import registry as metrics
from src.models.workflow import JobStatus, JobType
from src.repositories.interactions import interaction_repository
from src.repositories.jobs import job_repository
from src.services.signal_jobs import trigger_signal_jobs
from src.utils.db import async_session_factory
from src.workers.failure_handler import job_failure_handler


@dataclass(frozen=True)
class ClaimedResultJob:
    id: UUID
    job_type: JobType
    interaction_id: UUID
    session_id: UUID
    lead_id: UUID
    campaign_id: UUID
    payload: dict


class ResultWorkerService:
    def __init__(self, worker_id: str | None = None):
        self.worker_id = worker_id or f"result-worker-{socket.gethostname()}"

    async def run_once(self, *, limit: int = settings.JOB_CLAIM_BATCH_SIZE) -> int:
        snapshots = await self._claim_jobs(limit=limit)
        for snapshot in snapshots:
            await self._process(snapshot)
        return len(snapshots)

    async def _claim_jobs(self, *, limit: int) -> list[ClaimedResultJob]:
        async with async_session_factory() as session:
            async with session.begin():
                jobs = await job_repository.claim_ready_jobs(
                    session,
                    worker_id=self.worker_id,
                    job_types=[JobType.SIGNAL_JOBS, JobType.LEAD_STAGE_UPDATE],
                    limit=limit,
                )
                return [
                    ClaimedResultJob(
                        id=job.id,
                        job_type=job.job_type,
                        interaction_id=job.interaction_id,
                        session_id=job.session_id,
                        lead_id=job.lead_id,
                        campaign_id=job.campaign_id,
                        payload=job.payload or {},
                    )
                    for job in jobs
                ]

    async def _process(self, snapshot: ClaimedResultJob) -> None:
        try:
            if snapshot.job_type == JobType.SIGNAL_JOBS:
                await self._process_signal(snapshot)
            elif snapshot.job_type == JobType.LEAD_STAGE_UPDATE:
                await self._process_lead_stage(snapshot)
        except Exception as exc:
            await self._handle_failure(snapshot, exc.__class__.__name__, str(exc))
            return

        async with async_session_factory() as session:
            async with session.begin():
                job = await job_repository.get(session, snapshot.id)
                if job is None or job.status != JobStatus.RUNNING:
                    return
                await job_repository.mark_succeeded(session, job)
                metrics.workflow_jobs_total.labels(
                    job_type=job.job_type.value, status="succeeded"
                ).inc()
                await audit_logger.emit(
                    session,
                    AuditContext.from_job(job, stage="result_worker"),
                    "succeeded",
                    "result_job_completed",
                )

    async def _process_signal(self, snapshot: ClaimedResultJob) -> None:
        await trigger_signal_jobs(
            interaction_id=str(snapshot.interaction_id),
            session_id=str(snapshot.session_id),
            campaign_id=str(snapshot.campaign_id),
            analysis_result=snapshot.payload.get("analysis_result") or {},
        )

    async def _process_lead_stage(self, snapshot: ClaimedResultJob) -> None:
        async with async_session_factory() as session:
            async with session.begin():
                await interaction_repository.update_lead_stage(
                    session,
                    lead_id=snapshot.lead_id,
                    call_stage=(snapshot.payload.get("analysis_result") or {}).get(
                        "call_stage", "unknown"
                    ),
                )

    async def _handle_failure(
        self, snapshot: ClaimedResultJob, error_code: str, error_message: str
    ) -> None:
        async with async_session_factory() as session:
            async with session.begin():
                job = await job_repository.get(session, snapshot.id)
                if job is None:
                    return
                await job_failure_handler.handle(
                    session,
                    job,
                    stage="result_worker",
                    error_code=error_code,
                    error_message=error_message,
                )


result_worker_service = ResultWorkerService()
