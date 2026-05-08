from __future__ import annotations

import socket
from datetime import datetime
from typing import Any, Dict, List
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.llm.token_estimator import token_estimator
from src.logging.audit import AuditContext, audit_logger
from src.metrics import registry as metrics
from src.models.workflow import InteractionJob, JobPriority, JobType
from src.repositories.interactions import interaction_repository
from src.repositories.jobs import job_repository
from src.scheduler.classifier import interaction_classifier
from src.workers.failure_handler import job_failure_handler


class WorkflowOrchestrator:
    def __init__(self, worker_id: str | None = None):
        self.worker_id = worker_id or f"orchestrator-{socket.gethostname()}"

    async def run_once(
        self, session: AsyncSession, *, limit: int = settings.JOB_CLAIM_BATCH_SIZE
    ) -> int:
        await job_repository.recover_stale_running_jobs(
            session, worker_id=self.worker_id
        )
        jobs = await job_repository.claim_ready_jobs(
            session,
            worker_id=self.worker_id,
            job_types=[JobType.ORCHESTRATE_INTERACTION],
            limit=limit,
        )

        processed = 0
        for job in jobs:
            await self._process_job(session, job)
            processed += 1
        return processed

    async def _process_job(self, session: AsyncSession, job: InteractionJob) -> None:
        try:
            await audit_logger.emit(
                session,
                AuditContext.from_job(job, stage="orchestrator"),
                "running",
                "workflow_orchestration_started",
            )
            await self._create_child_jobs(session, job)
            await job_repository.mark_succeeded(
                session, job, result={"orchestrated_at": datetime.utcnow().isoformat()}
            )
            metrics.workflow_jobs_total.labels(
                job_type=job.job_type.value, status="succeeded"
            ).inc()
            await audit_logger.emit(
                session,
                AuditContext.from_job(job, stage="orchestrator"),
                "succeeded",
                "workflow_orchestration_completed",
            )
        except Exception as exc:
            await job_failure_handler.handle(
                session,
                job,
                stage="orchestrator",
                error_code=exc.__class__.__name__,
                error_message=str(exc),
            )

    async def _create_child_jobs(
        self, session: AsyncSession, job: InteractionJob
    ) -> None:
        payload = job.payload or {}
        interaction_id = UUID(str(job.interaction_id))
        transcript = self._transcript(payload)
        transcript_text = payload.get("transcript_text") or self._transcript_text(transcript)
        additional_data = payload.get("additional_data") or {}

        classification = interaction_classifier.classify(
            transcript=transcript,
            transcript_text=transcript_text,
            additional_data=additional_data,
        )

        # Recording and transcript analysis do not depend on each other. Keep
        # them as separate jobs so a slow recording never blocks the LLM path.
        recording_payload = dict(payload)
        if payload.get("call_sid"):
            await job_repository.create_if_absent(
                session,
                interaction_id=interaction_id,
                session_id=job.session_id,
                lead_id=job.lead_id,
                campaign_id=job.campaign_id,
                customer_id=job.customer_id,
                job_type=JobType.RECORDING_FETCH,
                priority=classification.priority,
                idempotency_key=f"interaction:{interaction_id}:recording:v1",
                payload=recording_payload,
                max_attempts=settings.RECORDING_MAX_ATTEMPTS,
            )

        if classification.skip_llm:
            # A wrong-number or two-line hangup still needs downstream state,
            # but there is no point spending LLM quota to learn that.
            await interaction_repository.mark_short_call(session, interaction_id)
            analysis_result = {
                "call_stage": classification.call_stage,
                "entities": {},
                "summary": "Short transcript; LLM skipped.",
                "usage": {"total_tokens": 0},
            }
            await self._queue_result_jobs(
                session,
                job,
                payload=payload,
                analysis_result=analysis_result,
                priority=JobPriority.HIGH,
                suffix="short_call",
            )
            return

        await interaction_repository.mark_processing(session, interaction_id, "queued")
        token_estimate = token_estimator.estimate(transcript_text)
        llm_payload = dict(payload)
        llm_payload["classification"] = {
            "priority": classification.priority.value,
            "reason": classification.reason,
        }
        await job_repository.create_if_absent(
            session,
            interaction_id=interaction_id,
            session_id=job.session_id,
            lead_id=job.lead_id,
            campaign_id=job.campaign_id,
            customer_id=job.customer_id,
            job_type=JobType.LLM_ANALYSIS,
            priority=classification.priority,
            idempotency_key=f"interaction:{interaction_id}:llm:v1",
            payload=llm_payload,
            token_estimate=token_estimate,
        )
        await audit_logger.emit(
            session,
            AuditContext.from_job(job, stage="priority_scheduler"),
            "queued",
            "llm_job_queued",
            metadata={
                "priority": classification.priority.value,
                "classification_reason": classification.reason,
                "token_estimate": token_estimate,
            },
        )

    async def _queue_result_jobs(
        self,
        session: AsyncSession,
        job: InteractionJob,
        *,
        payload: Dict[str, Any],
        analysis_result: Dict[str, Any],
        priority: JobPriority,
        suffix: str,
    ) -> None:
        result_payload = dict(payload)
        result_payload["analysis_result"] = analysis_result
        call_stage = analysis_result.get("call_stage", "unknown")

        await job_repository.create_if_absent(
            session,
            interaction_id=job.interaction_id,
            session_id=job.session_id,
            lead_id=job.lead_id,
            campaign_id=job.campaign_id,
            customer_id=job.customer_id,
            job_type=JobType.SIGNAL_JOBS,
            priority=priority,
            idempotency_key=f"interaction:{job.interaction_id}:signal:{suffix}:v1",
            payload=result_payload,
        )
        await job_repository.create_if_absent(
            session,
            interaction_id=job.interaction_id,
            session_id=job.session_id,
            lead_id=job.lead_id,
            campaign_id=job.campaign_id,
            customer_id=job.customer_id,
            job_type=JobType.LEAD_STAGE_UPDATE,
            priority=priority,
            idempotency_key=f"interaction:{job.interaction_id}:lead:{call_stage}:v1",
            payload=result_payload,
        )

    def _transcript(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        conversation_data = payload.get("conversation_data") or {}
        transcript = conversation_data.get("transcript") or []
        return transcript if isinstance(transcript, list) else []

    def _transcript_text(self, transcript: List[Dict[str, Any]]) -> str:
        return "\n".join(
            f"{turn.get('role', 'unknown')}: {turn.get('content', '')}"
            for turn in transcript
        )


workflow_orchestrator = WorkflowOrchestrator()
