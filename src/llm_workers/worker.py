from __future__ import annotations

import socket
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List
from uuid import UUID

from src.llm.client import RateLimitExceeded
from src.logging.audit import AuditContext, audit_logger
from src.metrics import registry as metrics
from src.models.workflow import InteractionJob, JobPriority, JobStatus, JobType
from src.rate_limiter.service import llm_rate_limiter
from src.repositories.interactions import interaction_repository
from src.repositories.jobs import job_repository
from src.services.post_call_processor import PostCallContext, PostCallProcessor
from src.utils.db import async_session_factory
from src.workers.failure_handler import job_failure_handler


@dataclass(frozen=True)
class ClaimedLLMJob:
    id: UUID
    interaction_id: UUID
    session_id: UUID
    lead_id: UUID
    campaign_id: UUID
    customer_id: UUID
    priority: JobPriority
    attempts: int
    token_estimate: int
    payload: Dict[str, Any]


class LLMWorkerService:
    def __init__(self, worker_id: str | None = None):
        self.worker_id = worker_id or f"llm-worker-{socket.gethostname()}"
        self.processor = PostCallProcessor()

    async def run_once(self, *, limit: int = 10) -> int:
        admitted = await self._claim_admitted_jobs(limit=limit)
        for snapshot in admitted:
            await self._process_snapshot(snapshot)
        return len(admitted)

    async def _claim_admitted_jobs(self, *, limit: int) -> List[ClaimedLLMJob]:
        async with async_session_factory() as session:
            async with session.begin():
                await job_repository.recover_stale_running_jobs(
                    session, worker_id=self.worker_id
                )
                jobs = await job_repository.claim_ready_jobs(
                    session,
                    worker_id=self.worker_id,
                    job_types=[JobType.LLM_ANALYSIS],
                    limit=limit,
                )

                admitted: List[ClaimedLLMJob] = []
                for job in jobs:
                    admission = await llm_rate_limiter.admit(
                        session,
                        customer_id=job.customer_id,
                        estimated_tokens=max(job.token_estimate, 1),
                        priority=job.priority,
                        customer_budget_tokens=self._customer_budget(job.payload),
                    )
                    if not admission.admitted:
                        run_after = datetime.utcnow() + timedelta(
                            seconds=admission.defer_seconds
                        )
                        await job_repository.defer(
                            session,
                            job,
                            run_after=run_after,
                            error_code=admission.reason,
                            error_message="LLM admission deferred by rate limiter",
                        )
                        await audit_logger.emit(
                            session,
                            AuditContext.from_job(job, stage="llm_scheduler"),
                            "deferred",
                            "llm_job_deferred_by_rate_limiter",
                            error_code=admission.reason,
                            metadata={
                                "defer_seconds": admission.defer_seconds,
                                "token_estimate": job.token_estimate,
                                "customer_budget_tokens": admission.customer_budget_tokens,
                                "global_remaining_tokens": admission.global_remaining_tokens,
                            },
                        )
                        continue

                    admitted.append(self._snapshot(job))
                    await audit_logger.emit(
                        session,
                        AuditContext.from_job(job, stage="llm_scheduler"),
                        "admitted",
                        "llm_job_admitted",
                        metadata={
                            "token_estimate": job.token_estimate,
                            "priority": job.priority.value,
                            "global_remaining_tokens": admission.global_remaining_tokens,
                        },
                    )

                return admitted

    async def _process_snapshot(self, snapshot: ClaimedLLMJob) -> None:
        ctx = self._context_from_snapshot(snapshot)
        try:
            result = await self.processor.process_post_call(
                ctx, single_prompt=True, persist_result=False
            )
        except RateLimitExceeded as exc:
            await self._handle_failure(
                snapshot,
                "LLM_RATE_LIMIT",
                str(exc),
                retry_after_seconds=exc.retry_after_seconds,
            )
            return
        except Exception as exc:
            await self._handle_failure(
                snapshot,
                exc.__class__.__name__,
                str(exc),
            )
            return

        async with async_session_factory() as session:
            async with session.begin():
                job = await job_repository.get(session, snapshot.id)
                if job is None or job.status != JobStatus.RUNNING:
                    return

                await interaction_repository.update_analysis_result(
                    session,
                    snapshot.interaction_id,
                    call_stage=result.call_stage,
                    entities=result.entities,
                    summary=result.summary,
                    tokens_used=result.tokens_used,
                    provider=result.provider,
                    model=result.model,
                )
                await llm_rate_limiter.record_usage(
                    session,
                    customer_id=snapshot.customer_id,
                    actual_tokens=result.tokens_used,
                    customer_budget_tokens=self._customer_budget(snapshot.payload),
                )
                await job_repository.mark_succeeded(
                    session,
                    job,
                    result=result.raw_response,
                    tokens_used=result.tokens_used,
                )
                await self._queue_result_jobs(session, job, snapshot.payload, result.raw_response)
                metrics.workflow_jobs_total.labels(
                    job_type=job.job_type.value, status="succeeded"
                ).inc()
                metrics.llm_tokens_total.labels(
                    customer_id=str(snapshot.customer_id),
                    campaign_id=str(snapshot.campaign_id),
                    model=result.model,
                ).inc(result.tokens_used)
                metrics.llm_latency_ms.observe(result.latency_ms)
                await audit_logger.emit(
                    session,
                    AuditContext.from_job(job, stage="llm_worker"),
                    "succeeded",
                    "llm_analysis_completed",
                    metadata={
                        "call_stage": result.call_stage,
                        "tokens_used": result.tokens_used,
                        "latency_ms": result.latency_ms,
                    },
                )

    async def _handle_failure(
        self,
        snapshot: ClaimedLLMJob,
        error_code: str,
        error_message: str,
        retry_after_seconds: int | None = None,
    ) -> None:
        async with async_session_factory() as session:
            async with session.begin():
                job = await job_repository.get(session, snapshot.id)
                if job is None:
                    return
                await job_failure_handler.handle(
                    session,
                    job,
                    stage="llm_worker",
                    error_code=error_code,
                    error_message=error_message,
                    retry_after_seconds=retry_after_seconds,
                )

    async def _queue_result_jobs(
        self,
        session,
        job: InteractionJob,
        payload: Dict[str, Any],
        analysis_result: Dict[str, Any],
    ) -> None:
        call_stage = analysis_result.get("call_stage", "unknown")
        result_payload = dict(payload)
        result_payload["analysis_result"] = analysis_result
        priority = job.priority if isinstance(job.priority, JobPriority) else JobPriority.MEDIUM

        await job_repository.create_if_absent(
            session,
            interaction_id=job.interaction_id,
            session_id=job.session_id,
            lead_id=job.lead_id,
            campaign_id=job.campaign_id,
            customer_id=job.customer_id,
            job_type=JobType.SIGNAL_JOBS,
            priority=priority,
            idempotency_key=f"interaction:{job.interaction_id}:signal:{call_stage}:v1",
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

    def _snapshot(self, job: InteractionJob) -> ClaimedLLMJob:
        return ClaimedLLMJob(
            id=job.id,
            interaction_id=job.interaction_id,
            session_id=job.session_id,
            lead_id=job.lead_id,
            campaign_id=job.campaign_id,
            customer_id=job.customer_id,
            priority=job.priority,
            attempts=job.attempts,
            token_estimate=job.token_estimate,
            payload=job.payload or {},
        )

    def _context_from_snapshot(self, snapshot: ClaimedLLMJob) -> PostCallContext:
        payload = snapshot.payload
        ended_at = payload.get("ended_at")
        parsed_ended_at = (
            datetime.fromisoformat(ended_at) if isinstance(ended_at, str) else datetime.utcnow()
        )
        return PostCallContext(
            interaction_id=str(snapshot.interaction_id),
            session_id=str(snapshot.session_id),
            lead_id=str(snapshot.lead_id),
            campaign_id=str(snapshot.campaign_id),
            customer_id=str(snapshot.customer_id),
            agent_id=str(payload.get("agent_id", "")),
            call_sid=str(payload.get("call_sid") or ""),
            transcript_text=str(payload.get("transcript_text") or ""),
            conversation_data=payload.get("conversation_data") or {},
            additional_data=payload.get("additional_data") or {},
            ended_at=parsed_ended_at,
            exotel_account_id=payload.get("exotel_account_id"),
        )

    def _customer_budget(self, payload: Dict[str, Any]) -> int | None:
        additional = payload.get("additional_data") or {}
        budget = additional.get("customer_token_budget_per_minute")
        return int(budget) if budget else None


llm_worker_service = LLMWorkerService()
