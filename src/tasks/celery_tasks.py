import asyncio
import logging
from typing import Any, Dict
from uuid import UUID

from src.config import settings
from src.llm_workers.worker import llm_worker_service
from src.models.workflow import JobPriority, JobType
from src.orchestrator.workflow import workflow_orchestrator
from src.recording.worker import recording_worker_service
from src.repositories.jobs import job_repository
from src.tasks.celery_app import celery_app
from src.utils.db import async_session_factory
from src.workers.result_worker import result_worker_service

logger = logging.getLogger(__name__)


def _run(coro):
    return asyncio.run(coro)


@celery_app.task(
    name="process_interaction_end_background_task",
    bind=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def process_interaction_end_background_task(self, payload: Dict[str, Any]):
    """
    Backward-compatible entry point.

    Older callers may still enqueue the historical Celery task. We no longer do
    the work inside Celery. We persist an ORCHESTRATE_INTERACTION job and then
    run one orchestrator tick. If this Celery message is lost, the new webhook
    path still has the durable DB row; if this compatibility task is retried,
    idempotency prevents duplicate processing.
    """
    return _run(_persist_legacy_payload_and_orchestrate(payload))


async def _persist_legacy_payload_and_orchestrate(payload: Dict[str, Any]) -> Dict[str, Any]:
    async with async_session_factory() as session:
        async with session.begin():
            job = await job_repository.create_if_absent(
                session,
                interaction_id=UUID(str(payload["interaction_id"])),
                session_id=UUID(str(payload["session_id"])),
                lead_id=UUID(str(payload["lead_id"])),
                campaign_id=UUID(str(payload["campaign_id"])),
                customer_id=UUID(str(payload["customer_id"])),
                job_type=JobType.ORCHESTRATE_INTERACTION,
                priority=JobPriority.HIGH,
                idempotency_key=f"interaction:{payload['interaction_id']}:orchestrate:v1",
                payload=payload,
            )

    processed = await _run_orchestrator(limit=1)
    return {"job_id": str(job.id), "orchestrated": processed}


@celery_app.task(
    name="run_workflow_orchestrator_task",
    bind=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_workflow_orchestrator_task(self, limit: int = settings.JOB_CLAIM_BATCH_SIZE):
    return _run(_run_orchestrator(limit=limit))


async def _run_orchestrator(limit: int) -> int:
    async with async_session_factory() as session:
        async with session.begin():
            return await workflow_orchestrator.run_once(session, limit=limit)


@celery_app.task(
    name="run_llm_worker_task",
    bind=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_llm_worker_task(self, limit: int = 10):
    return _run(llm_worker_service.run_once(limit=limit))


@celery_app.task(
    name="run_recording_worker_task",
    bind=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_recording_worker_task(self, limit: int = settings.JOB_CLAIM_BATCH_SIZE):
    return _run(recording_worker_service.run_once(limit=limit))


@celery_app.task(
    name="run_result_worker_task",
    bind=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_result_worker_task(self, limit: int = settings.JOB_CLAIM_BATCH_SIZE):
    return _run(result_worker_service.run_once(limit=limit))


@celery_app.task(
    name="run_all_workers_once_task",
    bind=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_all_workers_once_task(self):
    async def _run_all() -> Dict[str, int]:
        orchestrated = await _run_orchestrator(limit=settings.JOB_CLAIM_BATCH_SIZE)
        recordings = await recording_worker_service.run_once(
            limit=settings.JOB_CLAIM_BATCH_SIZE
        )
        llm = await llm_worker_service.run_once(limit=10)
        results = await result_worker_service.run_once(
            limit=settings.JOB_CLAIM_BATCH_SIZE
        )
        return {
            "orchestrated": orchestrated,
            "recordings": recordings,
            "llm": llm,
            "results": results,
        }

    return _run(_run_all())
