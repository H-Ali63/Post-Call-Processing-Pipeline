import logging
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.logging.audit import AuditContext, audit_logger
from src.queues.durable import durable_interaction_queue
from src.repositories.interactions import interaction_repository
from src.security.webhooks import validate_webhook_signature
from src.tasks.celery_tasks import run_workflow_orchestrator_task
from src.utils.db import get_db

logger = logging.getLogger(__name__)
router = APIRouter()


class InteractionEndRequest(BaseModel):
    call_sid: Optional[str] = None
    duration_seconds: Optional[int] = None
    call_status: Optional[str] = None
    additional_data: Optional[Dict[str, Any]] = None


class InteractionEndResponse(BaseModel):
    status: str
    interaction_id: str
    message: str
    workflow_job_id: Optional[str] = None


@router.post(
    "/session/{session_id}/interaction/{interaction_id}/end",
    response_model=InteractionEndResponse,
)
async def end_interaction(
    session_id: UUID,
    interaction_id: UUID,
    payload: InteractionEndRequest,
    webhook_request: Request,
    db: AsyncSession = Depends(get_db),
):
    """
    Accept the telephony call-end webhook and enqueue a durable workflow.

    The endpoint intentionally does no LLM, recording, signal, or lead-stage
    side effects inline. It writes the interaction status and an idempotent
    ORCHESTRATE_INTERACTION job in one DB transaction, then best-effort nudges
    Celery. If Celery/Redis is down, the durable job remains claimable.
    """
    try:
        await validate_webhook_signature(webhook_request)
        ended_at = datetime.utcnow()

        async with db.begin():
            interaction = await interaction_repository.get(db, interaction_id)
            if interaction is None:
                raise HTTPException(status_code=404, detail="Interaction not found")
            if interaction.session_id != session_id:
                raise HTTPException(
                    status_code=409,
                    detail="Interaction does not belong to requested session",
                )

            await interaction_repository.mark_ended(
                db,
                interaction,
                ended_at=ended_at,
                duration_seconds=payload.duration_seconds,
                call_sid=payload.call_sid,
            )
            job = await durable_interaction_queue.enqueue_interaction_ended(
                db,
                interaction=interaction,
                call_sid=payload.call_sid,
                duration_seconds=payload.duration_seconds,
                additional_data=payload.additional_data,
                ended_at=ended_at,
            )

        try:
            # Celery is just a nudge here. The job was already committed above,
            # so a broker outage should not turn into a lost interaction.
            run_workflow_orchestrator_task.apply_async(
                queue=settings.WORKFLOW_CELERY_QUEUE, kwargs={"limit": 10}
            )
        except Exception as exc:
            async with db.begin():
                await audit_logger.emit(
                    db,
                    AuditContext.from_job(job, stage="webhook"),
                    "queued",
                    "workflow_wakeup_failed",
                    error_code=exc.__class__.__name__,
                    metadata={"error": str(exc)},
                )

        return InteractionEndResponse(
            status="ok",
            interaction_id=str(interaction_id),
            workflow_job_id=str(job.id),
            message="Interaction ended, durable workflow queued",
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(
            "end_interaction_failed",
            extra={"interaction_id": str(interaction_id), "error": str(exc)},
        )
        raise HTTPException(status_code=500, detail="Internal server error")
