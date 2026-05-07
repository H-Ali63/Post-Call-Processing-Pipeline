from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from src.logging.audit import AuditContext, audit_logger
from src.models.interaction import Interaction
from src.models.workflow import InteractionJob, JobPriority, JobType
from src.repositories.jobs import job_repository


class DurableInteractionQueue:
    async def enqueue_interaction_ended(
        self,
        session: AsyncSession,
        *,
        interaction: Interaction,
        call_sid: Optional[str],
        duration_seconds: Optional[int],
        additional_data: Optional[Dict[str, Any]],
        ended_at: datetime,
    ) -> InteractionJob:
        payload = {
            "interaction_id": str(interaction.id),
            "session_id": str(interaction.session_id),
            "lead_id": str(interaction.lead_id),
            "campaign_id": str(interaction.campaign_id),
            "customer_id": str(interaction.customer_id),
            "agent_id": str(interaction.agent_id),
            "call_sid": call_sid or interaction.call_sid,
            "duration_seconds": duration_seconds,
            "conversation_data": interaction.conversation_data or {},
            "transcript_text": interaction.transcript_text,
            "additional_data": additional_data or {},
            "ended_at": ended_at.isoformat(),
            "exotel_account_id": interaction.exotel_account_id,
        }
        job = await job_repository.create_if_absent(
            session,
            interaction_id=UUID(str(interaction.id)),
            session_id=UUID(str(interaction.session_id)),
            lead_id=UUID(str(interaction.lead_id)),
            campaign_id=UUID(str(interaction.campaign_id)),
            customer_id=UUID(str(interaction.customer_id)),
            job_type=JobType.ORCHESTRATE_INTERACTION,
            priority=JobPriority.HIGH,
            idempotency_key=f"interaction:{interaction.id}:orchestrate:v1",
            payload=payload,
        )
        await audit_logger.emit(
            session,
            AuditContext.from_job(job, stage="webhook"),
            "queued",
            "interaction_end_workflow_queued",
            metadata={"call_sid_present": bool(payload["call_sid"])},
        )
        return job


durable_interaction_queue = DurableInteractionQueue()
