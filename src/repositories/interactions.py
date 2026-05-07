from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.interaction import Interaction, InteractionStatus
from src.models.lead import Lead


class InteractionRepository:
    async def get(self, session: AsyncSession, interaction_id: UUID) -> Optional[Interaction]:
        result = await session.execute(
            select(Interaction).where(Interaction.id == interaction_id)
        )
        return result.scalar_one_or_none()

    async def as_payload(
        self, session: AsyncSession, interaction_id: UUID
    ) -> Optional[Dict[str, Any]]:
        interaction = await self.get(session, interaction_id)
        if interaction is None:
            return None

        return {
            "id": str(interaction.id),
            "session_id": str(interaction.session_id),
            "lead_id": str(interaction.lead_id),
            "campaign_id": str(interaction.campaign_id),
            "customer_id": str(interaction.customer_id),
            "agent_id": str(interaction.agent_id),
            "call_sid": interaction.call_sid,
            "conversation_data": interaction.conversation_data or {},
            "interaction_metadata": interaction.interaction_metadata or {},
            "exotel_account_id": interaction.exotel_account_id,
            "transcript_text": interaction.transcript_text,
            "is_short_transcript": interaction.is_short_transcript,
        }

    async def mark_ended(
        self,
        session: AsyncSession,
        interaction: Interaction,
        *,
        ended_at: datetime,
        duration_seconds: Optional[int],
        call_sid: Optional[str],
    ) -> None:
        interaction.status = InteractionStatus.ENDED
        interaction.ended_at = ended_at
        interaction.duration_seconds = duration_seconds
        if call_sid:
            interaction.call_sid = call_sid

    async def mark_processing(
        self, session: AsyncSession, interaction_id: UUID, analysis_status: str
    ) -> None:
        interaction = await self.get(session, interaction_id)
        if interaction is None:
            return

        metadata = dict(interaction.interaction_metadata or {})
        metadata["analysis_status"] = analysis_status
        interaction.interaction_metadata = metadata

    async def update_analysis_result(
        self,
        session: AsyncSession,
        interaction_id: UUID,
        *,
        call_stage: str,
        entities: Dict[str, Any],
        summary: str,
        tokens_used: int,
        provider: str,
        model: str,
    ) -> None:
        interaction = await self.get(session, interaction_id)
        if interaction is None:
            return

        metadata = dict(interaction.interaction_metadata or {})
        metadata.update(
            {
                "analysis_status": "completed",
                "call_stage": call_stage,
                "entities": entities,
                "summary": summary,
                "tokens_used": tokens_used,
                "llm_provider": provider,
                "llm_model": model,
                "analyzed_at": datetime.utcnow().isoformat(),
            }
        )
        interaction.interaction_metadata = metadata

    async def mark_short_call(
        self, session: AsyncSession, interaction_id: UUID
    ) -> None:
        interaction = await self.get(session, interaction_id)
        if interaction is None:
            return

        metadata = dict(interaction.interaction_metadata or {})
        metadata.update(
            {
                "analysis_status": "skipped",
                "call_stage": "short_call",
                "skip_reason": "short_transcript",
                "tokens_used": 0,
            }
        )
        interaction.interaction_metadata = metadata

    async def update_recording_key(
        self, session: AsyncSession, interaction_id: UUID, s3_key: str
    ) -> None:
        interaction = await self.get(session, interaction_id)
        if interaction is None:
            return

        interaction.recording_s3_key = s3_key

    async def append_error(
        self,
        session: AsyncSession,
        interaction_id: UUID,
        *,
        stage: str,
        error_code: str,
        error_message: str,
    ) -> None:
        interaction = await self.get(session, interaction_id)
        if interaction is None:
            return

        errors = list(interaction.error_log or [])
        errors.append(
            {
                "stage": stage,
                "error_code": error_code,
                "error_message": error_message,
                "at": datetime.utcnow().isoformat(),
            }
        )
        interaction.error_log = errors
        interaction.retry_count = (interaction.retry_count or 0) + 1

    async def update_lead_stage(
        self,
        session: AsyncSession,
        *,
        lead_id: UUID,
        call_stage: str,
    ) -> None:
        result = await session.execute(select(Lead).where(Lead.id == lead_id))
        lead = result.scalar_one_or_none()
        if lead is None:
            return

        lead.stage = self._map_call_stage_to_lead_stage(call_stage)

    def _map_call_stage_to_lead_stage(self, call_stage: str) -> str:
        mapping = {
            "rebook_confirmed": "booked",
            "demo_booked": "booked",
            "callback_requested": "follow_up",
            "escalation_needed": "needs_attention",
            "not_interested": "closed_lost",
            "short_call": "short_call",
            "already_done": "completed",
        }
        return mapping.get(call_stage, call_stage or "unknown")


interaction_repository = InteractionRepository()
