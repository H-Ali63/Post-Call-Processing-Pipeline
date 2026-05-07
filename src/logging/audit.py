import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from src.models.workflow import AuditEvent, InteractionJob
from src.security.pii import redact_pii

logger = logging.getLogger("audit")


def _to_uuid(value: Any) -> Optional[UUID]:
    if value is None or value == "":
        return None
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


@dataclass(frozen=True)
class AuditContext:
    interaction_id: Optional[str] = None
    job_id: Optional[str] = None
    customer_id: Optional[str] = None
    campaign_id: Optional[str] = None
    stage: str = "unknown"
    retry_count: int = 0
    token_usage: int = 0

    @classmethod
    def from_job(cls, job: InteractionJob, stage: Optional[str] = None) -> "AuditContext":
        return cls(
            interaction_id=str(job.interaction_id),
            job_id=str(job.id),
            customer_id=str(job.customer_id),
            campaign_id=str(job.campaign_id),
            stage=stage or job.job_type.value,
            retry_count=job.attempts,
            token_usage=job.tokens_used,
        )


class AuditLogger:
    async def emit(
        self,
        session: Optional[AsyncSession],
        context: AuditContext,
        status: str,
        message: str,
        *,
        error_code: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        safe_metadata = redact_pii(metadata or {})
        log_extra = {
            "interaction_id": context.interaction_id,
            "customer_id": context.customer_id,
            "campaign_id": context.campaign_id,
            "job_id": context.job_id,
            "stage": context.stage,
            "retry_count": context.retry_count,
            "token_usage": context.token_usage,
            "status": status,
            "error_code": error_code,
            "metadata": safe_metadata,
        }

        if error_code:
            logger.warning(message, extra=log_extra)
        else:
            logger.info(message, extra=log_extra)

        if session is None:
            return

        event = AuditEvent(
            interaction_id=_to_uuid(context.interaction_id),
            job_id=_to_uuid(context.job_id),
            customer_id=_to_uuid(context.customer_id),
            campaign_id=_to_uuid(context.campaign_id),
            stage=context.stage,
            status=status,
            retry_count=context.retry_count,
            token_usage=context.token_usage,
            error_code=error_code,
            message=message,
            event_metadata=safe_metadata,
        )
        session.add(event)


audit_logger = AuditLogger()
