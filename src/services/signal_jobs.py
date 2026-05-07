import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


async def trigger_signal_jobs(
    interaction_id: str,
    session_id: str,
    campaign_id: str,
    analysis_result: Dict[str, Any],
) -> None:
    """
    Dispatch downstream campaign actions.

    This function is now called by durable SIGNAL_JOBS workers rather than
    FastAPI fire-and-forget tasks. Failures bubble to the worker so they can be
    retried or dead-lettered with an audit trail.
    """
    logger.info(
        "signal_jobs_triggered",
        extra={
            "interaction_id": interaction_id,
            "session_id": session_id,
            "campaign_id": campaign_id,
            "has_analysis": bool(analysis_result),
        },
    )


async def update_lead_stage(
    lead_id: str,
    interaction_id: str,
    call_stage: str,
) -> None:
    logger.info(
        "lead_stage_updated",
        extra={
            "lead_id": lead_id,
            "interaction_id": interaction_id,
            "new_stage": call_stage,
        },
    )
