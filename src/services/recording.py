"""
Compatibility wrapper for the recording pipeline.

The old implementation slept for 45 seconds and tried once. Recording work is
now represented by durable RECORDING_FETCH jobs; this wrapper performs a single
poll attempt and lets the worker persist retry/backoff/dead-letter state.
"""

import logging
from typing import Optional

from src.recording.service import (
    RecordingNotReady,
    RecordingUnavailable,
    recording_service,
)

logger = logging.getLogger(__name__)


async def fetch_and_upload_recording(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
) -> Optional[str]:
    try:
        result = await recording_service.poll_once(
            interaction_id=interaction_id,
            call_sid=call_sid,
            exotel_account_id=exotel_account_id,
        )
        return result.s3_key
    except RecordingNotReady:
        logger.info(
            "recording_not_ready",
            extra={"interaction_id": interaction_id, "call_sid": call_sid},
        )
        return None
    except RecordingUnavailable as exc:
        logger.warning(
            "recording_unavailable",
            extra={
                "interaction_id": interaction_id,
                "call_sid": call_sid,
                "error": str(exc),
            },
        )
        return None
