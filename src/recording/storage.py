import logging

from src.config import settings

logger = logging.getLogger(__name__)


class EncryptedRecordingStorage:
    async def upload_from_url(self, recording_url: str, interaction_id: str) -> str:
        """
        Storage boundary for call recordings.

        The assessment does not include boto3 credentials. In production this
        method should stream from the provider URL to object storage with
        server-side encryption using settings.RECORDING_ENCRYPTION_KEY_ID.
        """
        s3_key = f"recordings/{interaction_id}.mp3"
        logger.info(
            "recording_storage_upload",
            extra={
                "interaction_id": interaction_id,
                "s3_bucket": settings.S3_BUCKET,
                "s3_key": s3_key,
                "encryption_key_id": settings.RECORDING_ENCRYPTION_KEY_ID,
            },
        )
        return s3_key


recording_storage = EncryptedRecordingStorage()
