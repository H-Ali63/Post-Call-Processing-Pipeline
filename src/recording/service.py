from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import httpx

from src.recording.storage import EncryptedRecordingStorage, recording_storage


class RecordingNotReady(Exception):
    pass


class RecordingUnavailable(Exception):
    pass


@dataclass(frozen=True)
class RecordingResult:
    s3_key: str
    recording_url: str


class RecordingService:
    def __init__(self, storage: EncryptedRecordingStorage | None = None):
        self._storage = storage or recording_storage

    async def poll_once(
        self,
        *,
        interaction_id: str,
        call_sid: str,
        exotel_account_id: str,
    ) -> RecordingResult:
        if not call_sid or not exotel_account_id:
            raise RecordingUnavailable("Missing call SID or Exotel account ID")

        recording_url = await self._fetch_exotel_recording_url(
            call_sid, exotel_account_id
        )
        s3_key = await self._storage.upload_from_url(recording_url, interaction_id)
        return RecordingResult(s3_key=s3_key, recording_url=recording_url)

    async def _fetch_exotel_recording_url(
        self, call_sid: str, account_id: str
    ) -> str:
        url = f"https://api.exotel.com/v1/Accounts/{account_id}/Calls/{call_sid}/Recording"

        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(url)

        if response.status_code == 200:
            data = response.json()
            recording_url = data.get("recording_url")
            if recording_url:
                return recording_url
            raise RecordingNotReady("Recording response did not include a URL")

        if response.status_code == 404:
            raise RecordingNotReady("Recording is not ready yet")

        if response.status_code in {400, 410}:
            raise RecordingUnavailable(
                f"Recording permanently unavailable: HTTP {response.status_code}"
            )

        response.raise_for_status()
        raise RecordingNotReady("Recording is not ready yet")


recording_service = RecordingService()
