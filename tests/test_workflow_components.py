from datetime import datetime

import pytest

from src.models.workflow import JobPriority
from src.rate_limiter.budget import (
    BudgetAdmissionPolicy,
    BudgetState,
)
from src.recording.service import RecordingNotReady, RecordingService
from src.retries.policy import ExponentialBackoffPolicy
from src.scheduler.classifier import InteractionClassifier
from src.security.pii import REDACTED, redact_pii


def test_short_transcript_skips_llm(sample_transcripts):
    transcript = sample_transcripts["short_call_hangup"]["transcript"]
    classification = InteractionClassifier().classify(
        transcript=transcript,
        transcript_text="\n".join(turn["content"] for turn in transcript),
        additional_data={},
    )

    assert classification.skip_llm is True
    assert classification.call_stage == "short_call"


def test_high_value_call_gets_high_priority(sample_transcripts):
    transcript = sample_transcripts["demo_booked"]["transcript"]
    classification = InteractionClassifier().classify(
        transcript=transcript,
        transcript_text="\n".join(turn["content"] for turn in transcript),
        additional_data={},
    )

    assert classification.skip_llm is False
    assert classification.priority == JobPriority.HIGH


def test_customer_budget_isolation_blocks_noisy_customer():
    policy = BudgetAdmissionPolicy()
    decision = policy.decide(
        global_state=BudgetState(
            token_limit=10_000,
            request_limit=100,
            reserved_tokens=1_000,
            reserved_requests=10,
        ),
        customer_state=BudgetState(
            token_limit=2_000,
            request_limit=100,
            reserved_tokens=1_900,
            reserved_requests=10,
        ),
        estimated_tokens=500,
        priority=JobPriority.LOW,
    )

    assert decision.allowed is False
    assert decision.reason == "CUSTOMER_TOKEN_BUDGET"


def test_high_priority_can_use_overflow_headroom():
    policy = BudgetAdmissionPolicy()
    decision = policy.decide(
        global_state=BudgetState(
            token_limit=10_000,
            request_limit=100,
            reserved_tokens=1_000,
            reserved_requests=10,
        ),
        customer_state=BudgetState(
            token_limit=2_000,
            request_limit=100,
            reserved_tokens=1_900,
            reserved_requests=10,
        ),
        estimated_tokens=500,
        priority=JobPriority.HIGH,
    )

    assert decision.allowed is True
    assert decision.reason == "ADMITTED_OVERFLOW"


def test_global_rate_limit_blocks_all_customers_at_capacity():
    policy = BudgetAdmissionPolicy()
    decision = policy.decide(
        global_state=BudgetState(
            token_limit=10_000,
            request_limit=100,
            reserved_tokens=9_800,
            reserved_requests=10,
        ),
        customer_state=BudgetState(
            token_limit=5_000,
            request_limit=100,
            reserved_tokens=0,
            reserved_requests=0,
        ),
        estimated_tokens=500,
        priority=JobPriority.HIGH,
    )

    assert decision.allowed is False
    assert decision.reason == "GLOBAL_RATE_LIMIT"


def test_exponential_backoff_persists_next_retry_time():
    policy = ExponentialBackoffPolicy(base_seconds=10, max_seconds=300)
    decision = policy.decide(attempt=3, max_attempts=5, now=datetime(2026, 5, 7))

    assert decision.should_retry is True
    assert decision.delay_seconds == 40
    assert decision.next_run_at == datetime(2026, 5, 7, 0, 0, 40)


def test_dead_letter_after_retry_exhaustion():
    policy = ExponentialBackoffPolicy(base_seconds=10, max_seconds=300)
    decision = policy.decide(attempt=5, max_attempts=5, now=datetime(2026, 5, 7))

    assert decision.should_retry is False
    assert decision.next_run_at is None


def test_pii_redaction_removes_transcript_and_phone():
    redacted = redact_pii(
        {
            "interaction_id": "i1",
            "lead_phone": "+919876543210",
            "transcript": [{"content": "sensitive"}],
        }
    )

    assert redacted["interaction_id"] == "i1"
    assert redacted["lead_phone"] == REDACTED
    assert redacted["transcript"] == REDACTED


@pytest.mark.asyncio
async def test_recording_poll_once_not_ready(monkeypatch):
    class FakeResponse:
        status_code = 404

        def json(self):
            return {}

        def raise_for_status(self):
            raise AssertionError("should not raise for 404 not ready")

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, url):
            return FakeResponse()

    monkeypatch.setattr("src.recording.service.httpx.AsyncClient", FakeClient)

    with pytest.raises(RecordingNotReady):
        await RecordingService().poll_once(
            interaction_id="i1",
            call_sid="call1",
            exotel_account_id="acct1",
        )
