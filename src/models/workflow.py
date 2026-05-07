import enum
import uuid

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from src.models.base import Base


class JobPriority(str, enum.Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DEFERRED = "DEFERRED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    DEAD_LETTERED = "DEAD_LETTERED"


class JobType(str, enum.Enum):
    ORCHESTRATE_INTERACTION = "ORCHESTRATE_INTERACTION"
    LLM_ANALYSIS = "LLM_ANALYSIS"
    RECORDING_FETCH = "RECORDING_FETCH"
    SIGNAL_JOBS = "SIGNAL_JOBS"
    LEAD_STAGE_UPDATE = "LEAD_STAGE_UPDATE"


class InteractionJob(Base):
    __tablename__ = "interaction_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    interaction_id = Column(
        UUID(as_uuid=True), ForeignKey("interactions.id"), nullable=False, index=True
    )
    session_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    lead_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    campaign_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    customer_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    job_type = Column(
        Enum(JobType, native_enum=False), nullable=False, index=True
    )
    status = Column(
        Enum(JobStatus, native_enum=False),
        default=JobStatus.PENDING,
        nullable=False,
        index=True,
    )
    priority = Column(
        Enum(JobPriority, native_enum=False),
        default=JobPriority.MEDIUM,
        nullable=False,
        index=True,
    )

    idempotency_key = Column(String(255), nullable=False)
    payload = Column(JSONB, default=dict, nullable=False)
    result = Column(JSONB, default=dict, nullable=False)

    token_estimate = Column(Integer, default=0, nullable=False)
    tokens_used = Column(Integer, default=0, nullable=False)

    attempts = Column(Integer, default=0, nullable=False)
    max_attempts = Column(Integer, default=5, nullable=False)
    run_after = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )

    locked_by = Column(String(255), nullable=True, index=True)
    locked_at = Column(DateTime(timezone=True), nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    last_error_code = Column(String(100), nullable=True)
    last_error_message = Column(Text, nullable=True)

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_interaction_jobs_idempotency_key"),
        Index(
            "idx_interaction_jobs_ready",
            "status",
            "run_after",
            "priority",
            "created_at",
        ),
        Index("idx_interaction_jobs_customer_status", "customer_id", "status"),
    )


class CustomerTokenUsage(Base):
    __tablename__ = "customer_token_usage"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    window_start = Column(DateTime(timezone=True), nullable=False, index=True)

    budget_tokens = Column(Integer, nullable=False)
    reserved_tokens = Column(Integer, default=0, nullable=False)
    used_tokens = Column(Integer, default=0, nullable=False)
    requests_reserved = Column(Integer, default=0, nullable=False)
    requests_used = Column(Integer, default=0, nullable=False)

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "customer_id", "window_start", name="uq_customer_token_usage_window"
        ),
    )


class AuditEvent(Base):
    __tablename__ = "audit_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    interaction_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    job_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    customer_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    campaign_id = Column(UUID(as_uuid=True), nullable=True, index=True)

    stage = Column(String(100), nullable=False, index=True)
    status = Column(String(50), nullable=False, index=True)
    retry_count = Column(Integer, default=0, nullable=False)
    token_usage = Column(Integer, default=0, nullable=False)
    error_code = Column(String(100), nullable=True, index=True)
    message = Column(Text, nullable=True)
    event_metadata = Column("metadata", JSONB, default=dict, nullable=False)

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )


class DeadLetterJob(Base):
    __tablename__ = "dead_letter_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    original_job_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    interaction_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    customer_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    campaign_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    job_type = Column(String(100), nullable=False, index=True)
    stage = Column(String(100), nullable=False, index=True)
    payload = Column(JSONB, default=dict, nullable=False)
    attempts = Column(Integer, default=0, nullable=False)
    last_error_code = Column(String(100), nullable=True)
    last_error_message = Column(Text, nullable=True)

    failed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    replayed_at = Column(DateTime(timezone=True), nullable=True)
