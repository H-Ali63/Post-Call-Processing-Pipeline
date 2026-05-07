import os
from typing import List


class Settings:
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@localhost:5432/voicebot",
    )
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/1")
    CELERY_RESULT_BACKEND: str = os.getenv(
        "CELERY_RESULT_BACKEND", "redis://localhost:6379/2"
    )

    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "openai")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "gpt-4o")
    LLM_API_KEY: str = os.getenv("LLM_API_KEY", "sk-mock-key-for-assessment")
    LLM_TOKENS_PER_MINUTE: int = int(os.getenv("LLM_TOKENS_PER_MINUTE", "90000"))
    LLM_REQUESTS_PER_MINUTE: int = int(os.getenv("LLM_REQUESTS_PER_MINUTE", "500"))
    LLM_AVG_TOKENS_PER_CALL: int = int(os.getenv("LLM_AVG_TOKENS_PER_CALL", "1500"))
    LLM_MAX_PROMPT_TOKENS: int = int(os.getenv("LLM_MAX_PROMPT_TOKENS", "12000"))
    LLM_RESERVATION_SAFETY_FACTOR: float = float(
        os.getenv("LLM_RESERVATION_SAFETY_FACTOR", "1.15")
    )

    CUSTOMER_DEFAULT_TOKENS_PER_MINUTE: int = int(
        os.getenv("CUSTOMER_DEFAULT_TOKENS_PER_MINUTE", "15000")
    )
    CUSTOMER_MIN_RESERVED_TOKENS_PER_MINUTE: int = int(
        os.getenv("CUSTOMER_MIN_RESERVED_TOKENS_PER_MINUTE", "3000")
    )

    RECORDING_INITIAL_RETRY_SECONDS: int = int(
        os.getenv("RECORDING_INITIAL_RETRY_SECONDS", "5")
    )
    RECORDING_MAX_RETRY_SECONDS: int = int(
        os.getenv("RECORDING_MAX_RETRY_SECONDS", "120")
    )
    RECORDING_MAX_ATTEMPTS: int = int(os.getenv("RECORDING_MAX_ATTEMPTS", "8"))
    S3_BUCKET: str = os.getenv("S3_BUCKET", "voicebot-recordings")
    RECORDING_ENCRYPTION_KEY_ID: str = os.getenv(
        "RECORDING_ENCRYPTION_KEY_ID", "local-dev-key"
    )

    POSTCALL_CELERY_QUEUE: str = os.getenv("POSTCALL_CELERY_QUEUE", "postcall_processing")
    WORKFLOW_CELERY_QUEUE: str = os.getenv("WORKFLOW_CELERY_QUEUE", "workflow")
    LLM_HIGH_PRIORITY_QUEUE: str = os.getenv("LLM_HIGH_PRIORITY_QUEUE", "llm_high")
    LLM_MEDIUM_PRIORITY_QUEUE: str = os.getenv("LLM_MEDIUM_PRIORITY_QUEUE", "llm_medium")
    LLM_LOW_PRIORITY_QUEUE: str = os.getenv("LLM_LOW_PRIORITY_QUEUE", "llm_low")

    WORKFLOW_MAX_ATTEMPTS: int = int(os.getenv("WORKFLOW_MAX_ATTEMPTS", "5"))
    WORKFLOW_BASE_RETRY_SECONDS: int = int(
        os.getenv("WORKFLOW_BASE_RETRY_SECONDS", "10")
    )
    WORKFLOW_MAX_RETRY_SECONDS: int = int(
        os.getenv("WORKFLOW_MAX_RETRY_SECONDS", "900")
    )
    JOB_CLAIM_BATCH_SIZE: int = int(os.getenv("JOB_CLAIM_BATCH_SIZE", "25"))
    JOB_LOCK_TIMEOUT_SECONDS: int = int(os.getenv("JOB_LOCK_TIMEOUT_SECONDS", "300"))
    SCHEDULER_IDLE_SLEEP_SECONDS: float = float(
        os.getenv("SCHEDULER_IDLE_SLEEP_SECONDS", "1.0")
    )

    WEBHOOK_SIGNING_SECRET: str = os.getenv("WEBHOOK_SIGNING_SECRET", "")
    WEBHOOK_SIGNATURE_HEADER: str = os.getenv(
        "WEBHOOK_SIGNATURE_HEADER", "X-Webhook-Signature"
    )
    PII_LOG_FIELDS: List[str] = [
        "phone",
        "lead_phone",
        "email",
        "name",
        "transcript",
        "transcript_text",
        "recording_url",
    ]


settings = Settings()
