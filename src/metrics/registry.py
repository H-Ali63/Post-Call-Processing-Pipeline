from prometheus_client import Counter, Gauge, Histogram, generate_latest


workflow_jobs_total = Counter(
    "workflow_jobs_total",
    "Workflow jobs by type and terminal status",
    ["job_type", "status"],
)

workflow_retries_total = Counter(
    "workflow_retries_total",
    "Workflow retry attempts by job type and error code",
    ["job_type", "error_code"],
)

workflow_dead_letters_total = Counter(
    "workflow_dead_letters_total",
    "Workflow jobs moved to dead letter",
    ["job_type", "error_code"],
)

llm_tokens_total = Counter(
    "llm_tokens_total",
    "LLM tokens used by customer and campaign",
    ["customer_id", "campaign_id", "model"],
)

llm_latency_ms = Histogram(
    "llm_latency_ms",
    "LLM request latency in milliseconds",
    buckets=(100, 250, 500, 1000, 2500, 5000, 10000, 30000),
)

queue_depth = Gauge(
    "workflow_queue_depth",
    "Current durable queue depth by job type and status",
    ["job_type", "status"],
)

rate_limit_utilization = Gauge(
    "llm_rate_limit_utilization",
    "LLM rate-limit utilization ratio",
    ["scope"],
)

recording_attempts_total = Counter(
    "recording_attempts_total",
    "Recording polling attempts by status",
    ["status"],
)


def prometheus_payload() -> bytes:
    return generate_latest()
