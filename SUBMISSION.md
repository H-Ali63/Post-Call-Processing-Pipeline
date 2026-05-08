# Post-Call Processing Pipeline - Design Document

**Author:** Md Haidar Ali  
**Date:** 2026-05-07

## 1. Assumptions

1. Postgres is the durable source of truth. Redis and Celery can restart without losing workflow state.
2. LLM limits are hard limits. A worker must reserve request and token capacity before calling the provider.
3. Short transcripts under four turns do not contain enough signal to justify LLM spend.
4. Customer budgets are per-minute reservations. High-priority jobs may use overflow only when global headroom is healthy.
5. Recording upload and transcript analysis are independent. A slow recording should not hold up the analysis path.

## 2. Problem Diagnosis

The old system treated Redis/Celery as the ledger for work. That meant a broker restart, process restart, or swallowed exception could leave an interaction half-processed with no durable retry record. The LLM path also fired requests without global or customer-level admission control, so rate limits showed up as provider errors instead of scheduler decisions.

## 3. Architecture Overview

```text
Telephony provider
    |
    v
FastAPI webhook
    |
    | writes interaction status + ORCHESTRATE_INTERACTION job
    v
Postgres interaction_jobs
    |
    v
Workflow orchestrator
    |
    +--> RECORDING_FETCH job -> recording worker -> encrypted storage
    |
    +--> short call -> SIGNAL_JOBS + LEAD_STAGE_UPDATE
    |
    +--> LLM_ANALYSIS job -> priority scheduler -> rate limiter -> LLM worker
                                      |
                                      v
                         SIGNAL_JOBS + LEAD_STAGE_UPDATE

Audit trail: audit_events
Token ledger: customer_token_usage
Terminal failures: dead_letter_jobs
```

Celery remains useful, but only as an execution trigger. The database decides what exists, what is running, what should retry, and what is dead-lettered.

## 4. Rate Limit Management

LLM jobs are claimed by priority, then admitted through `src/rate_limiter`. The limiter checks:

- global requests per minute
- global tokens per minute
- per-customer tokens per minute

If a job cannot fit, it is deferred by setting `interaction_jobs.run_after`. This makes rate pressure visible and replayable instead of turning it into an unhandled 429. Provider-side 429s are still caught as a defensive fallback and converted into persisted retries.

## 5. Per-Customer Token Budgeting

Each customer gets a default token budget from `CUSTOMER_DEFAULT_TOKENS_PER_MINUTE`, or a per-call override from `additional_data.customer_token_budget_per_minute`. A noisy customer over budget is deferred while other customers can continue. High-priority work can borrow overflow only when global utilization is below 70%.

## 6. Differentiated Processing

`src/scheduler/classifier.py` classifies work using transcript size, keywords, disposition, and metadata:

- `HIGH`: bookings, confirmations, demos, escalations
- `MEDIUM`: callbacks or ambiguous follow-up
- `LOW`: not interested, already done, low-value outcomes
- skip LLM: fewer than four transcript turns

This is deliberately lightweight. It keeps obvious decisions cheap while leaving full analysis to the LLM where it matters.

## 7. Recording Pipeline

The fixed 45-second sleep is gone. Recording fetches are `RECORDING_FETCH` jobs. Each attempt polls once:

- not ready -> retry with backoff
- permanently unavailable -> dead-letter
- uploaded -> update `interactions.recording_s3_key`

Every retry and terminal failure writes an audit event.

## 8. Reliability & Durability

`interaction_jobs` stores the job type, status, priority, idempotency key, attempt count, max attempts, lock owner, lock time, run-after time, payload, result, and last error. Workers claim rows with `FOR UPDATE SKIP LOCKED`, and stale running jobs are moved back to `DEFERRED` after the lock timeout.

## 9. Auditability & Observability

`audit_events` stores:

- `interaction_id`
- `customer_id`
- `campaign_id`
- `job_id`
- `stage`
- `status`
- `retry_count`
- `token_usage`
- `error_code`
- metadata with PII redaction

Prometheus metrics are exposed through `/metrics` for job outcomes, retries, dead letters, LLM tokens, latency, queue depth, and recording attempts.

## 10. Data Model

Added tables:

```text
interaction_jobs
customer_token_usage
audit_events
dead_letter_jobs
```

The schema is in `data/schema.sql`. The Alembic migration is:

```text
migrations/versions/20260507_0001_durable_workflow_tables.py
```

## 11. Security

Webhook HMAC validation is supported with `WEBHOOK_SIGNING_SECRET`. Audit metadata is redacted before logging or persistence. Recording writes go through an encrypted storage abstraction with `RECORDING_ENCRYPTION_KEY_ID` ready for a real KMS-backed upload implementation.

## 12. API Interface

The endpoint path is unchanged:

```text
POST /api/v1/session/{session_id}/interaction/{interaction_id}/end
```

The response now includes `workflow_job_id`. That gives support and on-call engineers a stable ID to trace from webhook receipt to final outcome.

## 13. Trade-offs

I chose a DB-backed workflow instead of adding Temporal or another workflow engine because this assignment already has Postgres and needs a locally testable implementation. The trade-off is that very high volume may eventually need partitioned job tables or a purpose-built workflow system. The current design is still a large reliability improvement because work is now durable and inspectable.

## 14. Known Weaknesses

Customer budget configuration is still basic. A production version should add a customer configuration table instead of relying on defaults and webhook metadata. The provider client is also intentionally minimal; the mock path is enough for local tests, but a production client should add full provider-specific request/response handling.
