# Post-Call Processing Pipeline - Design Document

**Author:** Md Haidar Ali
**Date:** 2026-05-07

## 1. Assumptions

1. Postgres is the durable source of truth; Redis/Celery may restart without data loss.
2. LLM rate limits are hard limits. Admission must happen before provider calls.
3. Short transcripts under 4 turns do not need LLM analysis.
4. Customer token budgets are per-minute reservations, with high-priority overflow only when global headroom exists.
5. Recording upload and LLM analysis are independent workflow branches.

## 2. Problem Diagnosis

The old pipeline put real work into `asyncio.create_task`, Redis lists, and one monolithic Celery task. That caused silent task loss, duplicate retry paths, blocking recording sleeps, no dead-letter queue, no durable audit trail, and uncontrolled LLM calls.

## 3. Architecture Overview

```text
Webhook API
  -> interaction status update + interaction_jobs row
  -> Workflow Orchestrator
  -> Durable child jobs:
       RECORDING_FETCH
       LLM_ANALYSIS or short-call result jobs
  -> Priority/rate-limited LLM Worker
  -> SIGNAL_JOBS + LEAD_STAGE_UPDATE
  -> audit_events / dead_letter_jobs / customer_token_usage
```

Celery is now a wake-up/execution layer only. Job state, retry state, idempotency, and terminal outcomes live in Postgres.

### HLD Diagram-
                        ┌────────────────────┐
                        │ Telephony Provider │
                        └─────────┬──────────┘
                                  │
                                  ▼
                     POST /interaction/end
                                  │
                                  ▼
                    ┌────────────────────────┐
                    │ FastAPI Webhook Layer  │
                    └─────────┬──────────────┘
                              │
                              ▼
                 ┌───────────────────────────┐
                 │ interaction_jobs (DB)     │
                 │ Durable Workflow State    │
                 └─────────┬─────────────────┘
                           │
                           ▼
                ┌────────────────────────────┐
                │ Workflow Orchestrator      │
                └─────────┬──────────────────┘
                          │
         ┌────────────────┼──────────────────┐
         ▼                ▼                  ▼
 ┌─────────────┐  ┌──────────────┐  ┌────────────────┐
 │Recording Job│  │Priority Queue│  │Short Call Skip │
 └──────┬──────┘  └──────┬───────┘  └────────────────┘
        │                │
        ▼                ▼
 ┌─────────────┐  ┌─────────────────────┐
 │Retry Poller│  │Rate Limit Scheduler │
 └──────┬──────┘  └─────────┬───────────┘
        │                   │
        ▼                   ▼
 ┌─────────────┐   ┌──────────────────┐
 │S3 Recording │   │LLM Worker Pool   │
 └─────────────┘   └────────┬─────────┘
                             │
                             ▼
                  ┌────────────────────┐
                  │ Result Processing  │
                  └─────────┬──────────┘
                            ▼
              CRM / Dashboard / Follow-up

## 4. Rate Limit Management

`src/rate_limiter` reserves capacity before LLM execution using global RPM/TPM and per-customer TPM windows in `customer_token_usage`. If capacity is unavailable, the job is deferred with a future `run_after`; provider 429s are caught and persisted as retryable workflow events.

## 5. Per-Customer Token Budgeting

Each customer gets `CUSTOMER_DEFAULT_TOKENS_PER_MINUTE`. A customer over budget is deferred without consuming another customer's reservation. High-priority jobs can borrow overflow only when global utilization is below 70%.

## 6. Differentiated Processing

`src/scheduler/classifier.py` classifies interactions using transcript length, keywords, and supplied disposition/customer metadata. High-value calls are queued before medium and low. Short transcripts skip LLM entirely.

## 7. Recording Pipeline

`asyncio.sleep(45)` is removed. `RECORDING_FETCH` jobs poll once, then defer with exponential backoff. Exhaustion moves the job to `dead_letter_jobs` with audit events.

## 8. Reliability & Durability

`interaction_jobs` has idempotency keys, status, attempts, max attempts, lock ownership, `run_after`, and terminal status. Workers claim rows using DB locks, recover stale locks, and persist retries/dead letters.

## 9. Auditability & Observability

`audit_events` persists `interaction_id`, `customer_id`, `campaign_id`, stage, status, retry count, token usage, error code, and metadata. Prometheus metrics are exposed at `/metrics`.

## 10. Data Model

Added:

```text
interaction_jobs
customer_token_usage
audit_events
dead_letter_jobs
```

SQL is in `data/schema.sql`; Alembic migration is `migrations/versions/20260507_0001_durable_workflow_tables.py`.

## 11. Security

Webhook HMAC validation is supported through `WEBHOOK_SIGNING_SECRET`. Audit metadata is PII-redacted. Recording storage is behind an encrypted storage abstraction with a KMS key setting.

## 12. API Interface

The endpoint path stays the same. The response adds `workflow_job_id` so callers/on-call can trace durable work.

## 13. Trade-offs

DB-backed workflow is simpler and more inspectable than introducing Temporal in this assignment. At very high scale, this could evolve to Temporal or a partitioned queue table, but Postgres row claiming is adequate and testable for the requested refactor.

## 14. Known Weaknesses

Customer-specific budget configuration is still supplied through defaults or metadata, not a full admin-managed table. The LLM provider client is a minimal boundary suitable for the mock assessment and should be expanded for production provider payloads.
