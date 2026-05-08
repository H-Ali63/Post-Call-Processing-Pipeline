# VoiceBot Post-Call Workflow

This repo now contains a durable post-call processing pipeline for completed
voice interactions. The important change is that Postgres owns workflow state;
Celery and Redis only wake workers up and move work forward.

## Current Flow

```text
POST /api/v1/session/{session_id}/interaction/{interaction_id}/end
    -> update interaction status
    -> insert idempotent interaction_jobs row
    -> workflow orchestrator creates child jobs
    -> recording worker polls/uploads audio
    -> LLM worker admits jobs through the rate limiter
    -> result worker runs signal jobs and lead-stage updates
```

Short transcripts under four turns skip LLM analysis. Long transcripts are
classified into `HIGH`, `MEDIUM`, or `LOW` priority before they reach the LLM
worker.

## What Changed

- Durable queue tables: `interaction_jobs`, `dead_letter_jobs`, `audit_events`,
  and `customer_token_usage`.
- No `asyncio.create_task` fire-and-forget work in the webhook.
- No fixed `asyncio.sleep(45)` recording delay.
- No Redis retry queue. Retries and dead letters are stored in Postgres.
- Global and per-customer LLM token admission happens before provider calls.
- The old binary dialler freeze is replaced with a proportional capacity signal.
- Prometheus metrics are exposed at `/metrics`.
- Webhook HMAC validation is available through `WEBHOOK_SIGNING_SECRET`.

## Important Files

| Path | Purpose |
| --- | --- |
| `src/api/endpoints.py` | Webhook entry point; writes durable workflow jobs. |
| `src/models/workflow.py` | SQLAlchemy models for jobs, audit, budgets, and dead letters. |
| `src/orchestrator/workflow.py` | Turns one webhook job into recording, LLM, or short-call result jobs. |
| `src/repositories/jobs.py` | DB-backed queue claiming, idempotency, retry, and dead-letter state. |
| `src/rate_limiter/` | Budget admission policy and per-minute token accounting. |
| `src/llm_workers/worker.py` | Priority-aware, rate-limited LLM execution. |
| `src/recording/worker.py` | Recording polling with backoff and visible failure handling. |
| `src/workers/result_worker.py` | Durable signal-job and lead-stage updates. |
| `src/logging/audit.py` | Structured audit event persistence with PII redaction. |
| `migrations/versions/20260507_0001_durable_workflow_tables.py` | Alembic migration for the new workflow tables. |

## Local Setup

```bash
python -m pip install -r requirements.txt
docker compose up -d
python -m alembic upgrade head
python -m pytest -q
```

For a fresh Docker database, `data/schema.sql` already includes the new tables.
For an existing database, run the Alembic migration.

## Running Workers

The Celery tasks are intentionally small worker ticks. In production these would
normally be scheduled repeatedly or run by separate queues.

```bash
celery -A src.tasks.celery_app.celery_app worker -Q workflow,llm_medium,llm_high,llm_low
```

Useful task names:

- `run_workflow_orchestrator_task`
- `run_llm_worker_task`
- `run_recording_worker_task`
- `run_result_worker_task`
- `run_all_workers_once_task`

## Notes

The mock LLM path is still available when `LLM_API_KEY` is left as
`sk-mock-key-for-assessment`, so the test suite does not need real provider
credentials. Customer-specific token budgets can be passed in webhook
`additional_data.customer_token_budget_per_minute`; otherwise the default from
`src/config.py` is used.
