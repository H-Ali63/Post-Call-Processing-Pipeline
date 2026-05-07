from celery import Celery

from src.config import settings

celery_app = Celery(
    "voicebot",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_acks_on_failure_or_timeout=False,
    worker_prefetch_multiplier=1,
    task_default_delivery_mode=2,
    task_default_queue=settings.POSTCALL_CELERY_QUEUE,
    result_persistent=True,
    result_expires=86400,
    broker_transport_options={
        "visibility_timeout": 3600,
        "queue_order_strategy": "priority",
    },
    task_routes={
        "run_workflow_orchestrator_task": {"queue": settings.WORKFLOW_CELERY_QUEUE},
        "run_llm_worker_task": {"queue": settings.LLM_MEDIUM_PRIORITY_QUEUE},
        "run_recording_worker_task": {"queue": settings.WORKFLOW_CELERY_QUEUE},
        "run_result_worker_task": {"queue": settings.WORKFLOW_CELERY_QUEUE},
        "run_all_workers_once_task": {"queue": settings.WORKFLOW_CELERY_QUEUE},
    },
)
