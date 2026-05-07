from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.metrics import registry
from src.models.workflow import InteractionJob


class WorkflowMetricsCollector:
    async def refresh_queue_depths(self, session: AsyncSession) -> None:
        result = await session.execute(
            select(
                InteractionJob.job_type,
                InteractionJob.status,
                func.count(),
            ).group_by(InteractionJob.job_type, InteractionJob.status)
        )
        for job_type, status, count in result.all():
            registry.queue_depth.labels(
                job_type=job_type.value if hasattr(job_type, "value") else str(job_type),
                status=status.value if hasattr(status, "value") else str(status),
            ).set(count)


workflow_metrics_collector = WorkflowMetricsCollector()
