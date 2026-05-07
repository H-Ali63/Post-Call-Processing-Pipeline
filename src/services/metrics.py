import logging
import time

from src.metrics import registry as prometheus
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)


class PostCallMetricsTracker:
    async def track_processing_started(self, interaction_id: str) -> None:
        await redis_client.set(
            f"postcall:metrics:{interaction_id}:start",
            str(time.time()),
            ex=86400,
        )

    async def track_processing_completed(
        self,
        interaction_id: str,
        tokens_used: int,
        latency_ms: float,
        *,
        customer_id: str = "unknown",
        campaign_id: str = "unknown",
        model: str = "unknown",
    ) -> None:
        start = await redis_client.get(f"postcall:metrics:{interaction_id}:start")
        wall_time_s = time.time() - float(start) if start else 0

        prometheus.llm_tokens_total.labels(
            customer_id=customer_id, campaign_id=campaign_id, model=model
        ).inc(tokens_used)
        prometheus.llm_latency_ms.observe(latency_ms)

        logger.info(
            "postcall_metrics",
            extra={
                "interaction_id": interaction_id,
                "customer_id": customer_id,
                "campaign_id": campaign_id,
                "tokens_used": tokens_used,
                "llm_latency_ms": latency_ms,
                "total_wall_time_s": round(wall_time_s, 2),
            },
        )

    async def track_processing_failed(
        self, interaction_id: str, error: str, *, stage: str = "postcall"
    ) -> None:
        logger.error(
            "postcall_failed_permanently",
            extra={
                "interaction_id": interaction_id,
                "stage": stage,
                "error": error,
            },
        )


metrics_tracker = PostCallMetricsTracker()
