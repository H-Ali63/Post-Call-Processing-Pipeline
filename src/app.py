from fastapi import FastAPI, Response

from src.api.endpoints import router
from src.metrics.registry import prometheus_payload

app = FastAPI(
    title="VoiceBot Post-Call Processing",
    version="1.0.0",
)

app.include_router(router, prefix="/api/v1")


@app.get("/metrics")
async def metrics() -> Response:
    return Response(content=prometheus_payload(), media_type="text/plain")
