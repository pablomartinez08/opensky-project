"""
Router de Health & Stats — Refactorizado SOLID (Principio D).

Depende de abstracciones vía request.app.state, no de imports globales.
"""
from datetime import datetime
from fastapi import APIRouter, Request
from models import HealthResponse, StatsResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health(request: Request):
    """Comprueba si el backend está operativo y conectado a Kafka y Neo4j."""
    consumer = request.app.state.kafka_consumer
    repo = request.app.state.repository
    start = request.app.state.start_time

    uptime = (datetime.utcnow() - start).total_seconds()
    return HealthResponse(
        status="ok",
        kafka_connected=consumer.connected,
        neo4j_connected=repo.is_connected(),
        uptime_seconds=round(uptime, 1),
    )


@router.get("/stats", response_model=StatsResponse)
async def get_stats(request: Request):
    """Métricas globales para alimentar el dashboard."""
    store = request.app.state.alert_store
    return StatsResponse(**store.get_stats())
