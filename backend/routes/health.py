"""
Router de Health & Stats del Backend.
"""
from datetime import datetime
from fastapi import APIRouter
from models import HealthResponse, StatsResponse
from kafka_consumer import kafka_connected, stats, start_time
from neo4j_client import is_connected as neo4j_is_connected

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health():
    """Comprueba si el backend está operativo y conectado a Kafka y Neo4j."""
    uptime = (datetime.utcnow() - start_time).total_seconds()
    return HealthResponse(
        status="ok",
        kafka_connected=kafka_connected,
        neo4j_connected=neo4j_is_connected(),
        uptime_seconds=round(uptime, 1),
    )


@router.get("/stats", response_model=StatsResponse)
async def get_stats():
    """Métricas globales para alimentar el dashboard."""
    return StatsResponse(**stats)
