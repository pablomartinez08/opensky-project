"""
Main — FastAPI Application
Sistema de Detección de Anomalías Aéreas - Backend API
"""
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from routes.health import router as health_router
from routes.alerts import router as alerts_router
from routes.websocket import router as ws_router
from kafka_consumer import consume_alerts
from neo4j_client import close_driver

# ══════════════════════════════════════════════════════════════
# Logging
# ══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# Lifespan: arranca el consumer Kafka al iniciar la app
# ══════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Ciclo de vida de la app: arranca y para el consumer de Kafka."""
    logger.info("═" * 60)
    logger.info("  🚀 Backend API — Sistema de Anomalías Aéreas")
    logger.info("  📥 Consumer:  Kafka topic 'flight-alerts'")
    logger.info("  🕸️  Grafo:     Neo4j (bolt://neo4j:7687)")
    logger.info("  🔌 WebSocket: ws://localhost:8000/ws/alerts")
    logger.info("═" * 60)

    # Arrancar consumer en background
    task = asyncio.create_task(consume_alerts())
    yield
    # Parar consumer y cerrar Neo4j al shutdown
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    close_driver()
    logger.info("🛑 Backend detenido")


# ══════════════════════════════════════════════════════════════
# App FastAPI
# ══════════════════════════════════════════════════════════════
app = FastAPI(
    title="Flight Anomaly Detection API",
    description="Backend para el sistema de detección de anomalías aéreas en tiempo real",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS: permitir conexiones del frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(health_router)
app.include_router(alerts_router)
app.include_router(ws_router)

# Servir frontend estático
app.mount("/", StaticFiles(directory="static", html=True), name="static")
