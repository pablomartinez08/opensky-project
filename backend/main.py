"""
Main — FastAPI Application (Refactorizado SOLID).

Composition Root: aquí se crean todas las instancias y se inyectan
las dependencias. Ningún otro módulo hace 'new' ni importa globales.
"""
import asyncio
import logging
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from routes.health import router as health_router
from routes.alerts import router as alerts_router
from routes.websocket import router as ws_router

from services.alert_store import AlertStore
from services.ws_manager import WebSocketManager
from services.neo4j_repository import Neo4jRepository
from services.alert_processor import AlertProcessor
from kafka_consumer import KafkaAlertConsumer

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
# Lifespan — Composition Root (Dependency Injection)
# ══════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Ciclo de vida de la app.
    Aquí se crean TODAS las instancias y se inyectan como dependencias.
    """
    logger.info("═" * 60)
    logger.info("  🚀 Backend API — Sistema de Anomalías Aéreas (SOLID)")
    logger.info("  📥 Consumer:  Kafka topic 'flight-alerts'")
    logger.info("  🕸️  Grafo:     Neo4j (bolt://neo4j:7687)")
    logger.info("  🔌 WebSocket: ws://localhost:8000/ws/alerts")
    logger.info("═" * 60)

    # ── 1. Crear instancias (Principio D: inyección por constructor) ──
    store = AlertStore(maxlen=2000)
    ws_manager = WebSocketManager(flush_interval=0.5)
    repository = Neo4jRepository()
    processor = AlertProcessor(store=store, ws_manager=ws_manager, repository=repository)
    consumer = KafkaAlertConsumer(processor=processor)

    # ── 2. Inyectar en app.state para que los routers accedan ──
    app.state.alert_store = store
    app.state.ws_manager = ws_manager
    app.state.repository = repository
    app.state.processor = processor
    app.state.kafka_consumer = consumer
    app.state.start_time = datetime.utcnow()

    # ── 3. Arrancar tareas en background ──
    consumer_task = asyncio.create_task(consumer.consume())
    ws_flush_task = asyncio.create_task(ws_manager.flush_loop())

    yield

    # ── 4. Shutdown limpio ──
    consumer_task.cancel()
    ws_flush_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
    try:
        await ws_flush_task
    except asyncio.CancelledError:
        pass
    repository.close()
    logger.info("🛑 Backend detenido")


# ══════════════════════════════════════════════════════════════
# App FastAPI
# ══════════════════════════════════════════════════════════════
app = FastAPI(
    title="Flight Anomaly Detection API",
    description="Backend SOLID para el sistema de detección de anomalías aéreas en tiempo real",
    version="2.0.0",
    lifespan=lifespan,
)

# CORS
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
