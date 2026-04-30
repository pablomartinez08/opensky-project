"""
Tests de Carga — Rendimiento, memoria y concurrencia.
Ejecutar con: pytest tests/test_load.py -v
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import asyncio
import time
import concurrent.futures
from datetime import datetime
from fastapi.testclient import TestClient
from fastapi import FastAPI

from routes.health import router as health_router
from routes.alerts import router as alerts_router
from services.alert_store import AlertStore
from services.ws_manager import WebSocketManager
from services.neo4j_repository import NullRepository
from services.alert_processor import AlertProcessor
from kafka_consumer import KafkaAlertConsumer


@pytest.fixture
def load_setup():
    """Crea un entorno de carga con dependencias SOLID."""
    app = FastAPI()
    app.include_router(health_router)
    app.include_router(alerts_router)

    store = AlertStore(maxlen=2000)
    ws_manager = WebSocketManager()
    repository = NullRepository()
    processor = AlertProcessor(store=store, ws_manager=ws_manager, repository=repository)
    consumer = KafkaAlertConsumer(processor=processor)

    app.state.alert_store = store
    app.state.ws_manager = ws_manager
    app.state.repository = repository
    app.state.processor = processor
    app.state.kafka_consumer = consumer
    app.state.start_time = datetime.utcnow()

    client = TestClient(app)
    return client, store, processor


def _make_alert(i):
    """Genera una alerta de test con datos únicos."""
    return {
        "tipo_alerta": "vertical",
        "estado": "inicio",
        "severidad": ["baja", "media", "alta", "critica"][i % 4],
        "timestamp": f"2026-01-01T{(i // 60) % 24:02d}:{i % 60:02d}:00Z",
        "latitud": 40.0 + (i * 0.001),
        "longitud": -3.0 + (i * 0.001),
        "icao24": f"test{i:05d}",
        "callsign": f"TST{i:04d}",
        "altitud_actual": 10000 - i * 10,
        "altitud_previa": 10000,
        "delta_altitud": -(i * 10),
    }


# ══════════════════════════════════════════════════════════════
# Tests de Carga
# ══════════════════════════════════════════════════════════════
class TestLoadPerformance:
    """Pruebas de rendimiento y tolerancia a fallos."""

    def test_throughput_1000_alerts(self, load_setup):
        """CARGA: inyectar 1.000 alertas en ráfaga y verificar < 5 segundos."""
        _, store, processor = load_setup
        loop = asyncio.get_event_loop()

        start = time.time()
        for i in range(1000):
            loop.run_until_complete(processor.process(_make_alert(i)))
        elapsed = time.time() - start

        assert store.total == 1000, f"Se esperaban 1000, se obtuvieron {store.total}"
        assert elapsed < 5.0, f"Demasiado lento: {elapsed:.2f}s (máx 5s)"
        print(f"\n  ⚡ 1.000 alertas procesadas en {elapsed:.2f}s "
              f"({1000/elapsed:.0f} alertas/seg)")

    def test_memory_deque_limit(self, load_setup):
        """MEMORIA: el deque no supera maxlen (2000) aunque se inyecten 3000."""
        _, store, processor = load_setup
        loop = asyncio.get_event_loop()

        for i in range(3000):
            loop.run_until_complete(processor.process(_make_alert(i)))

        assert store.total == 3000, "El contador total debe reflejar todas"
        assert store.size <= 2000, f"Deque overflow: {store.size} > 2000"
        print(f"\n  💾 3.000 alertas procesadas, deque={store.size}, "
              f"total={store.total}")

    def test_stats_after_overflow(self, load_setup):
        """STATS: los contadores sobreviven al overflow del deque."""
        _, store, processor = load_setup
        loop = asyncio.get_event_loop()

        for i in range(2500):
            loop.run_until_complete(processor.process(_make_alert(i)))

        stats = store.get_stats()
        assert stats["total_alertas"] == 2500
        # Con i%4, cada severidad debería tener ~625
        assert stats["por_severidad"]["baja"] > 600
        assert stats["por_severidad"]["critica"] > 600

    def test_concurrent_reads(self, load_setup):
        """CONCURRENCIA: 10 peticiones GET /alerts simultáneas → todas 200."""
        client, store, processor = load_setup
        loop = asyncio.get_event_loop()

        # Pre-cargar 100 alertas
        for i in range(100):
            loop.run_until_complete(processor.process(_make_alert(i)))

        # 10 lecturas concurrentes
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(lambda: client.get("/api/v1/alerts")) for _ in range(10)]
            for f in concurrent.futures.as_completed(futures):
                results.append(f.result())

        assert all(r.status_code == 200 for r in results), \
            "No todas las respuestas fueron 200"
        print(f"\n  🔄 10 peticiones concurrentes: todas 200 OK")

    def test_filter_performance(self, load_setup):
        """RENDIMIENTO: filtrar por severidad con 1000 alertas < 1 segundo."""
        client, store, processor = load_setup
        loop = asyncio.get_event_loop()

        for i in range(1000):
            loop.run_until_complete(processor.process(_make_alert(i)))

        start = time.time()
        res = client.get("/api/v1/alerts?severidad=critica&limit=50")
        elapsed = time.time() - start

        assert res.status_code == 200
        data = res.json()
        assert data["total"] > 0
        assert elapsed < 1.0, f"Filtrado lento: {elapsed:.2f}s"
        print(f"\n  🔍 Filtrado de 1.000 alertas en {elapsed*1000:.1f}ms")

    def test_alert_id_lookup_performance(self, load_setup):
        """RENDIMIENTO: búsqueda por ID con 1000 alertas es O(1)."""
        client, store, processor = load_setup
        loop = asyncio.get_event_loop()

        last_alert = None
        for i in range(1000):
            last_alert = loop.run_until_complete(processor.process(_make_alert(i)))

        start = time.time()
        res = client.get(f"/api/v1/alerts/{last_alert['id']}")
        elapsed = time.time() - start

        assert res.status_code == 200
        assert elapsed < 0.1, f"Lookup lento: {elapsed*1000:.1f}ms"
        print(f"\n  🎯 Lookup por ID en {elapsed*1000:.1f}ms (O(1))")
