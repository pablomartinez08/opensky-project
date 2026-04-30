"""
Tests E2E — Flujo completo de alertas (End-to-End).
Simula el pipeline: alerta → AlertProcessor → API → WebSocket.
Ejecutar con: pytest tests/test_e2e.py -v
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import asyncio
from datetime import datetime
from fastapi.testclient import TestClient
from fastapi import FastAPI

from routes.health import router as health_router
from routes.alerts import router as alerts_router
from routes.websocket import router as ws_router
from services.alert_store import AlertStore
from services.ws_manager import WebSocketManager
from services.neo4j_repository import NullRepository
from services.alert_processor import AlertProcessor
from kafka_consumer import KafkaAlertConsumer


# ══════════════════════════════════════════════════════════════
# Datos de prueba — Ejemplos reales del schema contract v1.1.0
# ══════════════════════════════════════════════════════════════
ALERT_VERTICAL = {
    "tipo_alerta": "vertical", "estado": "inicio", "severidad": "alta",
    "timestamp": "2026-03-24T18:30:12Z", "latitud": 48.35, "longitud": 11.79,
    "origen_pais": "Germany", "en_tierra": False, "icao24": "3c6752",
    "callsign": "DLH1234", "altitud_actual": 9100, "altitud_previa": 11200,
    "delta_altitud": -2100, "velocidad_vertical_ms": -105.0,
    "velocidad_kmh": 852.3, "heading": 210.5,
}

ALERT_DENSIDAD = {
    "tipo_alerta": "alta_densidad", "estado": "actualizacion", "severidad": "media",
    "timestamp": "2026-03-24T18:30:00Z", "latitud": 40.47, "longitud": -3.56,
    "celda_h3": "86395b467ffffff", "num_vuelos_actual": 85,
    "threshold_dinamico": 12.5,
    "vuelos_afectados": ["IBE301", "RYR872", "VLG44A"],
    "ventana_inicio": "2026-03-24T18:25:00Z", "ventana_fin": "2026-03-24T18:30:00Z",
}

ALERT_ZONA_MUERTA = {
    "tipo_alerta": "zona_muerta", "estado": "inicio", "severidad": "alta",
    "timestamp": "2026-03-24T18:30:00Z", "latitud": 42.78, "longitud": 0.56,
    "celda_h3": "841f91fffffffff", "num_vuelos_actual": 0, "num_vuelos_previo": 12,
    "ventana_inicio": "2026-03-24T18:25:00Z", "ventana_fin": "2026-03-24T18:30:00Z",
}

ALERT_CEP = {
    "tipo_alerta": "cep_aterrizaje_emergencia", "estado": "fin", "severidad": "critica",
    "timestamp": "2026-03-24T18:31:44Z", "latitud": 47.45, "longitud": 8.56,
    "origen_pais": "Switzerland", "en_tierra": True, "icao24": "4b1806",
    "callsign": "SWR285", "velocidad_kmh": 0.0, "heading": 135.2,
    "patron": "descenso_brusco → velocidad_cero",
    "altitud_inicio_patron": 8200, "altitud_final": 430, "duracion_vel_cero_seg": 78,
}


# ══════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════
@pytest.fixture
def e2e_setup():
    """Crea un entorno E2E completo con todas las dependencias SOLID."""
    app = FastAPI()
    app.include_router(health_router)
    app.include_router(alerts_router)
    app.include_router(ws_router)

    store = AlertStore(maxlen=500)
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


# ══════════════════════════════════════════════════════════════
# Tests E2E
# ══════════════════════════════════════════════════════════════
class TestE2EFlow:
    """Simula el flujo completo: Alerta → Processor → API → Verificación."""

    def test_full_pipeline_vertical(self, e2e_setup):
        """E2E: inyectar alerta vertical → verificar en GET /alerts y /stats."""
        client, store, processor = e2e_setup

        # Simular procesamiento (como si viniera de Kafka)
        asyncio.get_event_loop().run_until_complete(
            processor.process(dict(ALERT_VERTICAL))
        )

        # Verificar que aparece en la API
        res = client.get("/api/v1/alerts")
        assert res.status_code == 200
        data = res.json()
        assert data["total"] == 1
        assert data["alertas"][0]["tipo_alerta"] == "vertical"
        assert data["alertas"][0]["callsign"] == "DLH1234"

        # Verificar contadores
        stats = client.get("/stats").json()
        assert stats["total_alertas"] == 1
        assert stats["por_tipo"]["vertical"] == 1
        assert stats["por_severidad"]["alta"] == 1

    def test_full_pipeline_all_types(self, e2e_setup):
        """E2E: inyectar los 4 tipos de alerta → verificar contadores."""
        client, store, processor = e2e_setup
        loop = asyncio.get_event_loop()

        for alert in [ALERT_VERTICAL, ALERT_DENSIDAD, ALERT_ZONA_MUERTA, ALERT_CEP]:
            loop.run_until_complete(processor.process(dict(alert)))

        stats = client.get("/stats").json()
        assert stats["total_alertas"] == 4
        assert stats["por_tipo"]["vertical"] == 1
        assert stats["por_tipo"]["alta_densidad"] == 1
        assert stats["por_tipo"]["zona_muerta"] == 1
        assert stats["por_tipo"]["cep_aterrizaje_emergencia"] == 1

    def test_filter_by_type(self, e2e_setup):
        """E2E: filtrar alertas por tipo_alerta."""
        client, store, processor = e2e_setup
        loop = asyncio.get_event_loop()

        for alert in [ALERT_VERTICAL, ALERT_DENSIDAD, ALERT_ZONA_MUERTA, ALERT_CEP]:
            loop.run_until_complete(processor.process(dict(alert)))

        # Filtrar solo vertical
        res = client.get("/api/v1/alerts?tipo=vertical")
        data = res.json()
        assert data["total"] == 1
        assert data["alertas"][0]["tipo_alerta"] == "vertical"

        # Filtrar CEP
        res = client.get("/api/v1/alerts?tipo=cep_aterrizaje_emergencia")
        data = res.json()
        assert data["total"] == 1
        assert data["alertas"][0]["severidad"] == "critica"

    def test_filter_by_severity(self, e2e_setup):
        """E2E: filtrar alertas por severidad."""
        client, store, processor = e2e_setup
        loop = asyncio.get_event_loop()

        for alert in [ALERT_VERTICAL, ALERT_DENSIDAD, ALERT_ZONA_MUERTA, ALERT_CEP]:
            loop.run_until_complete(processor.process(dict(alert)))

        res = client.get("/api/v1/alerts?severidad=critica")
        data = res.json()
        assert data["total"] == 1
        assert data["alertas"][0]["tipo_alerta"] == "cep_aterrizaje_emergencia"

    def test_pagination(self, e2e_setup):
        """E2E: verificar paginación con limit y offset."""
        client, store, processor = e2e_setup
        loop = asyncio.get_event_loop()

        # Inyectar 20 alertas
        for i in range(20):
            loop.run_until_complete(processor.process({
                "tipo_alerta": "vertical", "estado": "inicio",
                "severidad": "baja", "timestamp": f"2026-01-01T00:{i:02d}:00Z",
                "latitud": 40 + i * 0.1, "longitud": -3.0,
            }))

        # Página 1: primeras 5
        res = client.get("/api/v1/alerts?limit=5&offset=0")
        data = res.json()
        assert data["total"] == 20
        assert len(data["alertas"]) == 5

        # Página 2
        res = client.get("/api/v1/alerts?limit=5&offset=5")
        data = res.json()
        assert len(data["alertas"]) == 5

    def test_get_alert_by_id(self, e2e_setup):
        """E2E: obtener alerta por UUID."""
        client, store, processor = e2e_setup
        loop = asyncio.get_event_loop()

        result = loop.run_until_complete(processor.process(dict(ALERT_VERTICAL)))
        alert_id = result["id"]

        res = client.get(f"/api/v1/alerts/{alert_id}")
        assert res.status_code == 200
        data = res.json()
        assert data["id"] == alert_id
        assert data["callsign"] == "DLH1234"

    def test_websocket_connection(self, e2e_setup):
        """E2E: verificar que el WebSocket acepta conexiones."""
        client, _, _ = e2e_setup
        with client.websocket_connect("/ws/alerts") as ws:
            # La conexión se establece sin error
            assert ws is not None
