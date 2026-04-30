"""
Tests unitarios para la API del Backend — Refactorizado SOLID.
Usa NullRepository para no depender de Neo4j en tests.
Ejecutar con: pytest tests/test_api.py -v
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient
from datetime import datetime

from models import FlightAlert
from services.alert_store import AlertStore
from services.ws_manager import WebSocketManager
from services.neo4j_repository import NullRepository
from services.alert_processor import AlertProcessor
from kafka_consumer import KafkaAlertConsumer


# ══════════════════════════════════════════════════════════════
# Fixture: App de test con dependencias inyectadas (SOLID)
# ══════════════════════════════════════════════════════════════
@pytest.fixture
def app_with_deps():
    """Crea una app FastAPI con todas las dependencias SOLID inyectadas."""
    from fastapi import FastAPI
    from routes.health import router as health_router
    from routes.alerts import router as alerts_router

    app = FastAPI()
    app.include_router(health_router)
    app.include_router(alerts_router)

    store = AlertStore(maxlen=100)
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

    return app, store, processor


@pytest.fixture
def client(app_with_deps):
    app, _, _ = app_with_deps
    return TestClient(app)


@pytest.fixture
def store(app_with_deps):
    _, store, _ = app_with_deps
    return store


# ══════════════════════════════════════════════════════════════
# Tests del modelo Pydantic
# ══════════════════════════════════════════════════════════════
class TestFlightAlertModel:
    """Verifica que el modelo Pydantic acepta los ejemplos del schema contract."""

    def test_vertical_alert(self):
        data = {
            "tipo_alerta": "vertical", "estado": "inicio", "severidad": "alta",
            "timestamp": "2026-03-24T18:30:12Z", "latitud": 48.35, "longitud": 11.79,
            "origen_pais": "Germany", "en_tierra": False, "icao24": "3c6752",
            "callsign": "DLH1234", "altitud_actual": 9100, "altitud_previa": 11200,
            "delta_altitud": -2100, "velocidad_vertical_ms": -105.0,
            "velocidad_kmh": 852.3, "heading": 210.5,
        }
        alert = FlightAlert(**data)
        assert alert.tipo_alerta == "vertical"
        assert alert.severidad == "alta"
        assert alert.delta_altitud == -2100

    def test_alta_densidad_alert(self):
        data = {
            "tipo_alerta": "alta_densidad", "estado": "actualizacion", "severidad": "media",
            "timestamp": "2026-03-24T18:30:00Z", "latitud": 40.47, "longitud": -3.56,
            "celda_h3": "86395b467ffffff", "num_vuelos_actual": 85,
            "vuelos_afectados": ["IBE301", "RYR872", "VLG44A"],
            "ventana_inicio": "2026-03-24T18:25:00Z", "ventana_fin": "2026-03-24T18:30:00Z",
        }
        alert = FlightAlert(**data)
        assert alert.num_vuelos_actual == 85
        assert len(alert.vuelos_afectados) == 3

    def test_zona_muerta_alert(self):
        data = {
            "tipo_alerta": "zona_muerta", "estado": "inicio", "severidad": "alta",
            "timestamp": "2026-03-24T18:30:00Z", "latitud": 42.78, "longitud": 0.56,
            "celda_h3": "841f91fffffffff", "num_vuelos_actual": 0, "num_vuelos_previo": 12,
        }
        alert = FlightAlert(**data)
        assert alert.num_vuelos_actual == 0
        assert alert.num_vuelos_previo == 12

    def test_cep_alert(self):
        data = {
            "tipo_alerta": "cep_aterrizaje_emergencia", "estado": "fin",
            "severidad": "critica", "timestamp": "2026-03-24T18:31:44Z",
            "latitud": 47.45, "longitud": 8.56, "origen_pais": "Switzerland",
            "en_tierra": True, "icao24": "4b1806", "callsign": "SWR285",
            "velocidad_kmh": 0.0, "heading": 135.2,
            "patron": "descenso_brusco → velocidad_cero",
            "altitud_inicio_patron": 8200, "altitud_final": 430,
            "duracion_vel_cero_seg": 78,
        }
        alert = FlightAlert(**data)
        assert alert.severidad == "critica"

    def test_minimal_alert(self):
        data = {
            "tipo_alerta": "vertical", "estado": "inicio",
            "severidad": "baja", "timestamp": "2026-01-01T00:00:00Z",
        }
        alert = FlightAlert(**data)
        assert alert.icao24 is None
        assert alert.celda_h3 is None


# ══════════════════════════════════════════════════════════════
# Tests de la API REST
# ══════════════════════════════════════════════════════════════
class TestAPIEndpoints:
    """Tests de los endpoints REST con dependencias inyectadas."""

    def test_health(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "kafka_connected" in data
        assert "neo4j_connected" in data

    def test_stats_initial(self, client):
        response = client.get("/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["total_alertas"] == 0
        assert data["por_tipo"]["vertical"] == 0

    def test_alerts_empty(self, client):
        response = client.get("/api/v1/alerts")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0
        assert data["alertas"] == []

    def test_alert_not_found(self, client):
        response = client.get("/api/v1/alerts/nonexistent-id")
        assert response.status_code == 404

    def test_alerts_after_adding(self, client, store):
        store.add({
            "tipo_alerta": "vertical", "estado": "inicio",
            "severidad": "alta", "timestamp": "2026-01-01T00:00:00Z",
            "latitud": 40.0, "longitud": -3.0,
        })
        response = client.get("/api/v1/alerts")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["alertas"][0]["tipo_alerta"] == "vertical"


# ══════════════════════════════════════════════════════════════
# Tests SOLID — Verificar inyección de dependencias
# ══════════════════════════════════════════════════════════════
class TestSOLIDPrinciples:
    """Verifica que la arquitectura SOLID funciona correctamente."""

    def test_alert_store_independence(self):
        """S — AlertStore funciona sin ninguna otra dependencia."""
        store = AlertStore(maxlen=10)
        a = store.add({"tipo_alerta": "vertical", "estado": "inicio",
                       "severidad": "baja", "timestamp": "2026-01-01T00:00:00Z"})
        assert a["id"] is not None
        assert store.total == 1

    def test_null_repository_substitution(self):
        """L — NullRepository sustituye a Neo4jRepository sin error."""
        repo = NullRepository()
        repo.persist({"id": "test", "tipo_alerta": "vertical"})
        assert repo.get_alerts_by_flight("abc") == []
        assert repo.is_connected() is False

    def test_processor_with_null_repo(self):
        """D — AlertProcessor funciona con cualquier AlertRepository."""
        store = AlertStore()
        ws = WebSocketManager()
        repo = NullRepository()
        processor = AlertProcessor(store=store, ws_manager=ws, repository=repo)
        # No debería lanzar excepción
        import asyncio
        alerta = asyncio.get_event_loop().run_until_complete(
            processor.process({"tipo_alerta": "vertical", "estado": "inicio",
                              "severidad": "alta", "timestamp": "2026-01-01T00:00:00Z"})
        )
        assert alerta["id"] is not None
        assert store.total == 1
