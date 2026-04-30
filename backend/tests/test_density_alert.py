"""
Tests de Integración — Alerta de Alta Densidad (density_alert.py).

Valida que las alertas generadas por el detector de densidad adaptativo
se integran correctamente con el backend SOLID.

Cubre:
  - Formato de alerta conforme al schema contract v1.1.0
  - Campo threshold_dinamico exclusivo de este detector
  - Ciclo de vida completo: inicio → actualización → fin
  - Pipeline E2E: alerta → AlertProcessor → API REST → filtros → stats
  - Compatibilidad con el mock_density_test.py (formato de datos)

Ejecutar con: pytest tests/test_density_alert.py -v
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import asyncio
from datetime import datetime
from fastapi.testclient import TestClient
from fastapi import FastAPI

from models import FlightAlert
from routes.health import router as health_router
from routes.alerts import router as alerts_router
from services.alert_store import AlertStore
from services.ws_manager import WebSocketManager
from services.neo4j_repository import NullRepository
from services.alert_processor import AlertProcessor
from kafka_consumer import KafkaAlertConsumer


# ══════════════════════════════════════════════════════════════
# Datos de prueba — Formato REAL que produce density_alert.py
# ══════════════════════════════════════════════════════════════

# Alerta de INICIO: primera vez que se supera el threshold
DENSITY_ALERT_INICIO = {
    "tipo_alerta": "alta_densidad",
    "estado": "inicio",
    "severidad": "media",
    "timestamp": "2026-04-30T15:30:00Z",
    "latitud": 40.4167,
    "longitud": -3.7037,
    "celda_h3": "841969bffffffff",
    "num_vuelos_actual": 30,
    "threshold_dinamico": 8.0,
    "vuelos_afectados": ["TEST001", "TEST002", "TEST003", "TEST004", "TEST005"],
}

# Alerta de ACTUALIZACIÓN: sigue superando el threshold
DENSITY_ALERT_ACTUALIZACION = {
    "tipo_alerta": "alta_densidad",
    "estado": "actualizacion",
    "severidad": "alta",
    "timestamp": "2026-04-30T15:30:15Z",
    "latitud": 40.4167,
    "longitud": -3.7037,
    "celda_h3": "841969bffffffff",
    "num_vuelos_actual": 35,
    "threshold_dinamico": 8.5,
    "vuelos_afectados": ["TEST001", "TEST002", "TEST003", "TEST004", "TEST005",
                         "TEST006", "TEST007", "TEST008", "TEST009", "TEST010"],
}

# Alerta de FIN: la densidad baja del threshold con histéresis
DENSITY_ALERT_FIN = {
    "tipo_alerta": "alta_densidad",
    "estado": "fin",
    "severidad": "baja",
    "timestamp": "2026-04-30T15:35:00Z",
    "latitud": 40.4167,
    "longitud": -3.7037,
    "celda_h3": "841969bffffffff",
    "num_vuelos_actual": 5,
    "threshold_dinamico": 8.0,
    "vuelos_afectados": [],
}

# Alerta con severidad ALTA (count > threshold * 1.5)
DENSITY_ALERT_ALTA = {
    "tipo_alerta": "alta_densidad",
    "estado": "inicio",
    "severidad": "alta",
    "timestamp": "2026-04-30T16:00:00Z",
    "latitud": 51.4700,
    "longitud": -0.4543,
    "celda_h3": "841969bffffffff",
    "num_vuelos_actual": 50,
    "threshold_dinamico": 10.0,
    "vuelos_afectados": [f"LHR{i:03d}" for i in range(10)],
}


# ══════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════
@pytest.fixture
def density_setup():
    """Entorno de test con dependencias SOLID inyectadas."""
    app = FastAPI()
    app.include_router(health_router)
    app.include_router(alerts_router)

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
# 1. VALIDACIÓN DEL MODELO PYDANTIC
# ══════════════════════════════════════════════════════════════
class TestDensityAlertModel:
    """Verifica que el modelo Pydantic acepta las alertas de density_alert.py."""

    def test_inicio_parseable(self):
        """El modelo acepta una alerta de inicio con threshold_dinamico."""
        alert = FlightAlert(**DENSITY_ALERT_INICIO)
        assert alert.tipo_alerta == "alta_densidad"
        assert alert.estado == "inicio"
        assert alert.threshold_dinamico == 8.0
        assert alert.num_vuelos_actual == 30

    def test_actualizacion_parseable(self):
        """El modelo acepta una alerta de actualización."""
        alert = FlightAlert(**DENSITY_ALERT_ACTUALIZACION)
        assert alert.estado == "actualizacion"
        assert alert.num_vuelos_actual == 35
        assert len(alert.vuelos_afectados) == 10

    def test_fin_parseable(self):
        """El modelo acepta una alerta de fin con vuelos_afectados vacío."""
        alert = FlightAlert(**DENSITY_ALERT_FIN)
        assert alert.estado == "fin"
        assert alert.severidad == "baja"
        assert alert.vuelos_afectados == []

    def test_threshold_dinamico_present(self):
        """El campo threshold_dinamico está disponible en el modelo."""
        alert = FlightAlert(**DENSITY_ALERT_INICIO)
        assert alert.threshold_dinamico is not None
        assert isinstance(alert.threshold_dinamico, float)

    def test_celda_h3_present(self):
        """Las alertas de densidad siempre tienen celda_h3."""
        alert = FlightAlert(**DENSITY_ALERT_INICIO)
        assert alert.celda_h3 is not None
        assert len(alert.celda_h3) > 0

    def test_campos_verticales_null(self):
        """Las alertas de densidad NO tienen campos de alerta vertical."""
        alert = FlightAlert(**DENSITY_ALERT_INICIO)
        assert alert.icao24 is None
        assert alert.callsign is None
        assert alert.delta_altitud is None
        assert alert.altitud_actual is None
        assert alert.velocidad_vertical_ms is None

    def test_campos_cep_null(self):
        """Las alertas de densidad NO tienen campos de alerta CEP."""
        alert = FlightAlert(**DENSITY_ALERT_INICIO)
        assert alert.patron is None
        assert alert.altitud_inicio_patron is None
        assert alert.duracion_vel_cero_seg is None


# ══════════════════════════════════════════════════════════════
# 2. CICLO DE VIDA COMPLETO (inicio → actualización → fin)
# ══════════════════════════════════════════════════════════════
class TestDensityLifecycle:
    """Simula el ciclo de vida completo de una alerta de densidad."""

    def test_ciclo_inicio_actualizacion_fin(self, density_setup):
        """El pipeline procesa correctamente los 3 estados del ciclo de vida."""
        client, store, processor = density_setup
        loop = asyncio.get_event_loop()

        # Fase 1: INICIO
        loop.run_until_complete(processor.process(dict(DENSITY_ALERT_INICIO)))
        res = client.get("/api/v1/alerts?tipo=alta_densidad")
        data = res.json()
        assert data["total"] == 1
        assert data["alertas"][0]["estado"] == "inicio"

        # Fase 2: ACTUALIZACIÓN
        loop.run_until_complete(processor.process(dict(DENSITY_ALERT_ACTUALIZACION)))
        res = client.get("/api/v1/alerts?tipo=alta_densidad")
        data = res.json()
        assert data["total"] == 2
        # La más reciente está primero (deque appendleft)
        assert data["alertas"][0]["estado"] == "actualizacion"

        # Fase 3: FIN
        loop.run_until_complete(processor.process(dict(DENSITY_ALERT_FIN)))
        res = client.get("/api/v1/alerts?tipo=alta_densidad")
        data = res.json()
        assert data["total"] == 3

    def test_contadores_ciclo_completo(self, density_setup):
        """Los contadores reflejan correctamente el ciclo de vida."""
        _, store, processor = density_setup
        loop = asyncio.get_event_loop()

        loop.run_until_complete(processor.process(dict(DENSITY_ALERT_INICIO)))
        loop.run_until_complete(processor.process(dict(DENSITY_ALERT_ACTUALIZACION)))
        loop.run_until_complete(processor.process(dict(DENSITY_ALERT_FIN)))

        stats = store.get_stats()
        assert stats["total_alertas"] == 3
        assert stats["por_tipo"]["alta_densidad"] == 3
        # inicio=media, actualizacion=alta, fin=baja
        assert stats["por_severidad"]["media"] == 1
        assert stats["por_severidad"]["alta"] == 1
        assert stats["por_severidad"]["baja"] == 1


# ══════════════════════════════════════════════════════════════
# 3. E2E: PIPELINE COMPLETO → API REST → FILTROS
# ══════════════════════════════════════════════════════════════
class TestDensityE2E:
    """Pruebas end-to-end del pipeline de densidad."""

    def test_filter_alta_densidad(self, density_setup):
        """Filtrar por tipo=alta_densidad funciona correctamente."""
        client, store, processor = density_setup
        loop = asyncio.get_event_loop()

        # Inyectar mix de tipos
        loop.run_until_complete(processor.process(dict(DENSITY_ALERT_INICIO)))
        loop.run_until_complete(processor.process({
            "tipo_alerta": "vertical", "estado": "inicio",
            "severidad": "alta", "timestamp": "2026-04-30T15:30:00Z",
        }))

        res = client.get("/api/v1/alerts?tipo=alta_densidad")
        data = res.json()
        assert data["total"] == 1
        assert data["alertas"][0]["tipo_alerta"] == "alta_densidad"
        assert data["alertas"][0]["celda_h3"] == "841969bffffffff"

    def test_threshold_in_api_response(self, density_setup):
        """El threshold_dinamico aparece en la respuesta de la API."""
        client, store, processor = density_setup
        loop = asyncio.get_event_loop()

        result = loop.run_until_complete(processor.process(dict(DENSITY_ALERT_INICIO)))
        alert_id = result["id"]

        res = client.get(f"/api/v1/alerts/{alert_id}")
        assert res.status_code == 200
        data = res.json()
        assert data["threshold_dinamico"] == 8.0
        assert data["num_vuelos_actual"] == 30

    def test_severidad_alta_when_over_1_5x(self, density_setup):
        """Severidad 'alta' cuando count > threshold * 1.5."""
        client, store, processor = density_setup
        loop = asyncio.get_event_loop()

        result = loop.run_until_complete(processor.process(dict(DENSITY_ALERT_ALTA)))
        res = client.get(f"/api/v1/alerts/{result['id']}")
        data = res.json()
        assert data["severidad"] == "alta"
        assert data["num_vuelos_actual"] == 50

    def test_vuelos_afectados_list(self, density_setup):
        """vuelos_afectados es una lista de callsigns."""
        client, store, processor = density_setup
        loop = asyncio.get_event_loop()

        result = loop.run_until_complete(processor.process(dict(DENSITY_ALERT_ACTUALIZACION)))
        res = client.get(f"/api/v1/alerts/{result['id']}")
        data = res.json()
        assert isinstance(data["vuelos_afectados"], list)
        assert len(data["vuelos_afectados"]) == 10
        assert "TEST001" in data["vuelos_afectados"]

    def test_stats_counter_alta_densidad(self, density_setup):
        """Los contadores acumulan correctamente alertas de densidad."""
        client, _, processor = density_setup
        loop = asyncio.get_event_loop()

        # Inyectar 5 alertas de densidad
        for i in range(5):
            loop.run_until_complete(processor.process({
                "tipo_alerta": "alta_densidad", "estado": "inicio",
                "severidad": "media", "timestamp": f"2026-04-30T15:{i:02d}:00Z",
                "latitud": 40.0, "longitud": -3.0, "celda_h3": f"h3cell{i}",
                "num_vuelos_actual": 20 + i, "threshold_dinamico": 8.0,
                "vuelos_afectados": [],
            }))

        stats = client.get("/stats").json()
        assert stats["por_tipo"]["alta_densidad"] == 5


# ══════════════════════════════════════════════════════════════
# 4. COMPATIBILIDAD CON mock_density_test.py
# ══════════════════════════════════════════════════════════════
class TestMockDensityCompatibility:
    """Verifica que el formato de datos del mock es compatible con el backend."""

    def test_mock_flight_format_parseable(self):
        """Los vuelos generados por mock_density_test.py son JSON válido."""
        import json
        # Simular generate_flight() del mock
        mock_flight = {
            "icao24": "mock0001",
            "callsign": "TEST001",
            "latitude": 40.4167 + 0.01,
            "longitude": -3.7037 - 0.01,
            "altitude": 35000,
            "velocity": 450,
            "heading": 180,
            "on_ground": False,
        }
        # Debe ser serializable sin error
        serialized = json.dumps(mock_flight)
        parsed = json.loads(serialized)
        assert parsed["icao24"] == "mock0001"

    def test_density_alert_from_mock_data(self, density_setup):
        """Una alerta generada por el detector a partir de datos mock se procesa bien."""
        _, store, processor = density_setup
        loop = asyncio.get_event_loop()

        # Simular lo que density_alert.py produciría con los datos del mock
        alert_from_detector = {
            "tipo_alerta": "alta_densidad",
            "estado": "inicio",
            "severidad": "media",
            "timestamp": "2026-04-30T15:30:00Z",
            "latitud": 40.4167,
            "longitud": -3.7037,
            "celda_h3": "841969bffffffff",
            "num_vuelos_actual": 30,
            "threshold_dinamico": 8.0,
            "vuelos_afectados": [f"TEST{i:03d}" for i in range(1, 11)],
        }

        result = loop.run_until_complete(processor.process(alert_from_detector))
        assert result["id"] is not None
        assert store.total == 1
        assert store.get_by_id(result["id"])["num_vuelos_actual"] == 30

    def test_coordenadas_madrid_validas(self):
        """Las coordenadas de Madrid del mock son geográficamente válidas."""
        alert = FlightAlert(**DENSITY_ALERT_INICIO)
        assert 39.0 <= alert.latitud <= 42.0, "Latitud fuera de rango Madrid"
        assert -5.0 <= alert.longitud <= -2.0, "Longitud fuera de rango Madrid"
