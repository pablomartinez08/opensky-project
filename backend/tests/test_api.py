"""
Tests unitarios para la API del Backend.
Ejecutar con: pytest tests/test_api.py -v
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient
from models import FlightAlert


# ══════════════════════════════════════════════════════════════
# Tests del modelo Pydantic
# ══════════════════════════════════════════════════════════════
class TestFlightAlertModel:
    """Verifica que el modelo Pydantic acepta los ejemplos del schema contract."""

    def test_vertical_alert(self):
        data = {
            "tipo_alerta": "vertical",
            "estado": "inicio",
            "severidad": "alta",
            "timestamp": "2026-03-24T18:30:12Z",
            "latitud": 48.3456,
            "longitud": 11.789,
            "origen_pais": "Germany",
            "en_tierra": False,
            "icao24": "3c6752",
            "callsign": "DLH1234",
            "altitud_actual": 9100,
            "altitud_previa": 11200,
            "delta_altitud": -2100,
            "velocidad_vertical_ms": -105.0,
            "velocidad_kmh": 852.3,
            "heading": 210.5,
        }
        alert = FlightAlert(**data)
        assert alert.tipo_alerta == "vertical"
        assert alert.severidad == "alta"
        assert alert.delta_altitud == -2100

    def test_alta_densidad_alert(self):
        data = {
            "tipo_alerta": "alta_densidad",
            "estado": "actualizacion",
            "severidad": "media",
            "timestamp": "2026-03-24T18:30:00Z",
            "latitud": 40.4719,
            "longitud": -3.5626,
            "celda_h3": "86395b467ffffff",
            "num_vuelos_actual": 85,
            "vuelos_afectados": ["IBE301", "RYR872", "VLG44A"],
            "ventana_inicio": "2026-03-24T18:25:00Z",
            "ventana_fin": "2026-03-24T18:30:00Z",
        }
        alert = FlightAlert(**data)
        assert alert.tipo_alerta == "alta_densidad"
        assert alert.num_vuelos_actual == 85
        assert len(alert.vuelos_afectados) == 3

    def test_zona_muerta_alert(self):
        data = {
            "tipo_alerta": "zona_muerta",
            "estado": "inicio",
            "severidad": "alta",
            "timestamp": "2026-03-24T18:30:00Z",
            "latitud": 42.7812,
            "longitud": 0.5634,
            "celda_h3": "841f91fffffffff",
            "num_vuelos_actual": 0,
            "num_vuelos_previo": 12,
        }
        alert = FlightAlert(**data)
        assert alert.num_vuelos_actual == 0
        assert alert.num_vuelos_previo == 12

    def test_cep_alert(self):
        data = {
            "tipo_alerta": "cep_aterrizaje_emergencia",
            "estado": "fin",
            "severidad": "critica",
            "timestamp": "2026-03-24T18:31:44Z",
            "latitud": 47.4502,
            "longitud": 8.5618,
            "origen_pais": "Switzerland",
            "en_tierra": True,
            "icao24": "4b1806",
            "callsign": "SWR285",
            "velocidad_kmh": 0.0,
            "heading": 135.2,
            "patron": "descenso_brusco → velocidad_cero",
            "altitud_inicio_patron": 8200,
            "altitud_final": 430,
            "duracion_vel_cero_seg": 78,
        }
        alert = FlightAlert(**data)
        assert alert.severidad == "critica"
        assert alert.patron == "descenso_brusco → velocidad_cero"

    def test_minimal_alert(self):
        """Una alerta con solo los campos obligatorios."""
        data = {
            "tipo_alerta": "vertical",
            "estado": "inicio",
            "severidad": "baja",
            "timestamp": "2026-01-01T00:00:00Z",
        }
        alert = FlightAlert(**data)
        assert alert.icao24 is None
        assert alert.celda_h3 is None


# ══════════════════════════════════════════════════════════════
# Tests de la API (requiere import de main)
# ══════════════════════════════════════════════════════════════
class TestAPIEndpoints:
    """Tests de los endpoints REST."""

    @pytest.fixture
    def client(self):
        """Crea un cliente de test de FastAPI."""
        from main import app
        return TestClient(app)

    def test_health(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "kafka_connected" in data
        assert "neo4j_connected" in data

    def test_stats(self, client):
        response = client.get("/stats")
        assert response.status_code == 200
        data = response.json()
        assert "total_alertas" in data
        assert "por_tipo" in data
        assert "por_severidad" in data

    def test_alerts_list_empty(self, client):
        response = client.get("/api/v1/alerts")
        assert response.status_code == 200
        data = response.json()
        assert "total" in data
        assert "alertas" in data
        assert isinstance(data["alertas"], list)

    def test_alert_not_found(self, client):
        response = client.get("/api/v1/alerts/nonexistent-id")
        assert response.status_code == 404
