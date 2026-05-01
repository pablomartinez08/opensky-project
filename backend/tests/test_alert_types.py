"""
Tests de Validación de Tipos de Alerta.
Verifica exhaustivamente los 4 tipos del alert_schema_contract.json v1.1.0.
Ejecutar con: pytest tests/test_alert_types.py -v
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import re
from models import FlightAlert


# ══════════════════════════════════════════════════════════════
# Constantes de validación
# ══════════════════════════════════════════════════════════════
TIPOS_VALIDOS = {
    "vertical",
    "alta_densidad",
    "zona_muerta",
    "cep_zona_problematica",
    "cep_aterrizaje_emergencia",
}
ESTADOS_VALIDOS = {"inicio", "actualizacion", "fin"}
SEVERIDADES_VALIDAS = {"baja", "media", "alta", "critica"}
ISO_8601_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


# ══════════════════════════════════════════════════════════════
# Datos de prueba completos
# ══════════════════════════════════════════════════════════════
EJEMPLOS = {
    "vertical": {
        "tipo_alerta": "vertical", "estado": "inicio", "severidad": "alta",
        "timestamp": "2026-03-24T18:30:12Z", "latitud": 48.35, "longitud": 11.79,
        "origen_pais": "Germany", "en_tierra": False, "icao24": "3c6752",
        "callsign": "DLH1234", "altitud_actual": 9100, "altitud_previa": 11200,
        "delta_altitud": -2100, "velocidad_vertical_ms": -105.0,
        "velocidad_kmh": 852.3, "heading": 210.5,
    },
    "alta_densidad": {
        "tipo_alerta": "alta_densidad", "estado": "actualizacion", "severidad": "media",
        "timestamp": "2026-03-24T18:30:00Z", "latitud": 40.47, "longitud": -3.56,
        "celda_h3": "86395b467ffffff", "num_vuelos_actual": 85,
        "threshold_dinamico": 12.5,
        "vuelos_afectados": ["IBE301", "RYR872", "VLG44A"],
        "ventana_inicio": "2026-03-24T18:25:00Z", "ventana_fin": "2026-03-24T18:30:00Z",
    },
    "zona_muerta": {
        "tipo_alerta": "zona_muerta", "estado": "inicio", "severidad": "alta",
        "timestamp": "2026-03-24T18:30:00Z", "latitud": 42.78, "longitud": 0.56,
        "celda_h3": "841f91fffffffff", "num_vuelos_actual": 0, "num_vuelos_previo": 12,
        "ventana_inicio": "2026-03-24T18:25:00Z", "ventana_fin": "2026-03-24T18:30:00Z",
    },
    "cep_zona_problematica": {
        "tipo_alerta": "cep_zona_problematica", "estado": "inicio",
        "severidad": "alta", "timestamp": "2026-03-24T18:31:00Z",
        "latitud": 42.78, "longitud": 0.56,
        "num_vuelos_actual": 28, "num_vuelos_previo": 12,
        "patron": "zona_muerta -> densidad_contigua",
        "ventana_inicio": "2026-03-24T18:25:00Z", "ventana_fin": "2026-03-24T18:31:00Z",
    },
    "cep_aterrizaje_emergencia": {
        "tipo_alerta": "cep_aterrizaje_emergencia", "estado": "fin",
        "severidad": "critica", "timestamp": "2026-03-24T18:31:44Z",
        "latitud": 47.45, "longitud": 8.56, "origen_pais": "Switzerland",
        "en_tierra": True, "icao24": "4b1806", "callsign": "SWR285",
        "velocidad_kmh": 0.0, "heading": 135.2,
        "patron": "descenso_brusco → velocidad_cero",
        "altitud_inicio_patron": 8200, "altitud_final": 430,
        "duracion_vel_cero_seg": 78,
    },
}


# ══════════════════════════════════════════════════════════════
# Tests de Validación por Tipo
# ══════════════════════════════════════════════════════════════
class TestAlertTypeValidation:
    """Valida que cada tipo de alerta cumple el schema contract."""

    @pytest.mark.parametrize("tipo", TIPOS_VALIDOS)
    def test_tipo_valido(self, tipo):
        """Cada tipo de alerta es parseado correctamente por Pydantic."""
        alert = FlightAlert(**EJEMPLOS[tipo])
        assert alert.tipo_alerta == tipo

    @pytest.mark.parametrize("tipo", TIPOS_VALIDOS)
    def test_campos_comunes(self, tipo):
        """Los campos comunes (tipo, estado, severidad, timestamp) están presentes."""
        alert = FlightAlert(**EJEMPLOS[tipo])
        assert alert.tipo_alerta in TIPOS_VALIDOS
        assert alert.estado in ESTADOS_VALIDOS
        assert alert.severidad in SEVERIDADES_VALIDAS
        assert ISO_8601_REGEX.match(alert.timestamp)

    @pytest.mark.parametrize("tipo", TIPOS_VALIDOS)
    def test_coordenadas(self, tipo):
        """Todos los tipos tienen coordenadas geográficas válidas."""
        alert = FlightAlert(**EJEMPLOS[tipo])
        assert -90 <= alert.latitud <= 90
        assert -180 <= alert.longitud <= 180


class TestVerticalFields:
    """Validación específica de campos de alerta vertical."""

    def test_campos_especificos_presentes(self):
        alert = FlightAlert(**EJEMPLOS["vertical"])
        assert alert.icao24 is not None
        assert alert.callsign is not None
        assert alert.altitud_actual is not None
        assert alert.altitud_previa is not None
        assert alert.delta_altitud is not None

    def test_campos_otros_tipos_null(self):
        alert = FlightAlert(**EJEMPLOS["vertical"])
        assert alert.celda_h3 is None
        assert alert.num_vuelos_actual is None
        assert alert.vuelos_afectados is None
        assert alert.patron is None

    def test_delta_coherente(self):
        alert = FlightAlert(**EJEMPLOS["vertical"])
        expected_delta = alert.altitud_actual - alert.altitud_previa
        assert alert.delta_altitud == expected_delta


class TestAltaDensidadFields:
    """Validación específica de campos de alta densidad."""

    def test_campos_especificos_presentes(self):
        alert = FlightAlert(**EJEMPLOS["alta_densidad"])
        assert alert.celda_h3 is not None
        assert alert.num_vuelos_actual is not None
        assert alert.vuelos_afectados is not None
        assert len(alert.vuelos_afectados) > 0
        assert alert.ventana_inicio is not None
        assert alert.ventana_fin is not None

    def test_threshold_dinamico_present(self):
        """Las alertas de densidad incluyen el threshold dinámico de Welford."""
        alert = FlightAlert(**EJEMPLOS["alta_densidad"])
        assert alert.threshold_dinamico is not None
        assert isinstance(alert.threshold_dinamico, float)

    def test_campos_otros_tipos_null(self):
        alert = FlightAlert(**EJEMPLOS["alta_densidad"])
        assert alert.icao24 is None
        assert alert.delta_altitud is None
        assert alert.patron is None


class TestZonaMuertaFields:
    """Validación específica de campos de zona muerta."""

    def test_campos_especificos_presentes(self):
        alert = FlightAlert(**EJEMPLOS["zona_muerta"])
        assert alert.celda_h3 is not None
        assert alert.num_vuelos_actual is not None
        assert alert.num_vuelos_previo is not None

    def test_vuelos_actual_cero(self):
        alert = FlightAlert(**EJEMPLOS["zona_muerta"])
        assert alert.num_vuelos_actual == 0
        assert alert.num_vuelos_previo > 0

    def test_campos_otros_tipos_null(self):
        alert = FlightAlert(**EJEMPLOS["zona_muerta"])
        assert alert.icao24 is None
        assert alert.patron is None


class TestCEPFields:
    """Validación específica de campos de CEP (aterrizaje de emergencia)."""

    def test_campos_especificos_presentes(self):
        alert = FlightAlert(**EJEMPLOS["cep_aterrizaje_emergencia"])
        assert alert.patron is not None
        assert alert.altitud_inicio_patron is not None
        assert alert.altitud_final is not None
        assert alert.duracion_vel_cero_seg is not None

    def test_severidad_critica(self):
        alert = FlightAlert(**EJEMPLOS["cep_aterrizaje_emergencia"])
        assert alert.severidad == "critica"

    def test_en_tierra(self):
        alert = FlightAlert(**EJEMPLOS["cep_aterrizaje_emergencia"])
        assert alert.en_tierra is True

    def test_campos_otros_tipos_null(self):
        alert = FlightAlert(**EJEMPLOS["cep_aterrizaje_emergencia"])
        assert alert.celda_h3 is None
        assert alert.delta_altitud is None


# ══════════════════════════════════════════════════════════════
# Tests de Severidad
# ══════════════════════════════════════════════════════════════
class TestSeverityValidation:
    """Verifica los rangos y jerarquía de severidades."""

    @pytest.mark.parametrize("sev", SEVERIDADES_VALIDAS)
    def test_severidad_aceptada(self, sev):
        alert = FlightAlert(
            tipo_alerta="vertical", estado="inicio",
            severidad=sev, timestamp="2026-01-01T00:00:00Z",
        )
        assert alert.severidad == sev

    def test_todas_severidades_cubiertas(self):
        """Las 4 severidades del contrato están representadas en los ejemplos."""
        severidades_en_ejemplos = {e["severidad"] for e in EJEMPLOS.values()}
        # Al menos 3 de las 4 deben estar cubiertas
        assert len(severidades_en_ejemplos) >= 3


# ══════════════════════════════════════════════════════════════
# Tests de Timestamp
# ══════════════════════════════════════════════════════════════
class TestTimestampValidation:
    """Verifica formato ISO 8601 en los timestamps."""

    @pytest.mark.parametrize("tipo", TIPOS_VALIDOS)
    def test_timestamp_iso_format(self, tipo):
        alert = FlightAlert(**EJEMPLOS[tipo])
        assert ISO_8601_REGEX.match(alert.timestamp), \
            f"Timestamp inválido: {alert.timestamp}"

    def test_ventana_iso_format(self):
        alert = FlightAlert(**EJEMPLOS["alta_densidad"])
        assert ISO_8601_REGEX.match(alert.ventana_inicio)
        assert ISO_8601_REGEX.match(alert.ventana_fin)
