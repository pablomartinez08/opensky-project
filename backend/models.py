"""
Modelos Pydantic para el Sistema de Detección de Anomalías Aéreas.
Basados estrictamente en alert_schema_contract.json v1.1.0
"""
from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime


# ══════════════════════════════════════════════════════════════
# Modelo principal de Alerta (refleja el contrato JSON 1:1)
# ══════════════════════════════════════════════════════════════
class FlightAlert(BaseModel):
    """Alerta de vuelo completa según el schema contract v1.1.0"""
    # ID asignado por el backend al consumir de Kafka
    id: Optional[str] = None

    # ── Campos comunes (SIEMPRE presentes) ──
    tipo_alerta: str            # vertical | alta_densidad | zona_muerta | cep_aterrizaje_emergencia
    estado: str                 # inicio | actualizacion | fin
    severidad: str              # baja | media | alta | critica
    timestamp: str              # ISO 8601
    latitud: Optional[float] = None
    longitud: Optional[float] = None
    origen_pais: Optional[str] = None
    en_tierra: Optional[bool] = None

    # ── Campos opcionales (según tipo_alerta) ──
    icao24: Optional[str] = None
    callsign: Optional[str] = None
    altitud_actual: Optional[float] = None
    altitud_previa: Optional[float] = None
    delta_altitud: Optional[float] = None
    velocidad_vertical_ms: Optional[float] = None
    velocidad_kmh: Optional[float] = None
    heading: Optional[float] = None
    celda_h3: Optional[str] = None
    num_vuelos_actual: Optional[int] = None
    num_vuelos_previo: Optional[int] = None
    vuelos_afectados: Optional[List[str]] = None
    ventana_inicio: Optional[str] = None
    ventana_fin: Optional[str] = None
    patron: Optional[str] = None
    altitud_inicio_patron: Optional[float] = None
    altitud_final: Optional[float] = None
    duracion_vel_cero_seg: Optional[float] = None


# ══════════════════════════════════════════════════════════════
# Respuestas de la API
# ══════════════════════════════════════════════════════════════
class AlertListResponse(BaseModel):
    """Respuesta paginada de alertas"""
    total: int
    limit: int
    offset: int
    alertas: List[FlightAlert]


class StatsResponse(BaseModel):
    """Métricas globales para el dashboard"""
    total_alertas: int
    alertas_criticas: int
    por_tipo: dict
    por_severidad: dict
    ultima_actualizacion: Optional[str] = None


class HealthResponse(BaseModel):
    """Estado del servidor"""
    status: str
    kafka_connected: bool
    neo4j_connected: bool
    uptime_seconds: float
    version: str = "1.0.0"
