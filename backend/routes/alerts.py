"""
Router de Alertas: REST endpoints para listar, filtrar y consultar Neo4j.
"""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from models import FlightAlert, AlertListResponse
from kafka_consumer import alertas_store, alertas_by_id
from neo4j_client import get_alertas_por_vuelo, get_zonas_calientes, get_vuelos_reincidentes

router = APIRouter(prefix="/api/v1")


# ══════════════════════════════════════════════════════════════
# REST — Alertas en memoria
# ══════════════════════════════════════════════════════════════
@router.get("/alerts", response_model=AlertListResponse)
async def list_alerts(
    tipo: Optional[str] = Query(None, description="Filtrar por tipo_alerta"),
    severidad: Optional[str] = Query(None, description="Filtrar por severidad"),
    estado: Optional[str] = Query(None, description="Filtrar por estado"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Listar alertas activas con filtros y paginación."""
    filtered = list(alertas_store)

    if tipo:
        filtered = [a for a in filtered if a.get("tipo_alerta") == tipo]
    if severidad:
        filtered = [a for a in filtered if a.get("severidad") == severidad]
    if estado:
        filtered = [a for a in filtered if a.get("estado") == estado]

    total = len(filtered)
    page = filtered[offset: offset + limit]

    return AlertListResponse(
        total=total,
        limit=limit,
        offset=offset,
        alertas=[FlightAlert(**a) for a in page],
    )


@router.get("/alerts/{alert_id}", response_model=FlightAlert)
async def get_alert(alert_id: str):
    """Detalle de una alerta por su UUID."""
    alerta = alertas_by_id.get(alert_id)
    if not alerta:
        raise HTTPException(status_code=404, detail="Alerta no encontrada")
    return FlightAlert(**alerta)


# ══════════════════════════════════════════════════════════════
# REST — Consultas Neo4j (Grafos)
# ══════════════════════════════════════════════════════════════
@router.get("/neo4j/vuelo/{icao24}")
async def neo4j_vuelo(icao24: str):
    """Historial de alertas de un avión concreto desde Neo4j."""
    alertas = get_alertas_por_vuelo(icao24)
    return {"icao24": icao24, "total": len(alertas), "alertas": alertas}


@router.get("/neo4j/zonas-calientes")
async def neo4j_zonas(limit: int = Query(10, ge=1, le=50)):
    """Top zonas H3 con más alertas."""
    zonas = get_zonas_calientes(limit)
    return {"total": len(zonas), "zonas": zonas}


@router.get("/neo4j/vuelos-reincidentes")
async def neo4j_reincidentes(min_alertas: int = Query(3, ge=1)):
    """Vuelos con más de N alertas en el grafo."""
    vuelos = get_vuelos_reincidentes(min_alertas)
    return {"total": len(vuelos), "vuelos": vuelos}
