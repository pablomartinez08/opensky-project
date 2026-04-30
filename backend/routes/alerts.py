"""
Router de Alertas — Refactorizado SOLID (Principio D).

Depende de abstracciones vía request.app.state, no de imports globales.
Los endpoints REST consultan AlertStore y AlertRepository inyectados.
"""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, Request
from models import FlightAlert, AlertListResponse

router = APIRouter(prefix="/api/v1")


# ══════════════════════════════════════════════════════════════
# REST — Alertas en memoria (vía AlertStore)
# ══════════════════════════════════════════════════════════════
@router.get("/alerts", response_model=AlertListResponse)
async def list_alerts(
    request: Request,
    tipo: Optional[str] = Query(None, description="Filtrar por tipo_alerta"),
    severidad: Optional[str] = Query(None, description="Filtrar por severidad"),
    estado: Optional[str] = Query(None, description="Filtrar por estado"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Listar alertas activas con filtros y paginación."""
    store = request.app.state.alert_store
    page, total = store.list_alerts(tipo=tipo, severidad=severidad, estado=estado,
                                     limit=limit, offset=offset)

    return AlertListResponse(
        total=total,
        limit=limit,
        offset=offset,
        alertas=[FlightAlert(**a) for a in page],
    )


@router.get("/alerts/{alert_id}", response_model=FlightAlert)
async def get_alert(request: Request, alert_id: str):
    """Detalle de una alerta por su UUID."""
    store = request.app.state.alert_store
    alerta = store.get_by_id(alert_id)
    if not alerta:
        raise HTTPException(status_code=404, detail="Alerta no encontrada")
    return FlightAlert(**alerta)


# ══════════════════════════════════════════════════════════════
# REST — Consultas Neo4j (vía AlertRepository)
# ══════════════════════════════════════════════════════════════
@router.get("/neo4j/vuelo/{icao24}")
async def neo4j_vuelo(request: Request, icao24: str):
    """Historial de alertas de un avión concreto desde el repositorio."""
    repo = request.app.state.repository
    alertas = repo.get_alerts_by_flight(icao24)
    return {"icao24": icao24, "total": len(alertas), "alertas": alertas}


@router.get("/neo4j/zonas-calientes")
async def neo4j_zonas(request: Request, limit: int = Query(10, ge=1, le=50)):
    """Top zonas H3 con más alertas."""
    repo = request.app.state.repository
    zonas = repo.get_hot_zones(limit)
    return {"total": len(zonas), "zonas": zonas}


@router.get("/neo4j/vuelos-reincidentes")
async def neo4j_reincidentes(request: Request, min_alertas: int = Query(3, ge=1)):
    """Vuelos con más de N alertas en el grafo."""
    repo = request.app.state.repository
    vuelos = repo.get_repeat_flights(min_alertas)
    return {"total": len(vuelos), "vuelos": vuelos}
