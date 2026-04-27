"""
Router WebSocket para streaming de alertas en tiempo real.
"""
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from kafka_consumer import ws_connections

logger = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    """
    WebSocket endpoint que emite cada alerta nueva en tiempo real.
    El consumer de Kafka escribe en ws_connections y este endpoint
    simplemente mantiene la conexión abierta.
    """
    await websocket.accept()
    ws_connections.append(websocket)
    logger.info("🔌 Cliente WebSocket conectado. Total: %d", len(ws_connections))

    try:
        # Mantener la conexión abierta esperando mensajes del cliente (keepalive)
        while True:
            # Esperar cualquier mensaje del cliente (ping/pong o cierre)
            await websocket.receive_text()
    except WebSocketDisconnect:
        logger.info("🔌 Cliente WebSocket desconectado")
    except Exception as e:
        logger.warning("⚠️ Error WebSocket: %s", e)
    finally:
        if websocket in ws_connections:
            ws_connections.remove(websocket)
        logger.info("🔌 Conexiones WebSocket activas: %d", len(ws_connections))
