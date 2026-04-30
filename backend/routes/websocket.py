"""
Router WebSocket — Refactorizado SOLID (Principio D).

Depende de WebSocketManager vía request.app.state.
"""
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    """
    WebSocket endpoint que emite alertas en tiempo real.
    Delega la gestión de conexiones al WebSocketManager.
    """
    ws_manager = websocket.app.state.ws_manager

    await ws_manager.connect(websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.warning("⚠️ Error WebSocket: %s", e)
    finally:
        ws_manager.disconnect(websocket)
