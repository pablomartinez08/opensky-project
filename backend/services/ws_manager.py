"""
WebSocketManager — Gestión de conexiones WebSocket (Principio S: Single Responsibility).

Responsabilidad ÚNICA: mantener la lista de conexiones WS activas,
bufferizar mensajes y enviarlos en batch.
No sabe nada de alertas, Kafka ni almacenamiento.
"""
import json
import asyncio
import logging
from typing import List

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class WebSocketManager:
    """
    Gestor de conexiones WebSocket con batching.

    Principios SOLID:
      S — Solo gestión de conexiones y broadcasting.
      O — Extensible: se puede heredar para añadir autenticación.
      I — Interfaz mínima: connect, disconnect, buffer, flush.
    """

    def __init__(self, flush_interval: float = 0.5):
        self._connections: List[WebSocket] = []
        self._buffer: List[dict] = []
        self._flush_interval = flush_interval

    async def connect(self, websocket: WebSocket):
        """Acepta y registra una nueva conexión WebSocket."""
        await websocket.accept()
        self._connections.append(websocket)
        logger.info("🔌 Cliente WS conectado. Total: %d", len(self._connections))

    def disconnect(self, websocket: WebSocket):
        """Elimina una conexión cerrada."""
        if websocket in self._connections:
            self._connections.remove(websocket)
        logger.info("🔌 Cliente WS desconectado. Total: %d", len(self._connections))

    def buffer(self, alerta: dict):
        """Añade una alerta al buffer de envío."""
        self._buffer.append(alerta)

    async def flush_loop(self):
        """Bucle infinito que vacía el buffer cada flush_interval segundos."""
        while True:
            await asyncio.sleep(self._flush_interval)
            if not self._buffer or not self._connections:
                continue

            batch = list(self._buffer)
            self._buffer.clear()

            msg_json = json.dumps(batch)
            dead: List[WebSocket] = []

            for ws in self._connections:
                try:
                    await ws.send_text(msg_json)
                except Exception:
                    dead.append(ws)

            for ws in dead:
                self._connections.remove(ws)

            if batch:
                logger.info(
                    "📤 Batch WS: %d alertas → %d clientes",
                    len(batch), len(self._connections),
                )

    @property
    def active_connections(self) -> int:
        """Número de conexiones WebSocket activas."""
        return len(self._connections)
