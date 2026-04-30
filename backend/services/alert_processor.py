"""
AlertProcessor — Orquestador de procesamiento de alertas (Principios S + D).

Responsabilidad ÚNICA: coordinar el flujo de procesamiento de una alerta.
Depende SOLO de abstracciones (AlertStore, WSManager, AlertRepository),
no de implementaciones concretas → Dependency Inversion.
"""
import logging

from services.alert_store import AlertStore
from services.ws_manager import WebSocketManager
from services.neo4j_repository import AlertRepository

logger = logging.getLogger(__name__)


class AlertProcessor:
    """
    Orquestador que procesa alertas entrantes delegando en servicios especializados.

    Principios SOLID:
      S — Solo orquestación: no almacena, no envía, no persiste.
      D — Depende de abstracciones (AlertStore, WSManager, AlertRepository),
          nunca de clases concretas. Se inyectan por constructor.
      O — Cerrada para modificación: añadir un nuevo sink (ej: Elasticsearch)
          requiere solo una nueva dependencia, no cambiar este código.
    """

    def __init__(
        self,
        store: AlertStore,
        ws_manager: WebSocketManager,
        repository: AlertRepository,
    ):
        self._store = store
        self._ws = ws_manager
        self._repo = repository

    async def process(self, alerta: dict) -> dict:
        """
        Pipeline de procesamiento de una alerta:
        1. Almacenar en memoria (con UUID y redondeo)
        2. Persistir en el repositorio (Neo4j u otro)
        3. Bufferizar para broadcast WebSocket
        """
        # 1. Almacenar (el store asigna UUID y redondea)
        alerta = self._store.add(alerta)

        # 2. Persistir en repositorio
        try:
            self._repo.persist(alerta)
        except Exception as e:
            logger.error("⚠️ Error en repositorio: %s", e)

        # 3. Bufferizar para WebSocket
        self._ws.buffer(alerta)

        logger.info(
            "🚨 Alerta: %s | %s | %s | %s",
            alerta.get("tipo_alerta"),
            alerta.get("severidad"),
            alerta.get("callsign") or alerta.get("celda_h3") or "N/A",
            alerta.get("id", "")[:8],
        )

        return alerta
