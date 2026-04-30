"""
AlertStore — Almacén de alertas en memoria (Principio S: Single Responsibility).

Responsabilidad ÚNICA: almacenar, indexar, filtrar y contar alertas.
No sabe nada de Kafka, WebSocket ni Neo4j.
"""
import uuid
from collections import deque
from typing import Optional, List, Dict
from datetime import datetime


class AlertStore:
    """
    Almacén en memoria para alertas de vuelo.

    Principios SOLID:
      S — Solo almacenamiento y filtrado, sin efectos externos.
      O — Extensible: se puede heredar para añadir persistencia sin modificar.
      I — Interfaz mínima: add, get_by_id, list_alerts, get_stats.
    """

    def __init__(self, maxlen: int = 2000):
        self._store: deque = deque(maxlen=maxlen)
        self._by_id: Dict[str, dict] = {}
        self._maxlen = maxlen
        self._stats = {
            "total_alertas": 0,
            "alertas_criticas": 0,
            "por_tipo": {
                "vertical": 0,
                "alta_densidad": 0,
                "zona_muerta": 0,
                "cep_aterrizaje_emergencia": 0,
            },
            "por_severidad": {
                "baja": 0,
                "media": 0,
                "alta": 0,
                "critica": 0,
            },
            "ultima_actualizacion": None,
        }

    def add(self, alerta: dict) -> dict:
        """
        Añade una alerta al almacén.
        - Asigna UUID si no lo tiene.
        - Redondea campos numéricos.
        - Actualiza contadores.
        Devuelve la alerta procesada.
        """
        if "id" not in alerta or alerta["id"] is None:
            alerta["id"] = str(uuid.uuid4())

        alerta = self._round_fields(alerta)

        self._store.appendleft(alerta)
        self._by_id[alerta["id"]] = alerta

        # Limpiar índice si crece más que el deque
        if len(self._by_id) > self._maxlen + 500:
            self._rebuild_index()

        self._update_stats(alerta)
        return alerta

    def get_by_id(self, alert_id: str) -> Optional[dict]:
        """Devuelve una alerta por su UUID, o None si no existe."""
        return self._by_id.get(alert_id)

    def list_alerts(
        self,
        tipo: Optional[str] = None,
        severidad: Optional[str] = None,
        estado: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple:
        """
        Filtra y pagina alertas. Devuelve (alertas_paginadas, total_filtrado).
        """
        filtered = list(self._store)

        if tipo:
            filtered = [a for a in filtered if a.get("tipo_alerta") == tipo]
        if severidad:
            filtered = [a for a in filtered if a.get("severidad") == severidad]
        if estado:
            filtered = [a for a in filtered if a.get("estado") == estado]

        total = len(filtered)
        page = filtered[offset: offset + limit]
        return page, total

    def get_stats(self) -> dict:
        """Devuelve una copia de los contadores actuales."""
        return dict(self._stats)

    @property
    def total(self) -> int:
        """Número total de alertas procesadas (nunca se resetea)."""
        return self._stats["total_alertas"]

    @property
    def size(self) -> int:
        """Número de alertas actualmente en el deque."""
        return len(self._store)

    # ── Métodos privados ──

    def _update_stats(self, alerta: dict):
        """Actualiza contadores internos."""
        self._stats["total_alertas"] += 1
        tipo = alerta.get("tipo_alerta", "")
        sev = alerta.get("severidad", "")
        if tipo in self._stats["por_tipo"]:
            self._stats["por_tipo"][tipo] += 1
        if sev in self._stats["por_severidad"]:
            self._stats["por_severidad"][sev] += 1
        if sev == "critica":
            self._stats["alertas_criticas"] += 1
        self._stats["ultima_actualizacion"] = alerta.get("timestamp")

    def _rebuild_index(self):
        """Reconstruye el diccionario de IDs a partir del deque."""
        self._by_id.clear()
        for a in self._store:
            self._by_id[a["id"]] = a

    @staticmethod
    def _round_fields(alerta: dict) -> dict:
        """Redondea campos numéricos para evitar decimales excesivos."""
        for key in ("delta_altitud", "altitud_actual", "altitud_previa",
                    "velocidad_vertical_ms", "velocidad_kmh", "heading",
                    "latitud", "longitud"):
            v = alerta.get(key)
            if isinstance(v, float):
                alerta[key] = round(v, 2)
        return alerta
