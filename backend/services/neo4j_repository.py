"""
Neo4jRepository — Persistencia de alertas en grafo (Principios O + L + D).

Responsabilidad ÚNICA: operaciones CRUD sobre Neo4j.
Implementa la interfaz abstracta AlertRepository para cumplir
Dependency Inversion: el código de negocio depende de la abstracción,
no de la implementación concreta.
"""
import os
import logging
from abc import ABC, abstractmethod
from typing import List, Optional

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# Interfaz abstracta (Principio D + I)
# ══════════════════════════════════════════════════════════════
class AlertRepository(ABC):
    """
    Contrato abstracto para repositorios de alertas.

    Principios SOLID:
      D — Dependency Inversion: el consumer y los routers dependen
          de esta abstracción, no de Neo4j directamente.
      I — Interface Segregation: solo métodos necesarios.
      L — Liskov Substitution: cualquier implementación sustituye
          a otra sin romper el sistema.
    """

    @abstractmethod
    def persist(self, alerta: dict) -> None:
        """Persiste una alerta en el almacén."""
        ...

    @abstractmethod
    def get_alerts_by_flight(self, icao24: str) -> List[dict]:
        """Devuelve el historial de alertas de un vuelo."""
        ...

    @abstractmethod
    def get_hot_zones(self, limit: int = 10) -> List[dict]:
        """Devuelve las zonas con más alertas."""
        ...

    @abstractmethod
    def get_repeat_flights(self, min_alerts: int = 3) -> List[dict]:
        """Devuelve vuelos reincidentes."""
        ...

    @abstractmethod
    def is_connected(self) -> bool:
        """Verifica la conectividad con el almacén."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Cierra la conexión."""
        ...


# ══════════════════════════════════════════════════════════════
# Implementación concreta con Neo4j (Principio O: Open/Closed)
# ══════════════════════════════════════════════════════════════
class Neo4jRepository(AlertRepository):
    """
    Implementación de AlertRepository usando Neo4j.

    Principios SOLID:
      S — Solo operaciones Neo4j, nada más.
      O — Cerrada para modificación: si quieres MongoDB, crea
          MongoRepository(AlertRepository) sin tocar este código.
      L — Sustituible por cualquier otra AlertRepository.
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self._uri = uri or os.getenv("NEO4J_URI", "bolt://neo4j:7687")
        self._user = user or os.getenv("NEO4J_USER", "neo4j")
        self._password = password or os.getenv("NEO4J_PASSWORD", "opensky_secret")
        self._driver = None

    def _get_driver(self):
        """Lazy initialization del driver Neo4j."""
        if self._driver is None:
            try:
                from neo4j import GraphDatabase
                self._driver = GraphDatabase.driver(
                    self._uri, auth=(self._user, self._password)
                )
                self._driver.verify_connectivity()
                logger.info("✅ Conectado a Neo4j en %s", self._uri)
            except Exception as e:
                logger.warning("⚠️ No se pudo conectar a Neo4j: %s", e)
                self._driver = None
        return self._driver

    def is_connected(self) -> bool:
        d = self._get_driver()
        if d is None:
            return False
        try:
            d.verify_connectivity()
            return True
        except Exception:
            return False

    def close(self) -> None:
        if self._driver:
            self._driver.close()
            self._driver = None
            logger.info("🔌 Conexión Neo4j cerrada")

    def persist(self, alerta: dict) -> None:
        d = self._get_driver()
        if d is None:
            return

        try:
            with d.session() as session:
                if alerta.get("icao24"):
                    session.run("""
                        MERGE (v:Vuelo {icao24: $icao24})
                        SET v.callsign = $callsign,
                            v.origin_country = $pais
                        CREATE (a:Alerta {
                            id: $alert_id,
                            tipo: $tipo,
                            estado: $estado,
                            severidad: $severidad,
                            timestamp: $ts,
                            altitud: $alt,
                            latitud: $lat,
                            longitud: $lon
                        })
                        CREATE (v)-[:TIENE_ALERTA]->(a)
                    """,
                        icao24=alerta.get("icao24"),
                        callsign=alerta.get("callsign", "UNKNOWN"),
                        pais=alerta.get("origen_pais", ""),
                        alert_id=alerta.get("id", ""),
                        tipo=alerta.get("tipo_alerta"),
                        estado=alerta.get("estado"),
                        severidad=alerta.get("severidad"),
                        ts=alerta.get("timestamp", ""),
                        alt=alerta.get("altitud_actual"),
                        lat=alerta.get("latitud"),
                        lon=alerta.get("longitud"),
                    )

                h3 = alerta.get("celda_h3")
                if h3:
                    session.run("""
                        MERGE (z:ZonaH3 {celda: $h3})
                        SET z.lat = $lat, z.lon = $lon
                        WITH z
                        MATCH (a:Alerta {id: $alert_id})
                        CREATE (a)-[:OCURRE_EN]->(z)
                    """,
                        h3=h3,
                        lat=alerta.get("latitud"),
                        lon=alerta.get("longitud"),
                        alert_id=alerta.get("id", ""),
                    )

                    if alerta.get("icao24"):
                        session.run("""
                            MATCH (v:Vuelo {icao24: $icao24})
                            MATCH (z:ZonaH3 {celda: $h3})
                            MERGE (v)-[:VUELA_SOBRE]->(z)
                        """,
                            icao24=alerta.get("icao24"),
                            h3=h3,
                        )
        except Exception as e:
            logger.error("❌ Error al persistir en Neo4j: %s", e)

    def get_alerts_by_flight(self, icao24: str) -> List[dict]:
        d = self._get_driver()
        if d is None:
            return []
        with d.session() as session:
            result = session.run("""
                MATCH (v:Vuelo {icao24: $icao24})-[:TIENE_ALERTA]->(a:Alerta)
                RETURN a ORDER BY a.timestamp DESC LIMIT 50
            """, icao24=icao24)
            return [dict(record["a"]) for record in result]

    def get_hot_zones(self, limit: int = 10) -> List[dict]:
        d = self._get_driver()
        if d is None:
            return []
        with d.session() as session:
            result = session.run("""
                MATCH (a:Alerta)-[:OCURRE_EN]->(z:ZonaH3)
                RETURN z.celda AS celda, z.lat AS lat, z.lon AS lon,
                       COUNT(a) AS total_alertas
                ORDER BY total_alertas DESC LIMIT $limit
            """, limit=limit)
            return [dict(record) for record in result]

    def get_repeat_flights(self, min_alerts: int = 3) -> List[dict]:
        d = self._get_driver()
        if d is None:
            return []
        with d.session() as session:
            result = session.run("""
                MATCH (v:Vuelo)-[:TIENE_ALERTA]->(a:Alerta)
                WITH v, COUNT(a) AS alertas
                WHERE alertas >= $min
                RETURN v.icao24 AS icao24, v.callsign AS callsign,
                       v.origin_country AS pais, alertas
                ORDER BY alertas DESC LIMIT 20
            """, min=min_alerts)
            return [dict(record) for record in result]


# ══════════════════════════════════════════════════════════════
# NullRepository — Para tests sin Neo4j (Principio L: Liskov)
# ══════════════════════════════════════════════════════════════
class NullRepository(AlertRepository):
    """
    Implementación nula para tests y entornos sin Neo4j.
    Demuestra Liskov Substitution: sustituye a Neo4jRepository
    sin que el sistema se rompa.
    """

    def persist(self, alerta: dict) -> None:
        pass

    def get_alerts_by_flight(self, icao24: str) -> List[dict]:
        return []

    def get_hot_zones(self, limit: int = 10) -> List[dict]:
        return []

    def get_repeat_flights(self, min_alerts: int = 3) -> List[dict]:
        return []

    def is_connected(self) -> bool:
        return False

    def close(self) -> None:
        pass
