"""
Cliente Neo4j para persistencia de grafos de alertas.
Gestiona la conexión y las operaciones CRUD sobre nodos Vuelo, Alerta y ZonaH3.
"""
import os
import logging
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
# Configuración
# ══════════════════════════════════════════════════════════════
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "opensky_secret")

driver = None


def get_driver():
    """Devuelve o crea el singleton del driver Neo4j."""
    global driver
    if driver is None:
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            driver.verify_connectivity()
            logger.info("✅ Conectado a Neo4j en %s", NEO4J_URI)
        except Exception as e:
            logger.warning("⚠️ No se pudo conectar a Neo4j: %s", e)
            driver = None
    return driver


def close_driver():
    """Cierra la conexión con Neo4j."""
    global driver
    if driver:
        driver.close()
        driver = None
        logger.info("🔌 Conexión Neo4j cerrada")


def is_connected() -> bool:
    """Comprueba si Neo4j está accesible."""
    d = get_driver()
    if d is None:
        return False
    try:
        d.verify_connectivity()
        return True
    except Exception:
        return False


# ══════════════════════════════════════════════════════════════
# Escritura: Persistir una alerta como grafo
# ══════════════════════════════════════════════════════════════
def persistir_alerta(alerta: dict):
    """
    Crea o actualiza nodos Vuelo, Alerta y ZonaH3 con sus relaciones.
    - MERGE para Vuelo y ZonaH3 (evita duplicados)
    - CREATE para Alerta (cada alerta es única)
    """
    d = get_driver()
    if d is None:
        return

    try:
        with d.session() as session:
            # Si la alerta tiene icao24, crear nodo Vuelo + Alerta
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

            # Si la alerta tiene celda H3, crear nodo ZonaH3
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

                # Si hay icao24, enlazar también vuelo con zona
                if alerta.get("icao24"):
                    session.run("""
                        MATCH (v:Vuelo {icao24: $icao24})
                        MATCH (z:ZonaH3 {celda: $h3})
                        MERGE (v)-[:VUELA_SOBRE]->(z)
                    """,
                        icao24=alerta.get("icao24"),
                        h3=h3,
                    )

        logger.debug("📝 Alerta persistida en Neo4j: %s", alerta.get("id"))
    except Exception as e:
        logger.error("❌ Error al persistir en Neo4j: %s", e)


# ══════════════════════════════════════════════════════════════
# Lectura: Consultas Cypher
# ══════════════════════════════════════════════════════════════
def get_alertas_por_vuelo(icao24: str) -> list:
    """Devuelve el historial de alertas de un avión concreto."""
    d = get_driver()
    if d is None:
        return []
    with d.session() as session:
        result = session.run("""
            MATCH (v:Vuelo {icao24: $icao24})-[:TIENE_ALERTA]->(a:Alerta)
            RETURN a ORDER BY a.timestamp DESC LIMIT 50
        """, icao24=icao24)
        return [dict(record["a"]) for record in result]


def get_zonas_calientes(limit: int = 10) -> list:
    """Devuelve las zonas H3 con más alertas."""
    d = get_driver()
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


def get_vuelos_reincidentes(min_alertas: int = 3) -> list:
    """Devuelve vuelos con más de N alertas."""
    d = get_driver()
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
        """, min=min_alertas)
        return [dict(record) for record in result]
