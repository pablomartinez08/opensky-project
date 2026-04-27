"""
Consumer Kafka asíncrono para el topic 'flight-alerts'.
Consume alertas generadas por Flink y las distribuye a:
1. Almacén en memoria (deque de últimas 2000 alertas)
2. Neo4j (persistencia de grafos)
3. WebSocket (broadcast en tiempo real, con batching)
"""
import os
import json
import uuid
import asyncio
import logging
from collections import deque
from datetime import datetime

from aiokafka import AIOKafkaConsumer

from neo4j_client import persistir_alerta

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
# Configuración
# ══════════════════════════════════════════════════════════════
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "flight-alerts")
KAFKA_GROUP = "backend-consumer"

# ══════════════════════════════════════════════════════════════
# Estado global compartido con los routers
# ══════════════════════════════════════════════════════════════
alertas_store: deque = deque(maxlen=2000)
alertas_by_id: dict = {}
kafka_connected: bool = False
start_time: datetime = datetime.utcnow()

# Lista de conexiones WebSocket activas
ws_connections: list = []

# Buffer de alertas pendientes de enviar por WebSocket (batching)
ws_buffer: list = []

# Contadores para /stats (independientes del deque, NUNCA se resetean)
stats = {
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


# ══════════════════════════════════════════════════════════════
# Consumer principal
# ══════════════════════════════════════════════════════════════
async def consume_alerts():
    """Bucle principal del consumer Kafka asíncrono."""
    global kafka_connected

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
    )

    # Intentar conectar con reintentos
    while True:
        try:
            await consumer.start()
            kafka_connected = True
            logger.info("✅ Consumer Kafka conectado al topic '%s'", KAFKA_TOPIC)
            break
        except Exception as e:
            kafka_connected = False
            logger.warning("⏳ Esperando a Kafka... (%s)", e)
            await asyncio.sleep(3)

    # Arrancar el flush de WebSocket en paralelo
    asyncio.create_task(ws_flush_loop())

    try:
        async for msg in consumer:
            alerta = msg.value
            await process_alert(alerta)
    except asyncio.CancelledError:
        logger.info("🛑 Consumer Kafka detenido")
    except Exception as e:
        logger.error("❌ Error en consumer Kafka: %s", e)
        kafka_connected = False
    finally:
        await consumer.stop()
        kafka_connected = False


def _round_alert(alerta: dict) -> dict:
    """Redondea los campos numéricos para evitar decimales infinitos."""
    for key in ("delta_altitud", "altitud_actual", "altitud_previa",
                "velocidad_vertical_ms", "velocidad_kmh", "heading",
                "latitud", "longitud"):
        v = alerta.get(key)
        if isinstance(v, float):
            alerta[key] = round(v, 2)
    return alerta


async def process_alert(alerta: dict):
    """Procesa una alerta: almacena, persiste en Neo4j, y bufferiza para WebSocket."""

    # 1. Asignar UUID único y redondear decimales
    alerta["id"] = str(uuid.uuid4())
    alerta = _round_alert(alerta)

    # 2. Almacenar en memoria
    alertas_store.appendleft(alerta)
    alertas_by_id[alerta["id"]] = alerta

    # Limpiar alertas_by_id si crece demasiado (mantener sincronizado con deque)
    if len(alertas_by_id) > 2500:
        ids_in_store = {a["id"] for a in alertas_store}
        alertas_by_id.clear()
        for a in alertas_store:
            alertas_by_id[a["id"]] = a

    # 3. Actualizar contadores (estos NUNCA se resetean)
    stats["total_alertas"] += 1
    tipo = alerta.get("tipo_alerta", "")
    sev = alerta.get("severidad", "")
    if tipo in stats["por_tipo"]:
        stats["por_tipo"][tipo] += 1
    if sev in stats["por_severidad"]:
        stats["por_severidad"][sev] += 1
    if sev == "critica":
        stats["alertas_criticas"] += 1
    stats["ultima_actualizacion"] = alerta.get("timestamp")

    # 4. Persistir en Neo4j (síncrono pero rápido)
    try:
        persistir_alerta(alerta)
    except Exception as e:
        logger.error("⚠️ Error al persistir en Neo4j: %s", e)

    # 5. Añadir al buffer de WebSocket (se envía en batch cada 500ms)
    ws_buffer.append(alerta)


async def ws_flush_loop():
    """Envía alertas bufferizadas por WebSocket cada 500ms en batch."""
    while True:
        await asyncio.sleep(0.5)
        if not ws_buffer or not ws_connections:
            continue

        # Vaciar el buffer en un batch
        batch = list(ws_buffer)
        ws_buffer.clear()

        # Enviar como array JSON (1 mensaje con N alertas)
        msg_json = json.dumps(batch)
        dead_connections = []
        for ws in ws_connections:
            try:
                await ws.send_text(msg_json)
            except Exception:
                dead_connections.append(ws)

        for ws in dead_connections:
            ws_connections.remove(ws)

        if batch:
            logger.info("📤 Batch WS: %d alertas → %d clientes", len(batch), len(ws_connections))
