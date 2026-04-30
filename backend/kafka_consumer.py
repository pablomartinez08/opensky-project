"""
Consumer Kafka asíncrono — Refactorizado SOLID (Principio S).

Responsabilidad ÚNICA: conectarse a Kafka y consumir mensajes.
Delega TODO el procesamiento al AlertProcessor inyectado.
"""
import os
import json
import asyncio
import logging

from aiokafka import AIOKafkaConsumer

from services.alert_processor import AlertProcessor

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
# Configuración
# ══════════════════════════════════════════════════════════════
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "flight-alerts")
KAFKA_GROUP = "backend-consumer"


class KafkaAlertConsumer:
    """
    Consumer Kafka asíncrono con responsabilidad única.

    Principios SOLID:
      S — Solo consume de Kafka. No almacena, no persiste, no emite WS.
      D — Depende de AlertProcessor (abstracción), no de stores concretos.
    """

    def __init__(self, processor: AlertProcessor):
        self._processor = processor
        self.connected: bool = False

    async def consume(self):
        """Bucle principal del consumer Kafka asíncrono."""
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
                self.connected = True
                logger.info("✅ Consumer Kafka conectado al topic '%s'", KAFKA_TOPIC)
                break
            except Exception as e:
                self.connected = False
                logger.warning("⏳ Esperando a Kafka... (%s)", e)
                await asyncio.sleep(3)

        try:
            async for msg in consumer:
                alerta = msg.value
                await self._processor.process(alerta)
        except asyncio.CancelledError:
            logger.info("🛑 Consumer Kafka detenido")
        except Exception as e:
            logger.error("❌ Error en consumer Kafka: %s", e)
            self.connected = False
        finally:
            await consumer.stop()
            self.connected = False
