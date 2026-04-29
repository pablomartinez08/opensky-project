"""
═══════════════════════════════════════════════════════════════════
  UC-01 — Alerta Vertical: Ascenso / Descenso Brusco
  DataStream API · KeyedProcessFunction · ValueState
═══════════════════════════════════════════════════════════════════
  Detecta cuando una aeronave experimenta un cambio de altitud
  anómalo entre dos lecturas consecutivas de OpenSky (~10s).

  Flink mantiene un ValueState por cada icao24 con:
    - La altitud del poll anterior
    - Si hay una alerta activa (ciclo: inicio → actualizacion → fin)

  Publica las alertas en el topic Kafka 'flight-alerts'
  respetando al 100% el contrato alert_schema_contract.json v1.1.0
═══════════════════════════════════════════════════════════════════
"""

import json
from datetime import datetime, timezone

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


# ══════════════════════════════════════════════════════════════
# UMBRALES DE DETECCIÓN (calibrados para tráfico mundial)
# ══════════════════════════════════════════════════════════════
#   Con ~10.000 vuelos globales y polls cada ~15s:
#   - Descenso normal de aterrizaje: 5-15 m/s → 75-225m entre polls
#   - Descenso agresivo (go-around): 15-25 m/s → 225-375m
#   - Emergencia REAL: >30 m/s → >450m entre polls
#   Usamos umbrales altos para filtrar solo eventos excepcionales.
# ══════════════════════════════════════════════════════════════
UMBRAL_CRITICA = 4000   # |Δ alt| >= 4000m → severidad "critica" (emergencia real)
UMBRAL_ALTA    = 2500   # |Δ alt| >= 2500m → severidad "alta"    (maniobra extrema)
UMBRAL_MEDIA   = 1500   # |Δ alt| >= 1500m → severidad "media"   (evento significativo)
UMBRAL_BAJA    = 800    # |Δ alt| >=  800m → severidad "baja"    (cambio anómalo)

# Filtro adicional: velocidad vertical instantánea (m/s) de OpenSky
# Si |vertical_rate| < este umbral, ignoramos el delta (probablemente gap de datos)
UMBRAL_VERTICAL_RATE = 20  # m/s — normal comercial: 5-15 m/s


# ══════════════════════════════════════════════════════════════
# PASO 1: Parsear el JSON crudo de Kafka
# ══════════════════════════════════════════════════════════════
class ParseJsonFunction(MapFunction):
    """Convierte el string JSON de Kafka en un diccionario de Python"""
    def map(self, value):
        try:
            data = json.loads(value)
            # Descartar aviones en tierra (no tienen anomalías de altitud relevantes)
            if data.get("on_ground", False):
                return None
            # Descartar si no tiene altitud o icao24
            if data.get("altitude") is None or data.get("icao24") is None:
                return None
            return data
        except Exception:
            return None


# ══════════════════════════════════════════════════════════════
# PASO 2: Extraer la clave (icao24) del diccionario
# ══════════════════════════════════════════════════════════════
class ExtractIcao24(MapFunction):
    """
    Convierte el dict en una tupla (icao24, dict) para poder hacer key_by.
    Necesario porque key_by en PyFlink con PICKLED_BYTE_ARRAY
    necesita un key_selector que devuelva un tipo primitivo.
    """
    def map(self, value):
        if value is None:
            return None
        return value


# ══════════════════════════════════════════════════════════════
# PASO 3: Detección Stateful — El "cerebro" del sistema
# ══════════════════════════════════════════════════════════════
class VerticalAlertDetector(KeyedProcessFunction):
    """
    UC-01 — Alerta Vertical con ciclo de vida de estado.

    Por cada icao24, mantiene un ValueState con:
      - prev_altitude : float  → Altitud de la lectura anterior
      - alert_active  : bool   → Si ya hay una alerta viva para este vuelo

    Lógica del ciclo de vida:
      1. Si detecta anomalía y NO había alerta → emite estado="inicio"
      2. Si detecta anomalía y YA había alerta → emite estado="actualizacion"
      3. Si NO detecta anomalía pero HABÍA alerta → emite estado="fin"
      4. Si no detecta anomalía y no había alerta → no emite nada (silencio)
    """

    def open(self, runtime_context: RuntimeContext):
        """Se ejecuta una vez al arrancar el operador. Registra el estado."""
        self.state = runtime_context.get_state(
            ValueStateDescriptor("vertical_state", Types.PICKLED_BYTE_ARRAY())
        )

    def _calcular_severidad(self, abs_delta):
        """Determina la severidad según el valor absoluto del delta de altitud"""
        if abs_delta >= UMBRAL_CRITICA:
            return "critica"
        elif abs_delta >= UMBRAL_ALTA:
            return "alta"
        elif abs_delta >= UMBRAL_MEDIA:
            return "media"
        elif abs_delta >= UMBRAL_BAJA:
            return "baja"
        return None  # No supera ningún umbral → sin anomalía

    def _construir_alerta(self, flight, prev_altitude, delta, severidad, estado):
        """
        Construye el JSON de alerta respetando el contrato
        alert_schema_contract.json v1.1.0 al 100%.
        """
        velocity = flight.get("velocity", 0) or 0
        return {
            # ──── Campos comunes (SIEMPRE presentes) ────
            "tipo_alerta": "vertical",
            "estado": estado,
            "severidad": severidad,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "latitud": flight.get("latitude"),
            "longitud": flight.get("longitude"),
            "origen_pais": flight.get("origin_country"),
            "en_tierra": flight.get("on_ground", False),

            # ──── Campos específicos de alerta vertical ────
            "icao24": flight.get("icao24"),
            "callsign": flight.get("callsign"),
            "altitud_actual": flight.get("altitude"),
            "altitud_previa": prev_altitude,
            "delta_altitud": delta,
            "velocidad_vertical_ms": flight.get("vertical_rate"),
            "velocidad_kmh": round(velocity * 3.6, 1),
            "heading": flight.get("heading"),

            # ──── Campos null (otros tipos de alerta) ────
            "celda_h3": None,
            "num_vuelos_actual": None,
            "num_vuelos_previo": None,
            "vuelos_afectados": None,
            "ventana_inicio": None,
            "ventana_fin": None,
            "patron": None,
            "altitud_inicio_patron": None,
            "altitud_final": None,
            "duracion_vel_cero_seg": None
        }

    def process_element(self, flight, ctx):
        """
        Se ejecuta por cada evento entrante, agrupado por icao24.
        Es el corazón del procesador.
        """
        if flight is None:
            return

        altitude = flight.get("altitude")
        if altitude is None:
            return

        # ── Recuperar estado previo ──
        prev = self.state.value()

        if prev is None:
            # Primera vez que vemos este avión: guardar y esperar al siguiente poll
            self.state.update({
                "prev_altitude": altitude,
                "alert_active": False
            })
            return

        prev_altitude = prev["prev_altitude"]
        alert_was_active = prev["alert_active"]

        # ── Calcular delta de altitud ──
        delta = altitude - prev_altitude
        abs_delta = abs(delta)

        # ── Filtro por vertical_rate (segundo nivel de validación) ──
        # Si OpenSky reporta una vertical_rate normal, el delta grande
        # probablemente se debe a un gap de datos (poll perdido), no a
        # una anomalía real. Solo alertamos si vertical_rate también es alto.
        vertical_rate = flight.get("vertical_rate")
        if vertical_rate is not None and abs(vertical_rate) < UMBRAL_VERTICAL_RATE:
            # Tasa vertical normal → no es anomalía, solo actualizar estado
            self.state.update({
                "prev_altitude": altitude,
                "alert_active": False
            })
            return

        severidad = self._calcular_severidad(abs_delta)

        if severidad:
            # ═══ HAY ANOMALÍA CONFIRMADA ═══
            estado = "inicio" if not alert_was_active else "actualizacion"
            alert = self._construir_alerta(flight, prev_altitude, delta, severidad, estado)

            self.state.update({
                "prev_altitude": altitude,
                "alert_active": True
            })

            yield json.dumps(alert, ensure_ascii=False)

        else:
            # ═══ NO HAY ANOMALÍA ═══
            # Silencio: no emitimos alertas "fin" para reducir ruido.
            # El frontend ya maneja la desaparición natural de alertas antiguas.
            self.state.update({
                "prev_altitude": altitude,
                "alert_active": False
            })


# ══════════════════════════════════════════════════════════════
# MAIN — Topología del Job de Flink
# ══════════════════════════════════════════════════════════════
def main():
    # 1. Configurar Entorno de Flink
    env = StreamExecutionEnvironment.get_execution_environment()

    # 1.1 Registrar el conector JAR de Kafka
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar")

    # 2. Kafka Consumer (ENTRADA: topic 'flight_data')
    consumer_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink-vertical-alert',
        'auto.offset.reset': 'latest'
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics='flight_data',
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_props
    )

    # 3. Kafka Producer (SALIDA: topic 'flight-alerts')
    kafka_producer = FlinkKafkaProducer(
        topic='flight-alerts',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    print("═══════════════════════════════════════════════════════════")
    print("  🚀 UC-01 — Detector de Anomalías Verticales            ")
    print("  📥 Entrada:  Kafka topic 'flight_data'                  ")
    print("  📤 Salida:   Kafka topic 'flight-alerts'                ")
    print("  🧠 Estado:   ValueState por icao24                      ")
    print("═══════════════════════════════════════════════════════════")

    # ── Pipeline ──
    # Leer strings JSON crudos desde Kafka
    stream = env.add_source(kafka_consumer)

    # Parsear JSON → dict de Python (descartando aviones en tierra y sin datos)
    parsed = stream \
        .map(ParseJsonFunction(), output_type=Types.PICKLED_BYTE_ARRAY()) \
        .filter(lambda flight: flight is not None)

    # Agrupar por icao24 (cada avión tiene su propio estado)
    keyed = parsed.key_by(
        lambda flight: flight.get("icao24", "unknown"),
        key_type=Types.STRING()
    )

    # Aplicar la detección con estado (KeyedProcessFunction + ValueState)
    alerts = keyed.process(VerticalAlertDetector(), output_type=Types.STRING())

    # ── Doble Sink ──
    # 1) Imprimir en consola del TaskManager (para debugging / demo)
    alerts.print()
    # 2) Publicar en topic Kafka 'flight-alerts' (para el Backend/Frontend)
    alerts.add_sink(kafka_producer)

    # Ejecutar la topología
    env.execute("UC-01 Vertical Alert Detector")


if __name__ == '__main__':
    main()
