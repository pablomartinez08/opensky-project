"""
UC-04 - Correlador CEP de alertas.

Consume las alertas generadas por los otros jobs desde 'flight-alerts' y
emite alertas CEP de nivel superior al mismo topic.

Patrones implementados:
  1) zona_muerta + alta_densidad en celdas H3 vecinas:
     emite 'cep_zona_problematica' por posible meteorologia o cierre de zona.
  2) alerta vertical con descenso fuerte y altitud final cercana a cero:
     emite 'cep_aterrizaje_emergencia' por posible aterrizaje de emergencia.

Importante: el job ignora las alertas CEP para evitar realimentarse con sus
propias salidas.
"""

import json
import time
from datetime import datetime, timezone

import h3
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC_ALERTS = "flight-alerts"
CONSUMER_GROUP = "flink-cep-alert-correlator"

CORRELATION_WINDOW_MS = 180_000
STATE_TTL_MS = 300_000
NEIGHBOR_RING = 1
LOW_ALTITUDE_THRESHOLD_M = 100
MIN_VERTICAL_DROP_M = 800


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def h3_neighbors(cell, ring):
    try:
        if hasattr(h3, "grid_disk"):
            return set(h3.grid_disk(cell, ring))
        return set(h3.k_ring(cell, ring))
    except Exception:
        return {cell}


def severity_rank(severity):
    return {
        "baja": 1,
        "media": 2,
        "alta": 3,
        "critica": 4,
    }.get(severity, 1)


def max_severity(*values):
    ranks = {"baja": 1, "media": 2, "alta": 3, "critica": 4}
    reverse = {v: k for k, v in ranks.items()}
    return reverse[max(ranks.get(v, 1) for v in values)]


class ParseAlert(MapFunction):
    """Parsea alertas JSON y descarta las alertas CEP para evitar feedback loop."""

    def map(self, value):
        try:
            alert = json.loads(value)
            if str(alert.get("tipo_alerta", "")).startswith("cep_"):
                return None
            if alert.get("tipo_alerta") not in {"vertical", "alta_densidad", "zona_muerta"}:
                return None
            return alert
        except Exception:
            return None


class CepCorrelator(KeyedProcessFunction):
    """Mantiene estado global reciente para correlacionar alertas heterogeneas."""

    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_state(
            ValueStateDescriptor("cep_correlator_state", Types.PICKLED_BYTE_ARRAY())
        )

    def _empty_state(self):
        return {
            "dead_zones": {},
            "density_zones": {},
            "vertical_events": {},
            "emitted": {},
        }

    def _cleanup(self, state, now_ms):
        cutoff = now_ms - STATE_TTL_MS
        for bucket in ("dead_zones", "density_zones", "vertical_events", "emitted"):
            state[bucket] = {
                key: value
                for key, value in state[bucket].items()
                if value.get("ts", 0) >= cutoff
            }

    def _was_emitted(self, state, key):
        return key in state["emitted"]

    def _mark_emitted(self, state, key, now_ms):
        state["emitted"][key] = {"ts": now_ms}

    def _build_base_alert(self, source, pattern, severity, alert_type):
        return {
            "tipo_alerta": alert_type,
            "estado": "inicio",
            "severidad": severity,
            "timestamp": utc_now(),
            "latitud": source.get("latitud"),
            "longitud": source.get("longitud"),
            "origen_pais": source.get("origen_pais"),
            "en_tierra": source.get("en_tierra"),
            "icao24": source.get("icao24"),
            "callsign": source.get("callsign"),
            "altitud_actual": None,
            "altitud_previa": None,
            "delta_altitud": None,
            "velocidad_vertical_ms": None,
            "velocidad_kmh": source.get("velocidad_kmh"),
            "heading": source.get("heading"),
            "celda_h3": None,
            "num_vuelos_actual": None,
            "num_vuelos_previo": None,
            "vuelos_afectados": None,
            "ventana_inicio": None,
            "ventana_fin": None,
            "patron": pattern,
            "altitud_inicio_patron": None,
            "altitud_final": None,
            "duracion_vel_cero_seg": None,
        }

    def _build_zone_cep(self, dead_zone, density_zone):
        severity = max_severity(dead_zone.get("severidad"), density_zone.get("severidad"), "alta")
        alert = self._build_base_alert(
            dead_zone,
            "zona_muerta -> densidad_contigua",
            "critica" if severity_rank(severity) >= 3 else "alta",
            "cep_zona_problematica",
        )
        alert["en_tierra"] = None
        alert["origen_pais"] = None
        alert["velocidad_kmh"] = None
        alert["heading"] = None
        alert["num_vuelos_actual"] = density_zone.get("num_vuelos_actual")
        alert["num_vuelos_previo"] = dead_zone.get("num_vuelos_previo")
        alert["ventana_inicio"] = dead_zone.get("ventana_inicio")
        alert["ventana_fin"] = density_zone.get("ventana_fin") or dead_zone.get("ventana_fin")
        return alert

    def _build_vertical_cep(self, vertical):
        alert = self._build_base_alert(
            vertical,
            "descenso_brusco -> altitud_cero",
            "critica",
            "cep_aterrizaje_emergencia",
        )
        alert["en_tierra"] = vertical.get("en_tierra")
        alert["altitud_inicio_patron"] = vertical.get("altitud_previa")
        alert["altitud_final"] = vertical.get("altitud_actual")
        alert["velocidad_kmh"] = vertical.get("velocidad_kmh")
        alert["heading"] = vertical.get("heading")
        return alert

    def _find_zone_correlations(self, state, source_type, source, now_ms):
        if source_type == "zona_muerta":
            dead_zone = source
            dead_cell = dead_zone.get("celda_h3")
            if not dead_cell:
                return

            neighbors = h3_neighbors(dead_cell, NEIGHBOR_RING)
            for density_cell, density_zone in state["density_zones"].items():
                if density_cell not in neighbors:
                    continue
                if abs(now_ms - density_zone["ts"]) > CORRELATION_WINDOW_MS:
                    continue

                emit_key = f"zone:{dead_cell}:{density_cell}:{dead_zone.get('ventana_inicio')}"
                if not self._was_emitted(state, emit_key):
                    self._mark_emitted(state, emit_key, now_ms)
                    yield self._build_zone_cep(dead_zone, density_zone)

        elif source_type == "alta_densidad":
            density_zone = source
            density_cell = density_zone.get("celda_h3")
            if not density_cell:
                return

            neighbors = h3_neighbors(density_cell, NEIGHBOR_RING)
            for dead_cell, dead_zone in state["dead_zones"].items():
                if dead_cell not in neighbors:
                    continue
                if abs(now_ms - dead_zone["ts"]) > CORRELATION_WINDOW_MS:
                    continue

                emit_key = f"zone:{dead_cell}:{density_cell}:{dead_zone.get('ventana_inicio')}"
                if not self._was_emitted(state, emit_key):
                    self._mark_emitted(state, emit_key, now_ms)
                    yield self._build_zone_cep(dead_zone, density_zone)

    def _is_vertical_emergency(self, alert):
        altitude = alert.get("altitud_actual")
        delta = alert.get("delta_altitud")
        if altitude is None or delta is None:
            return False
        return altitude <= LOW_ALTITUDE_THRESHOLD_M and delta <= -MIN_VERTICAL_DROP_M

    def process_element(self, alert, ctx):
        if alert is None:
            return

        now_ms = int(time.time() * 1000)
        state = self.state.value() or self._empty_state()
        self._cleanup(state, now_ms)

        tipo = alert.get("tipo_alerta")
        estado = alert.get("estado")

        if tipo == "zona_muerta" and estado in {"inicio", "actualizacion"}:
            cell = alert.get("celda_h3")
            if cell:
                item = dict(alert)
                item["ts"] = now_ms
                state["dead_zones"][cell] = item
                for cep in self._find_zone_correlations(state, tipo, item, now_ms):
                    yield json.dumps(cep, ensure_ascii=False)

        elif tipo == "alta_densidad" and estado in {"inicio", "actualizacion"}:
            cell = alert.get("celda_h3")
            if cell:
                item = dict(alert)
                item["ts"] = now_ms
                state["density_zones"][cell] = item
                for cep in self._find_zone_correlations(state, tipo, item, now_ms):
                    yield json.dumps(cep, ensure_ascii=False)

        elif tipo == "vertical" and estado in {"inicio", "actualizacion"}:
            icao24 = alert.get("icao24")
            if icao24:
                item = dict(alert)
                item["ts"] = now_ms
                state["vertical_events"][icao24] = item

                emit_key = f"vertical:{icao24}:{alert.get('timestamp')}"
                if self._is_vertical_emergency(item) and not self._was_emitted(state, emit_key):
                    self._mark_emitted(state, emit_key, now_ms)
                    yield json.dumps(self._build_vertical_cep(item), ensure_ascii=False)

        self.state.update(state)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar")
    env.set_parallelism(1)

    consumer_props = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "latest",
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC_ALERTS,
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_props,
    )

    kafka_producer = FlinkKafkaProducer(
        topic=KAFKA_TOPIC_ALERTS,
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
    )

    print("============================================================")
    print("  UC-04 - CEP Alert Correlator")
    print("  Entrada: Kafka topic 'flight-alerts'")
    print("  Salida:  Kafka topic 'flight-alerts'")
    print("  Patrones: zona_muerta+density, vertical+altitud_cero")
    print("============================================================")

    stream = env.add_source(kafka_consumer)

    parsed = (
        stream
        .map(ParseAlert(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda alert: alert is not None)
    )

    keyed = parsed.key_by(lambda _: "cep-global", key_type=Types.STRING())
    cep_alerts = keyed.process(CepCorrelator(), output_type=Types.STRING())

    cep_alerts.print()
    cep_alerts.add_sink(kafka_producer)

    env.execute("UC-04 CEP Alert Correlator")


if __name__ == "__main__":
    main()
