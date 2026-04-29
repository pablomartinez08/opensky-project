"""
╔══════════════════════════════════════════════════════════════════╗
║  ALERTA DE ALTA DENSIDAD — DataStream API (PyFlink)            ║
║                                                                ║
║  Agrupa los vuelos mediante el índice espacial H3. Mantiene un ║
║  estado de los vuelos activos por celda y genera una alerta    ║
║  si el número de vuelos supera un umbral dinámico basado en    ║
║  la zona geográfica (ej. Nueva York vs Océano).                ║
╚══════════════════════════════════════════════════════════════════╝
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import h3

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import (
    MapFunction,
    FilterFunction,
    KeyedProcessFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ValueStateDescriptor

# ─── Configuración ────────────────────────────────────────────────
KAFKA_BOOTSTRAP     = "kafka:9092"
KAFKA_TOPIC_IN      = "flight_data"
KAFKA_TOPIC_OUT     = "flight-alerts"       # Conforme al schema contract
GROUP_ID_PREFIX     = "flink-density-alert-test"
AUTO_OFFSET_RESET   = "earliest"            # Leer el topic desde el inicio para pruebas

H3_RESOLUTION       = 4                     # Ajustable (4 = celdas de ~1700 km2)
FLIGHT_EXPIRY_MS    = 120_000               # 2 minutos para considerar un vuelo "inactivo"
SAMPLE_INTERVAL_MS  = 30_000                # Muestreo estadístico cada 30 segundos
MIN_SAMPLES_WARMUP  = 10                    # Muestras mínimas antes de activar alertas
THRESHOLD_SIGMA     = 3                     # Sensibilidad (Mean + 3 * StdDev)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("DensityAlert")


# ═══════════════════════════════════════════════════════════════════
# 1. FUNCIONES AUXILIARES
# ═══════════════════════════════════════════════════════════════════

def h3_point_to_index(lat: float, lng: float, res: int) -> Optional[str]:
    """Convierte lat/lng a índice H3, compatible con v3 y v4."""
    try:
        if hasattr(h3, 'latlng_to_cell'):
            return h3.latlng_to_cell(lat, lng, res)
        return h3.geo_to_h3(lat, lng, res)
    except Exception:
        return None


def h3_index_to_geo(h3_index: str) -> tuple[float, float]:
    """Obtiene el centro geográfico de una celda H3."""
    try:
        if hasattr(h3, 'cell_to_latlng'):
            return h3.cell_to_latlng(h3_index)
        return h3.h3_to_geo(h3_index)
    except Exception:
        return 0.0, 0.0


class ParseAndEnrichWithH3(MapFunction):
    """
    Convierte JSON y calcula la celda H3 para cada evento.
    Si no tiene coordenadas válidas, lo descarta devolviendo None.
    """
    def map(self, value: str):
        try:
            flight = json.loads(value)
            lat = flight.get("latitude")
            lng = flight.get("longitude")
            
            if lat is not None and lng is not None:
                h3_index = h3_point_to_index(lat, lng, H3_RESOLUTION)
                if h3_index:
                    flight["h3_index"] = h3_index
                    return flight
            return None
        except Exception:
            return None


class FilterValid(FilterFunction):
    def filter(self, value):
        return value is not None


# ═══════════════════════════════════════════════════════════════════
# 2. FUNCIÓN PRINCIPAL: KEYED PROCESS FUNCTION (Manejo de estado)
# ═══════════════════════════════════════════════════════════════════

class HighDensityDetector(KeyedProcessFunction):
    """
    KeyedProcessFunction que utiliza el algoritmo de Welford para mantener
    un baseline estadístico de vuelos por celda H3 y detectar anomalías.
    """

    def open(self, runtime_context: RuntimeContext):
        # Vuelos activos en la celda
        flights_desc = ValueStateDescriptor("active_flights", Types.STRING())
        self.flights_state = runtime_context.get_state(flights_desc)

        # Estado de alerta (booleano)
        alert_desc = ValueStateDescriptor("is_alerting", Types.BOOLEAN())
        self.alerting_state = runtime_context.get_state(alert_desc)

        # Estadísticas: {"n": count, "mean": m, "M2": sum_of_squares}
        stats_desc = ValueStateDescriptor("stats", Types.STRING())
        self.stats_state = runtime_context.get_state(stats_desc)

        # Control del timer de muestreo
        timer_desc = ValueStateDescriptor("timer_ts", Types.LONG())
        self.timer_state = runtime_context.get_state(timer_desc)

    def update_stats(self, stats: Optional[Dict], x: int) -> Dict:
        if stats is None:
            return {"n": 1, "mean": float(x), "M2": 0.0}
        n = stats["n"] + 1
        delta = x - stats["mean"]
        mean = stats["mean"] + delta / n
        delta2 = x - mean
        M2 = stats["M2"] + delta * delta2
        return {"n": n, "mean": mean, "M2": M2}

    def compute_threshold(self, stats: Optional[Dict]) -> float:
        if stats is None or stats["n"] < MIN_SAMPLES_WARMUP:
            return 999.0
        variance = stats["M2"] / (stats["n"] - 1)
        std = variance ** 0.5
        return max(3.0, stats["mean"] + THRESHOLD_SIGMA * std)

    def process_element(self, flight: dict, ctx: KeyedProcessFunction.Context):
        h3_idx = ctx.get_current_key()
        icao24 = flight.get("icao24", "N/A")
        callsign = flight.get("callsign", "UNKNOWN")
        
        current_ts = int(time.time() * 1000)

        # 1. Actualizar vuelos activos
        encoded = self.flights_state.value()
        flights_map = json.loads(encoded) if encoded else {}
        flights_map[icao24] = {"ts": current_ts, "callsign": callsign}

        # Limpiar expirados
        keys_to_remove = [k for k, v in flights_map.items() if current_ts - v["ts"] > FLIGHT_EXPIRY_MS]
        for k in keys_to_remove: del flights_map[k]
        
        self.flights_state.update(json.dumps(flights_map))
        active_count = len(flights_map)

        # 2. Programar Timer de muestreo si no existe
        if self.timer_state.value() is None:
            next_timer = current_ts + SAMPLE_INTERVAL_MS
            ctx.timer_service().register_processing_time_timer(next_timer)
            self.timer_state.update(next_timer)

        # 3. Lógica de Alertas
        is_alerting = self.alerting_state.value() or False
        stats_json = self.stats_state.value()
        stats = json.loads(stats_json) if stats_json else None
        threshold = self.compute_threshold(stats)

        if active_count >= threshold:
            lat_centro, lng_centro = h3_index_to_geo(h3_idx)
            alert_json = {
                "tipo_alerta": "alta_densidad",
                "estado": "actualizacion" if is_alerting else "inicio",
                "severidad": "media" if active_count < threshold * 1.5 else "alta",
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "latitud": lat_centro,
                "longitud": lng_centro,
                "celda_h3": h3_idx,
                "num_vuelos_actual": active_count,
                "threshold_dinamico": round(threshold, 2),
                "vuelos_afectados": [v["callsign"] for v in flights_map.values() if v["callsign"] != "UNKNOWN"][:10],
            }
            self.alerting_state.update(True)
            yield json.dumps(alert_json, ensure_ascii=False)

        elif is_alerting and active_count < (threshold * 0.8): # Histéresis
            lat_centro, lng_centro = h3_index_to_geo(h3_idx)
            alert_json = {
                "tipo_alerta": "alta_densidad", "estado": "fin", "severidad": "baja",
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "latitud": lat_centro, "longitud": lng_centro, "celda_h3": h3_idx,
                "num_vuelos_actual": active_count, "threshold_dinamico": round(threshold, 2),
                "vuelos_afectados": [],
            }
            self.alerting_state.update(False)
            yield json.dumps(alert_json, ensure_ascii=False)

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext):
        encoded = self.flights_state.value()
        flights_map = json.loads(encoded) if encoded else {}
        active_count = len(flights_map)

        stats_json = self.stats_state.value()
        stats = json.loads(stats_json) if stats_json else None
        new_stats = self.update_stats(stats, active_count)
        self.stats_state.update(json.dumps(new_stats))

        next_timer = timestamp + SAMPLE_INTERVAL_MS
        ctx.timer_service().register_processing_time_timer(next_timer)
        self.timer_state.update(next_timer)


# ═══════════════════════════════════════════════════════════════════
# 3. FORMATEADOR PARA CONSOLA
# ═══════════════════════════════════════════════════════════════════

class AlertPrinter(MapFunction):
    def map(self, value: str):
        try:
            alert = json.loads(value)
            estado = alert.get("estado")
            h3_idx = alert.get("celda_h3")
            count = alert.get("num_vuelos_actual")
            vuelos = ", ".join(alert.get("vuelos_afectados", [])[:5])
            threshold = alert.get("threshold_dinamico", "N/A")
            
            if estado in ["inicio", "actualizacion"]:
                return (
                    f"\n{'='*60}\n"
                    f"🟢 ALERTA: ALTA DENSIDAD [{estado.upper()}]\n"
                    f"{'='*60}\n"
                    f"  Celda H3 : {h3_idx} (Threshold Dinámico: {threshold})\n"
                    f"  Vuelos   : {count} aeronaves detectadas\n"
                    f"  Callsigns: {vuelos}...\n"
                    f"{'='*60}\n"
                )
            else:
                return (f"✅ [FIN] Densidad normalizada en celda {h3_idx} (Vuelos: {count})")
        except Exception:
            return value


# ═══════════════════════════════════════════════════════════════════
# 4. MAIN — TOPOLOGÍA DEL JOB
# ═══════════════════════════════════════════════════════════════════

def main():
    log.info("Iniciando job de densidad H3...")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar")
    env.set_parallelism(1)

    group_id = f"{GROUP_ID_PREFIX}-{int(time.time())}"
    consumer_props = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": AUTO_OFFSET_RESET,
    }
    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC_IN,
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_props,
    )

    raw_stream = env.add_source(kafka_consumer)

    enriched = raw_stream.map(ParseAndEnrichWithH3(), output_type=Types.PICKLED_BYTE_ARRAY())
    valid_enriched = enriched.filter(FilterValid())

    keyed = valid_enriched.key_by(lambda flight: flight.get("h3_index", "0"))

    alerts = keyed.process(HighDensityDetector(), output_type=Types.STRING())

    pretty = alerts.map(AlertPrinter(), output_type=Types.STRING())
    pretty.print()

    producer_props = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
    }
    kafka_producer = FlinkKafkaProducer(
        topic=KAFKA_TOPIC_OUT,
        serialization_schema=SimpleStringSchema(),
        producer_config=producer_props,
    )
    alerts.add_sink(kafka_producer)

    log.info("Lanzando topología: OpenSky Density Tracker H3")
    env.execute("OpenSky Density Tracker (H3)")


if __name__ == "__main__":
    main()
