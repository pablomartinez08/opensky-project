"""
UC-03 - Detector de zonas muertas hexagonales.

Detecta celdas H3 que pasan de tener muchos aviones en una ventana
temporal a tener muy pocos o ninguno en la ventana siguiente.

Entrada:  Kafka topic 'flight_data'
Salida:   Kafka topic 'flight-alerts'
"""

import json
from datetime import datetime, timezone

import h3
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


H3_RESOLUTION = 5
WINDOW_SECONDS = 60
GRACE_SECONDS = 5
WINDOW_MS = WINDOW_SECONDS * 1000
GRACE_MS = GRACE_SECONDS * 1000

MIN_PREVIOUS_FLIGHTS = 4
MAX_CURRENT_FLIGHTS = 2
MIN_DROP_RATIO = 0.70
RECOVERY_RATIO = 0.65


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_from_ms(epoch_ms):
    return datetime.fromtimestamp(epoch_ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def latlng_to_cell(latitude, longitude, resolution):
    if hasattr(h3, "latlng_to_cell"):
        return h3.latlng_to_cell(latitude, longitude, resolution)
    return h3.geo_to_h3(latitude, longitude, resolution)


def cell_to_latlng(cell):
    if hasattr(h3, "cell_to_latlng"):
        return h3.cell_to_latlng(cell)
    return h3.h3_to_geo(cell)


class ParseJsonFunction(MapFunction):
    """Parsea el JSON crudo de Kafka y descarta lecturas sin posicion"""

    def map(self, value):
        try:
            data = json.loads(value)
            if data.get("icao24") is None:
                return None
            if data.get("latitude") is None or data.get("longitude") is None:
                return None
            if data.get("on_ground", False):
                return None
            return data
        except Exception:
            return None


class AddH3Cell(MapFunction):
    """Añade la celda H3 al evento de vuelo"""

    def map(self, flight):
        try:
            latitude = float(flight["latitude"])
            longitude = float(flight["longitude"])
            flight["celda_h3"] = latlng_to_cell(latitude, longitude, H3_RESOLUTION)
            return flight
        except Exception:
            return None


class DeadZoneDetector(KeyedProcessFunction):
    """
    Mantiene estado por celda H3.

    La zona muerta es ausencia de eventos, asi que se usan timers por celda.
    """

    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_state(
            ValueStateDescriptor("dead_zone_state", Types.PICKLED_BYTE_ARRAY())
        )

    def _window_start(self, timestamp_ms):
        return (timestamp_ms // WINDOW_MS) * WINDOW_MS

    def _empty_state(self, cell, window_start):
        lat, lon = cell_to_latlng(cell)
        return {
            "cell": cell,
            "center_lat": lat,
            "center_lon": lon,
            "current_window_start": window_start,
            "current_flights": {},
            "prev_count": None,
            "alert_active": False,
            "baseline_count": None,
        }

    def _register_window_timer(self, ctx, window_start):
        fire_at = window_start + WINDOW_MS + GRACE_MS
        ctx.timer_service().register_processing_time_timer(fire_at)

    def _severity(self, previous_count, current_count):
        if previous_count <= 0:
            return "baja"

        drop_ratio = (previous_count - current_count) / previous_count
        if previous_count >= 25 and drop_ratio >= 0.95:
            return "critica"
        if previous_count >= 15 and drop_ratio >= 0.90:
            return "alta"
        if drop_ratio >= 0.80:
            return "media"
        return "baja"

    def _build_alert(self, state, current_count, previous_count, severity, status, window_start):
        return {
            "tipo_alerta": "zona_muerta",
            "estado": status,
            "severidad": severity,
            "timestamp": utc_now(),
            "latitud": state["center_lat"],
            "longitud": state["center_lon"],
            "origen_pais": None,
            "en_tierra": None,
            "icao24": None,
            "callsign": None,
            "altitud_actual": None,
            "altitud_previa": None,
            "delta_altitud": None,
            "velocidad_vertical_ms": None,
            "velocidad_kmh": None,
            "heading": None,
            "celda_h3": state["cell"],
            "num_vuelos_actual": current_count,
            "num_vuelos_previo": previous_count,
            "vuelos_afectados": None,
            "ventana_inicio": iso_from_ms(window_start),
            "ventana_fin": iso_from_ms(window_start + WINDOW_MS),
            "patron": None,
            "altitud_inicio_patron": None,
            "altitud_final": None,
            "duracion_vel_cero_seg": None,
        }

    def _is_dead_zone_start(self, previous_count, current_count):
        if previous_count is None or previous_count < MIN_PREVIOUS_FLIGHTS:
            return False
        drop_ratio = (previous_count - current_count) / previous_count
        return current_count <= MAX_CURRENT_FLIGHTS and drop_ratio >= MIN_DROP_RATIO

    def _is_still_dead(self, baseline_count, current_count):
        if baseline_count is None:
            return False
        return current_count <= MAX_CURRENT_FLIGHTS or current_count <= baseline_count * RECOVERY_RATIO

    def _finalize_current_window(self, ctx, state):
        current_count = len(state["current_flights"])
        previous_count = state["prev_count"]
        window_start = state["current_window_start"]

        if state["alert_active"]:
            baseline_count = state["baseline_count"]
            if self._is_still_dead(baseline_count, current_count):
                severity = self._severity(baseline_count, current_count)
                yield self._build_alert(
                    state, current_count, baseline_count, severity, "actualizacion", window_start
                )
            else:
                severity = self._severity(baseline_count, current_count)
                yield self._build_alert(
                    state, current_count, baseline_count, severity, "fin", window_start
                )
                state["alert_active"] = False
                state["baseline_count"] = None

        elif self._is_dead_zone_start(previous_count, current_count):
            severity = self._severity(previous_count, current_count)
            yield self._build_alert(
                state, current_count, previous_count, severity, "inicio", window_start
            )
            state["alert_active"] = True
            state["baseline_count"] = previous_count

        state["prev_count"] = current_count
        state["current_window_start"] = window_start + WINDOW_MS
        state["current_flights"] = {}

        if state["alert_active"] or state["prev_count"] >= MIN_PREVIOUS_FLIGHTS:
            self._register_window_timer(ctx, state["current_window_start"])

    def process_element(self, flight, ctx):
        if flight is None:
            return

        cell = flight.get("celda_h3")
        if not cell:
            return

        now_ms = ctx.timer_service().current_processing_time()
        event_window_start = self._window_start(now_ms)
        state = self.state.value()

        if state is None:
            state = self._empty_state(cell, event_window_start)
            self._register_window_timer(ctx, event_window_start)

        while state["current_window_start"] < event_window_start:
            for alert in self._finalize_current_window(ctx, state):
                yield json.dumps(alert, ensure_ascii=False)

        icao24 = flight.get("icao24")
        if icao24:
            state["current_flights"][icao24] = {
                "callsign": flight.get("callsign"),
                "latitude": flight.get("latitude"),
                "longitude": flight.get("longitude"),
            }

        self.state.update(state)

    def on_timer(self, timestamp, ctx):
        state = self.state.value()
        if state is None:
            return

        expected_timer = state["current_window_start"] + WINDOW_MS + GRACE_MS
        if timestamp != expected_timer:
            return

        for alert in self._finalize_current_window(ctx, state):
            yield json.dumps(alert, ensure_ascii=False)

        self.state.update(state)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar")

    consumer_props = {
        "bootstrap.servers": "kafka:9092",
        "group.id": "flink-dead-zone-alert",
        "auto.offset.reset": "latest",
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics="flight_data",
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_props,
    )

    kafka_producer = FlinkKafkaProducer(
        topic="flight-alerts",
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": "kafka:9092"},
    )

    print("============================================================")
    print("  UC-03 - Detector de zonas muertas hexagonales")
    print("  Entrada: Kafka topic 'flight_data'")
    print("  Salida:  Kafka topic 'flight-alerts'")
    print(f"  H3 resolution: {H3_RESOLUTION}")
    print(f"  Ventana: {WINDOW_SECONDS}s")
    print("============================================================")

    stream = env.add_source(kafka_consumer)

    parsed = (
        stream
        .map(ParseJsonFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda flight: flight is not None)
        .map(AddH3Cell(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda flight: flight is not None)
    )

    keyed = parsed.key_by(
        lambda flight: flight.get("celda_h3", "unknown"),
        key_type=Types.STRING(),
    )

    alerts = keyed.process(DeadZoneDetector(), output_type=Types.STRING())

    alerts.print()
    alerts.add_sink(kafka_producer)

    env.execute("UC-03 Dead Zone Hexagonal Detector")


if __name__ == "__main__":
    main()
