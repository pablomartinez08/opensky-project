"""
Mock para probar el detector de zonas muertas.

Fases:
  1) WARMUP  - envia muchos vuelos en la misma celda H3 durante una ventana.
  2) DROP    - deja la celda casi vacia durante la ventana siguiente.
  3) RECOVER - vuelve a enviar vuelos para provocar el fin de la alerta.

Configuracion alineada con flink_app/dead_zone_detector.py:
  WINDOW_SECONDS       = 60
  MIN_PREVIOUS_FLIGHTS = 8
  MAX_CURRENT_FLIGHTS  = 1
  MIN_DROP_RATIO       = 0.80
"""

import json
import random
import time
from kafka import KafkaProducer


KAFKA_SERVER = "localhost:9094"
TOPIC = "flight_data"

# Madrid. Con jitter pequeno, todos los vuelos caen en la misma celda H3 res=5.
CENTER_LAT = 40.4167
CENTER_LNG = -3.7037

ACTIVE_FLIGHTS = 12
DROP_FLIGHTS = 0
RECOVERY_FLIGHTS = 8
INTERVAL_SECONDS = 5

# Un poco mas que la ventana del detector para asegurar que Flink cierre ventanas.
WARMUP_SECONDS = 70
DROP_SECONDS = 75
RECOVERY_SECONDS = 70


def build_flight(id_num, jitter=0.01):
    return {
        "icao24": f"dead{id_num:03x}",
        "callsign": f"DZ{id_num:03d}",
        "origin_country": "Mock Country",
        "longitude": CENTER_LNG + random.uniform(-jitter, jitter),
        "latitude": CENTER_LAT + random.uniform(-jitter, jitter),
        "altitude": random.randint(7000, 11000),
        "on_ground": False,
        "velocity": random.randint(180, 260),
        "heading": random.randint(0, 359),
        "vertical_rate": 0,
    }


def send_batch(producer, count):
    for i in range(count):
        producer.send(TOPIC, build_flight(i))
    producer.flush()


def run_phase(name, seconds, flights_per_batch, producer):
    iterations = seconds // INTERVAL_SECONDS
    print(f"\n--- {name}: {flights_per_batch} vuelos cada {INTERVAL_SECONDS}s durante {seconds}s ---")

    for i in range(iterations):
        send_batch(producer, flights_per_batch)
        elapsed = (i + 1) * INTERVAL_SECONDS
        print(f"[{i + 1}/{iterations}] t={elapsed}s - enviados {flights_per_batch} vuelos")
        time.sleep(INTERVAL_SECONDS)


def main():
    print(f"Conectando a Kafka en {KAFKA_SERVER}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Conectado. Iniciando simulacion de zona muerta.")
    print("Comprueba Kafka UI -> topic flight-alerts -> tipo_alerta = zona_muerta")

    run_phase("FASE 1 WARMUP / celda activa", WARMUP_SECONDS, ACTIVE_FLIGHTS, producer)
    run_phase("FASE 2 DROP / celda muerta", DROP_SECONDS, DROP_FLIGHTS, producer)
    run_phase("FASE 3 RECOVERY / recuperacion", RECOVERY_SECONDS, RECOVERY_FLIGHTS, producer)

    print("\nTest completado. Si el job esta activo, deberias ver inicio y fin de zona_muerta.")


if __name__ == "__main__":
    main()
