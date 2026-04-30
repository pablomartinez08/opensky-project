"""
Mock para probar el detector de alta densidad adaptativo.

Fases:
  1) BASELINE  — Envía tráfico normal (5 vuelos) para que Flink
     construya su estadística base (~30 segundos).
  2) SPIKE     — Inyecta 30 vuelos de golpe en la misma celda H3.
  3) MANTENER  — Sigue enviando los 30 vuelos para que no expiren
     y Flink tenga tiempo de evaluarlos.

Configuración alineada con density_alert.py:
  SAMPLE_INTERVAL_MS  = 5_000   (muestreo cada 5s)
  MIN_SAMPLES_WARMUP  = 3       (3 muestras mínimo → 15s de warmup)
  MIN_THRESHOLD_BUFFER = 3      (threshold = mean + max(sigma*std, 3))
  FLIGHT_EXPIRY_MS    = 120_000 (vuelos expiran a los 2 min sin refresh)
"""

import json
import time
import random
from kafka import KafkaProducer

# ─── Configuración ────────────────────────────────────────────
KAFKA_SERVER = "localhost:9094"
TOPIC = "flight_data"

# Coordenadas de prueba (Centro de Madrid) — todos caerán en la misma celda H3 res=4
CENTER_LAT = 40.4167
CENTER_LNG = -3.7037


def generate_flight(id_num, jitter=0.02):
    """Genera un vuelo con un icao24 único basado en id_num."""
    return {
        "icao24": f"mock{id_num:04x}",
        "callsign": f"TEST{id_num:03d}",
        "latitude": CENTER_LAT + random.uniform(-jitter, jitter),
        "longitude": CENTER_LNG + random.uniform(-jitter, jitter),
        "altitude": random.randint(5000, 40000),
        "velocity": random.randint(200, 900),
        "heading": random.randint(0, 359),
        "on_ground": False,
    }


def send_batch(producer, count, offset=0):
    """Envía un lote de vuelos y hace flush."""
    for i in range(count):
        producer.send(TOPIC, generate_flight(offset + i))
    producer.flush()


def main():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except Exception as e:
        print(f"❌ Error conectando a Kafka ({KAFKA_SERVER}): {e}")
        return

    print("🚀 Simulador de densidad para sistema adaptativo")
    print("=" * 55)

    # ─────────────────────────────────────────────────────────
    # FASE 1: BASELINE — establecer media ≈ 5 vuelos
    # ─────────────────────────────────────────────────────────
    baseline_flights = 5
    baseline_iterations = 8        # 8 × 5s = 40s (Flink recoge ~8 muestras)
    interval_s = 5                 # Alineado con SAMPLE_INTERVAL_MS = 5_000

    print(f"\n📊 FASE 1: Baseline ({baseline_flights} vuelos × {baseline_iterations} iteraciones)")
    print(f"   Duración estimada: {baseline_iterations * interval_s}s")
    print(f"   Flink registrará mean≈{baseline_flights}, threshold≈{baseline_flights + 3}")

    for i in range(baseline_iterations):
        send_batch(producer, baseline_flights, offset=0)
        elapsed = (i + 1) * interval_s
        print(f"   [{i+1}/{baseline_iterations}] {baseline_flights} vuelos enviados (t={elapsed}s)")
        time.sleep(interval_s)

    # ─────────────────────────────────────────────────────────
    # FASE 2: SPIKE — inyectar muchos vuelos
    # ─────────────────────────────────────────────────────────
    spike_flights = 30
    expected_threshold = baseline_flights + 3   # mean + MIN_THRESHOLD_BUFFER

    print(f"\n🔴 FASE 2: ¡SPIKE! ({spike_flights} vuelos nuevos)")
    print(f"   Threshold estimado: ~{expected_threshold}")
    print(f"   Total activos: {spike_flights} >> {expected_threshold} → DEBERÍA ALERTAR")

    # Enviar los vuelos del spike (offset=100 para icao24 distintos a baseline)
    send_batch(producer, spike_flights, offset=100)
    print(f"   ✅ {spike_flights} vuelos spike enviados")

    # ─────────────────────────────────────────────────────────
    # FASE 3: MANTENER — refrescar para que no expiren
    # ─────────────────────────────────────────────────────────
    maintain_s = 60
    maintain_iterations = maintain_s // interval_s

    print(f"\n🔄 FASE 3: Manteniendo {spike_flights} vuelos vivos ({maintain_s}s)")

    for i in range(maintain_iterations):
        # Enviar tanto los baseline como los spike para mantener todos activos
        send_batch(producer, spike_flights, offset=100)
        print(f"   [{i+1}/{maintain_iterations}] Refresh enviado")
        time.sleep(interval_s)

    print("\n" + "=" * 55)
    print("✅ Test completado. Comprueba:")
    print("   • Logs de Flink: docker compose logs -f density-alert")
    print("   • Kafka UI: http://localhost:8080 → topic 'flight-alerts'")
    print("=" * 55)


if __name__ == "__main__":
    main()
