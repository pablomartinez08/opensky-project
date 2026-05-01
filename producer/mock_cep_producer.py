"""
Mock para probar el job UC-04 CEP.

Este script NO envia vuelos crudos a 'flight_data'. Envia alertas precursoras
directamente a 'flight-alerts' para que flink_app/cep_alert_correlator.py las
correlacione y emita una alerta CEP derivada.

Escenarios:
  1) zona_muerta + alta_densidad en la misma celda H3:
     simula trafico que desaparece de una zona y reaparece concentrado cerca.
     Resultado esperado: 'cep_zona_problematica'.
  2) vertical con descenso fuerte y altitud final 0:
     simula posible aterrizaje de emergencia.
     Resultado esperado: 'cep_aterrizaje_emergencia'.
"""

import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer


KAFKA_SERVER = "localhost:9094"
TOPIC = "flight-alerts"

TEST_CELL = "841f91fffffffff"
TEST_LAT = 40.4167
TEST_LNG = -3.7037


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def send_alert(producer, alert):
    producer.send(TOPIC, alert)
    producer.flush()
    print(
        f"Enviada alerta precursora: {alert['tipo_alerta']} | "
        f"{alert['estado']} | {alert['severidad']}"
    )


def base_alert(tipo, severidad="alta", estado="inicio"):
    return {
        "tipo_alerta": tipo,
        "estado": estado,
        "severidad": severidad,
        "timestamp": utc_now(),
        "latitud": TEST_LAT,
        "longitud": TEST_LNG,
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
        "celda_h3": None,
        "num_vuelos_actual": None,
        "num_vuelos_previo": None,
        "vuelos_afectados": None,
        "ventana_inicio": None,
        "ventana_fin": None,
        "patron": None,
        "altitud_inicio_patron": None,
        "altitud_final": None,
        "duracion_vel_cero_seg": None,
    }


def dead_zone_alert():
    alert = base_alert("zona_muerta", severidad="alta")
    alert.update(
        {
            "celda_h3": TEST_CELL,
            "num_vuelos_actual": 0,
            "num_vuelos_previo": 14,
            "ventana_inicio": utc_now(),
            "ventana_fin": utc_now(),
        }
    )
    return alert


def high_density_alert():
    alert = base_alert("alta_densidad", severidad="alta")
    alert.update(
        {
            "celda_h3": TEST_CELL,
            "num_vuelos_actual": 28,
            "vuelos_afectados": ["CEP001", "CEP002", "CEP003", "CEP004"],
            "ventana_inicio": utc_now(),
            "ventana_fin": utc_now(),
            "threshold_dinamico": 8.0,
        }
    )
    return alert


def vertical_emergency_alert():
    alert = base_alert("vertical", severidad="critica")
    alert.update(
        {
            "origen_pais": "Mock Country",
            "en_tierra": True,
            "icao24": "cep999",
            "callsign": "CEP-TEST",
            "altitud_actual": 0,
            "altitud_previa": 1400,
            "delta_altitud": -1400,
            "velocidad_vertical_ms": -45,
            "velocidad_kmh": 0.0,
            "heading": 180,
            "celda_h3": None,
        }
    )
    return alert


def main():
    print(f"Conectando a Kafka en {KAFKA_SERVER}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print("Conectado. Enviando escenarios CEP al topic flight-alerts.")

    print("\n--- ESCENARIO 1: zona_muerta + alta_densidad ---")
    send_alert(producer, dead_zone_alert())
    time.sleep(2)
    send_alert(producer, high_density_alert())

    print("\n--- ESCENARIO 2: vertical + altitud cero ---")
    time.sleep(2)
    send_alert(producer, vertical_emergency_alert())

    print("\nListo. Busca alertas nuevas tipo 'cep_zona_problematica' y 'cep_aterrizaje_emergencia'.")


if __name__ == "__main__":
    main()
