import json
import time
from kafka import KafkaProducer

# Configuración de Kafka (usando KRaft)
KAFKA_SERVER = "localhost:9094"
KAFKA_TOPIC = "flight_data"

def send_event(producer, icao24, callsign, alt, vert_rate=0):
    """Envía un evento de vuelo simulado a Kafka."""
    flight_info = {
        "icao24": icao24,
        "callsign": callsign,
        "origin_country": "Mock Country",
        "longitude": -3.5,
        "latitude": 40.5,
        "altitude": alt,
        "on_ground": False,
        "velocity": 250,  # ~900 km/h
        "heading": 90,
        "vertical_rate": vert_rate
    }
    print(f"📡 Enviando {icao24} a {alt}m...")
    producer.send(KAFKA_TOPIC, flight_info)
    producer.flush()

def main():
    print(f"Conectando a Kafka en {KAFKA_SERVER}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("¡Conectado! Iniciando simulación de caída libre...\n")

    avion_id = "MOCK99"
    callsign = "TEST-FLIGHT"

    # --- PASO 1: Vuelo Normal ---
    print("--- PASO 1: Vuelo Normal ---")
    send_event(producer, avion_id, callsign, alt=10000, vert_rate=0)
    time.sleep(3)

    # --- PASO 2: Caída Crítica (Pierde 3500m) ---
    print("\n--- PASO 2: Caída Crítica (Debería lanzar alerta CRITICA - INICIO) ---")
    send_event(producer, avion_id, callsign, alt=6500, vert_rate=-50)
    time.sleep(3)

    # --- PASO 3: Sigue Cayendo (Pierde 1500m) ---
    print("\n--- PASO 3: Sigue Cayendo (Debería lanzar alerta MEDIA - ACTUALIZACION) ---")
    send_event(producer, avion_id, callsign, alt=5000, vert_rate=-20)
    time.sleep(3)

    # --- PASO 4: Vuelo Estabilizado (No pierde altura) ---
    print("\n--- PASO 4: Se estabiliza (Debería lanzar alerta BAJA - FIN) ---")
    send_event(producer, avion_id, callsign, alt=5000, vert_rate=0)
    time.sleep(3)
    
    print("\n✅ Simulación completada.")

if __name__ == "__main__":
    main()
