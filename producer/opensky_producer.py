import os
import json
import time
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

# OpenSky API URL
OPENSKY_URL = "https://opensky-network.org/api/states/all"
KAFKA_TOPIC = "flight_data"
# Apuntamos al puerto de Kafka externo al docker (KRaft listener)
KAFKA_SERVER = "localhost:9094"

# ════════════════════════════════════════════════════════════════
# 🔑 CREDENCIALES DE OPENSKY LOCALES (cargadas desde un archivo .env)
# ════════════════════════════════════════════════════════════════
OPENSKY_USERNAME = os.getenv("OPENSKY_USERNAME")
OPENSKY_PASSWORD = os.getenv("OPENSKY_PASSWORD")

def main():
    print(f"Conectando a Kafka en {KAFKA_SERVER}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("¡Conectado! Obteniendo datos de OpenSky...")

    while True:
        try:
            # Reducimos los datos a la Península Ibérica para no saturar al servidor gratis
            url = f"{OPENSKY_URL}?lamin=35.0&lomin=-10.0&lamax=44.0&lomax=5.0"
            headers = {'User-Agent': 'Mozilla/5.0'}
            
            if OPENSKY_USERNAME and OPENSKY_PASSWORD:
                response = requests.get(url, headers=headers, auth=(OPENSKY_USERNAME, OPENSKY_PASSWORD), timeout=10)
            else:
                response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                states = data.get("states", [])
                
                if states:
                    print(f"Obtenidos {len(states)} vuelos. Enviando a Kafka...")
                    for state in states:
                        # Formato del vector de estado de OpenSky:
                        # 0: icao24, 1: callsign, 2: origin_country, 5: longitude, 6: latitude,
                        # 7: baro_altitude, 8: on_ground, 9: velocity, 10: true_track, 11: vertical_rate
                        flight_info = {
                            "icao24": state[0],
                            "callsign": state[1].strip() if state[1] else "UNKNOWN",
                            "origin_country": state[2],
                            "longitude": state[5],
                            "latitude": state[6],
                            "altitude": state[7],
                            "on_ground": state[8],
                            "velocity": state[9],
                            "heading": state[10],
                            "vertical_rate": state[11]
                        }
                        producer.send(KAFKA_TOPIC, flight_info)
                    
                    producer.flush()
                else:
                    print("No se encontraron vuelos en esta zona.")
            else:
                print(f"Error HTTP al obtener datos: {response.status_code}")
                
        except Exception as e:
            print(f"Error de conexión: {e}")
            
        # La API pública de OpenSky suele tirar 429 si detecta bots. 15s es más seguro.
        print("Esperando 15 segundos para la siguiente petición...")
        time.sleep(15)

if __name__ == "__main__":
    main()
