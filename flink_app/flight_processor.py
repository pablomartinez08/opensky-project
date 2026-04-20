import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, FilterFunction
class ParseJsonFunction(MapFunction):
    """Parsea el String de Kafka a un Diccionario en Python"""
    def map(self, value):
        try:
            return json.loads(value)
        except Exception:
            return None
class HighAltitudeFilter(FilterFunction):
    """Filtra los vuelos que estén por encima de 10.000 metros"""
    def filter(self, value):
        if value is None:
            return False
        alt = value.get("altitude")
        return alt is not None and alt > 10000
class FlightFormatter(MapFunction):
    """Formatea la salida para imprimirla de manera legible"""
    def map(self, flight):
        callsign = flight.get("callsign", "N/A")
        country = flight.get("origin_country", "N/A")
        altitude = flight.get("altitude", 0)
        velocity = flight.get("velocity", 0)
        
        # Ojo: la API de OpenSky devuelve velocidad en m/s. Transformamos a km/h
        vel_kmh = velocity * 3.6 if velocity else 0
        
        return f"✈️ [ALTA ALTITUD] Vuelo {callsign} ({country}) detectado a {altitude}m de altura. Velocidad: {vel_kmh:.1f} km/h"
def main():
    # 1. Configurar Entorno de Flink
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # 1.1 Registrar el conector JAR de Kafka (lo descargamos en el Dockerfile y lo guardamos en /opt/flink/lib)
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar")
    
    # 2. Configurar la fuente de datos (Kafka Consumer)
    properties = {
        'bootstrap.servers': 'kafka:9092', # Usamos el host interno de kafka de la red de docker
        'group.id': 'flink-flight-consumer',
        'auto.offset.reset': 'latest'
    }
    
    # SimpleStringSchema lee los eventos en crudo (strings JSON)
    kafka_consumer = FlinkKafkaConsumer(
        topics='flight_data',
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )
    
    print("Iniciando aplicación Flink de análisis de vuelos de OpenSky...")
    # 3. Flujo principal del programa
    stream = env.add_source(kafka_consumer)
    
    parsed_stream = stream.map(ParseJsonFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
    
    filtered_stream = parsed_stream.filter(HighAltitudeFilter())
    
    output_stream = filtered_stream.map(FlightFormatter(), output_type=Types.STRING())
    
    # 4. Sumidero (Sink) de los datos: Imprimir en pantalla
    output_stream.print()
    # 5. Ejecutar la topología en el clúster (JobManager)
    env.execute("OpenSky Real-Time Flight Tracker")
if __name__ == '__main__':
    main()