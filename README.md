# opensky-project

## Descripción

Este repositorio incluye una topología Flink en `opensky-project` que procesa datos de vuelos desde Kafka, genera alertas verticales y de alta densidad, e integra todo en un clúster local con Docker.

## Estructura clave

- `Dockerfile` - Imagen base Flink con Python y la librería `h3` instalada.
- `docker-compose.yml` - Define los servicios Kafka, Kafka UI, JobManager, TaskManager y el job de densidad.
- `flink_app/vertical_detector.py` - Job Flink que detecta anomalías de ascenso/descenso brusco.
- `flink_app/density_alert.py` - Job Flink que detecta alta densidad de tráfico usando celdas H3.
- `producer/` - Contiene productores Python para enviar datos de vuelos a Kafka.

## Cómo levantar todo el sistema

1. Abre una terminal en `opensky-project`.
2. Ejecuta:

```bash
docker-compose up --build
```

Esto hará lo siguiente:

- **kafka**: arranca un broker Kafka en el puerto `9092` y expone `9094` para accesos externos.
- **kafka-ui**: arranca Kafka UI en `http://localhost:8080` para inspeccionar topics y datos.
- **jobmanager**: arranca el JobManager de Flink en `http://localhost:8081`.
- **taskmanager**: arranca el TaskManager de Flink para ejecutar los jobs.
- **density-alert**: construye la misma imagen Flink y envía el job `flink_app/density_alert.py` al clúster.

## Qué hace cada paso

- `docker-compose up --build`
  - Reconstruye la imagen Flink con las dependencias necesarias.
  - Arranca Kafka y su UI.
  - Inicia el clúster Flink.
  - Envía el job de alerta de densidad a Flink.

- `kafka` / `kafka-ui`
  - Kafka recibe eventos de vuelo en el topic `flight_data`.
  - Kafka UI permite ver topics, productores y consumidores.

- `jobmanager` / `taskmanager`
  - Ejecutan los jobs Flink.
  - El JobManager coordina las tareas.
  - El TaskManager procesa los datos y mantiene el estado.

- `density-alert`
  - Registro los eventos de vuelo desde `flight_data`.
  - Calcula la celda H3 de cada vuelo.
  - Cuenta vuelos activos por celda.
  - Genera alertas en `flight-alerts` cuando la densidad supera el umbral dinámico.

## Notas adicionales

- El job `density_alert.py` publica alertas en el topic Kafka `flight-alerts`.
- Si quieres también ejecutar el detector vertical, puedes añadir un servicio extra similar a `density-alert` apuntando a `flink_app/vertical_detector.py`.

## Ejecutar scripts Python

1. Instala las dependencias necesarias para los productores:

```bash
cd producer
pip install -r requirements.txt
```

2. Con Kafka arrancado desde `docker-compose up --build`, ejecuta uno de estos scripts:

- `python mock_anomaly_producer.py`
  - Envía datos de prueba al topic `flight_data`.
- `python mock_density_producer.py`
  - Envía muchos vuelos de prueba en la misma zona para generar una alerta de alta densidad.
- `python opensky_producer.py`
  - Envía datos reales desde la API de OpenSky (requiere `.env` con `OPENSKY_USERNAME` y `OPENSKY_PASSWORD` si usas autenticación).

Ambos productores se conectan a Kafka en `localhost:9094` y publican en `flight_data`.

## Ver alertas de densidad y de altura

- Abre Kafka UI en `http://localhost:8080`.
- Conecta al cluster `local` y selecciona el topic `flight-alerts`.
- En los mensajes verás el campo `tipo_alerta` con valores:
  - `alta_densidad` para alertas de densidad.
  - `vertical` para alertas de ascenso/descenso brusco.

También puedes usar un consumidor Kafka si prefieres ver mensajes en la terminal.

## Lanzar el detector vertical manualmente

Si quieres ejecutar `flink_app/density_alert.py` hazlo desde el JobManager de Flink:

```bash
docker-compose exec jobmanager bash
/opt/flink/bin/flink run -d -py /opt/flink/flink_app/density_alert.py
```

3.  Ver las alertas de densidad en consola:
    ```bash
    docker logs -f opensky-project-taskmanager-1
    ```

Esto añadirá el detector vertical al clúster y sus alertas también llegarán al topic `flight-alerts`.
