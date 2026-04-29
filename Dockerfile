FROM flink:1.18.1-scala_2.12

# Instalar Python 3.10 y utilidades
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev wget && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

# Instalar apache-flink versión 1.18.1 y las dependencias Python necesarias
RUN pip3 install apache-flink==1.18.1 h3

# Descargar el conector de Kafka para Flink
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar
