# Flink 2.2.0 + Iceberg 1.12.0 + Kafka SQL connector 5.0.0-2.2
# Keep connector/runtime jars directly in /opt/flink/lib so Flink loads their factories.
#
# Two build targets share one jar layer:
#   runtime   (default for jobmanager/taskmanager) the Flink image used by the cluster
#   bootstrap the same image plus jq, psql and python3, used by the one-shot pipeline-bootstrap
#             service so it can run the Flink SQL client against the JobManager
FROM apache/flink:2.2.0-scala_2.12-java17 AS runtime

USER root
RUN mkdir -p /opt/flink/lib \
    && chown -R flink:flink /opt/flink/lib
# The Prometheus reporter plugin ships with the base image; it is switched on through FLINK_PROPERTIES
# (metrics.reporter.prom.*) in compose.yaml and scraped by Prometheus on :9249.

USER flink
WORKDIR /opt/flink

ARG ICEBERG_VERSION=1.12.0
ARG HADOOP_VERSION=3.4.2
ARG FLINK_KAFKA_CONNECTOR_VERSION=5.0.0-2.2
ARG KAFKA_CLIENTS_VERSION=4.2.0

RUN set -eux; \
    curl -fL --retry 3 --retry-delay 2 \
      "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.2/${ICEBERG_VERSION}/iceberg-flink-runtime-2.2-${ICEBERG_VERSION}.jar" \
      -o "/opt/flink/lib/iceberg-flink-runtime-2.2-${ICEBERG_VERSION}.jar"; \
    curl -fL --retry 3 --retry-delay 2 \
      "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      -o "/opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fL --retry 3 --retry-delay 2 \
      "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar" \
      -o "/opt/flink/lib/hadoop-client-api-${HADOOP_VERSION}.jar"; \
    curl -fL --retry 3 --retry-delay 2 \
      "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar" \
      -o "/opt/flink/lib/hadoop-client-runtime-${HADOOP_VERSION}.jar"; \
    curl -fL --retry 3 --retry-delay 2 \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar" \
      -o "/opt/flink/lib/flink-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar"; \
    curl -fL --retry 3 --retry-delay 2 \
      "https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      -o "/opt/flink/lib/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"

FROM runtime AS bootstrap
USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends jq postgresql-client python3 ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    mkdir -p /opt/atlasflow && chown flink:flink /opt/atlasflow
USER flink
WORKDIR /opt/atlasflow
