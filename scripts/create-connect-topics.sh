#!/usr/bin/env bash
# Create the single consolidated business CDC topic (host mode).
# In the one-command deployment this is done automatically by the `kafka-init` service.
set -Eeuo pipefail
# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

TOPIC="atlasflow.cdc.v1"

log_info "Creating the single business topic: ${TOPIC}"
compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --create --if-not-exists \
  --topic "${TOPIC}" --partitions 3 --replication-factor 1 \
  --config retention.ms=259200000 || die "Could not create Kafka topic ${TOPIC}."

compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list ||
  die "Could not list Kafka topics."
