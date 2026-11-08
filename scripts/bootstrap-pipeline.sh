#!/usr/bin/env bash
# One-shot, idempotent pipeline bootstrap. Runs inside the `pipeline-bootstrap` container
# (ATLASFLOW_MODE=container) on every `docker compose up`, replacing the manual runbook steps 2-6:
#
#   1. PostgreSQL publication + monitoring role      (setup-cdc-publications.sh)
#   2. Bronze Flink job                              (submit-streaming-stage.sh bronze)
#   3. Debezium connector                            (register-debezium-connectors.sh)
#   4. Wait for the initial snapshot to reach Bronze (wait-for-cdc-snapshot.py)
#   5. Silver Flink job + wait for the snapshot      (submit-streaming-stage.sh silver)
#   6. Gold streaming job                            (start-gold-stream.sh)
#
# The Kafka topic is created beforehand by the `kafka-init` service. Jobs that are already RUNNING
# are left untouched, so re-running this script never creates duplicates.
set -Eeuo pipefail
export ATLASFLOW_MODE="${ATLASFLOW_MODE:-container}"
export LOG_NAME="pipeline-bootstrap"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"
need_cmd curl jq psql python3

SNAPSHOT_MIN_ROWS="${SNAPSHOT_MIN_ROWS:-34}"
SNAPSHOT_TIMEOUT="${SNAPSHOT_TIMEOUT:-420}"

step() { log_info "[$1/6] $2"; }

on_error() {
  local code=$? line=$1
  log_error "Bootstrap failed at line ${line} (exit ${code}). Inspect: docker compose logs pipeline-bootstrap"
  exit "${code}"
}
trap 'on_error ${LINENO}' ERR

log_info "Waiting for dependencies (PostgreSQL, Flink, Kafka Connect, Trino)..."
wait_until 300 3 "PostgreSQL" pg_isready -q -h "${POSTGRES_HOST:-postgres}" -p "${POSTGRES_INTERNAL_PORT:-5432}"
wait_until 300 3 "Flink JobManager REST API" flink_ready
wait_until 300 3 "Kafka Connect REST API" http_ok "${CONNECT_URL}/"
wait_until 300 3 "Trino" http_ok "${TRINO_URL}/v1/info"

step 1 "Preparing the PostgreSQL CDC publication"
"${SCRIPT_DIR}/setup-cdc-publications.sh"

if ! flink_job_running "atlasflow-bronze-cdc" && ! flink_job_running "atlasflow-silver-cdc-normalize" \
   && [[ "$(curl -fsS --max-time 10 "${FLINK_REST_URL}/jobs/overview" | jq '.jobs | length')" == "0" ]]; then
  if python3 "${SCRIPT_DIR}/wait-for-cdc-snapshot.py" --url "${TRINO_URL}" --table atlasflow.bronze.cdc_events \
       --minimum-rows 1 --timeout 5 >/dev/null 2>&1; then
    log_warn "Flink has no running jobs but Bronze already holds data (restart/recovery). Jobs are resubmitted" \
             "from the earliest Kafka offset, so Bronze/Silver may receive duplicate history rows. Gold stays correct" \
             "because it is keyed by shipment_id."
  fi
fi

step 2 "Starting the Bronze Flink job (before Debezium, so the consumer is ready for the snapshot)"
"${SCRIPT_DIR}/submit-streaming-stage.sh" bronze

step 3 "Registering the Debezium connector"
"${SCRIPT_DIR}/register-debezium-connectors.sh"

step 4 "Waiting for the initial CDC snapshot to reach Bronze"
python3 "${SCRIPT_DIR}/wait-for-cdc-snapshot.py" --url "${TRINO_URL}" --table atlasflow.bronze.cdc_events \
  --minimum-rows "${SNAPSHOT_MIN_ROWS}" --timeout "${SNAPSHOT_TIMEOUT}"

step 5 "Starting the Silver Flink job"
"${SCRIPT_DIR}/submit-streaming-stage.sh" silver
python3 "${SCRIPT_DIR}/wait-for-cdc-snapshot.py" --url "${TRINO_URL}" --table atlasflow.silver.cdc_changes \
  --minimum-rows "${SNAPSHOT_MIN_ROWS}" --timeout "${SNAPSHOT_TIMEOUT}"

step 6 "Starting the Gold streaming Flink job"
"${SCRIPT_DIR}/start-gold-stream.sh"

log_pass "AtlasFlow pipeline is up: bronze, silver and gold Flink jobs are RUNNING and Debezium is capturing changes."
