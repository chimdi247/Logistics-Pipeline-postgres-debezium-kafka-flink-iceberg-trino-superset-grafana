#!/usr/bin/env bash
# Create the keyed Gold Iceberg table and submit the continuous Gold control-tower stream.
set -Eeuo pipefail
# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"
need_cmd curl jq

GOLD_JOB="atlasflow-gold-control-tower-stream"
REQUIRED_JOB="atlasflow-silver-cdc-normalize"
GOLD_LOG="/tmp/atlasflow-lite-gold-stream.log"

access_key="$(env_value RUSTFS_ACCESS_KEY)"
secret_key="$(env_value RUSTFS_SECRET_KEY)"
validate_object_store_keys "${access_key}" "${secret_key}"

if flink_job_running "${GOLD_JOB}"; then
  log_pass "Gold streaming job is already RUNNING; not creating a duplicate."
  exit 0
fi
flink_job_running "${REQUIRED_JOB}" || die "Start Silver first. Required RUNNING jobs missing: ${REQUIRED_JOB}"

RUNTIME_DIR="${RUNTIME_ROOT}/gold"
GOLD_DIR="${ROOT_DIR}/sql/gold"
mkdir -p "${RUNTIME_DIR}"
bootstrap_path="${RUNTIME_DIR}/bootstrap_gold_tables.sql"
stream_path="${RUNTIME_DIR}/gold_control_tower_stream.sql"
trap 'cleanup_files "${bootstrap_path}" "${stream_path}"' EXIT

render_template "${GOLD_DIR}/bootstrap_gold_tables.sql.template" "${bootstrap_path}" "${access_key}" "${secret_key}"
log_info "Creating the keyed Gold control-tower table..."
flink_sql_run "gold" "bootstrap_gold_tables.sql" || die "Gold upsert table bootstrap failed."

render_template "${GOLD_DIR}/stream_shipment_control_tower.sql.template" "${stream_path}" "${access_key}" "${secret_key}"
log_info "Submitting ${GOLD_JOB}..."
flink_sql_submit_detached "gold" "gold_control_tower_stream.sql" "${GOLD_LOG}" || die "Could not submit the Gold streaming query."

if ! wait_for_flink_job "${GOLD_JOB}" 150; then
  flink_sql_log_tail "${GOLD_LOG}" 120
  die "Gold streaming job did not reach RUNNING state. Fix the first Flink SQL error in the log above."
fi
log_pass "${GOLD_JOB} is RUNNING."
log_info "Gold will keep updating as CDC changes arrive. Run the Trino query pack to verify results."
