#!/usr/bin/env bash
# Bootstrap the Iceberg table for a stage and submit its single streaming Flink SQL job.
# Usage: submit-streaming-stage.sh <bronze|silver>
set -Eeuo pipefail
# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"
need_cmd curl jq

usage() { echo "Usage: $0 <bronze|silver>" >&2; exit 2; }
[[ $# -eq 1 ]] || usage
case "${1,,}" in
  bronze)
    STAGE="Bronze"; LAYER="bronze"; EXPECTED_JOB="atlasflow-bronze-cdc"
    TEMPLATE="01_cdc_to_bronze.sql.template"; BOOTSTRAP="bootstrap_bronze_tables.sql.template"
    TRANSFORM="01_cdc_to_bronze.sql"; BOOTSTRAP_RENDERED="bootstrap_bronze_tables.sql"
    LOG_PATH="/tmp/atlasflow-lite-bronze.log" ;;
  silver)
    STAGE="Silver"; LAYER="silver"; EXPECTED_JOB="atlasflow-silver-cdc-normalize"
    TEMPLATE="01_cdc_to_silver.sql.template"; BOOTSTRAP="bootstrap_silver_tables.sql.template"
    TRANSFORM="01_cdc_to_silver.sql"; BOOTSTRAP_RENDERED="bootstrap_silver_tables.sql"
    LOG_PATH="/tmp/atlasflow-lite-silver.log" ;;
  *) usage ;;
esac

TEMPLATE_DIR="${ROOT_DIR}/sql/${LAYER}"
RUNTIME_DIR="${RUNTIME_ROOT}/${LAYER}"

access_key="$(env_value RUSTFS_ACCESS_KEY)"
secret_key="$(env_value RUSTFS_SECRET_KEY)"
validate_object_store_keys "${access_key}" "${secret_key}"

if flink_job_running "${EXPECTED_JOB}"; then
  log_pass "${EXPECTED_JOB} is already RUNNING; not creating a duplicate."
  exit 0
fi

mkdir -p "${RUNTIME_DIR}"
bootstrap_path="${RUNTIME_DIR}/${BOOTSTRAP_RENDERED}"
transform_path="${RUNTIME_DIR}/${TRANSFORM}"
trap 'cleanup_files "${bootstrap_path}" "${transform_path}"' EXIT

render_template "${TEMPLATE_DIR}/${BOOTSTRAP}" "${bootstrap_path}" "${access_key}" "${secret_key}"
log_info "Bootstrapping the ${STAGE} Iceberg table..."
flink_sql_run "${LAYER}" "${BOOTSTRAP_RENDERED}" || die "${STAGE} Iceberg bootstrap failed. Inspect Flink and Iceberg REST logs."

render_template "${TEMPLATE_DIR}/${TEMPLATE}" "${transform_path}" "${access_key}" "${secret_key}"
log_info "Submitting the single ${STAGE} transformation: ${EXPECTED_JOB}"
flink_sql_submit_detached "${LAYER}" "${TRANSFORM}" "${LOG_PATH}" || die "Could not start the ${STAGE} SQL client."

if ! wait_for_flink_job "${EXPECTED_JOB}" 150; then
  log_warn "SQL client log (${LOG_PATH}):"
  flink_sql_log_tail "${LOG_PATH}" 100
  die "${EXPECTED_JOB} did not reach RUNNING state. Fix the first Flink SQL error in the log above."
fi
log_pass "${EXPECTED_JOB} is RUNNING."
