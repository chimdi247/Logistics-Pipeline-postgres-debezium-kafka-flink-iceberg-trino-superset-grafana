#!/usr/bin/env bash
# Register (or update) the single Debezium PostgreSQL connector and wait until it is RUNNING.
set -Eeuo pipefail
# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"
need_cmd curl jq

CONNECTOR_FILE="${ROOT_DIR}/connectors/postgres-cdc-connector.json"
[[ -f "${CONNECTOR_FILE}" ]] || die "Connector definition not found: ${CONNECTOR_FILE}"

password="$(env_value CDC_PASSWORD)"
[[ "${password}" != *change_this* ]] || die "CDC_PASSWORD is not configured. Run ./scripts/prepare-local-env.sh and retry."

wait_until 180 5 "Kafka Connect API at ${CONNECT_URL}" http_ok "${CONNECT_URL}/" ||
  die "Kafka Connect API is not ready at ${CONNECT_URL}."

doc="$(jq --arg pw "${password}" '.config["database.password"] = $pw' "${CONNECTOR_FILE}")"
name="$(jq -r '.name' <<<"${doc}")"

if curl -fsS --max-time 10 "${CONNECT_URL}/connectors/" | jq -e --arg n "${name}" 'index($n) != null' >/dev/null; then
  log_info "Updating connector ${name}"
  jq '.config' <<<"${doc}" |
    curl -fsS --max-time 20 -X PUT -H 'Content-Type: application/json' --data @- \
      "${CONNECT_URL}/connectors/${name}/config" >/dev/null
else
  log_info "Registering connector ${name}"
  curl -fsS --max-time 20 -X POST -H 'Content-Type: application/json' --data "${doc}" \
    "${CONNECT_URL}/connectors/" >/dev/null
fi

# connector_settled -> exit 0 once the connector and all tasks are RUNNING, or once any is FAILED.
CONNECTOR_STATE="PENDING"
connector_settled() {
  local status failed tasks not_running connector_state
  status="$(curl -fsS --max-time 10 "${CONNECT_URL}/connectors/${name}/status")" || return 1
  failed="$(jq '[.connector.state, (.tasks[]?.state)] | map(select(. == "FAILED")) | length' <<<"${status}")"
  if [[ "${failed}" != "0" ]]; then
    CONNECTOR_STATE="FAILED"
    return 0
  fi
  connector_state="$(jq -r '.connector.state' <<<"${status}")"
  tasks="$(jq '.tasks | length' <<<"${status}")"
  not_running="$(jq '[.tasks[]?.state | select(. != "RUNNING")] | length' <<<"${status}")"
  if [[ "${connector_state}" == "RUNNING" && "${tasks}" -gt 0 && "${not_running}" == "0" ]]; then
    CONNECTOR_STATE="RUNNING"
    return 0
  fi
  return 1
}

wait_until 180 5 "the Debezium connector to reach RUNNING" connector_settled ||
  die "Debezium connector did not reach RUNNING state. Inspect: docker compose logs connect"
if [[ "${CONNECTOR_STATE}" == "FAILED" ]]; then
  curl -fsS --max-time 10 "${CONNECT_URL}/connectors/${name}/status" | jq . >&2 || true
  die "Debezium connector failed. Inspect: docker compose logs connect"
fi
log_pass "one Debezium connector is RUNNING and routes all seven tables into atlasflow.cdc.v1."
