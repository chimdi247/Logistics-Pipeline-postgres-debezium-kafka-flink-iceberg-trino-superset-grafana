#!/usr/bin/env bash
# Shared helpers for the AtlasFlow shell scripts. Source this file; do not execute it.
# shellcheck shell=bash
#
# Two execution modes (ATLASFLOW_MODE):
#   host       (default) Run by an operator on the Ubuntu VM. Talks to the stack
#                        through `docker compose exec` and the loopback-mapped ports.
#   container            Run inside the `pipeline-bootstrap` container, which shares the
#                        Flink JobManager's network namespace and talks to services by
#                        their Compose service names. No Docker socket is needed.

if [[ -n "${ATLASFLOW_COMMON_LOADED:-}" ]]; then
  return 0
fi
ATLASFLOW_COMMON_LOADED=1

COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${COMMON_DIR}/../.." && pwd)"

ATLASFLOW_MODE="${ATLASFLOW_MODE:-host}"
LOG_FORMAT="${LOG_FORMAT:-text}"        # text | json  (json is what Alloy ships to Loki)
LOG_NAME="${LOG_NAME:-$(basename "${0:-atlasflow}" .sh)}"

FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
TRINO_URL="${TRINO_URL:-http://localhost:18080}"

if [[ "${ATLASFLOW_MODE}" == "container" ]]; then
  RUNTIME_ROOT="${RUNTIME_ROOT:-/tmp/atlasflow-runtime}"
else
  RUNTIME_ROOT="${RUNTIME_ROOT:-${ROOT_DIR}/runtime}"
fi

# --------------------------------------------------------------------------- logging
if [[ -t 1 && "${LOG_FORMAT}" != "json" ]]; then
  C_RED=$'\033[31m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'; C_CYAN=$'\033[36m'; C_RESET=$'\033[0m'
else
  C_RED=""; C_GREEN=""; C_YELLOW=""; C_CYAN=""; C_RESET=""
fi

_json_escape() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/\\n}"
  s="${s//$'\r'/}"
  s="${s//$'\t'/\\t}"
  printf '%s' "$s"
}

_emit() { # level colour message
  local level="$1" colour="$2" msg="$3"
  if [[ "${LOG_FORMAT}" == "json" ]]; then
    printf '{"timestamp":"%s","level":"%s","logger":"%s","message":"%s"}\n' \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${level}" "${LOG_NAME}" "$(_json_escape "${msg}")"
  else
    printf '%s%s%s\n' "${colour}" "${msg}" "${C_RESET}"
  fi
}

log_info() { _emit info "${C_CYAN}" "$*"; }
log_pass() { _emit info "${C_GREEN}" "PASS: $*"; }
log_warn() { _emit warn "${C_YELLOW}" "WARN: $*" >&2; }
log_error() { _emit error "${C_RED}" "ERROR: $*" >&2; }
die() { log_error "$*"; exit 1; }

# --------------------------------------------------------------------------- generic helpers
need_cmd() {
  local cmd
  for cmd in "$@"; do
    command -v "${cmd}" >/dev/null 2>&1 || die "Required command '${cmd}' was not found on PATH."
  done
}

# env_value NAME -> prints the value from the process environment, else from ROOT/.env.
env_value() {
  local name="$1" value
  value="${!name:-}"
  if [[ -z "${value}" && -f "${ROOT_DIR}/.env" ]]; then
    value="$(grep -E "^${name}=" "${ROOT_DIR}/.env" | tail -n 1 | cut -d= -f2- | tr -d '\r' || true)"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    value="${value%\"}"; value="${value#\"}"
    value="${value%\'}"; value="${value#\'}"
  fi
  [[ -n "${value}" ]] || die "${name} is missing from .env. Run ./scripts/prepare-local-env.sh first."
  printf '%s' "${value}"
}

# env_value_optional NAME -> like env_value but prints nothing (and succeeds) when unset.
env_value_optional() {
  local name="$1" value
  value="${!name:-}"
  if [[ -z "${value}" && -f "${ROOT_DIR}/.env" ]]; then
    value="$(grep -E "^${name}=" "${ROOT_DIR}/.env" | tail -n 1 | cut -d= -f2- | tr -d '\r' || true)"
    value="${value%\"}"; value="${value#\"}"; value="${value%\'}"; value="${value#\'}"
  fi
  printf '%s' "${value}"
}

compose() {
  need_cmd docker
  (cd "${ROOT_DIR}" && docker compose "$@")
}

# wait_until TIMEOUT_SECONDS INTERVAL_SECONDS DESCRIPTION COMMAND [ARGS...]
wait_until() {
  local timeout="$1" interval="$2" description="$3"
  shift 3
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep "${interval}"
  done
  log_error "Timed out after ${timeout}s waiting for ${description}."
  return 1
}

http_ok() { curl -fsS --max-time 10 -o /dev/null "$1"; }

# --------------------------------------------------------------------------- SQL templates
validate_object_store_keys() {
  local key
  for key in "$@"; do
    [[ "${key}" =~ ^[A-Za-z0-9_-]{8,80}$ ]] ||
      die "RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY must each be 8-80 letters, digits, underscores, or hyphens."
  done
}

# render_template SOURCE TARGET ACCESS_KEY SECRET_KEY
render_template() {
  local source="$1" target="$2" access_key="$3" secret_key="$4" text
  [[ -f "${source}" ]] || die "SQL template not found: ${source}"
  text="$(cat "${source}")"
  text="${text//__RUSTFS_ACCESS_KEY__/${access_key}}"
  text="${text//__RUSTFS_SECRET_KEY__/${secret_key}}"
  if grep -Eq '__[A-Z0-9_]+__' <<<"${text}"; then
    die "Unresolved placeholders remain in ${source}."
  fi
  (umask 077 && printf '%s\n' "${text}" >"${target}")
}

# --------------------------------------------------------------------------- Flink
# flink_job_running JOB_NAME -> exit 0 when a job with that name is RUNNING
flink_job_running() {
  curl -fsS --max-time 10 "${FLINK_REST_URL}/jobs/overview" |
    jq -e --arg name "$1" '[.jobs[]? | select(.name == $name and .state == "RUNNING")] | length > 0' >/dev/null
}

flink_ready() { curl -fsS --max-time 10 "${FLINK_REST_URL}/overview" >/dev/null 2>&1; }

# flink_sql_run LAYER FILE -> run a (blocking) SQL script, used for DDL bootstrap
flink_sql_run() {
  local layer="$1" file="$2"
  if [[ "${ATLASFLOW_MODE}" == "container" ]]; then
    /opt/flink/bin/sql-client.sh -f "${RUNTIME_ROOT}/${layer}/${file}"
  else
    compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh -f "/opt/atlasflow/runtime/${layer}/${file}"
  fi
}

# flink_sql_submit_detached LAYER FILE LOGFILE -> start a streaming SQL job without blocking
flink_sql_submit_detached() {
  local layer="$1" file="$2" logfile="$3"
  if [[ "${ATLASFLOW_MODE}" == "container" ]]; then
    nohup /opt/flink/bin/sql-client.sh -f "${RUNTIME_ROOT}/${layer}/${file}" >"${logfile}" 2>&1 </dev/null &
  else
    compose exec -d -T flink-jobmanager /bin/sh -lc \
      "nohup /opt/flink/bin/sql-client.sh -f '/opt/atlasflow/runtime/${layer}/${file}' > '${logfile}' 2>&1 < /dev/null &"
  fi
}

flink_sql_log_tail() {
  local logfile="$1" lines="${2:-100}"
  if [[ "${ATLASFLOW_MODE}" == "container" ]]; then
    tail -n "${lines}" "${logfile}" 2>/dev/null || true
  else
    compose exec -T flink-jobmanager /bin/sh -lc "tail -n ${lines} '${logfile}'" 2>/dev/null || true
  fi
}

# wait_for_flink_job JOB_NAME TIMEOUT_SECONDS -> exit 0 once RUNNING
wait_for_flink_job() {
  wait_until "$2" 4 "Flink job $1 to be RUNNING" flink_job_running "$1"
}

# --------------------------------------------------------------------------- PostgreSQL
# psql_admin [psql args...] -> run psql as the admin role (stdin is passed through)
psql_admin() {
  local admin
  admin="$(env_value POSTGRES_USER)"
  if [[ "${ATLASFLOW_MODE}" == "container" ]]; then
    PGPASSWORD="$(env_value POSTGRES_PASSWORD)" psql -X -v ON_ERROR_STOP=1 \
      -h "${POSTGRES_HOST:-postgres}" -p "${POSTGRES_INTERNAL_PORT:-5432}" -U "${admin}" "$@"
  else
    compose exec -T postgres psql -X -v ON_ERROR_STOP=1 -U "${admin}" "$@"
  fi
}

cleanup_files() { # remove rendered SQL files that contain object-store credentials
  local path
  for path in "$@"; do
    if [[ -n "${path}" && -e "${path}" ]]; then
      rm -f "${path}"
    fi
  done
}
