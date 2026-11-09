#!/usr/bin/env bash
# Generate .env from .env.example (preserving existing non-placeholder secrets) and, optionally,
# create the Python virtual environment used by the host-side helper scripts.
# Usage: prepare-local-env.sh [--skip-venv]
set -Eeuo pipefail
# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

SKIP_VENV=0
for arg in "$@"; do
  case "${arg}" in
    --skip-venv) SKIP_VENV=1 ;;
    -h|--help) sed -n '2,5p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) die "Unknown option: ${arg}" ;;
  esac
done

cd "${ROOT_DIR}"
EXAMPLE_PATH="${ROOT_DIR}/.env.example"
ENV_PATH="${ROOT_DIR}/.env"
[[ -f "${EXAMPLE_PATH}" ]] || die ".env.example is missing from the project root."

SECRET_KEYS=" POSTGRES_PASSWORD CDC_PASSWORD POSTGRES_EXPORTER_PASSWORD RUSTFS_ACCESS_KEY RUSTFS_SECRET_KEY SUPERSET_DB_PASSWORD SUPERSET_SECRET_KEY SUPERSET_ADMIN_PASSWORD GRAFANA_ADMIN_PASSWORD "

random_hex() {
  local bytes="${1:-24}"
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex "${bytes}"
  else
    head -c "${bytes}" /dev/urandom | od -An -tx1 | tr -d ' \n'
  fi
}

declare -A existing=()
if [[ -f "${ENV_PATH}" ]]; then
  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line%$'\r'}"
    if [[ "${line}" =~ ^[[:space:]]*([A-Z0-9_]+)=(.*)$ ]]; then
      existing["${BASH_REMATCH[1]}"]="${BASH_REMATCH[2]}"
    fi
  done <"${ENV_PATH}"
fi

tmp="$(mktemp)"
trap 'rm -f "${tmp}"' EXIT
while IFS= read -r line || [[ -n "${line}" ]]; do
  line="${line%$'\r'}"
  if [[ ! "${line}" =~ ^([A-Z0-9_]+)=(.*)$ ]]; then
    printf '%s\n' "${line}" >>"${tmp}"
    continue
  fi
  key="${BASH_REMATCH[1]}"
  default="${BASH_REMATCH[2]}"
  value="${existing[${key}]-${default}}"
  if [[ -z "${value// /}" || "${value}" == change_this_* ]]; then
    if [[ "${SECRET_KEYS}" == *" ${key} "* ]]; then
      value="$(random_hex 24)"
    else
      value="${default}"
    fi
  fi
  # SQL templates inject the object-store keys directly, so require simple safe characters.
  if [[ ( "${key}" == "RUSTFS_ACCESS_KEY" || "${key}" == "RUSTFS_SECRET_KEY" ) && ! "${value}" =~ ^[A-Za-z0-9_-]{8,80}$ ]]; then
    value="$(random_hex 24)"
  fi
  printf '%s=%s\n' "${key}" "${value}" >>"${tmp}"
done <"${EXAMPLE_PATH}"

install -m 600 "${tmp}" "${ENV_PATH}"
log_pass "local .env is ready (mode 600). Existing non-placeholder secrets were preserved."

if ((SKIP_VENV)); then
  log_info "Skipping Python virtual environment (--skip-venv)."
else
  command -v python3 >/dev/null 2>&1 || die "python3 (3.10+) was not found on PATH. Install it with: sudo apt-get install -y python3 python3-venv"
  if [[ ! -x "${ROOT_DIR}/.venv/bin/python" ]]; then
    python3 -m venv "${ROOT_DIR}/.venv" ||
      die "Could not create .venv. On Ubuntu run: sudo apt-get install -y python3-venv"
  fi
  "${ROOT_DIR}/.venv/bin/python" -m pip install --disable-pip-version-check -q -r "${ROOT_DIR}/requirements.txt" ||
    die "Python dependency installation failed."
  log_pass "Python environment and required dependencies are ready."
fi
log_info "Next command: docker compose up -d --build"
log_warn "Keep .env private; do not commit or share it."
