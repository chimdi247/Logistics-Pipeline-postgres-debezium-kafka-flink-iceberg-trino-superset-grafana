#!/usr/bin/env bash
# One-command deployment for an Ubuntu VM: prepare .env, build, start the whole stack and verify it.
# Usage: deploy.sh [--no-verify]
set -Eeuo pipefail
# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"
need_cmd docker

VERIFY=1
for arg in "$@"; do
  case "${arg}" in
    --no-verify) VERIFY=0 ;;
    -h|--help) sed -n '2,3p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) die "Unknown option: ${arg}" ;;
  esac
done

docker compose version >/dev/null 2>&1 || die "Docker Compose v2 is required (docker compose ...). Run scripts/install-docker-ubuntu.sh."
"${ROOT_DIR}/scripts/prepare-local-env.sh"

mkdir -p "${ROOT_DIR}"/runtime/{bronze,silver,gold}
log_info "Building images and starting the stack. The first start takes several minutes (image pulls, Flink jar download, Trino warm-up, pipeline bootstrap)..."
compose up -d --build

if ((VERIFY)); then
  log_info "Running the smoke test..."
  "${ROOT_DIR}/.venv/bin/python" "${ROOT_DIR}/scripts/smoke-test-atlasflow.py" --retries 5 ||
    log_warn "Smoke test reported failures. Inspect: docker compose ps && docker compose logs <service>"
fi
log_pass "Deployment finished. Open the UIs through an SSH tunnel; see DEPLOY-UBUNTU.md."
