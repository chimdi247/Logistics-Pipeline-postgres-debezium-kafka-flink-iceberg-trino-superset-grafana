#!/usr/bin/env bash
# Install a systemd unit that runs `docker compose up -d` at boot (re-running the idempotent
# pipeline bootstrap so the Flink streaming jobs are restored after a VM reboot).
# Usage: install-systemd-service.sh
set -Eeuo pipefail
# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

SUDO=""
[[ "${EUID}" -eq 0 ]] || SUDO="sudo"
UNIT_PATH="/etc/systemd/system/atlasflow.service"

[[ -f "${ROOT_DIR}/.env" ]] || die ".env is missing. Run ./scripts/prepare-local-env.sh first."
sed "s#__ROOT_DIR__#${ROOT_DIR}#g" "${ROOT_DIR}/deploy/atlasflow.service.template" | ${SUDO} tee "${UNIT_PATH}" >/dev/null
${SUDO} systemctl daemon-reload
${SUDO} systemctl enable atlasflow.service
log_pass "atlasflow.service installed and enabled (${UNIT_PATH})."
log_info "Start it now with: sudo systemctl start atlasflow.service"
