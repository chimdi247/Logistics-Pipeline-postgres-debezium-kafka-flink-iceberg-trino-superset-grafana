#!/usr/bin/env bash
# Install Docker Engine + Compose v2 and the host tools AtlasFlow needs on a fresh Ubuntu VM
# (22.04 / 24.04). Run as a normal user with sudo rights.
# Usage: install-docker-ubuntu.sh [--with-ufw]
set -Eeuo pipefail
# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

WITH_UFW=0
for arg in "$@"; do
  case "${arg}" in
    --with-ufw) WITH_UFW=1 ;;
    -h|--help) sed -n '2,5p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) die "Unknown option: ${arg}" ;;
  esac
done

[[ -r /etc/os-release ]] || die "Cannot detect the operating system."
# shellcheck disable=SC1091
. /etc/os-release
[[ "${ID}" == "ubuntu" ]] || die "This script supports Ubuntu only (detected: ${ID})."
SUDO=""
if [[ "${EUID}" -ne 0 ]]; then
  need_cmd sudo
  SUDO="sudo"
fi

mem_gb=$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)
disk_gb=$(df -BG --output=avail / | tail -n1 | tr -dc '0-9')
((mem_gb >= 14)) || log_warn "Only ~${mem_gb} GB RAM detected. The full stack (Flink, Trino, Superset, Kafka, Loki...) needs 16 GB or more."
((disk_gb >= 60)) || log_warn "Only ~${disk_gb} GB free on /. Plan for 60 GB or more (images, Kafka, Iceberg data, logs)."

log_info "Installing prerequisites..."
${SUDO} apt-get update -y
${SUDO} apt-get install -y --no-install-recommends ca-certificates curl gnupg jq git unzip python3 python3-venv openssl

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  log_pass "Docker and Compose are already installed: $(docker --version)"
else
  log_info "Adding the official Docker apt repository..."
  ${SUDO} install -m 0755 -d /etc/apt/keyrings
  ${SUDO} curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  ${SUDO} chmod a+r /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${UBUNTU_CODENAME:-${VERSION_CODENAME}} stable" |
    ${SUDO} tee /etc/apt/sources.list.d/docker.list >/dev/null
  ${SUDO} apt-get update -y
  ${SUDO} apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

${SUDO} systemctl enable --now docker
if [[ "${EUID}" -ne 0 ]] && ! id -nG "${USER}" | grep -qw docker; then
  ${SUDO} usermod -aG docker "${USER}"
  log_warn "Added ${USER} to the docker group. Log out and back in (or run 'newgrp docker') before using docker without sudo."
fi

if ((WITH_UFW)); then
  need_cmd ufw
  log_info "Configuring ufw: SSH only. Published container ports are bound to 127.0.0.1 by default."
  ${SUDO} ufw allow OpenSSH
  ${SUDO} ufw --force enable
  log_warn "Docker publishes ports via iptables and can bypass ufw. Keep BIND_ADDRESS=127.0.0.1 unless you also restrict access in your cloud firewall / security group."
fi

log_pass "Host is ready. Next: ./scripts/deploy.sh"
