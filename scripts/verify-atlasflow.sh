#!/usr/bin/env bash
# Static validation, Compose validation and the live smoke test.
# Usage: verify-atlasflow.sh [--static-only]
set -Eeuo pipefail
# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

STATIC_ONLY=0
for arg in "$@"; do
  case "${arg}" in
    --static-only|-StaticOnly) STATIC_ONLY=1 ;;
    -h|--help) sed -n '2,3p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) die "Unknown option: ${arg}" ;;
  esac
done

cd "${ROOT_DIR}"
if [[ ! -f .env || ! -x .venv/bin/python ]]; then
  "${ROOT_DIR}/scripts/prepare-local-env.sh"
fi
PYTHON="${ROOT_DIR}/.venv/bin/python"

"${PYTHON}" scripts/check-atlasflow-static.py || die "Static validation failed."
if ((STATIC_ONLY)); then
  log_pass "Static-only verification complete."
  exit 0
fi

compose config --quiet || die "docker compose config failed."
"${PYTHON}" scripts/smoke-test-atlasflow.py || die "Live smoke test failed; inspect the first failing service."
log_pass "AtlasFlow verification passed."
