#!/bin/sh
# Container entrypoint for the data generator.
#   1. waits for PostgreSQL
#   2. runs the (idempotent) fault-bundle scenario once so the control tower has something to flag
#   3. loops baseline batches forever (continuous mode)
set -eu

log() { # level message  -> one JSON line on stdout (shipped to Loki by Alloy)
  printf '{"timestamp":"%s","level":"%s","logger":"generator-entrypoint","message":"%s"}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" "$2"
}

if [ "${GENERATOR_ENABLED:-true}" != "true" ]; then
  log info "GENERATOR_ENABLED is not true; generator is idle. Stop this service or set it to true and recreate it."
  exec sleep infinity
fi

GEN="python /app/generators/generate_logistics_data.py"
COUNT="${GENERATOR_BATCH_COUNT:-4}"
INTERVAL="${GENERATOR_INTERVAL_SECONDS:-30}"

log info "waiting for PostgreSQL at ${POSTGRES_HOST:-postgres}:${POSTGRES_PORT:-5432}"
attempt=0
until python - <<'PY'
import os, sys, psycopg
try:
    psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname="atlasflow_ops",
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=5,
    ).close()
except Exception:
    sys.exit(1)
PY
do
  attempt=$((attempt + 1))
  if [ "${attempt}" -ge 60 ]; then
    log error "PostgreSQL did not become reachable after 60 attempts"
    exit 1
  fi
  sleep 5
done

log info "running the fault-bundle scenario once (idempotent)"
${GEN} --scenario fault-bundle

log info "starting continuous baseline generation: ${COUNT} events every ${INTERVAL}s"
exec ${GEN} --scenario baseline --continuous --count "${COUNT}" --interval-seconds "${INTERVAL}" --seed 7
