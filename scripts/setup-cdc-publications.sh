#!/usr/bin/env bash
# Create/validate the PostgreSQL publication used by Debezium and the read-only monitoring role
# used by postgres-exporter. Idempotent: safe to run on every start.
set -Eeuo pipefail
# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

PUBLICATION="atlasflow_publication"
TABLES="public.customers, public.orders, public.inventory_balances, public.carriers, public.shipments, public.tracking_events, public.disruptions"
EXPECTED=$'public.carriers\npublic.customers\npublic.disruptions\npublic.inventory_balances\npublic.orders\npublic.shipments\npublic.tracking_events'

exists="$(psql_admin -qAt -d atlasflow_ops -c \
  "SELECT CASE WHEN EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '${PUBLICATION}') THEN 'yes' ELSE 'no' END;")" ||
  die "Could not inspect the CDC publication. Confirm PostgreSQL is healthy."

if [[ "${exists//[[:space:]]/}" == "no" ]]; then
  psql_admin -d atlasflow_ops -c "CREATE PUBLICATION ${PUBLICATION} FOR TABLE ${TABLES};" ||
    die "Could not create the PostgreSQL publication."
fi

actual="$(psql_admin -qAt -d atlasflow_ops -c \
  "SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = '${PUBLICATION}' ORDER BY 1;")" ||
  die "Could not verify the publication table list."
actual="$(printf '%s\n' "${actual}" | tr -d '\r' | sed '/^[[:space:]]*$/d' | sort)"
if [[ "${actual}" != "${EXPECTED}" ]]; then
  die "Publication has an unexpected table list. Expected seven simplified tables; found: $(tr '\n' ',' <<<"${actual}")"
fi
log_pass "one PostgreSQL publication covers exactly the seven operational tables."

# Least-privilege role for postgres-exporter (pg_monitor is read-only). Created here instead of in
# the init scripts so it also works on a PostgreSQL volume that already exists.
monitor_password="$(env_value_optional POSTGRES_EXPORTER_PASSWORD)"
if [[ -n "${monitor_password}" && ! "${monitor_password}" =~ ^[A-Za-z0-9_-]+$ ]]; then
  die "POSTGRES_EXPORTER_PASSWORD may only contain letters, digits, underscores, or hyphens."
fi
if [[ -n "${monitor_password}" ]]; then
  psql_admin -d postgres -v "monitor_password=${monitor_password}" <<'SQL' >/dev/null
SELECT format('CREATE ROLE atlasflow_monitor LOGIN PASSWORD %L', :'monitor_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'atlasflow_monitor') \gexec
SELECT format('ALTER ROLE atlasflow_monitor PASSWORD %L', :'monitor_password') \gexec
GRANT pg_monitor TO atlasflow_monitor;
SQL
  log_pass "monitoring role atlasflow_monitor is ready for postgres-exporter."
else
  log_warn "POSTGRES_EXPORTER_PASSWORD is not set; postgres-exporter metrics will be unavailable."
fi
