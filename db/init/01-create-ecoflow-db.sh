#!/bin/bash
set -euo pipefail

PRIMARY_DB=${POSTGRES_DB:-postgres}
TARGET_DB=${TARGET_DB_NAME:-ecoflow_raw_events}
TARGET_OWNER=${TARGET_DB_OWNER:-${POSTGRES_USER:-postgres}}

psql_base=(psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${PRIMARY_DB}")

if "${psql_base[@]}" -tAc "SELECT 1 FROM pg_database WHERE datname='${TARGET_DB}'" | grep -q 1; then
    echo "Database ${TARGET_DB} already exists; skipping creation."
else
    echo "Creating database ${TARGET_DB} owned by ${TARGET_OWNER} ..."
    "${psql_base[@]}" -c "CREATE DATABASE \"${TARGET_DB}\" OWNER \"${TARGET_OWNER}\";"
fi
