#!/usr/bin/env bash
#
# Regenerates the DuckLake test fixture (catalog.sql + data/) checked in
# alongside this script. See README.md for why this exists and how the
# result is consumed by tests.
#
# Requirements: the DuckDB CLI (with network access, so the ducklake/
# postgres/tpch/json extensions can be installed on first use), plus a
# PostgreSQL 14 server reachable on 127.0.0.1:55432. That server is provided
# one of two ways, selected by PG_MODE:
#
#   docker (default when a Docker daemon is reachable): starts a throwaway
#     postgres:14 container.
#   local (fallback, and the only option on machines where Docker Desktop's
#     organization sign-in blocks unauthenticated `docker pull`/`docker run`):
#     starts a scratch PostgreSQL 14 server using the Homebrew installation
#     at PG_BIN (default /opt/homebrew/opt/postgresql@14/bin), with its data
#     directory in a temp dir that is deleted on exit.
#
# Both modes produce a byte-for-byte-equivalent catalog.sql (same server
# version, same user/db/port), so either is safe to use to regenerate the
# fixture.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
CATALOG_SQL="${SCRIPT_DIR}/catalog.sql"
FIXTURE_SQL="${SCRIPT_DIR}/fixture.sql"

CONTAINER_NAME="ducklake-fixture-postgres"
PG_PORT=55432
PG_USER="postgres"
PG_PASSWORD="ducklake"
PG_DB="ducklake"
PG_BIN="${PG_BIN:-/opt/homebrew/opt/postgresql@14/bin}"

DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"

if [ -z "${PG_MODE:-}" ]; then
    if docker info >/dev/null 2>&1; then
        PG_MODE="docker"
    else
        PG_MODE="local"
    fi
fi
echo "==> PG_MODE=${PG_MODE}"

PGDATA_DIR=""

cleanup() {
    if [ "${PG_MODE}" = "docker" ]; then
        docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
    else
        if [ -n "${PGDATA_DIR}" ] && [ -d "${PGDATA_DIR}" ]; then
            "${PG_BIN}/pg_ctl" -D "${PGDATA_DIR}" -m fast stop >/dev/null 2>&1 || true
            rm -rf "${PGDATA_DIR}"
        fi
    fi
    rm -f "${GENERATED_SQL:-}"
}
trap cleanup EXIT

echo "==> Cleaning up previous fixture output"
rm -f "${CATALOG_SQL}"
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"

# In case a previous run was interrupted before its own trap could fire.
if [ "${PG_MODE}" = "docker" ]; then
    docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
fi

if [ "${PG_MODE}" = "docker" ]; then
    echo "==> Starting postgres:14 in Docker (port ${PG_PORT})"
    docker run -d \
        --name "${CONTAINER_NAME}" \
        -e POSTGRES_USER="${PG_USER}" \
        -e POSTGRES_PASSWORD="${PG_PASSWORD}" \
        -e POSTGRES_DB="${PG_DB}" \
        -p "${PG_PORT}:5432" \
        postgres:14 >/dev/null

    echo "==> Waiting for postgres to accept connections"
    for _ in $(seq 1 60); do
        if docker exec "${CONTAINER_NAME}" pg_isready -U "${PG_USER}" -d "${PG_DB}" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    if ! docker exec "${CONTAINER_NAME}" pg_isready -U "${PG_USER}" -d "${PG_DB}" >/dev/null 2>&1; then
        echo "postgres never became ready" >&2
        exit 1
    fi
else
    echo "==> Starting a scratch PostgreSQL 14 server (port ${PG_PORT}) via ${PG_BIN}"
    if [ ! -x "${PG_BIN}/initdb" ]; then
        echo "PG_BIN=${PG_BIN} does not look like a postgresql@14 bin directory (initdb missing)" >&2
        exit 1
    fi

    PGDATA_DIR="$(mktemp -d -t ducklake-pg-XXXXXX)"
    "${PG_BIN}/initdb" -D "${PGDATA_DIR}" -U "${PG_USER}" --auth=trust -E UTF8 >/dev/null
    "${PG_BIN}/pg_ctl" -D "${PGDATA_DIR}" \
        -o "-p ${PG_PORT} -c listen_addresses=127.0.0.1 -k ${PGDATA_DIR}" \
        -l "${PGDATA_DIR}/server.log" -w start
    "${PG_BIN}/createdb" -h 127.0.0.1 -p "${PG_PORT}" -U "${PG_USER}" "${PG_DB}"

    echo "==> Waiting for postgres to accept connections"
    for _ in $(seq 1 60); do
        if "${PG_BIN}/pg_isready" -h 127.0.0.1 -p "${PG_PORT}" -U "${PG_USER}" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    if ! "${PG_BIN}/pg_isready" -h 127.0.0.1 -p "${PG_PORT}" -U "${PG_USER}" >/dev/null 2>&1; then
        echo "postgres never became ready" >&2
        exit 1
    fi
    # --auth=trust means the password DuckDB sends in the ATTACH connection
    # string below is never checked, so fixture.sql needs no change between
    # modes.
fi

echo "==> Generating fixture data with the DuckDB CLI"
GENERATED_SQL="$(mktemp -t ducklake-fixture-XXXXXX.sql)"
sed "s#@@DATA_PATH@@#${DATA_DIR}#g" "${FIXTURE_SQL}" > "${GENERATED_SQL}"

"${DUCKDB_BIN}" < "${GENERATED_SQL}"

echo "==> Dumping the DuckLake PostgreSQL catalog to catalog.sql"
if [ "${PG_MODE}" = "docker" ]; then
    docker exec "${CONTAINER_NAME}" \
        pg_dump -U "${PG_USER}" --no-owner --no-privileges --inserts "${PG_DB}" \
        | grep -v '^\\' \
        | grep -v 'transaction_timeout' \
        > "${CATALOG_SQL}"
else
    "${PG_BIN}/pg_dump" -h 127.0.0.1 -p "${PG_PORT}" -U "${PG_USER}" \
        --no-owner --no-privileges --inserts "${PG_DB}" \
        | grep -v '^\\' \
        | grep -v 'transaction_timeout' \
        > "${CATALOG_SQL}"
fi

echo "==> Verifying fixture output"
fail=0

check() {
    local description="$1"
    local count="$2"
    if [ "${count}" -lt 1 ]; then
        echo "FAIL: ${description} (found ${count})" >&2
        fail=1
    else
        echo "OK:   ${description} (found ${count})"
    fi
}

check "catalog.sql defines ducklake_snapshot" \
    "$(grep -c 'CREATE TABLE public\.ducklake_snapshot ' "${CATALOG_SQL}" || true)"
check "catalog.sql defines at least one ducklake_inlined_data_ table" \
    "$(grep -c 'CREATE TABLE public\.ducklake_inlined_data_' "${CATALOG_SQL}" || true)"
check "data/ contains Parquet files" \
    "$(find "${DATA_DIR}" -name '*.parquet' | wc -l | tr -d ' ')"
check "data/ contains at least one delete file" \
    "$(find "${DATA_DIR}" -name '*-delete*.parquet' | wc -l | tr -d ' ')"

echo
echo "catalog.sql: $(wc -l < "${CATALOG_SQL}" | tr -d ' ') lines, $(du -h "${CATALOG_SQL}" | cut -f1)"
echo "data/:       $(find "${DATA_DIR}" -name '*.parquet' | wc -l | tr -d ' ') parquet files, $(du -sh "${DATA_DIR}" | cut -f1)"

if [ "${fail}" -ne 0 ]; then
    echo "Fixture verification FAILED" >&2
    exit 1
fi

echo "Fixture generation complete."
