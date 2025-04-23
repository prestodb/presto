#!/usr/bin/env bash

set -euo pipefail

source "${BASH_SOURCE%/*}/../common/compose-commons.sh"
MYSQL_INIT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/mysql-init" && pwd)"
export MYSQL_INIT_PATH
docker compose \
    -f ${BASH_SOURCE%/*}/../common/standard.yml \
    -f ${BASH_SOURCE%/*}/docker-compose.yml \
    "$@"
