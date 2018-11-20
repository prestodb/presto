#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIRECTORY=${BASH_SOURCE%/*}

source "${SCRIPT_DIRECTORY}/../common/compose-commons.sh"

docker-compose \
    -f ${SCRIPT_DIRECTORY}/../common/standard.yml \
    -f ${SCRIPT_DIRECTORY}/docker-compose.yml \
    "$@"
