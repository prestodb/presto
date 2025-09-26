#!/usr/bin/env bash

source ${BASH_SOURCE%/*}/../common/compose-commons.sh

docker compose \
    -f ${BASH_SOURCE%/*}/../common/standard.yml \
    -f ${BASH_SOURCE%/*}/../common/kerberos.yml \
    -f ${BASH_SOURCE%/*}/docker-compose.yml \
    "$@"
