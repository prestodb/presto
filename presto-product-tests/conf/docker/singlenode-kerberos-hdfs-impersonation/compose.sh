#!/usr/bin/env bash

source ${BASH_SOURCE%/*}/../../../target/classes/presto.env
export PRESTO_VERSION

docker-compose \
-f ${BASH_SOURCE%/*}/../common/standard.yml \
-f ${BASH_SOURCE%/*}/../common/kerberos.yml \
-f ${BASH_SOURCE%/*}/../common/jdbc_db.yml \
-f ${BASH_SOURCE%/*}/docker-compose.yml \
"$@"
