#!/usr/bin/env bash

source ${BASH_SOURCE%/*}/../common/compose-commons.sh

TEMPTO_CONFIG_YAML_DEFAULT="${PRODUCT_TESTS_ROOT}/conf/tempto/tempto-configuration-for-docker-ldap.yaml"
TEMPTO_CONFIG_YAML=${TEMPTO_CONFIG_YAML:-${TEMPTO_CONFIG_YAML_DEFAULT}}
export_canonical_path TEMPTO_CONFIG_YAML

docker-compose \
-f ${BASH_SOURCE%/*}/../common/standard.yml \
-f ${BASH_SOURCE%/*}/../common/jdbc_db.yml \
-f ${BASH_SOURCE%/*}/../common/cassandra.yml \
-f ${BASH_SOURCE%/*}/docker-compose.yml \
"$@"
