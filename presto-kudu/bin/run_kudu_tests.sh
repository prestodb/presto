#!/bin/bash
#
# Runs all tests with Kudu server in docker containers.
#
# ./run_kudu_tests.sh <inferSchemaPrefix>
#
# <inferSchemaPrefix>    InferSchema setup: "null" = disabled,
#                                           ""     = enabled, using empty prefix,
#                                           "presto::" = enabled, using standard prefix
#
# If arguments are missing, defaults are 1 tablet server and disabled inferSchema.

set -euo pipefail -x

export KUDU_VERSION="1.10.0"
export KUDU_CLUSTERIP=$(docker network inspect bridge --format='{{index .IPAM.Config 0 "Gateway"}}')

PROJECT_ROOT="${BASH_SOURCE%/*}/../.."
DOCKER_COMPOSE_LOCATION="${BASH_SOURCE%/*}/../conf/docker-compose.yml"

# emulate schemas ?
if [ $# -lt 2 ]
then
  TEST_SCHEMA_EMULATION_PREFIX=null
else
  TEST_SCHEMA_EMULATION_PREFIX=$2
fi

function start_docker_container() {
  # stop already running containers
  docker-compose -f "${DOCKER_COMPOSE_LOCATION}" down || true

  # start containers
  docker-compose -f "${DOCKER_COMPOSE_LOCATION}" up -d
}

function cleanup_docker_container() {
  docker-compose -f "${DOCKER_COMPOSE_LOCATION}" down
}

start_docker_container

# run product tests
pushd ${PROJECT_ROOT}
# sleep to allow cluster be up
sleep 15s

set +e
./mvnw -pl presto-kudu test -P integration \
  -Dkudu.client.master-addresses=${KUDU_CLUSTERIP}:7051,${KUDU_CLUSTERIP}:7151,${KUDU_CLUSTERIP}:7251 \
  -Dkudu.schema-emulation.prefix=${TEST_SCHEMA_EMULATION_PREFIX}
EXIT_CODE=$?
set -e
popd

cleanup_docker_container

exit ${EXIT_CODE}
