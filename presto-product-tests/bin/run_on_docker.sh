#!/bin/bash -ex

# http://stackoverflow.com/questions/3572030/bash-script-absolute-path-with-osx
function absolutepath() {
    [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
}

function retry() {
  END=$(($(date +%s) + 600))

  while (( $(date +%s) < $END )); do
    set +e
    "$@"
    EXIT_CODE=$?
    set -e

    if [[ ${EXIT_CODE} == 0 ]]; then
      break
    fi
    sleep 5
  done

  return ${EXIT_CODE}
}

function hadoop_master_container(){
  docker ps --format '{{.Names}}' | grep hadoop-master
}

function check_hadoop() {
  docker exec $(hadoop_master_container) supervisorctl status hive-server2 | grep -i running
}

function stop_unnecessary_hadoop_services() {
  HADOOP_MASTER_CONTAINER=$(hadoop_master_container)
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl status
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl stop mapreduce-historyserver
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl stop yarn-resourcemanager
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl stop yarn-nodemanager
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl stop zookeeper
}

function docker_compose_network() {
  docker network ls | grep ${ENVIRONMENT} | cut  -f 1 -d ' '
}

function check_presto() {
  docker run \
    --rm -it \
    --net $(docker_compose_network) \
    -v "${PROJECT_ROOT}/presto-cli/target/:/cli" \
    teradatalabs/centos6-java8-oracle \
    java -jar /cli/presto-cli-${PRESTO_VERSION}-executable.jar --server presto-master:8080 \
    --execute "SHOW CATALOGS"  | grep -i hive
}

function run_product_tests() {
  docker run \
        --rm -it \
        --net $(docker_compose_network) \
        -v "${PRODUCT_TESTS_ROOT}:/presto-product-tests" \
        teradatalabs/centos6-java8-oracle \
        /presto-product-tests/bin/run.sh \
        --config-local /presto-product-tests/conf/tempto/tempto-configuration.yaml "$@"
}

function start_docker_logs() {
  docker-compose -f "${DOCKER_COMPOSE_LOCATION}" logs --no-color &
  DOCKER_LOG_PROCESS_ID=$!
}

function stop_docker_logs() {
  set +e
  kill ${DOCKER_LOG_PROCESS_ID}
  wait ${DOCKER_LOG_PROCESS_ID}
  set -e
}

ENVIRONMENT=$1

if [[ "$ENVIRONMENT" != "singlenode" && "$ENVIRONMENT" != "distributed" ]]; then
   echo "Usage: run_on_docker.sh <singlenode|distributed> <product test args>"
   exit 1
fi

shift 1

SCRIPT_DIR=$(dirname $(absolutepath "$0"))
PRODUCT_TESTS_ROOT="${SCRIPT_DIR}/.."
PROJECT_ROOT="${PRODUCT_TESTS_ROOT}/.."
DOCKER_COMPOSE_LOCATION="${PRODUCT_TESTS_ROOT}/conf/docker/${ENVIRONMENT}/docker-compose.yml"

# set presto version environment variable
source "${PRODUCT_TESTS_ROOT}/target/classes/presto.env"

# check docker and docker compose installation
docker-compose version
docker version

# try to stop already running containers
docker-compose -f "${PRODUCT_TESTS_ROOT}/conf/docker/singlenode/docker-compose.yml" down || true
docker-compose -f "${PRODUCT_TESTS_ROOT}/conf/docker/distributed/docker-compose.yml" down || true

# pull docker images
docker-compose -f "${DOCKER_COMPOSE_LOCATION}" pull

# start hadoop container
docker-compose -f "${DOCKER_COMPOSE_LOCATION}" up -d hadoop-master

# start docker logs to be able to check what is going on spawning hadoop-master container
start_docker_logs

# wait until hadoop processes is started
retry check_hadoop
stop_unnecessary_hadoop_services

# stop docker logs and start it again after the presto containers are spawned
stop_docker_logs

# start presto containers
docker-compose -f "${DOCKER_COMPOSE_LOCATION}" up -d

# start docker logs attached to both hadoop and presto containers
start_docker_logs

# wait until presto is started
retry check_presto

# run product tests
set +e
run_product_tests "$*"
EXIT_CODE=$?
set -x

# stop docker containers
docker-compose -f "${DOCKER_COMPOSE_LOCATION}" down

# stop docker logs
stop_docker_logs

exit ${EXIT_CODE}
