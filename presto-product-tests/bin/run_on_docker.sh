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
  docker-compose -f "${DOCKER_COMPOSE_LOCATION}" ps -q hadoop-master
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

function run_in_application_runner_container() {
  docker-compose -f "${DOCKER_COMPOSE_LOCATION}" run --rm -T application-runner "$@"
}

function check_presto() {
  run_in_application_runner_container \
    java -jar /presto-cli/target/presto-cli-${PRESTO_VERSION}-executable.jar \
    --server presto-master:8080 \
    --execute "SHOW CATALOGS" | grep -i hive
}

function run_product_tests() {
  run_in_application_runner_container \
    /presto-product-tests/bin/run.sh \
    --config-local /presto-product-tests/conf/tempto/tempto-configuration.yaml "$@"
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

PRESTO_SERVICES="presto-master"
if [[ "$ENVIRONMENT" == "distributed" ]]; then
   PRESTO_SERVICES="${PRESTO_SERVICES} presto-worker"
fi

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

# start docker logs for hadoop container
docker-compose -f "${DOCKER_COMPOSE_LOCATION}" logs --no-color hadoop-master &

# wait until hadoop processes is started
retry check_hadoop
stop_unnecessary_hadoop_services

# start presto containers
docker-compose -f "${DOCKER_COMPOSE_LOCATION}" up -d ${PRESTO_SERVICES}

# start docker logs for presto containers
docker-compose -f "${DOCKER_COMPOSE_LOCATION}" logs --no-color ${PRESTO_SERVICES} &

# wait until presto is started
retry check_presto

# run product tests
set +e
run_product_tests "$*"
EXIT_CODE=$?
set -x

# stop docker containers
docker-compose -f "${DOCKER_COMPOSE_LOCATION}" down

# wait for docker logs to stop
wait

exit ${EXIT_CODE}
