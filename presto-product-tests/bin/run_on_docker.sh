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
    java -jar ${DOCKER_PRESTO_VOLUME}/presto-cli/target/presto-cli-${PRESTO_VERSION}-executable.jar \
    --server presto-master:8080 \
    --execute "SHOW CATALOGS" | grep -i hive
}

function run_product_tests() {
  run_in_application_runner_container \
    ${DOCKER_PRESTO_VOLUME}/presto-product-tests/bin/run.sh \
    --config-local ${DOCKER_PRESTO_VOLUME}/presto-product-tests/conf/tempto/tempto-configuration.yaml "$@"
}

# docker-compose down is not good enough because it's ignores services created with "run" command
function stop_application_runner_containers() {
  APPLICATION_RUNNER_CONTAINERS=$(docker-compose -f "$1" ps -q application-runner)
  for CONTAINER_NAME in ${APPLICATION_RUNNER_CONTAINERS}
  do
    echo "Stopping: ${CONTAINER_NAME}"
    docker stop ${CONTAINER_NAME}
    echo "Container stopped: ${CONTAINER_NAME}"
  done
}

function cleanup_docker_containers() {
  # stop application runner containers started with "run"
  stop_application_runner_containers "${DOCKER_COMPOSE_LOCATION}"

  # stop containers started with "up"
  docker-compose -f "${DOCKER_COMPOSE_LOCATION}" down

  # docker logs processes are being terminated as soon as docker container are stopped
  # wait for docker logs termination
  wait
}

function termination_handler(){
  set +e
  cleanup_docker_containers
  exit 130
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
DOCKER_PRESTO_VOLUME="/docker/volumes/presto"

SINGLE_NODE_DOCKER_COMPOSE_LOCATION="${PRODUCT_TESTS_ROOT}/conf/docker/singlenode/docker-compose.yml"
DISTRIBUTED_DOCKER_COMPOSE_LOCATION="${PRODUCT_TESTS_ROOT}/conf/docker/distributed/docker-compose.yml"

PRESTO_SERVICES="presto-master"
if [[ "$ENVIRONMENT" == "distributed" ]]; then
   PRESTO_SERVICES="${PRESTO_SERVICES} presto-worker"
fi

# set presto version environment variable
source "${PRODUCT_TESTS_ROOT}/target/classes/presto.env"

# check docker and docker compose installation
docker-compose version
docker version

# stop already running containers
stop_application_runner_containers "${SINGLE_NODE_DOCKER_COMPOSE_LOCATION}"
stop_application_runner_containers "${DISTRIBUTED_DOCKER_COMPOSE_LOCATION}"
docker-compose -f "${SINGLE_NODE_DOCKER_COMPOSE_LOCATION}" down || true
docker-compose -f "${DISTRIBUTED_DOCKER_COMPOSE_LOCATION}" down || true

# catch terminate signals
trap termination_handler INT TERM

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
run_product_tests "$*" &
PRODUCT_TESTS_PROCESS_ID=$!
wait ${PRODUCT_TESTS_PROCESS_ID}
EXIT_CODE=$?
set -e

cleanup_docker_containers

exit ${EXIT_CODE}
