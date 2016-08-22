#!/bin/bash

set -e

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
  environment_compose ps -q hadoop-master
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
  environment_compose run --rm -T application-runner "$@"
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
    --config-local "${TEMPTO_CONFIGURATION}" "$@"
}

# docker-compose down is not good enough because it's ignores services created with "run" command
function stop_application_runner_containers() {
  local ENVIRONMENT=$1
  APPLICATION_RUNNER_CONTAINERS=$(environment_compose ps -q application-runner)
  for CONTAINER_NAME in ${APPLICATION_RUNNER_CONTAINERS}
  do
    echo "Stopping: ${CONTAINER_NAME}"
    docker stop ${CONTAINER_NAME}
    echo "Container stopped: ${CONTAINER_NAME}"
  done
}

function stop_all_containers() {
  local ENVIRONMENT
  for ENVIRONMENT in $(getAvailableEnvironments)
  do
     stop_docker_compose_containers ${ENVIRONMENT}
  done
}

function stop_docker_compose_containers() {
  local ENVIRONMENT=$1
  RUNNING_CONTAINERS=$(environment_compose ps -q)

  if [[ ! -z ${RUNNING_CONTAINERS} ]]; then
    # stop application runner containers started with "run"
    stop_application_runner_containers ${ENVIRONMENT}

    # stop containers started with "up"
    environment_compose down
  fi

  echo "Docker compose containers stopped: [$ENVIRONMENT]"
}

function prefetch_images_silently() {
  local IMAGES=`environment_compose config | grep 'image:' | awk '{ print $2 }' | sort | uniq`
  for IMAGE in $IMAGES
  do
    echo "Pulling docker image [$IMAGE]"
    docker pull $IMAGE > /dev/null
  done
}

function environment_compose() {
  "${DOCKER_CONF_LOCATION}/${ENVIRONMENT}/compose.sh" "$@"
}

function cleanup() {
  stop_application_runner_containers ${ENVIRONMENT}

  if [[ "${LEAVE_CONTAINERS_ALIVE_ON_EXIT}" != "true" ]]; then
    stop_docker_compose_containers ${ENVIRONMENT}
  fi

  # Ensure that the logs processes are terminated.
  # In most cases after the docker containers are stopped, logs processes must be terminated.
  # However when the `LEAVE_CONTAINERS_ALIVE_ON_EXIT` is set, docker containers are not being terminated.
  # Redirection of system error is supposed to hide the `process does not exist` and `process terminated` messages
  if [[ ! -z ${HADOOP_LOGS_PID} ]]; then
    kill ${HADOOP_LOGS_PID} 2>/dev/null || true
  fi
  if [[ ! -z ${PRESTO_LOGS_PID} ]]; then
    kill ${PRESTO_LOGS_PID} 2>/dev/null || true
  fi

  # docker logs processes are being terminated as soon as docker container are stopped
  # wait for docker logs termination
  wait 2>/dev/null || true
}

function terminate() {
  trap - INT TERM EXIT
  set +e
  cleanup
  exit 130
}

function getAvailableEnvironments() {
  for i in $(ls -d $DOCKER_CONF_LOCATION/*/); do echo ${i%%/}; done\
     | grep -v files | grep -v common | xargs -n1 basename
}

ENVIRONMENT=$1
SCRIPT_DIR=${BASH_SOURCE%/*}
PRODUCT_TESTS_ROOT="${SCRIPT_DIR}/.."
PROJECT_ROOT="${PRODUCT_TESTS_ROOT}/.."
DOCKER_CONF_LOCATION="${PRODUCT_TESTS_ROOT}/conf/docker"

# Get the list of valid environments
if [[ ! -f "$DOCKER_CONF_LOCATION/$ENVIRONMENT/compose.sh" ]]; then
   echo "Usage: run_on_docker.sh <`getAvailableEnvironments | tr '\n' '|'`> <product test args>"
   exit 1
fi

shift 1

DOCKER_PRESTO_VOLUME="/docker/volumes/presto"
TEMPTO_CONFIGURATION="/docker/volumes/tempto/tempto-configuration-local.yaml"

PRESTO_SERVICES="presto-master"
if [[ "$ENVIRONMENT" == "multinode" ]]; then
   PRESTO_SERVICES="${PRESTO_SERVICES} presto-worker"
fi

# set presto version environment variable
source "${PRODUCT_TESTS_ROOT}/target/classes/presto.env"

# check docker and docker compose installation
docker-compose version
docker version

stop_all_containers

if [[ "$CONTINUOUS_INTEGRATION" == 'true' ]]; then
    prefetch_images_silently
fi

# catch terminate signals
trap terminate INT TERM EXIT

# start hadoop container
environment_compose up -d hadoop-master

# start external database containers
environment_compose up -d mysql
environment_compose up -d postgres

# start docker logs for hadoop container
environment_compose logs --no-color hadoop-master &
HADOOP_LOGS_PID=$!

# wait until hadoop processes is started
retry check_hadoop
stop_unnecessary_hadoop_services

# start presto containers
environment_compose up -d ${PRESTO_SERVICES}

# start docker logs for presto containers
environment_compose logs --no-color ${PRESTO_SERVICES} &
PRESTO_LOGS_PID=$!

# wait until presto is started
retry check_presto

# run product tests
set +e
run_product_tests "$*" &
PRODUCT_TESTS_PROCESS_ID=$!
wait ${PRODUCT_TESTS_PROCESS_ID}
EXIT_CODE=$?
set -e

# execution finished successfully
# disable trap, run cleanup manually
trap - INT TERM EXIT
cleanup

exit ${EXIT_CODE}
