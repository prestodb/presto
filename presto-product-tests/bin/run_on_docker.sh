#!/bin/bash

set -euo pipefail

source "${BASH_SOURCE%/*}/lib.sh"

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
  HADOOP_MASTER_CONTAINER=$(hadoop_master_container)
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl status hive-server2 | grep -iq running && \
    docker exec ${HADOOP_MASTER_CONTAINER} netstat -lpn | grep -iq 0.0.0.0:10000
}

function run_in_application_runner_container() {
  local CONTAINER_NAME=$(environment_compose run -d application-runner "$@")
  echo "Showing logs from $CONTAINER_NAME:"
  docker logs -f $CONTAINER_NAME
  return $(docker inspect --format '{{.State.ExitCode}}' $CONTAINER_NAME)
}

function check_presto() {
  run_in_application_runner_container /docker/volumes/conf/docker/files/presto-cli.sh --execute "SHOW CATALOGS" | grep -iq hive
}

function run_product_tests() {
  local REPORT_DIR="${PRODUCT_TESTS_ROOT}/target/test-reports"
  rm -rf "${REPORT_DIR}"
  mkdir -p "${REPORT_DIR}"
  run_in_application_runner_container /docker/volumes/conf/docker/files/run-tempto.sh "$@" &
  PRODUCT_TESTS_PROCESS_ID=$!
  wait ${PRODUCT_TESTS_PROCESS_ID}
  local PRODUCT_TESTS_EXIT_CODE=$?

  #make the files in $REPORT_DIR modifiable by everyone, as they were created by root (by docker)
  run_in_application_runner_container chmod -R 777 "/docker/volumes/test-reports"

  return ${PRODUCT_TESTS_EXIT_CODE}
}

function prefetch_images_silently() {
  local IMAGES=$(docker_images_used)
  for IMAGE in $IMAGES
  do
    echo "Pulling docker image [$IMAGE]"
    docker pull $IMAGE > /dev/null
  done
}

function docker_images_used() {
  environment_compose config | grep 'image:' | awk '{ print $2 }' | sort | uniq
}

function cleanup() {
  stop_application_runner_containers ${ENVIRONMENT}

  if [[ "${LEAVE_CONTAINERS_ALIVE_ON_EXIT:-false}" != "true" ]]; then
    stop_docker_compose_containers ${ENVIRONMENT}
  fi

  # Ensure that the logs processes are terminated.
  # In most cases after the docker containers are stopped, logs processes must be terminated.
  # However when the `LEAVE_CONTAINERS_ALIVE_ON_EXIT` is set, docker containers are not being terminated.
  # Redirection of system error is supposed to hide the `process does not exist` and `process terminated` messages
  if test ! -z ${HADOOP_LOGS_PID:-}; then
    kill ${HADOOP_LOGS_PID} 2>/dev/null || true
  fi
  if test ! -z ${PRESTO_LOGS_PID:-}; then
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

function usage() {
  echo "Usage: run_on_docker.sh <`getAvailableEnvironments | tr '\n' '|' | sed 's/|$//'`> <product test args>"
  exit 1
 }

if [[ $# == 0 ]]; then
  usage
fi

ENVIRONMENT=$1
shift 1

# Get the list of valid environments
if [[ ! -f "$DOCKER_CONF_LOCATION/$ENVIRONMENT/compose.sh" ]]; then
  usage
fi

PRESTO_SERVICES="presto-master"
if [[ "$ENVIRONMENT" == "multinode" ]]; then
   PRESTO_SERVICES="${PRESTO_SERVICES} presto-worker"
elif [[ "$ENVIRONMENT" == "multinode-tls" ]]; then
   PRESTO_SERVICES="${PRESTO_SERVICES} presto-worker-1 presto-worker-2"
fi

# check docker and docker compose installation
docker-compose version
docker version

stop_all_containers

if [[ ${CONTINUOUS_INTEGRATION:-false} = true ]]; then
    prefetch_images_silently
    # This has to be done after fetching the images
    # or will present stale / no data for images that changed.
    echo "Docker images versions:"
    docker_images_used | xargs -n 1 docker inspect --format='ID: {{.ID}}, tags: {{.RepoTags}}'
fi

# catch terminate signals
trap terminate INT TERM EXIT

if [[ "$ENVIRONMENT" == "singlenode-sqlserver" ]]; then
  EXTERNAL_SERVICES="hadoop-master sqlserver"
elif [[ "$ENVIRONMENT" == "singlenode-ldap" ]]; then
  EXTERNAL_SERVICES="hadoop-master ldapserver"
elif [[ "$ENVIRONMENT" == "singlenode-mysql" ]]; then
  EXTERNAL_SERVICES="hadoop-master mysql"
elif [[ "$ENVIRONMENT" == "singlenode-postgresql" ]]; then
  EXTERNAL_SERVICES="hadoop-master postgres"
elif [[ "$ENVIRONMENT" == "singlenode-cassandra" ]]; then
  EXTERNAL_SERVICES="hadoop-master cassandra"
else
  EXTERNAL_SERVICES="hadoop-master"
fi

# display how test environment is configured
environment_compose config

environment_compose up -d ${EXTERNAL_SERVICES}

# start docker logs for the external services
environment_compose logs --no-color -f ${EXTERNAL_SERVICES} &

HADOOP_LOGS_PID=$!

# start presto containers
environment_compose up -d ${PRESTO_SERVICES}

# start docker logs for presto containers
environment_compose logs --no-color -f ${PRESTO_SERVICES} &
PRESTO_LOGS_PID=$!

# wait until hadoop processes are started
retry check_hadoop

# wait until presto is started
retry check_presto

# run product tests
set +e
run_product_tests "$@"
EXIT_CODE=$?
set -e

# execution finished successfully
# disable trap, run cleanup manually
trap - INT TERM EXIT
cleanup

exit ${EXIT_CODE}
