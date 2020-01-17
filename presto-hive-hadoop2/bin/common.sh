#!/bin/bash

set -euo pipefail -x

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

function hadoop_master_ip() {
  HADOOP_MASTER_CONTAINER=$(hadoop_master_container)
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $HADOOP_MASTER_CONTAINER
}

function check_hadoop() {
  HADOOP_MASTER_CONTAINER=$(hadoop_master_container)
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl status hive-server2 | grep -iq running && \
    docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl status hive-metastore | grep -iq running && \
    docker exec ${HADOOP_MASTER_CONTAINER} netstat -lpn | grep -iq 0.0.0.0:10000 &&
    docker exec ${HADOOP_MASTER_CONTAINER} netstat -lpn | grep -iq 0.0.0.0:9083
}

function exec_in_hadoop_master_container() {
  HADOOP_MASTER_CONTAINER=$(hadoop_master_container)
  docker exec ${HADOOP_MASTER_CONTAINER} "$@"
}

function stop_unnecessary_hadoop_services() {
  HADOOP_MASTER_CONTAINER=$(hadoop_master_container)
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl status
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl stop yarn-resourcemanager
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl stop yarn-nodemanager
}

function cleanup_docker_containers() {
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

SCRIPT_DIR="${BASH_SOURCE%/*}"
INTEGRATION_TESTS_ROOT="${SCRIPT_DIR}/.."
PROJECT_ROOT="${INTEGRATION_TESTS_ROOT}/.."
DOCKER_COMPOSE_LOCATION="${INTEGRATION_TESTS_ROOT}/conf/docker-compose.yml"

# check docker and docker compose installation
docker-compose version
docker version

# extract proxy IP
if [ -n "${DOCKER_MACHINE_NAME:-}" ]
then
  PROXY=`docker-machine ip`
else
  PROXY=127.0.0.1
fi

function start_docker_containers() {
  # stop already running containers
  docker-compose -f "${DOCKER_COMPOSE_LOCATION}" down || true

  # catch terminate signals
  trap termination_handler INT TERM

  # pull docker images
  if [[ "${CONTINUOUS_INTEGRATION:-false}" == 'true' ]]; then
    docker-compose -f "${DOCKER_COMPOSE_LOCATION}" pull
  fi

  # start containers
  docker-compose -f "${DOCKER_COMPOSE_LOCATION}" up -d

  # start docker logs for hadoop container
  docker-compose -f "${DOCKER_COMPOSE_LOCATION}" logs --no-color hadoop-master &

  # wait until hadoop processes is started
  retry check_hadoop
}
