#!/bin/bash

source "${BASH_SOURCE%/*}/locations.sh"

# docker-compose down is not good enough because it's ignores services created with "run" command
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

    # stop containers started with "up", removing their volumes
    # Some containers (SQL Server) fail to stop on Travis after running the tests. We don't have an easy way to
    # reproduce this locally. Since all the tests complete successfully, we ignore this failure.
    environment_compose kill
    environment_compose down --volumes || true
  fi

  echo "Docker compose containers stopped: [$ENVIRONMENT]"
}

function stop_application_runner_containers() {
  local ENVIRONMENT=$1
  APPLICATION_RUNNER_CONTAINERS=$(environment_compose ps -q application-runner)
  for CONTAINER_NAME in ${APPLICATION_RUNNER_CONTAINERS}
  do
    echo "Stopping: ${CONTAINER_NAME}"
    docker stop ${CONTAINER_NAME}
    echo "Container stopped: ${CONTAINER_NAME}"
  done
  echo "Removing dead application-runner containers"
  for CONTAINER in $(docker ps -aq --no-trunc --filter status=dead --filter status=exited --filter name=common_application-runner);
  do
    docker rm -v "${CONTAINER}"
  done
}

function environment_compose() {
  "${DOCKER_CONF_LOCATION}/${ENVIRONMENT}/compose.sh" "$@"
}

function getAvailableEnvironments() {
  for i in $(ls -d $DOCKER_CONF_LOCATION/*/); do echo ${i%%/}; done \
     | grep -v files | grep -v common | xargs -n1 basename
}

