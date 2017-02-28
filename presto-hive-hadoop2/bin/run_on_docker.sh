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

function hadoop_master_ip() {
  HADOOP_MASTER_CONTAINER=$(hadoop_master_container)
  docker inspect --format '{{ .NetworkSettings.IPAddress }}' $HADOOP_MASTER_CONTAINER
}

function check_hadoop() {
  docker exec $(hadoop_master_container) supervisorctl status hive-server2 | grep -i running
}

function exec_in_hadoop_master_container() {
  HADOOP_MASTER_CONTAINER=$(hadoop_master_container)
  docker exec ${HADOOP_MASTER_CONTAINER} "$@"
}

function stop_unnecessary_hadoop_services() {
  HADOOP_MASTER_CONTAINER=$(hadoop_master_container)
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl status
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl stop mapreduce-historyserver
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl stop yarn-resourcemanager
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl stop yarn-nodemanager
  docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl stop zookeeper
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

SCRIPT_DIR=$(dirname $(absolutepath "$0"))
INTEGRATION_TESTS_ROOT="${SCRIPT_DIR}/.."
PROJECT_ROOT="${INTEGRATION_TESTS_ROOT}/.."
DOCKER_COMPOSE_LOCATION="${INTEGRATION_TESTS_ROOT}/conf/docker-compose.yml"

# check docker and docker compose installation
docker-compose version
docker version

# stop already running containers
docker-compose -f "${DOCKER_COMPOSE_LOCATION}" down || true

# catch terminate signals
trap termination_handler INT TERM

# pull docker images
docker-compose -f "${DOCKER_COMPOSE_LOCATION}" pull

# start containers
docker-compose -f "${DOCKER_COMPOSE_LOCATION}" up -d

# start docker logs for hadoop container
docker-compose -f "${DOCKER_COMPOSE_LOCATION}" logs --no-color hadoop-master &

# wait until hadoop processes is started
retry check_hadoop

# generate test data
exec_in_hadoop_master_container su hive -s /usr/bin/hive -f /files/sql/create-test.sql
exec_in_hadoop_master_container su hive -s /usr/bin/hive -f /files/sql/create-test-hive13.sql

stop_unnecessary_hadoop_services

if [ -n "$DOCKER_MACHINE_NAME" ]
then
  PROXY=`docker-machine ip`
else
  PROXY=127.0.0.1
fi

# run product tests
pushd $PROJECT_ROOT
set +e
./mvnw -pl presto-hive-hadoop2 test -P test-hive-hadoop2 \
  -Dhive.hadoop2.timeZone=UTC \
  -DHADOOP_USER_NAME=hive \
  -Dhive.hadoop2.metastoreHost=hadoop-master \
  -Dhive.metastore.thrift.client.socks-proxy=$PROXY:1180 \
  -Dsun.net.spi.nameservice.provider.1=default \
  -Dsun.net.spi.nameservice.provider.2=dns,dnsjava \
  -Ddns.server=$PROXY \
  -Ddns.port=55353 \
  -Ddns.search=. &
PRODUCT_TESTS_PROCESS_ID=$!

wait ${PRODUCT_TESTS_PROCESS_ID}
EXIT_CODE=$?
set -e
popd

cleanup_docker_containers

exit ${EXIT_CODE}
