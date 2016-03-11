#!/bin/bash -ex

function retry() {
  END=$(($(date +%s) + 600))

  while (( $(date +%s) < $END )); do
    set +e
    "$@"
    EXIT_CODE=$?
    set -e

    if [[ $EXIT_CODE == 0 ]]; then
      break
    fi
    sleep 5
  done

  return $EXIT_CODE
}

function check_hadoop() {
  HADOOP_MASTER_CONTAINER=$(docker ps --format '{{.Names}}' | grep hadoop-master)
  docker exec $HADOOP_MASTER_CONTAINER supervisorctl status hive-server2 | grep -i running 
}

function stop_unnessery_hadoop_services() {
  HADOOP_MASTER_CONTAINER=$(docker ps --format '{{.Names}}' | grep hadoop-master)
  docker exec $HADOOP_MASTER_CONTAINER supervisorctl status 
  docker exec $HADOOP_MASTER_CONTAINER supervisorctl stop mapreduce-historyserver 
  docker exec $HADOOP_MASTER_CONTAINER supervisorctl stop yarn-resourcemanager 
  docker exec $HADOOP_MASTER_CONTAINER supervisorctl stop yarn-nodemanager 
  docker exec $HADOOP_MASTER_CONTAINER supervisorctl stop zookeeper 
}

function check_presto() {
  DOCKER_NETWORK=$(docker network ls | grep singlenode | cut  -f 1 -d ' ')
  echo 'SHOW CATALOGS;' | \
    docker run \
      -i \
      --rm \
      --net ${DOCKER_NETWORK} \
      -v $(readlink -f ../../../presto-cli/target/):/cli \
      teradatalabs/centos6-java8-oracle \
      java -jar /cli/presto-cli-${PRESTO_VERSION}-executable.jar --server presto-master:8080 | \
    grep -i hive
}

cd $(dirname $(readlink -f $0))

source ../../target/classes/presto.env

docker-compose version
docker version

docker-compose up -d hadoop-master
retry check_hadoop
stop_unnessery_hadoop_services

docker-compose up -d presto-master
retry check_presto

docker-compose logs &

set +e
java -Djava.util.logging.config.file=logging.properties -jar ../../target/presto-product-tests-${PRESTO_VERSION}-executable.jar $*
EXIT_CODE=$?
set -x

docker-compose stop
wait $!

exit $EXIT_CODE
