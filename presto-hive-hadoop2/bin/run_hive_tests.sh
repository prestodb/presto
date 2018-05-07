#!/bin/bash

set -euo pipefail -x

# http://stackoverflow.com/questions/3572030/bash-script-absolute-path-with-osx
function absolutepath() {
    [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
}

SCRIPT_DIR=$(dirname $(absolutepath "$0"))
. ${SCRIPT_DIR}/common.sh

cleanup_docker_containers
start_docker_containers

# generate test data
exec_in_hadoop_master_container su hive -s /usr/bin/hive -f /files/sql/create-test.sql
exec_in_hadoop_master_container su hive -s /usr/bin/hive -f /files/sql/create-test-hive13.sql

stop_unnecessary_hadoop_services

# run product tests
pushd ${PROJECT_ROOT}
set +e
./mvnw -pl presto-hive-hadoop2 test -P test-hive-hadoop2 \
  -Dhive.hadoop2.timeZone=UTC \
  -DHADOOP_USER_NAME=hive \
  -Dhive.hadoop2.metastoreHost=hadoop-master \
  -Dhive.hadoop2.timeZone=Asia/Kathmandu \
  -Dhive.metastore.thrift.client.socks-proxy=${PROXY}:1180 \
  -Dsun.net.spi.nameservice.provider.1=default \
  -Dsun.net.spi.nameservice.provider.2=dns,dnsjava \
  -Ddns.server=${PROXY} \
  -Ddns.port=55353 \
  -Ddns.search=.
EXIT_CODE=$?
set -e
popd

cleanup_docker_containers

exit ${EXIT_CODE}
