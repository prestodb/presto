#!/bin/bash

set -euo pipefail -x

. ${BASH_SOURCE%/*}/common.sh

cleanup_docker_containers
start_docker_containers

# generate test data
exec_in_hadoop_master_container su hive -s /usr/bin/hive -f /files/sql/create-test.sql
exec_in_hadoop_master_container su hive -s /usr/bin/hive -f /files/sql/create-test-hive13.sql

stop_unnecessary_hadoop_services

HADOOP_MASTER_IP=$(hadoop_master_ip)

# run product tests
pushd ${PROJECT_ROOT}
set +e
./mvnw -B -pl presto-hive-hadoop2 test -P test-hive-hadoop2 \
  -Dhive.hadoop2.timeZone=UTC \
  -DHADOOP_USER_NAME=hive \
  -Dhive.hadoop2.metastoreHost=localhost \
  -Dhive.hadoop2.metastorePort=9083 \
  -Dhive.hadoop2.databaseName=default \
  -Dhive.hadoop2.metastoreHost=hadoop-master \
  -Dhive.hadoop2.timeZone=Asia/Kathmandu \
  -Dhive.metastore.thrift.client.socks-proxy=${PROXY}:1180 \
  -Dhadoop-master-ip=${HADOOP_MASTER_IP}
EXIT_CODE=$?
set -e
popd

cleanup_docker_containers

exit ${EXIT_CODE}
