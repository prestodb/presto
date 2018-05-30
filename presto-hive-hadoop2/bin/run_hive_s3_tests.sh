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

# insert AWS credentials
exec_in_hadoop_master_container cp /etc/hadoop/conf/core-site.xml.s3-template /etc/hadoop/conf/core-site.xml
exec_in_hadoop_master_container sed -i -e "s|%AWS_ACCESS_KEY%|${AWS_ACCESS_KEY_ID}|g" -e "s|%AWS_SECRET_KEY%|${AWS_SECRET_ACCESS_KEY}|g" -e "s|%S3_BUCKET_ENDPOINT%|${S3_BUCKET_ENDPOINT}|g" \
 /etc/hadoop/conf/core-site.xml

# create test table
exec_in_hadoop_master_container /usr/bin/hive -e "CREATE EXTERNAL TABLE presto_test_s3(t_bigint bigint) LOCATION 's3a://${S3_BUCKET}/presto_test_s3/'"

stop_unnecessary_hadoop_services

# run product tests
pushd $PROJECT_ROOT
set +e
./mvnw -pl presto-hive-hadoop2 test -P test-hive-hadoop2-s3 \
  -DHADOOP_USER_NAME=hive \
  -Dhive.hadoop2.metastoreHost=localhost \
  -Dhive.hadoop2.metastorePort=9083 \
  -Dhive.hadoop2.databaseName=default \
  -Dhive.hadoop2.s3.awsAccessKey=${AWS_ACCESS_KEY_ID} \
  -Dhive.hadoop2.s3.awsSecretKey=${AWS_SECRET_ACCESS_KEY} \
  -Dhive.hadoop2.s3.writableBucket=${S3_BUCKET}
EXIT_CODE=$?
set -e
popd

cleanup_docker_containers

exit ${EXIT_CODE}
