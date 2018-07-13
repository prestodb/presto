#!/bin/bash

set -euo pipefail -x

. "${BASH_SOURCE%/*}/common.sh"

cleanup_docker_containers
start_docker_containers

# insert AWS credentials
exec_in_hadoop_master_container cp /etc/hadoop/conf/core-site.xml.s3-template /etc/hadoop/conf/core-site.xml
exec_in_hadoop_master_container sed -i \
  -e "s|%AWS_ACCESS_KEY%|${AWS_ACCESS_KEY_ID}|g" \
  -e "s|%AWS_SECRET_KEY%|${AWS_SECRET_ACCESS_KEY}|g" \
  -e "s|%S3_BUCKET_ENDPOINT%|${S3_BUCKET_ENDPOINT}|g" \
 /etc/hadoop/conf/core-site.xml

# create test table
table_path="s3a://${S3_BUCKET}/presto_test_external_fs/"
exec_in_hadoop_master_container hadoop fs -mkdir -p "${table_path}"
exec_in_hadoop_master_container hadoop fs -copyFromLocal -f /tmp/test1.csv "${table_path}"
exec_in_hadoop_master_container hadoop fs -copyFromLocal -f /tmp/test1.csv.gz "${table_path}"
exec_in_hadoop_master_container hadoop fs -copyFromLocal -f /tmp/test1.csv.lz4 "${table_path}"
exec_in_hadoop_master_container hadoop fs -copyFromLocal -f /tmp/test1.csv.bz2 "${table_path}"
exec_in_hadoop_master_container /usr/bin/hive -e "CREATE EXTERNAL TABLE presto_test_external_fs(t_bigint bigint) LOCATION '${table_path}'"

stop_unnecessary_hadoop_services

# restart hive-metastore to apply S3 changes in core-site.xml
docker exec $(hadoop_master_container) supervisorctl restart hive-metastore
retry check_hadoop

# run product tests
pushd $PROJECT_ROOT
set +e
./mvnw -B -pl presto-hive-hadoop2 test -P test-hive-hadoop2-s3 \
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
