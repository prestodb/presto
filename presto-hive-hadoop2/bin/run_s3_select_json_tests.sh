#!/bin/bash

set -euo pipefail -x

. "${BASH_SOURCE%/*}/common.sh"

# Similar to run_hive_s3_tests.sh, but overriding HADOOP_BASE_IMAGE. We need JSONSerDe for these tests, which is not available in prestodb/hdp2.6-hive.
# This image was found in src/test/java/com/facebook/presto/hive/containers/HiveHadoopContainer.java
# Please note that this also has different paths for config files - e.g. /opt/hadoop/etc/hadoop.
export HADOOP_BASE_IMAGE="prestodb/hive3.1-hive"
export DOCKER_IMAGES_VERSION="10"

cleanup_docker_containers
start_docker_containers

# obtain Hive version
TESTS_HIVE_VERSION_MAJOR=$(get_hive_major_version)

# insert AWS credentials
exec_in_hadoop_master_container cp /etc/hadoop/conf/core-site.xml.s3-template /opt/hadoop/etc/hadoop/core-site.xml
exec_in_hadoop_master_container sed -i \
  -e "s|%AWS_ACCESS_KEY%|${AWS_ACCESS_KEY_ID}|g" \
  -e "s|%AWS_SECRET_KEY%|${AWS_SECRET_ACCESS_KEY}|g" \
  -e "s|%S3_BUCKET_ENDPOINT%|${S3_BUCKET_ENDPOINT}|g" \
 /opt/hadoop/etc/hadoop/core-site.xml

exec_in_hadoop_master_container cp /etc/hive/conf/hive-site.xml.s3-template /opt/hive/conf/hive-site.xml
exec_in_hadoop_master_container sed -i \
  -e "s|%AWS_ACCESS_KEY%|${AWS_ACCESS_KEY_ID}|g" \
  -e "s|%AWS_SECRET_KEY%|${AWS_SECRET_ACCESS_KEY}|g" \
 /opt/hive/conf/hive-site.xml

table_path="s3a://${S3_BUCKET}/presto_test_external_fs_json/"
exec_in_hadoop_master_container hadoop fs -mkdir -p "${table_path}"
exec_in_hadoop_master_container hadoop fs -put -f /tmp/files/test_table.json{,.gz,.bz2} "${table_path}"
exec_in_hadoop_master_container /opt/hive/bin/hive -e "
    CREATE EXTERNAL TABLE presto_test_external_fs_json(col_1 bigint, col_2 bigint)
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
    LOCATION '${table_path}'"

stop_unnecessary_hadoop_services

# restart hive-metastore to apply S3 changes in core-site.xml
docker exec $(hadoop_master_container) supervisorctl restart hive-metastore
retry check_hadoop

# run product tests
pushd $PROJECT_ROOT
set +e
./mvnw -B -pl presto-hive-hadoop2 test -P test-hive-hadoop2-s3-select-json\
  -DHADOOP_USER_NAME=hive \
  -Dhive.hadoop2.metastoreHost=localhost \
  -Dhive.hadoop2.metastorePort=9083 \
  -Dhive.hadoop2.databaseName=default \
  -Dhive.hadoop2.s3.awsAccessKey=${AWS_ACCESS_KEY_ID} \
  -Dhive.hadoop2.s3.awsSecretKey=${AWS_SECRET_ACCESS_KEY} \
  -Dhive.hadoop2.hiveVersionMajor="${TESTS_HIVE_VERSION_MAJOR}" \
  -Dhive.hadoop2.s3.writableBucket=${S3_BUCKET}
EXIT_CODE=$?
set -e
popd

cleanup_docker_containers

exit ${EXIT_CODE}
