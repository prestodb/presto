#!/bin/bash

set -e

CONFIG=$1

if [[ "$CONFIG" != "singlenode" && "$CONFIG" != "multinode-master" && "$CONFIG" != "multinode-worker" ]]; then
   echo "Usage: launcher-wrapper <singlenode|multinode-master|multinode-worker> <launcher args>"
   exit 1
fi

PRESTO_VOLUME="/docker/volumes/presto"
PRESTO_CONFIG_DIRECTORY="${PRESTO_VOLUME}/presto-product-tests/conf/presto/etc"

shift 1

${PRESTO_VOLUME}/presto-server/target/presto-server-${PRESTO_VERSION}/bin/launcher \
  -Dnode.id=${HOSTNAME} \
  -Dcatalog.config-dir=${PRESTO_CONFIG_DIRECTORY}/catalog \
  --config=${PRESTO_CONFIG_DIRECTORY}/${CONFIG}.properties \
  --jvm-config=${PRESTO_CONFIG_DIRECTORY}/jvm.config \
  --log-levels-file=${PRESTO_CONFIG_DIRECTORY}/log.properties \
  --data-dir=/var/presto \
  $* 
