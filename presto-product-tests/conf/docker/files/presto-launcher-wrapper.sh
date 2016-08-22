#!/bin/bash

set -e

CONFIG=$1

if [[ "$CONFIG" != "singlenode" && "$CONFIG" != "multinode-master" && "$CONFIG" != "multinode-worker" && "$CONFIG" != "singlenode-kerberized" ]]; then
   echo "Usage: launcher-wrapper <singlenode|multinode-master|multinode-worker|singlenode-kerberized> <launcher args>"
   exit 1
fi

PRESTO_CONFIG_DIRECTORY="/docker/volumes/conf/presto/etc"

shift 1

/docker/volumes/presto-server/bin/launcher \
  -Dnode.id=${HOSTNAME} \
  -Dcatalog.config-dir=${PRESTO_CONFIG_DIRECTORY}/catalog \
  --config=${PRESTO_CONFIG_DIRECTORY}/${CONFIG}.properties \
  --jvm-config=${PRESTO_CONFIG_DIRECTORY}/jvm.config \
  --log-levels-file=${PRESTO_CONFIG_DIRECTORY}/log.properties \
  --data-dir=/var/presto \
  $* 
