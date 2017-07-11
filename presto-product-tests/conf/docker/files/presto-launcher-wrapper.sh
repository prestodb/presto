#!/bin/bash

set -e

CONFIG="$1"

PRESTO_CONFIG_DIRECTORY="/docker/volumes/conf/presto/etc"
CONFIG_PROPERTIES_LOCATION="${PRESTO_CONFIG_DIRECTORY}/${CONFIG}.properties"

if [[ ! -e ${CONFIG_PROPERTIES_LOCATION} ]]; then
   echo "${CONFIG_PROPERTIES_LOCATION} does not exist"
   exit 1
fi

shift 1

/docker/volumes/presto-server/bin/launcher \
  -Dnode.id="${HOSTNAME}" \
  -Dcatalog.config-dir="${PRESTO_CONFIG_DIRECTORY}"/catalog \
  --config="${PRESTO_CONFIG_DIRECTORY}/${CONFIG}".properties \
  --jvm-config="${PRESTO_CONFIG_DIRECTORY}"/jvm.config \
  --log-levels-file="${PRESTO_CONFIG_DIRECTORY}"/log.properties \
  --data-dir=/var/presto \
  "$@"
