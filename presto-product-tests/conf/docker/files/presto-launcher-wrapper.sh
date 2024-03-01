#!/bin/bash

set -euo pipefail

CONFIG="$1"

shift 1

PRESTO_CONFIG_DIRECTORY="/docker/volumes/conf/presto/etc"
CONFIG_PROPERTIES_LOCATION="${PRESTO_CONFIG_DIRECTORY}/${CONFIG}.properties"

if [[ ! -f "${CONFIG_PROPERTIES_LOCATION}" ]]; then
   echo "${CONFIG_PROPERTIES_LOCATION} does not exist" >&2
   exit 1
fi

/docker/volumes/presto-server/bin/launcher \
  -Dnode.id="${HOSTNAME}" \
  --etc-dir="${PRESTO_CONFIG_DIRECTORY}" \
  --config="${PRESTO_CONFIG_DIRECTORY}/${CONFIG}".properties \
  --data-dir=/var/presto \
  "$@"
