#!/bin/bash

set -euxo pipefail

CONFIG="$1"

shift 1

PRESTO_CONFIG_DIRECTORY="/docker/volumes/conf/presto/etc"
CONFIG_PROPERTIES_LOCATION="${PRESTO_CONFIG_DIRECTORY}/${CONFIG}.properties"

if [[ ! -f "${CONFIG_PROPERTIES_LOCATION}" ]]; then
   echo "${CONFIG_PROPERTIES_LOCATION} does not exist" >&2
   exit 1
fi

# If we have an overriden JDK volume mount, use it
# This is set to /dev/null ignore
if [ -d /docker/volumes/overridejdk ]; then
  export JAVA_HOME=/docker/volumes/overridejdk
  export PATH=$JAVA_HOME/bin:$PATH
fi

echo "Starting Presto with java set to :"
java -version

# Check if Java version is 17
# This relies on the version string adhering to the format "17.x.xx"
JVM_CONFIG="${PRESTO_CONFIG_DIRECTORY}/jvm.config"
JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
if [ "$JAVA_VERSION" == "17" ]; then
    echo "Java version is 17, setting custom JVM config"
    JVM_CONFIG="${PRESTO_CONFIG_DIRECTORY}/jvm17.config"
fi

/docker/volumes/presto-server/bin/launcher  \
  -Dnode.id="${HOSTNAME}" \
  --etc-dir="${PRESTO_CONFIG_DIRECTORY}" \
  --jvm-config="${JVM_CONFIG}" \
  --config="${PRESTO_CONFIG_DIRECTORY}/${CONFIG}".properties \
  --data-dir=/var/presto \
  "$@"
