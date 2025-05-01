#!/bin/bash

set -euo pipefail

# If we have an overriden JDK volume mount, use it
# This is set to /dev/null ignore
if [ -d /docker/volumes/overridejdk ]; then
  CONTAINER_JAVA_HOME=$JAVA_HOME
  export JAVA_HOME=/docker/volumes/overridejdk
  export PATH=$JAVA_HOME/bin:$PATH
fi

echo "Starting TemptoProductTestRunner with java set to :"
java -version

# Check if Java version is 17
# This relies on the version string adhering to the format "17.x.xx"
#JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
#if [ "$JAVA_VERSION" == "17" ]; then
#    echo "Java version is 17, setting custom JVM config"
#    JVM_CONFIG="${PRESTO_CONFIG_DIRECTORY}/jvm17.config"
#fi

DOCKER_TEMPTO_CONF_DIR="/docker/volumes/conf/tempto"
TEMPTO_CONFIG_FILES="tempto-configuration.yaml" # this comes from classpath
TEMPTO_CONFIG_FILES="${TEMPTO_CONFIG_FILES},${DOCKER_TEMPTO_CONF_DIR}/tempto-configuration-for-docker-default.yaml"

if ! test -z ${TEMPTO_PROFILE_CONFIG_FILE:-}; then
  TEMPTO_CONFIG_FILES="${TEMPTO_CONFIG_FILES},${TEMPTO_PROFILE_CONFIG_FILE}"
fi

if ! test -z ${TEMPTO_EXTRA_CONFIG_FILE:-}; then
  TEMPTO_CONFIG_FILES="${TEMPTO_CONFIG_FILES},${TEMPTO_EXTRA_CONFIG_FILE}"
fi

java \
  `#-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007` \
  "-Djava.util.logging.config.file=${DOCKER_TEMPTO_CONF_DIR}/logging.properties" \
  -Duser.timezone=Asia/Kathmandu \
  -cp "/docker/volumes/jdbc/driver.jar:/docker/volumes/presto-product-tests/presto-product-tests-executable.jar" \
  com.facebook.presto.tests.TemptoProductTestRunner \
  --report-dir "/docker/volumes/test-reports" \
  --config "${TEMPTO_CONFIG_FILES}" \
  "$@"
