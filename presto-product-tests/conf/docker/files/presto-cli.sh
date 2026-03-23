#!/bin/bash

set -euxo pipefail

echo "java --version"
echo $(java --version)
export JAVA_HOME=/docker/volumes/overridejdk
echo "/docker/volumes/overridejdk/bin/java --version"
echo $(/docker/volumes/overridejdk/bin/java --version)
/docker/volumes/overridejdk/bin/java -jar /docker/volumes/presto-cli/presto-cli-executable.jar ${CLI_ARGUMENTS} "$@"
