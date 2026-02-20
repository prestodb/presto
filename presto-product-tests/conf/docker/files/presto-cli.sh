#!/bin/bash

set -euxo pipefail

/docker/volumes/overridejdk/bin/java -version
/docker/volumes/overridejdk/bin/java -jar /docker/volumes/presto-cli/presto-cli-executable.jar ${CLI_ARGUMENTS} "$@"
