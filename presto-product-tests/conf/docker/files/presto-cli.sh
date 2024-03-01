#!/bin/bash

set -euxo pipefail

java -jar /docker/volumes/presto-cli/presto-cli-executable.jar ${CLI_ARGUMENTS} "$@"
