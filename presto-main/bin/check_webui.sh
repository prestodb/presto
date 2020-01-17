#!/bin/bash

# Perform basic validations on the Web UI Javascript code
#
# 1. Validate that the generated files that have been checked in to the webapp folder are in sync
#    with the source.
# 2. Make sure there are no type checker warnings reported by Flow

set -euo pipefail

WEBUI_ROOT="$(pwd)/${BASH_SOURCE%/*}/../src/main/resources/webapp"

# Fail if running the command to generate the `dist` folder again produces different results.
# The lockfile is generated code also, and must go through the same process.

pushd $(mktemp -d)

cp "${WEBUI_ROOT}/src/yarn.lock" .
cp -r "${WEBUI_ROOT}/dist" .

yarn --cwd ${WEBUI_ROOT}/src/ install

if ! diff -u ${WEBUI_ROOT}/src/yarn.lock yarn.lock; then
    echo "Generated lockfile did not match checked-in version"
    echo "Refer to the root README.md for instructions"
    exit 1
fi

if ! diff -u ${WEBUI_ROOT}/dist dist; then
    echo "ERROR: Generated dist folder did not match checked-in version"
    echo "Refer to the root README.md for instructions on generating Web UI"
    exit 1
fi

popd


# Fail on flow warnings

if ! yarn --cwd ${WEBUI_ROOT}/src/ run flow; then
    echo "ERROR: Flow found type errors while performing static analysis"
    exit 1
fi
