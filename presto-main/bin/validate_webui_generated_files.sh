#!/bin/bash
#
# Validate that the generated files that have been checked in to the webapp folder are in sync
# with the source.
#
# This is done by running the command to generate the `dist` folder again and comparing results.
# The lockfile is generated code also, and must go through the same process.

set -euo pipefail

WEBUI_ROOT="$(pwd)/${BASH_SOURCE%/*}/../src/main/resources/webapp"

pushd $(mktemp -d)

cp "${WEBUI_ROOT}/src/yarn.lock" .
cp -r "${WEBUI_ROOT}/dist" .

yarn install --cwd ${WEBUI_ROOT}/src/

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
