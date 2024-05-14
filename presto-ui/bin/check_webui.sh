#!/bin/bash

# Perform basic validations on the Web UI JavaScript code
#
# 1. Validate that the generated files that have been checked in to the webapp folder are in sync
#    with the source.
# 2. Make sure there are no type checker warnings reported by Flow

set -euo pipefail

# https://stackoverflow.com/questions/69692842/error-message-error0308010cdigital-envelope-routinesunsupported
export NODE_OPTIONS=--openssl-legacy-provider

WEBUI_ROOT="$(pwd)/${BASH_SOURCE%/*}/../src"

# Make sure the generated lockfile is the same as the check-in version.

pushd $(mktemp -d)

cp "${WEBUI_ROOT}/yarn.lock" .

yarn --cwd ${WEBUI_ROOT}/ install

if ! diff -u ${WEBUI_ROOT}/yarn.lock yarn.lock; then
    echo "Generated lockfile did not match checked-in version"
    echo "Refer to the CONTRIBUTING.md for instructions"
    exit 1
fi

popd

# Fail on flow warnings

if ! yarn --cwd ${WEBUI_ROOT}/ run flow; then
    echo "ERROR: Flow found type errors while performing static analysis"
    exit 1
fi
