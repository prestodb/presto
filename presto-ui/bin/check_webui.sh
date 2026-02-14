#!/bin/bash

# Perform basic validations on the Web UI JavaScript code
#
# 1. Validate that the generated files that have been checked in to the webapp folder are in sync
#    with the source.
# 2. Make sure there are no type checker warnings reported by TypeScript
# 3. Run ESLint to check for code quality issues
# 4. Run Jest tests to ensure code functionality

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

# Fail on typescript warnings

if ! yarn --cwd ${WEBUI_ROOT}/ run typecheck; then
    echo "ERROR: Typescript type errors found"
    exit 1
fi

# Fail on eslint errors

if ! yarn --cwd ${WEBUI_ROOT}/ run lint --quiet; then
    echo "ERROR: ESlint errors found"
    exit 1
fi

# Fail on test failures only (coverage thresholds disabled for now)

if ! yarn --cwd ${WEBUI_ROOT}/ run test:ci; then
    echo "ERROR: Tests failed"
    exit 1
fi