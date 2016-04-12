#!/bin/bash

set -e;

# http://stackoverflow.com/questions/3572030/bash-script-absolute-path-with-osx
function absolutepath() {
    [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
}

SCRIPT_DIR=$(dirname $(absolutepath "$0"))
PRODUCT_TESTS_ROOT="${SCRIPT_DIR}/.."
REPORT_DIR="${PRODUCT_TESTS_ROOT}/target/test-reports"

rm -rf "${REPORT_DIR}"
mkdir -p "${REPORT_DIR}"

source "${PRODUCT_TESTS_ROOT}/target/classes/presto.env"

set +e
java "-Djava.util.logging.config.file=${PRODUCT_TESTS_ROOT}/conf/tempto/logging.properties" \
    ${PRODUCT_TESTS_JVM_OPTIONS} \
    -jar "${PRODUCT_TESTS_ROOT}/target/presto-product-tests-${PRESTO_VERSION}-executable.jar" \
    --report-dir "${REPORT_DIR}" "$@"
EXIT_CODE=$?
set -e

# tests are run in docker container as a root
# make it possible to remove the report dir by the future builds
chmod -R 777 "${REPORT_DIR}"

exit ${EXIT_CODE}
