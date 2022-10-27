#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SCRIPT_DIR=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")
source $SCRIPT_DIR/release-centos-dockerfile/opt/common.sh

set -eE -o pipefail
trap 'error "Stage failed, exiting"; exit 5' SIGSTOP SIGINT SIGTERM SIGQUIT ERR

print_logo

export CPU_TARGET=${CPU_TARGET:-'avx'}
export BASE_IMAGE=${BASE_IMAGE:-'quay.io/centos/centos:stream8'}
export IMAGE_NAME=${IMAGE_NAME:-"presto/prestissimo-${CPU_TARGET}-centos"}
export IMAGE_TAG=${IMAGE_TAG:-"latest"}
export IMAGE_REGISTRY=${IMAGE_REGISTRY:-''}
export IMAGE_PUSH=${IMAGE_PUSH:-'0'}

export USER_FLAGS=${USER_FLAGS:-''}
export PRESTOCPP_ROOT_DIR=${PRESTOCPP_ROOT_DIR:-"$(readlink -f "$SCRIPT_DIR/..")"}
export PRESTODB_REPOSITORY=${PRESTODB_REPOSITORY:-"$(cd "${PRESTOCPP_ROOT_DIR}/.." && git config --get remote.origin.url)"}
export PRESTODB_CHECKOUT=${PRESTODB_CHECKOUT:-"$(cd "${PRESTOCPP_ROOT_DIR}/.." && git show -s --format="%H" HEAD)"}
BUILD_LOGS_FILEPATH="${SCRIPT_DIR}/$(date +%Y%m%d%H%M%S)-${USER}-${CPU_TARGET}-build-centos.log"
(
    prompt "Using build time variables:"
    prompt "------------"
    prompt "\tIMAGE_NAME=${IMAGE_NAME}"
    prompt "\tIMAGE_TAG=${IMAGE_TAG}"
    prompt "\tIMAGE_REGISTRY=${IMAGE_REGISTRY}"
    prompt "\tBASE_IMAGE=${BASE_IMAGE}"
    prompt "\tCPU_TARGET=${CPU_TARGET}"
    prompt "\tUSER_FLAGS=${USER_FLAGS}"
    prompt "------------"
    prompt "\tPRESTODB_REPOSITORY=${PRESTODB_REPOSITORY}"
    prompt "\tPRESTODB_CHECKOUT=${PRESTODB_CHECKOUT}"
    prompt "------------"
    prompt "Using build time computed variables:"
    prompt "\t[1/2] Base build image: ${BASE_IMAGE}"
    prompt "\t[2/2] Release image tag: ${IMAGE_REGISTRY}${IMAGE_NAME}:${IMAGE_TAG}"
    prompt "------------"
    prompt "Build logs: ${BUILD_LOGS_FILEPATH}"
    prompt "------------"
) 2>&1 |
tee "${BUILD_LOGS_FILEPATH}"
(
    prompt "[1/2] Preflight Git checks stage starting $(txt_yellow remote commit exisitance)" &&
    prompt "[1/2] Fetching remote repository" &&
    cd "${PRESTOCPP_ROOT_DIR}/.." > /dev/null &&
    git fetch --all > /dev/null &&
    prompt "[1/2] Checking if local hash is available on remote repository" &&
    git branch -r --contains $PRESTODB_CHECKOUT > /dev/null ||
    ( error '[1/2] Preflight stage failed, commit not found. Exiting.' && exit 1 )
) 2>&1 |
tee -a "${BUILD_LOGS_FILEPATH}"
(
    prompt "[2/2] Preflight CPU checks stage starting $(txt_yellow processor instructions)"
    error=0
    check=$(txt_green success)
    prompt "Velox build requires bellow CPU instructions to be available:"
    for flag in 'bmi|bmi1' 'bmi2' 'f16c';
    do
        echo $(cat /proc/cpuinfo) | grep -E -q " $flag " && check=$(txt_green success) || check=$(txt_red failed) error=1
        prompt "Testing (${flag}): \t$check"
    done
    prompt "Velox build suggest bellow CPU instructions to be available:"
    for flag in avx avx2 sse;
    do
        echo $(cat /proc/cpuinfo) | grep -q " $flag " && check=$(txt_green success) || check=$(txt_yellow failed)
        prompt "Testing (${flag}): \t$check"
    done
    [ $error -eq 0 ] || ( error 'Preflight checks failed, lack of CPU functionality. Exiting.' && exit 1 )
    prompt "[2/2] Preflight CPU checks $(txt_green success)"
) 2>&1 |
tee -a "${BUILD_LOGS_FILEPATH}"
(
    prompt "[1/1] Build stage starting $(txt_yellow ${IMAGE_REGISTRY}${IMAGE_NAME}:${IMAGE_TAG})" &&
    cd "${SCRIPT_DIR}" &&
    docker build $USER_FLAGS \
        --network=host \
        --build-arg http_proxy  \
        --build-arg https_proxy \
        --build-arg no_proxy    \
        --build-arg CPU_TARGET  \
        --build-arg BASE_IMAGE \
        --build-arg PRESTODB_REPOSITORY \
        --build-arg PRESTODB_CHECKOUT \
        --tag "${IMAGE_REGISTRY}${IMAGE_NAME}:${IMAGE_TAG}" \
        ./release-centos-dockerfile &&
    prompt "[1/1] Build finished" &&
    (
        [ "$IMAGE_PUSH" == "1" ] &&
        prompt "[1/1] Pushing image $(txt_yellow ${IMAGE_REGISTRY}${IMAGE_NAME}:${IMAGE_TAG})" &&
        docker push "${IMAGE_REGISTRY}${IMAGE_NAME}:${IMAGE_TAG}" || true
    ) &&
    prompt "[1/1] Build finished $(txt_green ${IMAGE_REGISTRY}${IMAGE_NAME}:${IMAGE_TAG})" ||
    ( error '[1/1] Build failed. Exiting' && exit 2 )
) 2>&1 |
tee -a "${BUILD_LOGS_FILEPATH}"

prompt "Prestissimo is ready for deployment"
prompt "Image tagged as: ${IMAGE_REGISTRY}${IMAGE_NAME}:${IMAGE_TAG}"
