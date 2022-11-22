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
export PRESTOCPP_ROOT_DIR=${PRESTOCPP_ROOT_DIR:-"$(readlink -f "$SCRIPT_DIR/..")"}
export REPOSITORY_ROOT=${REPOSITORY_ROOT:-"$(readlink -f "$PRESTOCPP_ROOT_DIR/..")"}
source $SCRIPT_DIR/release-centos-dockerfile/opt/common.sh

set -eE -o pipefail
trap 'error "Stage failed, exiting"; exit 5' SIGSTOP SIGINT SIGTERM SIGQUIT ERR

if [ "$(uname)" == "Darwin" ]; then
    export CPU_TARGET=${CPU_TARGET:-'arm64'}
else
    export CPU_TARGET=${CPU_TARGET:-'avx'}
fi

export IMAGE_CACHE_REGISTRY=${IMAGE_CACHE_REGISTRY:-'quay.io/centos/'}
export IMAGE_BASE_NAME=${IMAGE_BASE_NAME:-'centos:stream8'}
export BASE_IMAGE=${BASE_IMAGE:-"${IMAGE_CACHE_REGISTRY}${IMAGE_BASE_NAME}"}
export IMAGE_NAME=${IMAGE_NAME:-"presto/prestissimo-${CPU_TARGET}-centos"}
export IMAGE_TAG=${IMAGE_TAG:-"latest"}
export IMAGE_REGISTRY=${IMAGE_REGISTRY:-''}
export IMAGE_PUSH=${IMAGE_PUSH:-'0'}

export USER_FLAGS=${USER_FLAGS:-''}
export PRESTODB_REPOSITORY=${PRESTODB_REPOSITORY:-"$(git -C "${REPOSITORY_ROOT}" config --get remote.origin.url)"}
export PRESTODB_CHECKOUT=${PRESTODB_CHECKOUT:-"$(git -C "${REPOSITORY_ROOT}" show -s --format="%H" HEAD)"}

export DOCKER_BUILDKIT=1
BUILD_LOGS_FILEPATH="${SCRIPT_DIR}/$(date +%Y%m%d%H%M%S)-${USER}-${CPU_TARGET}-build-centos.log"

(
    prompt "Using build time variables:"
    prompt "------------"
    prompt "\tCPU_TARGET=${CPU_TARGET}"
    prompt "\tUSER_FLAGS=${USER_FLAGS}"
    prompt "------------"
    prompt "\tIMAGE_CACHE_REGISTRY=${IMAGE_CACHE_REGISTRY}"
    prompt "\tIMAGE_BASE_NAME=${IMAGE_BASE_NAME}"
    prompt "\tBASE_IMAGE=${BASE_IMAGE}"
    prompt "------------"
    prompt "\tIMAGE_NAME=${IMAGE_NAME}"
    prompt "\tIMAGE_TAG=${IMAGE_TAG}"
    prompt "\tIMAGE_REGISTRY=${IMAGE_REGISTRY}"
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
) 2>&1 | tee "${BUILD_LOGS_FILEPATH}"

(
    prompt "[1/2] Preflight Git checks stage starting $(txt_yellow remote commit exisitance)" &&
    prompt "[1/2] Fetching remote repository" &&
    git -C "${REPOSITORY_ROOT}" fetch --all > /dev/null &&
    prompt "[1/2] Checking if local hash is available on remote repository" &&
    git -C "${REPOSITORY_ROOT}" branch -r --contains $PRESTODB_CHECKOUT > /dev/null ||
    ( error '[1/2] Preflight stage failed, local commit not found on remote. Trying to ignore. Press CTRL+C to cancel.' && sleep 3 )
) 2>&1 | tee -a "${BUILD_LOGS_FILEPATH}"

! (
    prompt "[2/2] Preflight CPU checks stage starting $(txt_yellow processor instructions)"
    check=$(txt_green success)
    prompt "Velox build requires/suggest below CPU instructions to be available:"
    for flag in 'bmi|bmi1' 'bmi2' 'f16c' 'avx' 'avx2' 'sse'
    do
        if [ "$(uname)" == "Darwin" ]; then
            echo $(/usr/sbin/sysctl -n machdep.cpu.features machdep.cpu.leaf7_features) | grep -E -q " $flag " && check=$(txt_green success) || check=$(txt_red failed)
        else
            echo $(cat /proc/cpuinfo) | grep -E -q " $flag " && check=$(txt_green success) || check=$(txt_red failed)
        fi
        prompt "Testing (${flag}): \t$check"
    done
    prompt "[2/2] Preflight CPU checks $(txt_green success)"
) 2>&1 | tee -a "${BUILD_LOGS_FILEPATH}"

(
    prompt "[1/1] Build stage starting $(txt_yellow ${IMAGE_REGISTRY}${IMAGE_NAME}:${IMAGE_TAG})"
    if [ "$DOCKER_BUILDKIT" == "1" ]; then
        eval $(ssh-agent) 2>1 &&
        ! ssh-add $HOME/.ssh/* 2>1 &&
        export USER_FLAGS="${USER_FLAGS} --progress tty --ssh default=${SSH_AUTH_SOCK} " ||
        export DOCKER_BUILDKIT=0
    fi
    if [ "$DOCKER_BUILDKIT" == "0" ]; then
        sed -i 's/--mount=type=ssh//g' "${SCRIPT_DIR}/release-centos-dockerfile/Dockerfile"
    fi

    cd "${SCRIPT_DIR}" &&
    docker build $USER_FLAGS \
        --network=host \
        --build-arg http_proxy  \
        --build-arg https_proxy \
        --build-arg ftp_proxy   \
        --build-arg all_proxy   \
        --build-arg no_proxy    \
        --build-arg CPU_TARGET  \
        --build-arg BASE_IMAGE  \
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
) 2>&1 | tee -a "${BUILD_LOGS_FILEPATH}"

prompt "Prestissimo is ready for deployment"
prompt "Image tagged as: ${IMAGE_REGISTRY}${IMAGE_NAME}:${IMAGE_TAG}"
