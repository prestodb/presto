#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
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

# Minimal setup for Ubuntu 20.04.
set -eufx -o pipefail

# Folly must be built with the same compiler flags so that some low level types
# are the same size.
export COMPILER_FLAGS="-mavx2 -mfma -mavx -mf16c -masm=intel -mlzcnt"
FB_OS_VERSION=v2022.03.14.00
NPROC=$(getconf _NPROCESSORS_ONLN)
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}

# Install all velox and folly dependencies.
sudo apt install -y \
  g++ \
  cmake \
  ccache \
  ninja-build \
  checkinstall \
  git \
  libssl-dev \
  libboost-all-dev \
  libdouble-conversion-dev \
  libgoogle-glog-dev \
  libbz2-dev \
  libgflags-dev \
  libgtest-dev \
  libgmock-dev \
  libevent-dev \
  libprotobuf-dev \
  liblz4-dev \
  libzstd-dev \
  libre2-dev \
  libsnappy-dev \
  liblzo2-dev \
  protobuf-compiler

function run_and_time {
  time "$@"
  { echo "+ Finished running $*"; } 2> /dev/null
}

function prompt {
  (
    while true; do
      local input="${PROMPT_ALWAYS_RESPOND:-}"
      echo -n "$(tput bold)$* [Y, n]$(tput sgr0) "
      [[ -z "${input}" ]] && read input
      if [[ "${input}" == "Y" || "${input}" == "y" || "${input}" == "" ]]; then
        return 0
      elif [[ "${input}" == "N" || "${input}" == "n" ]]; then
        return 1
      fi
    done
  ) 2> /dev/null
}

# github_checkout $REPO $VERSION clones or re-uses an existing clone of the
# specified repo, checking out the requested version.
function github_checkout {
  local REPO=$1
  local VERSION=$2
  local DIRNAME=$(basename "$1")

  cd "${DEPENDENCY_DIR}"
  if [ -z "${DIRNAME}" ]; then
    echo "Failed to get repo name from $1"
    exit 1
  fi
  if [ -d "${DIRNAME}" ] && prompt "${DIRNAME} already exists. Delete?"; then
    rm -rf "${DIRNAME}"
  fi
  if [ ! -d "${DIRNAME}" ]; then
    git clone -q "https://github.com/${REPO}.git"
  fi
  cd "${DIRNAME}"
  git fetch -q
  git checkout "${VERSION}"
}

function cmake_install {
  local NAME=$(basename "$(pwd)")
  local BINARY_DIR=_build
  if [ -d "${BINARY_DIR}" ] && prompt "Do you want to rebuild ${NAME}?"; then
    rm -rf "${BINARY_DIR}"
  fi
  mkdir -p "${BINARY_DIR}"

  # CMAKE_POSITION_INDEPENDENT_CODE is required so that Velox can be built into dynamic libraries \
  cmake -Wno-dev -B"${BINARY_DIR}" \
    -GNinja \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_CXX_STANDARD=17 \
    "${INSTALL_PREFIX+-DCMAKE_PREFIX_PATH=}${INSTALL_PREFIX-}" \
    "${INSTALL_PREFIX+-DCMAKE_INSTALL_PREFIX=}${INSTALL_PREFIX-}" \
    -DCMAKE_CXX_FLAGS="${COMPILER_FLAGS}" \
    -DBUILD_TESTING=OFF \
    "$@"
  ninja -C "${BINARY_DIR}" install
}

function install_fmt {
  github_checkout fmtlib/fmt 8.0.0
  cmake_install -DFMT_TEST=OFF
}

function install_folly {
  github_checkout facebook/folly "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF
}

function install_velox_deps {
  run_and_time install_fmt
  run_and_time install_folly
}

(return 2> /dev/null) && return # If script was sourced, don't run commands.

(
  if [[ $# -ne 0 ]]; then
    for cmd in "$@"; do
      run_and_time "${cmd}"
    done
  else
    install_velox_deps
  fi
)

echo "All deps for Velox installed! Now try \"make\""
