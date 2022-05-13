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

# github_checkout $REPO $VERSION $GIT_CLONE_PARAMS clones or re-uses an existing clone of the
# specified repo, checking out the requested version.
function github_checkout {
  local REPO=$1
  shift
  local VERSION=$1
  shift
  local GIT_CLONE_PARAMS=$@
  local DIRNAME=$(basename $REPO)
  echo $DIRNAME
  cd "${DEPENDENCY_DIR}"
  if [ -z "${DIRNAME}" ]; then
    echo "Failed to get repo name from ${REPO}"
    exit 1
  fi
  if [ -d "${DIRNAME}" ] && prompt "${DIRNAME} already exists. Delete?"; then
    rm -rf "${DIRNAME}"
  fi
  if [ ! -d "${DIRNAME}" ]; then
    git clone -q -b $VERSION $GIT_CLONE_PARAMS "https://github.com/${REPO}.git"
  fi
  cd "${DIRNAME}"
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
    -DCMAKE_CXX_FLAGS="-mavx2 -mfma -mavx -mf16c -masm=intel -mlzcnt" \
    -DBUILD_TESTING=OFF \
    "$@"
  ninja -C "${BINARY_DIR}" install
}

