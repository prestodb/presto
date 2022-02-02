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

# Propagate errors and improve debugging.
set -eufx -o pipefail

function install_aws-sdk-cpp {
  local NAME="aws-sdk-cpp"
  local AWS_SDK_VERSION="1.9.96"

  if [ -d "${NAME}" ]; then
    read -r -p "do you want to rebuild '${NAME}'? (y/n) " confirm
    if [[ "${confirm}" =~ ^[yy]$ ]]; then
      rm -rf "${NAME}"
    else
      return 0
    fi
  fi

  git clone --depth 1 --recurse-submodules --branch "${AWS_SDK_VERSION}" https://github.com/aws/aws-sdk-cpp.git "${NAME}"
  mkdir "${NAME}_build"
  cd "${NAME}_build" || exit
  cmake ../"${NAME}" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_BUILD_TYPE=Debug \
    -DBUILD_ONLY:STRING="s3;identity-management" \
    -DBUILD_SHARED_LIBS:BOOL=OFF \
    -DMINIMIZE_SIZE:BOOL=ON \
    -DENABLE_TESTING:BOOL=OFF \
    -DCMAKE_INSTALL_PREFIX="${DEPENDENCY_DIR}/install" \
    -GNinja \
    .
  local _ret=$?
  if [ $_ret -ne 0 ] ; then
     echo "cmake returned with exit code $_ret, aborting!" >&2
     return $_ret
  fi
  ninja
  ninja install
}

cd "${DEPENDENCY_DIR}" || exit
# aws-sdk-cpp missing dependencies
yum install -y curl-devel

install_aws-sdk-cpp
_ret=$?
if [ $_ret -eq 0 ] ; then
   echo "All deps for Velox adapters installed!"
fi
