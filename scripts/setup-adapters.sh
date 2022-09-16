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

SCRIPTDIR=$(dirname "$0")
source $SCRIPTDIR/setup-helper-functions.sh

# Propagate errors and improve debugging.
set -eufx -o pipefail

SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
source $SCRIPTDIR/setup-helper-functions.sh
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}

function install_aws-sdk-cpp {
  local AWS_REPO_NAME="aws/aws-sdk-cpp"
  local AWS_SDK_VERSION="1.9.96"

  github_checkout $AWS_REPO_NAME $AWS_SDK_VERSION --depth 1 --recurse-submodules
  cmake_install -DCMAKE_BUILD_TYPE=Debug -DBUILD_SHARED_LIBS:BOOL=OFF -DMINIMIZE_SIZE:BOOL=ON -DENABLE_TESTING:BOOL=OFF -DBUILD_ONLY:STRING="s3;identity-management" -DCMAKE_INSTALL_PREFIX="${DEPENDENCY_DIR}/install"
}

function install_libhdfs3 {
  github_checkout apache/hawq master
  cd $DEPENDENCY_DIR/hawq/depends/libhdfs3
  if [[ "$OSTYPE" == darwin* ]]; then
     sed -i '' -e "/FIND_PACKAGE(GoogleTest REQUIRED)/d" ./CMakeLists.txt
     sed -i '' -e "s/dumpversion/dumpfullversion/" ./CMakeLists.txt
  fi

  if [[ "$OSTYPE" == linux-gnu* ]]; then
    sed -i "/FIND_PACKAGE(GoogleTest REQUIRED)/d" ./CMakeLists.txt
    sed -i "s/dumpversion/dumpfullversion/" ./CMake/Platform.cmake
  fi
  cmake_install
}

function install_protobuf {
  wget https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protobuf-all-21.4.tar.gz
  tar -xzf protobuf-all-21.4.tar.gz --no-same-owner
  cd protobuf-21.4
  ./configure --prefix=/usr
  make "-j$(nproc)"
  make install
  ldconfig
}

DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}
cd "${DEPENDENCY_DIR}" || exit
# aws-sdk-cpp missing dependencies

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
   yum -y install libxml2-devel libgsasl-devel libuuid-devel
fi

if [[ "$OSTYPE" == darwin* ]]; then
   brew install libxml2 gsasl
fi
install_protobuf
install_aws-sdk-cpp
install_libhdfs3

_ret=$?
if [ $_ret -eq 0 ] ; then
   echo "All deps for Velox adapters installed!"
fi
