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

# Folly must be built with the same compiler flags so that some low level types
# are the same size.
export COMPILER_FLAGS="-mavx2 -mfma -mavx -mf16c -masm=intel -mlzcnt"
BUILD_DIR=_build

set -eufx -o pipefail

# Enter build directory.
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

# Install all velox and folly dependencies.
sudo apt install -y \
  g++ \
  cmake \
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
  libfmt-dev \
  libprotobuf-dev \
  liblz4-dev \
  libzstd-dev \
  libre2-dev \
  libsnappy-dev \
  liblzo2-dev \
  protobuf-compiler

function install_folly {
  local NAME="folly"

  if [ -d "${NAME}" ]; then
    read -p "Do you want to rebuild '${NAME}'? (y/N) " confirm
    if [[ "${confirm}" =~ ^[Yy]$ ]]; then
      rm -rf "${NAME}"
    else
      return 0
    fi
  fi

  git clone https://github.com/facebook/folly.git "${NAME}"
  cd "${NAME}"
  cmake \
    -DCMAKE_CXX_FLAGS="$COMPILER_FLAGS" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_BUILD_TYPE=Debug \
    -GNinja \
    -DFOLLY_HAVE_INT128_T=1 \
    .
  ninja
  sudo checkinstall -y ninja install
}

install_folly

echo "All deps for Velox installed! Now try \"make\""
