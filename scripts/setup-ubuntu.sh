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
SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
source $SCRIPTDIR/setup-helper-functions.sh

# Folly must be built with the same compiler flags so that some low level types
# are the same size.
CPU_TARGET="${CPU_TARGET:-avx}"
COMPILER_FLAGS=$(get_cxx_flags "$CPU_TARGET")
export COMPILER_FLAGS
FB_OS_VERSION=v2023.12.04.00
FMT_VERSION=10.1.1
NPROC=$(getconf _NPROCESSORS_ONLN)
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}
export CMAKE_BUILD_TYPE=Release

# Install all velox and folly dependencies.
# The is an issue on 22.04 where a version conflict prevents glog install,
# installing libunwind first fixes this.
apt update && apt install sudo
sudo --preserve-env apt update && sudo --preserve-env apt install -y libunwind-dev && \
  sudo --preserve-env apt install -y \
  g++ \
  cmake \
  ccache \
  ninja-build \
  checkinstall \
  git \
  libc-ares-dev \
  libcurl4-openssl-dev \
  libssl-dev \
  libboost-all-dev \
  libicu-dev \
  libdouble-conversion-dev \
  libgoogle-glog-dev \
  libbz2-dev \
  libgflags-dev \
  libgmock-dev \
  libevent-dev \
  liblz4-dev \
  libzstd-dev \
  libre2-dev \
  libsnappy-dev \
  libsodium-dev \
  libthrift-dev \
  liblzo2-dev \
  libelf-dev \
  libdwarf-dev \
  bison \
  flex \
  libfl-dev \
  tzdata \
  wget

function install_fmt {
  github_checkout fmtlib/fmt "${FMT_VERSION}"
  cmake_install -DFMT_TEST=OFF
}

function install_folly {
  github_checkout facebook/folly "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF -DFOLLY_HAVE_INT128_T=ON
}

function install_fizz {
  github_checkout facebookincubator/fizz "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF -S fizz
}

function install_wangle {
  github_checkout facebook/wangle "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF -S wangle
}

function install_mvfst {
  github_checkout facebook/mvfst "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF
}

function install_fbthrift {
  github_checkout facebook/fbthrift "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF
}

function install_conda {
  mkdir -p conda && cd conda
  ARCH=$(uname -m)
  
  if [ "$ARCH" != "x86_64" ] && [ "$ARCH" != "aarch64" ]; then
    echo "Unsupported architecture: $ARCH"
    exit 1
  fi
  
  wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-$ARCH.sh
  
  MINICONDA_PATH=/opt/miniconda-for-velox
  bash Miniconda3-latest-Linux-$ARCH.sh -b -p $MINICONDA_PATH
}


function install_velox_deps {
  run_and_time install_fmt
  run_and_time install_folly
  run_and_time install_fizz
  run_and_time install_wangle
  run_and_time install_mvfst
  run_and_time install_fbthrift
  run_and_time install_conda
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

echo "All dependencies for Velox installed!"
