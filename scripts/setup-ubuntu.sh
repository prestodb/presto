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

# This script documents setting up a Centos8 host for Velox
# development.  Running it should make you ready to compile.
#
# Environment variables:
# * INSTALL_PREREQUISITES="N": Skip installation of packages for build.
# * PROMPT_ALWAYS_RESPOND="n": Automatically respond to interactive prompts.
#     Use "n" to never wipe directories.
#
# You can also run individual functions below by specifying them as arguments:
# $ scripts/setup-ubuntu.sh install_googletest install_fmt
#

# Minimal setup for Ubuntu 20.04.
set -eufx -o pipefail
SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
source $SCRIPTDIR/setup-helper-functions.sh

# Folly must be built with the same compiler flags so that some low level types
# are the same size.
CPU_TARGET="${CPU_TARGET:-avx}"
COMPILER_FLAGS=$(get_cxx_flags "$CPU_TARGET")
export COMPILER_FLAGS
FB_OS_VERSION=v2024.02.26.00
FMT_VERSION=10.1.1
BOOST_VERSION=boost-1.84.0
NPROC=$(getconf _NPROCESSORS_ONLN)
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}
export CMAKE_BUILD_TYPE=Release
SUDO="${SUDO:-"sudo --preserve-env"}"

# Install packages required for build.
function install_build_prerequisites {
  ${SUDO} apt update
  # The is an issue on 22.04 where a version conflict prevents glog install,
  # installing libunwind first fixes this.
  ${SUDO} apt install -y libunwind-dev
  ${SUDO} apt install -y \
    build-essential \
    cmake \
    ccache \
    ninja-build \
    checkinstall \
    git \
    wget
}

# Install packages required for build.
function install_velox_deps_from_apt {
  ${SUDO} apt update
  ${SUDO} apt install -y \
    libc-ares-dev \
    libcurl4-openssl-dev \
    libssl-dev \
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
    tzdata
}

function install_fmt {
  github_checkout fmtlib/fmt "${FMT_VERSION}"
  cmake_install -DFMT_TEST=OFF
}

function install_boost {
  github_checkout boostorg/boost "${BOOST_VERSION}" --recursive
  ./bootstrap.sh --prefix=/usr/local
  ${SUDO} ./b2 "-j$(nproc)" -d0 install threading=multi
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
  cmake_install -Denable_tests=OFF -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF
}

function install_conda {
  MINICONDA_PATH=/opt/miniconda-for-velox
  if [ -e ${MINICONDA_PATH} ]; then
    echo "File or directory already exists: ${MINICONDA_PATH}"
    return
  fi
  ARCH=$(uname -m)
  if [ "$ARCH" != "x86_64" ] && [ "$ARCH" != "aarch64" ]; then
    echo "Unsupported architecture: $ARCH"
    exit 1
  fi
  
  mkdir -p conda && cd conda
  wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-$ARCH.sh
  bash Miniconda3-latest-Linux-$ARCH.sh -b -p $MINICONDA_PATH
}


function install_velox_deps {
  run_and_time install_velox_deps_from_apt
  run_and_time install_fmt
  run_and_time install_boost
  run_and_time install_folly
  run_and_time install_fizz
  run_and_time install_wangle
  run_and_time install_mvfst
  run_and_time install_fbthrift
  run_and_time install_conda
}

function install_apt_deps {
  install_build_prerequisites
  install_velox_deps_from_apt
}

# For backward compatibility, invoke install_apt_deps
(return 2> /dev/null) && install_apt_deps && return # If script was sourced, don't run commands.

(
  if [[ $# -ne 0 ]]; then
    for cmd in "$@"; do
      run_and_time "${cmd}"
    done
    echo "All specified dependencies installed!"
  else
    if [ "${INSTALL_PREREQUISITES:-Y}" == "Y" ]; then
      echo "Installing build dependencies"
      run_and_time install_build_prerequisites
    else
      echo "Skipping installation of build dependencies since INSTALL_PREREQUISITES is not set"
    fi
    install_velox_deps
    echo "All dependencies for Velox installed!"
  fi
)

