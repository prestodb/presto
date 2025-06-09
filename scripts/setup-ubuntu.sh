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

# This script documents setting up a Ubuntu host for Velox
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

# Minimal setup for Ubuntu 22.04.
set -eufx -o pipefail
SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
source $SCRIPTDIR/setup-common.sh

SUDO="${SUDO:-"sudo --preserve-env"}"
USE_CLANG="${USE_CLANG:-false}"
export INSTALL_PREFIX=${INSTALL_PREFIX:-"/usr/local"}
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)/deps-download}
VERSION=$(cat /etc/os-release | grep VERSION_ID)
PYTHON_VENV=${PYTHON_VENV:-"${SCRIPTDIR}/../.venv"}

# On Ubuntu 20.04 dependencies need to be built using gcc11.
# On Ubuntu 22.04 gcc11 is already the system gcc installed.
if [[ ${VERSION} =~ "20.04" ]]; then
  export CC=/usr/bin/gcc-11
  export CXX=/usr/bin/g++-11
fi

function install_clang15 {
  if [[ ! ${VERSION} =~ "22.04" && ! ${VERSION} =~ "24.04" ]]; then
    echo "Warning: using the Clang configuration is for Ubuntu 22.04 and 24.04. Errors might occur."
  fi
  CLANG_PACKAGE_LIST=clang-15
  if [[ ${VERSION} =~ "22.04" ]]; then
    CLANG_PACKAGE_LIST="${CLANG_PACKAGE_LIST} gcc-12 g++-12 libc++-12-dev"
  fi
  ${SUDO} apt install ${CLANG_PACKAGE_LIST} -y
}

# For Ubuntu 20.04 we need add the toolchain PPA to get access to gcc11.
function install_gcc11_if_needed {
  if [[ ${VERSION} =~ "20.04" ]]; then
    ${SUDO} add-apt-repository ppa:ubuntu-toolchain-r/test -y
    ${SUDO} apt update
    ${SUDO} apt install gcc-11 g++-11 -y
  fi
}

# Install packages required for build.
function install_build_prerequisites {
  ${SUDO} apt update
  # The is an issue on 22.04 where a version conflict prevents glog install,
  # installing libunwind first fixes this.
  ${SUDO} apt install -y libunwind-dev
  ${SUDO} apt install -y \
    build-essential \
    python3-pip \
    ccache \
    curl \
    ninja-build \
    checkinstall \
    git \
    pkg-config \
    libtool \
    wget

  if [ ! -f ${PYTHON_VENV}/pyvenv.cfg ]; then
    echo "Creating Python Virtual Environment at ${PYTHON_VENV}"
    python3 -m venv ${PYTHON_VENV}
  fi
  source ${PYTHON_VENV}/bin/activate;
  # Install to /usr/local to make it available to all users.
  ${SUDO} pip3 install cmake==3.28.3

  install_gcc11_if_needed

  if [[ ${USE_CLANG} != "false" ]]; then
    install_clang15
  fi

}

# Install packages required to fix format
function install_format_prerequisites {
  pip3 install regex
  ${SUDO} apt install -y \
    clang-format \
    cmake-format
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
    libgtest-dev \
    libgmock-dev \
    libevent-dev \
    liblz4-dev \
    libzstd-dev \
    libre2-dev \
    libsnappy-dev \
    libsodium-dev \
    liblzo2-dev \
    libelf-dev \
    libdwarf-dev \
    bison \
    flex \
    libfl-dev \
    tzdata \
    libxxhash-dev
}

function install_conda {
  MINICONDA_PATH="${HOME:-/opt}/miniconda-for-velox"
  if [ -e ${MINICONDA_PATH} ]; then
    echo "File or directory already exists: ${MINICONDA_PATH}"
    return
  fi
  ARCH=$(uname -m)
  if [ "$ARCH" != "x86_64" ] && [ "$ARCH" != "aarch64" ]; then
    echo "Unsupported architecture: $ARCH"
    exit 1
  fi
  (
    mkdir -p conda && cd conda
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-$ARCH.sh -O Miniconda3-latest-Linux-$ARCH.sh
    bash Miniconda3-latest-Linux-$ARCH.sh -b -p $MINICONDA_PATH
  )
}

function install_cuda {
  # See https://developer.nvidia.com/cuda-downloads
  local arch=$(uname -m)
  local os_ver

  if [[ ${VERSION} =~ "24.04" ]]; then
    os_ver="ubuntu2404"
  elif [[ ${VERSION} =~ "22.04" ]]; then
    os_ver="ubuntu2204"
  elif [[ ${VERSION} =~ "20.04" ]]; then
    os_ver="ubuntu2004"
  else
    echo "Unsupported Ubuntu version: ${VERSION}" >&2
    return 1
  fi

  local cuda_repo
  if [[ "$arch" == "x86_64" ]]; then
    cuda_repo="${os_ver}/x86_64"
  elif [[ "$arch" == "aarch64" ]]; then
    cuda_repo="${os_ver}/sbsa"
  else
    echo "Unsupported architecture: $arch" >&2
    return 1
  fi

  if ! dpkg -l cuda-keyring 1>/dev/null; then
    wget https://developer.download.nvidia.com/compute/cuda/repos/${cuda_repo}/cuda-keyring_1.1-1_all.deb
    $SUDO dpkg -i cuda-keyring_1.1-1_all.deb
    rm cuda-keyring_1.1-1_all.deb
    $SUDO apt update
  fi

  local dashed="$(echo $1 | tr '.' '-')"
  $SUDO apt install -y \
    cuda-compat-$dashed \
    cuda-driver-dev-$dashed \
    cuda-minimal-build-$dashed \
    cuda-nvrtc-dev-$dashed \
    libcufile-dev-$dashed \
    libnuma1
}

function install_s3 {
  install_aws_deps

  local MINIO_OS="linux"
  install_minio ${MINIO_OS}
}

function install_gcs {
  # Dependencies of GCS, probably a workaround until the docker image is rebuilt
  apt install -y --no-install-recommends libc-ares-dev libcurl4-openssl-dev
  install_gcs-sdk-cpp
}

function install_abfs {
  # Dependencies of Azure Storage Blob cpp
  apt install -y openssl libxml2-dev
  install_azure-storage-sdk-cpp
}

function install_hdfs {
  apt install -y --no-install-recommends libxml2-dev libgsasl7-dev uuid-dev openjdk-8-jdk
  install_hdfs_deps
}

function install_adapters {
  run_and_time install_s3
  run_and_time install_gcs
  run_and_time install_abfs
  run_and_time install_hdfs
}

function install_velox_deps {
  run_and_time install_velox_deps_from_apt
  run_and_time install_fmt
  run_and_time install_protobuf
  run_and_time install_boost
  run_and_time install_fast_float
  run_and_time install_folly
  run_and_time install_fizz
  run_and_time install_wangle
  run_and_time install_mvfst
  run_and_time install_fbthrift
  run_and_time install_conda
  run_and_time install_duckdb
  run_and_time install_stemmer
  run_and_time install_thrift
  run_and_time install_arrow
  run_and_time install_xsimd
  run_and_time install_simdjson
  run_and_time install_geos
}

function install_apt_deps {
  install_build_prerequisites
  install_format_prerequisites
  install_velox_deps_from_apt
}

(return 2> /dev/null) && return # If script was sourced, don't run commands.

(
  if [[ ${USE_CLANG} != "false" ]]; then
    export CC=/usr/bin/clang-15
    export CXX=/usr/bin/clang++-15
  fi
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
    if [[ ${USE_CLANG} != "false" ]]; then
      echo "To use clang for the Velox build set the CC and CXX environment variables in your session."
      echo "  export CC=/usr/bin/clang-15"
      echo "  export CXX=/usr/bin/clang++-15"
    fi
    if [[ ${VERSION} =~ "20.04" && ${USE_CLANG} == "false" ]]; then
      echo "To build Velox gcc-11/g++11 is required. Set the CC and CXX environment variables in your session."
      echo "  export CC=/usr/bin/gcc-11"
      echo "  export CXX=/usr/bin/g++-11"
    fi
  fi
)
