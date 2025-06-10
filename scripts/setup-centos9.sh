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

# This script documents setting up a Centos9 host for Velox
# development.  Running it should make you ready to compile.
#
# Environment variables:
# * INSTALL_PREREQUISITES="N": Skip installation of packages for build.
# * PROMPT_ALWAYS_RESPOND="n": Automatically respond to interactive prompts.
#     Use "n" to never wipe directories.
#
# You can also run individual functions below by specifying them as arguments:
# $ scripts/setup-centos9.sh install_googletest install_fmt
#

set -efx -o pipefail
# Some of the packages must be build with the same compiler flags
# so that some low level types are the same size. Also, disable warnings.
SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
source $SCRIPTDIR/setup-common.sh
export CXXFLAGS=$(get_cxx_flags) # Used by boost.
export CFLAGS=${CXXFLAGS//"-std=c++17"/} # Used by LZO.
export COMPILER_FLAGS=${CXXFLAGS}
SUDO="${SUDO:-""}"
USE_CLANG="${USE_CLANG:-false}"
export INSTALL_PREFIX=${INSTALL_PREFIX:-"/usr/local"}
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)/deps-download}

function dnf_install {
  dnf install -y -q --setopt=install_weak_deps=False "$@"
}

function install_clang15 {
  dnf_install clang15 gcc-toolset-13-libatomic-devel
}

# Install packages required for build.
function install_build_prerequisites {
  dnf update -y
  dnf_install epel-release dnf-plugins-core # For ccache, ninja
  dnf config-manager --set-enabled crb
  dnf update -y
  dnf_install ninja-build cmake ccache gcc-toolset-12 git wget which
  dnf_install autoconf automake python3-devel pip libtool

  pip install cmake==3.30.4

  if [[ ${USE_CLANG} != "false" ]]; then
    install_clang15
  fi
}

# Install dependencies from the package managers.
function install_velox_deps_from_dnf {
  dnf_install libevent-devel \
    openssl-devel re2-devel libzstd-devel lz4-devel double-conversion-devel \
    libdwarf-devel elfutils-libelf-devel curl-devel libicu-devel bison flex \
    libsodium-devel zlib-devel gtest-devel gmock-devel xxhash-devel

  # install sphinx for doc gen
  pip install sphinx sphinx-tabs breathe sphinx_rtd_theme
}

function install_conda {
  dnf_install conda
}

function install_gflags {
  # Remove an older version if present.
  dnf remove -y gflags
  wget_and_untar https://github.com/gflags/gflags/archive/${GFLAGS_VERSION}.tar.gz gflags
  cmake_install_dir gflags -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DBUILD_gflags_LIB=ON -DLIB_SUFFIX=64
}

function install_cuda {
  # See https://developer.nvidia.com/cuda-downloads
  local arch=$(uname -m)
  local repo_url

  if [[ "$arch" == "x86_64" ]]; then
    repo_url="https://developer.download.nvidia.com/compute/cuda/repos/rhel9/x86_64/cuda-rhel9.repo"
  elif [[ "$arch" == "aarch64" ]]; then
    # Using SBSA (Server Base System Architecture) repository for ARM64 servers
    repo_url="https://developer.download.nvidia.com/compute/cuda/repos/rhel9/sbsa/cuda-rhel9.repo"
  else
    echo "Unsupported architecture: $arch" >&2
    return 1
  fi

  dnf config-manager --add-repo "$repo_url"
  local dashed="$(echo $1 | tr '.' '-')"
  dnf install -y \
    cuda-compat-$dashed \
    cuda-driver-devel-$dashed \
    cuda-minimal-build-$dashed \
    cuda-nvrtc-devel-$dashed \
    libcufile-devel-$dashed \
    numactl-libs
}

function install_s3 {
  install_aws_deps

  local MINIO_OS="linux"
  install_minio ${MINIO_OS}
}

function install_gcs {
  # Dependencies of GCS, probably a workaround until the docker image is rebuilt
  dnf -y install npm curl-devel c-ares-devel
  install_gcs-sdk-cpp
}

function install_abfs {
  # Dependencies of Azure Storage Blob cpp
  dnf -y install perl-IPC-Cmd openssl libxml2-devel
  install_azure-storage-sdk-cpp
}

function install_hdfs {
  dnf -y install libxml2-devel libgsasl-devel libuuid-devel krb5-devel
  install_hdfs_deps
  yum install -y java-1.8.0-openjdk-devel
}

function install_adapters {
  run_and_time install_s3
  run_and_time install_gcs
  run_and_time install_abfs
  run_and_time install_hdfs
}

function install_velox_deps {
  run_and_time install_velox_deps_from_dnf
  run_and_time install_conda
  run_and_time install_gflags
  run_and_time install_glog
  run_and_time install_lzo
  run_and_time install_snappy
  run_and_time install_boost
  run_and_time install_protobuf
  run_and_time install_fmt
  run_and_time install_fast_float
  run_and_time install_folly
  run_and_time install_fizz
  run_and_time install_wangle
  run_and_time install_mvfst
  run_and_time install_fbthrift
  run_and_time install_duckdb
  run_and_time install_stemmer
  run_and_time install_thrift
  run_and_time install_arrow
  run_and_time install_xsimd
  run_and_time install_simdjson
  run_and_time install_geos
}

(return 2> /dev/null) && return # If script was sourced, don't run commands.

(
  if [[ $# -ne 0 ]]; then
    if [[ ${USE_CLANG} != "false" ]]; then
      export CC=/usr/bin/clang-15
      export CXX=/usr/bin/clang++-15
    else
      # Activate gcc12; enable errors on unset variables afterwards.
      source /opt/rh/gcc-toolset-12/enable || exit 1
      set -u
    fi

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
    if [[ ${USE_CLANG} != "false" ]]; then
      export CC=/usr/bin/clang-15
      export CXX=/usr/bin/clang++-15
    else
      # Activate gcc12; enable errors on unset variables afterwards.
      source /opt/rh/gcc-toolset-12/enable || exit 1
      set -u
    fi
    install_velox_deps
    echo "All dependencies for Velox installed!"
    if [[ ${USE_CLANG} != "false" ]]; then
      echo "To use clang for the Velox build set the CC and CXX environment variables in your session."
      echo "  export CC=/usr/bin/clang-15"
      echo "  export CXX=/usr/bin/clang++-15"
    fi
    dnf clean all
  fi
)
