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
# shellcheck source-path=SCRIPT_DIR

# This script installs addition dependencies for the adapters build
# of Velox. The scrip expects base dependencies to already be installed.
#
# This script is split of from setup-centos9.sh to improve docker caching
#
# Environment variables:
# * INSTALL_PREREQUISITES="N": Skip installation of packages for build.
# * PROMPT_ALWAYS_RESPOND="n": Automatically respond to interactive prompts.
#     Use "n" to never wipe directories.
# * VELOX_CUDA_VERSION="12.8": Which version of CUDA to install, will pick up
#   CUDA_VERSION from the env

set -efx -o pipefail

VELOX_CUDA_VERSION=${CUDA_VERSION:-"12.8"}
SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
source "$SCRIPT_DIR"/setup-centos9.sh

function install_cuda {
  # See https://developer.nvidia.com/cuda-downloads
  local arch
  arch="$(uname -m)"
  local repo_url
  version="${1:-$VELOX_CUDA_VERSION}"

  if [[ $arch == "x86_64" ]]; then
    repo_url="https://developer.download.nvidia.com/compute/cuda/repos/rhel9/x86_64/cuda-rhel9.repo"
  elif [[ $arch == "aarch64" ]]; then
    # Using SBSA (Server Base System Architecture) repository for ARM64 servers
    repo_url="https://developer.download.nvidia.com/compute/cuda/repos/rhel9/sbsa/cuda-rhel9.repo"
  else
    echo "Unsupported architecture: $arch" >&2
    return 1
  fi

  dnf config-manager --add-repo "$repo_url"
  local dashed
  dashed="$(echo "$version" | tr '.' '-')"
  dnf_install \
    cuda-compat-"$dashed" \
    cuda-driver-devel-"$dashed" \
    cuda-minimal-build-"$dashed" \
    cuda-nvrtc-devel-"$dashed" \
    libcufile-devel-"$dashed" \
    numactl-libs
}

function install_adapters_deps_from_dnf {
  local gcs_deps=(curl-devel c-ares-devel re2-devel)
  local azure_deps=(perl-IPC-Cmd openssl-devel libxml2-devel)
  local hdfs_deps=(libxml2-devel libgsasl-devel libuuid-devel krb5-devel java-1.8.0-openjdk-devel)

  dnf_install "${azure_deps[@]}" "${gcs_deps[@]}" "${hdfs_deps[@]}"
}

function install_s3 {
  install_aws_deps
  local MINIO_OS="linux"
  install_minio ${MINIO_OS}
}

function install_adapters {
  run_and_time install_adapters_deps_from_dnf
  run_and_time install_s3
  run_and_time install_gcs-sdk-cpp
  run_and_time install_azure-storage-sdk-cpp
  run_and_time install_hdfs_deps
}

(return 2>/dev/null) && return # If script was sourced, don't run commands.

(
  if [[ $# -ne 0 ]]; then
    # Activate gcc12; enable errors on unset variables afterwards.
    source /opt/rh/gcc-toolset-12/enable || exit 1
    set -u

    for cmd in "$@"; do
      run_and_time "${cmd}"
    done
    echo "All specified dependencies installed!"
  else
    # Activate gcc12; enable errors on unset variables afterwards.
    source /opt/rh/gcc-toolset-12/enable || exit 1
    set -u
    install_cuda "$VELOX_CUDA_VERSION"
    install_adapters
    echo "All dependencies for the Velox Adapters installed!"
    if [[ ${USE_CLANG} != "false" ]]; then
      echo "To use clang for the Velox build set the CC and CXX environment variables in your session."
      echo "  export CC=/usr/bin/clang-15"
      echo "  export CXX=/usr/bin/clang++-15"
    fi
    dnf clean all
  fi
)
