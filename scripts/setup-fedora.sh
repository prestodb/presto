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

# This script documents setting up a Fedora host for Velox
# development.  Running it should make you ready to compile.
#
# Environment variables:
# * INSTALL_PREREQUISITES="N": Skip installation of packages for build.
# * PROMPT_ALWAYS_RESPOND="n": Automatically respond to interactive prompts.
#     Use "n" to never wipe directories.
#
# You can also run individual functions below by specifying them as arguments:
# $ scripts/setup-fedora.sh install_googletest install_fmt
#

set -efx -o pipefail
# Some of the packages must be build with the same compiler flags
# so that some low level types are the same size. Also, disable warnings.
SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
source "$SCRIPT_DIR"/setup-centos9.sh

# Install packages required for build.
function install_build_prerequisites {
  dnf update -y
  dnf_install dnf-plugins-core # For ccache, ninja
  dnf_install autoconf automake ccache git g++-14 libtool \
    ninja-build python3-pip python3-devel wget which

  # Set up gcc alternatives
  sudo alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 140 \
    --slave /usr/bin/g++ g++ /usr/bin/g++-14 \
    --slave /usr/bin/gcov gcov /usr/bin/gcov-14 \
    --slave /usr/bin/gcc-ar gcc-ar /usr/bin/gcc-ar-14 \
    --slave /usr/bin/gcc-ranlib gcc-ranlib /usr/bin/gcc-ranlib-14 \
    --slave /usr/bin/gcc-nm gcc-nm /usr/bin/gcc-nm-14

  install_uv
  uv_install cmake

  if [[ ${USE_CLANG} != "false" ]]; then
    install_clang15
  fi
}

(return 2>/dev/null) && return # If script was sourced, don't run commands.

(
  if [[ $# -ne 0 ]]; then
    if [[ ${USE_CLANG} != "false" ]]; then
      export CC=/usr/bin/clang-15
      export CXX=/usr/bin/clang++-15
    else
      # Activate gcc 14
      export CXX=/usr/bin/gcc-14
      export CXX=/usr/bin/g++-14
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
      # Activate gcc 14
      export CXX=/usr/bin/gcc-14
      export CXX=/usr/bin/g++-14
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
