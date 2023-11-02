#!/usr/bin/env bash
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

# Supports bash and zsh. Important if sourced on macOS, which uses zsh
SHELL_SOURCE="${BASH_SOURCE[0]:-${(%):-%x}}"

# Gets values from environment variables or uses the defaults
CONDA_ENV_NAME="${PRESTO_CONDA_ENV_NAME-presto-dev}"
CONDA_INSTALL_DIR="${PRESTO_CONDA_INSTALL_DIR-$HOME/miniconda3}"

# Imports helper functions, like github_checkout and cmake_install
source "$(dirname "${SHELL_SOURCE}")/../velox/scripts/setup-helper-functions.sh"

# Sets variables used by setup-helper-functions.sh
# Folly must be built with the same compiler flags so that some low level types
# are the same size
CPU_TARGET="${CPU_TARGET:-avx}"
COMPILER_FLAGS=$(get_cxx_flags "$CPU_TARGET")
export COMPILER_FLAGS
FB_OS_VERSION=v2022.11.14.00
NPROC="${NUM_THREADS-$(getconf _NPROCESSORS_ONLN)}"
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}
export CMAKE_BUILD_TYPE=Release

# Used by functions below
OS_NAME=$(uname)
ARCH_NAME=$(uname -m)

CMAKE_EXTRA_ARGS=""
if [[ "$OS_NAME" == "Linux" ]]; then
  CMAKE_EXTRA_ARGS+=" -DBUILD_SHARED_LIBS=ON"
elif [[ "$OS_NAME" == "Darwin" ]]; then
  # Building fbthrift as shared doesn't work on macOS ARM64
  CMAKE_EXTRA_ARGS+=" -DBUILD_SHARED_LIBS=OFF"
fi

# Note: this assumes curl is installed on the system
# https://docs.conda.io/projects/miniconda/en/latest/index.html#quick-command-line-install
function _conda_install_common {
  local shasum=$1
  local url=$2
  local expected_hash=$3
  local shell=$4

  # Checks if conda is already installed
  if [ -f "$CONDA_INSTALL_DIR"/bin/conda ]; then
    echo "Found directory $CONDA_INSTALL_DIR, skipping conda install"
  else
    # Downloads conda install script
    echo "Installing conda to $CONDA_INSTALL_DIR"
    mkdir -p "$CONDA_INSTALL_DIR"
    curl "$url" -o "$CONDA_INSTALL_DIR"/miniconda.sh
    actual_hash="$($shasum "$CONDA_INSTALL_DIR"/miniconda.sh | cut -d" " -f1)"
    if [ "$actual_hash" != "$expected_hash" ]; then
      echo "Invalid miniconda hash: $actual_hash, expected: $expected_hash"
      return 1
    fi
    # Runs conda install script
    bash "$CONDA_INSTALL_DIR"/miniconda.sh -b -u -p "$CONDA_INSTALL_DIR"
    # Copies conda settings to shell config
    "$CONDA_INSTALL_DIR"/bin/conda init "$shell"
    # Configures conda to use mamba, a faster solver for conda
    "$CONDA_INSTALL_DIR"/bin/conda install --yes -n base conda-libmamba-solver
    "$CONDA_INSTALL_DIR"/bin/conda config --set solver libmamba
    # Makes conda work without restarting the shell
    export PATH="$CONDA_INSTALL_DIR/bin/:$PATH"
    source "$CONDA_INSTALL_DIR/etc/profile.d/conda.sh"
  fi
}

function conda_install_linux {
  _conda_install_common \
    "sha256sum" \
    "https://repo.anaconda.com/miniconda/Miniconda3-py311_23.5.2-0-Linux-x86_64.sh" \
    "634d76df5e489c44ade4085552b97bebc786d49245ed1a830022b0b406de5817" \
    "bash"
}

function conda_install_mac {
  if [ "$ARCH_NAME" == "arm64" ]; then
    _conda_install_common \
      "shasum -a 256" \
      "https://repo.anaconda.com/miniconda/Miniconda3-py311_23.5.2-0-MacOSX-arm64.sh" \
      "c8f436dbde130f171d39dd7b4fca669c223f130ba7789b83959adc1611a35644" \
      "zsh"
  elif [ "$ARCH_NAME" == "x86_64" ]; then
    _conda_install_common \
      "shasum -a 256" \
      "https://repo.anaconda.com/miniconda/Miniconda3-py311_23.5.2-0-MacOSX-x86_64.sh" \
      "1622e7a0fa60a7d3d892c2d8153b54cd6ffe3e6b979d931320ba56bd52581d4b" \
      "zsh"
  else
    echo "Unsupported macOS architecture: $ARCH_NAME"
    exit 1
  fi
}

function conda_create_env {
  echo "Creating conda environment $CONDA_ENV_NAME"
  conda create --yes --no-default-packages -n "$CONDA_ENV_NAME"
}

function conda_activate_env {
  echo "Activating conda environment $CONDA_ENV_NAME"
  conda activate "$CONDA_ENV_NAME"
  echo "Using CONDA_PREFIX=$CONDA_PREFIX"
}

# Copied from velox/scripts/setup-ubuntu.sh
function run_and_time {
  time "$@"
  { echo "+ Finished running $*"; } 2> /dev/null
}

# Copied from velox/scripts/setup-ubuntu.sh
function prompt {
  (
    while true; do
      local input="${PROMPT_ALWAYS_RESPOND:-}"
      echo -n "$(tput bold)$* [Y, n]$(tput sgr0) "
      [[ -z "${input}" ]] && read input
      if [[ "${input}" == "Y" || "${input}" == "y" || "${input}" == "" ]]; then
        return 0
      elif [[ "${input}" == "N" || "${input}" == "n" ]]; then
        return 1
      fi
    done
  ) 2> /dev/null
}

# Copied from velox/scripts/setup-ubuntu.sh
function install_fmt {
  github_checkout fmtlib/fmt 8.0.1
  cmake_install $CMAKE_EXTRA_ARGS -DCMAKE_INSTALL_PREFIX="$CONDA_PREFIX" -DFMT_TEST=OFF
}

# Copied from velox/scripts/setup-ubuntu.sh
function install_folly {
  github_checkout facebook/folly "${FB_OS_VERSION}"
  cmake_install $CMAKE_EXTRA_ARGS -DCMAKE_INSTALL_PREFIX="$CONDA_PREFIX" -DBUILD_TESTS=OFF -DFOLLY_HAVE_INT128_T=ON
}

# Copied from velox/scripts/setup-ubuntu.sh
function install_fizz {
  github_checkout facebookincubator/fizz "${FB_OS_VERSION}"
  cmake_install $CMAKE_EXTRA_ARGS -DCMAKE_INSTALL_PREFIX="$CONDA_PREFIX" -DBUILD_TESTS=OFF -S fizz
}

# Copied from velox/scripts/setup-ubuntu.sh
function install_wangle {
  github_checkout facebook/wangle "${FB_OS_VERSION}"
  cmake_install $CMAKE_EXTRA_ARGS -DCMAKE_INSTALL_PREFIX="$CONDA_PREFIX" -DBUILD_TESTS=OFF -S wangle
}

# Copied from velox/scripts/setup-ubuntu.sh
function install_fbthrift {
  github_checkout facebook/fbthrift "${FB_OS_VERSION}"
  cmake_install $CMAKE_EXTRA_ARGS -DCMAKE_INSTALL_PREFIX="$CONDA_PREFIX" -DBUILD_TESTS=OFF
}

function install_six {
  pip3 install six
}

function install_proxygen {
  github_checkout facebook/proxygen "${FB_OS_VERSION}"
  cmake_install $CMAKE_EXTRA_ARGS -DCMAKE_INSTALL_PREFIX="$CONDA_PREFIX" -DBUILD_TESTS=OFF
}

function install_antlr4 {
  cd "${DEPENDENCY_DIR}"
    if [ -d "antlr4-cpp-runtime-4.9.3-source" ]; then
      rm -rf antlr4-cpp-runtime-4.9.3-source
    fi
  curl https://www.antlr.org/download/antlr4-cpp-runtime-4.9.3-source.zip \
    -o antlr4-cpp-runtime-4.9.3-source.zip
  mkdir antlr4-cpp-runtime-4.9.3-source && cd antlr4-cpp-runtime-4.9.3-source
  unzip ../antlr4-cpp-runtime-4.9.3-source.zip
  mkdir build && mkdir run && cd build
  cmake -DCMAKE_INSTALL_PREFIX="$CONDA_PREFIX" .. && make "-j${NPROC}" install
}

function install_system_deps {
  DEPS=""
  DEPS+=" bison=3.8.2"
  DEPS+=" boost=1.82.0"
  DEPS+=" boost-cpp=1.82.0"
  DEPS+=" c-ares=1.19.1"
  DEPS+=" cmake=3.27.5"
  DEPS+=" cxx-compiler=1.6.0"
  DEPS+=" double-conversion=3.3.0"
  DEPS+=" flex=2.6.4"
  DEPS+=" gflags=2.2.2"
  DEPS+=" glog=0.6.0"
  DEPS+=" gmock=1.14.0"
  DEPS+=" gperf=3.1"
  DEPS+=" icu=73.2"
  DEPS+=" kernel-headers_linux-64=4.18.0"
  DEPS+=" libevent=2.1.12"
  DEPS+=" libprotobuf=3.20.3"
  DEPS+=" libsodium=1.0.18"
  DEPS+=" libthrift=0.19.0"
  DEPS+=" lz4=4.3.2"
  DEPS+=" lz4-c=1.9.4"
  DEPS+=" lzo=2.10"
  DEPS+=" cmake=3.27.5"
  DEPS+=" make=4.3"
  DEPS+=" ninja=1.11.1"
  DEPS+=" openssl=3.1.2"
  DEPS+=" pkg-config=0.29.2"
  DEPS+=" re2=2023.03.02"
  DEPS+=" snappy=1.1.10"
  DEPS+=" tzdata=2023c"
  DEPS+=" unzip=6.0"
  DEPS+=" libzlib=1.2.13"
  DEPS+=" zlib=1.2.13"
  DEPS+=" zstd=1.5.5"
  if [[ "$OS_NAME" == "Linux" ]]; then
    DEPS+=" libdwarf=0.7.0"
    DEPS+=" libunwind=1.6.2"
  fi

  conda install --yes -c conda-forge $DEPS
  if [[ "$OS_NAME" == "Linux" ]]; then
    # ld cannot find libdwarf otherwise
    ln -s "$CONDA_PREFIX"/lib/libdwarf.so.0 "$CONDA_PREFIX"/lib/libdwarf.so
  fi
}

# Copied from velox/scripts/setup-ubuntu.sh
function install_velox_deps {
  echo "Installing velox dependencies"
  run_and_time install_fmt
  run_and_time install_folly
  run_and_time install_fizz
  run_and_time install_wangle
  run_and_time install_fbthrift
}

function install_presto_deps {
  echo "Installing presto dependencies"
  run_and_time install_six
  run_and_time install_proxygen
  run_and_time install_antlr4
}

function print_summary {
  set +x
  echo "All done!"
  echo "To enter conda environment, restart your shell and run:"
  echo "source $SHELL_SOURCE"
  echo "conda_activate_env"
}

# Doesn't run commands if the script is sourced
(return 2>/dev/null) && return

# Doesn't set -u because sourced scripts have unbound variables
set -exo pipefail

if [[ "$OS_NAME" == "Darwin" ]]; then
  conda_install_mac
elif [[ "$OS_NAME" == "Linux" ]]; then
  conda_install_linux
else
  echo "Unsupported OS: $OS_NAME"
  exit 1
fi
conda_create_env
conda_activate_env
install_system_deps
install_velox_deps
install_presto_deps
print_summary
