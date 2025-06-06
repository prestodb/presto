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

# This script documents setting up a macOS host for Velox
# development.  Running it should make you ready to compile.
#
# Environment variables:
# * INSTALL_PREREQUISITES="N": Skip installation of brew/pip deps.
# * PROMPT_ALWAYS_RESPOND="n": Automatically respond to interactive prompts.
#     Use "n" to never wipe directories.
#
# You can also run individual functions below by specifying them as arguments:
# $ scripts/setup-macos.sh install_googletest install_fmt
#

set -e # Exit on error.
set -x # Print commands that are executed.

SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
export INSTALL_PREFIX=${INSTALL_PREFIX:-"$(pwd)/deps-install"}
source $SCRIPTDIR/setup-common.sh
PYTHON_VENV=${PYTHON_VENV:-"${SCRIPTDIR}/../.venv"}
# Allow installed package headers to be picked up before brew package headers
# by tagging the brew packages to be system packages.
# This is used during package builds.
export OS_CXXFLAGS=" -isystem $(brew --prefix)/include "
export CMAKE_POLICY_VERSION_MINIMUM="3.5"

DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}
MACOS_VELOX_DEPS="bison flex gflags glog googletest icu4c libevent libsodium lz4 lzo openssl protobuf@21 simdjson snappy xz xxhash zstd"
MACOS_BUILD_DEPS="ninja cmake"

SUDO="${SUDO:-""}"

function update_brew {
  DEFAULT_BREW_PATH=/usr/local/bin/brew
  if [ `arch` == "arm64" ] ;
    then
      DEFAULT_BREW_PATH=$(which brew) ;
  fi
  BREW_PATH=${BREW_PATH:-$DEFAULT_BREW_PATH}
  $BREW_PATH update --auto-update --verbose
  $BREW_PATH developer off
}

function install_from_brew {
  pkg=$1
  if [[ "${pkg}" =~ ^([0-9a-z-]*):([0-9](\.[0-9\])*)$ ]];
  then
    pkg=${BASH_REMATCH[1]}
    ver=${BASH_REMATCH[2]}
    echo "Installing '${pkg}' at '${ver}'"
    tap="velox/local-${pkg}"
    brew tap-new "${tap}"
    brew extract "--version=${ver}" "${pkg}" "${tap}"
    brew install "${tap}/${pkg}@${ver}" || ( echo "Failed to install ${tap}/${pkg}@${ver}" ; exit 1 )
  else
    ( brew install --formula "${pkg}" && echo "Installation of ${pkg} is successful" || brew upgrade --formula "$pkg" ) || ( echo "Failed to install ${pkg}" ; exit 1 )
  fi
}

function install_build_prerequisites {
  for pkg in ${MACOS_BUILD_DEPS}
  do
    install_from_brew ${pkg}
  done
  if [ ! -f ${PYTHON_VENV}/pyvenv.cfg ]; then
    echo "Creating Python Virtual Environment at ${PYTHON_VENV}"
    python3 -m venv ${PYTHON_VENV}
  fi
  source ${PYTHON_VENV}/bin/activate; pip3 install cmake-format regex pyyaml

  # Install ccache
  curl -L https://github.com/ccache/ccache/releases/download/v${CCACHE_VERSION}/ccache-${CCACHE_VERSION}-darwin.tar.gz > ccache.tar.gz
  tar -xf ccache.tar.gz
  mv ccache-${CCACHE_VERSION}-darwin/ccache /usr/local/bin/
  rm -rf ccache-${CCACHE_VERSION}-darwin ccache.tar.gz
}

function install_velox_deps_from_brew {
  for pkg in ${MACOS_VELOX_DEPS}
  do
    install_from_brew ${pkg}
  done
}

function install_s3 {
  install_aws_deps

  local MINIO_OS="darwin"
  install_minio ${MINIO_OS}
}

function install_gcs {
  install_gcs-sdk-cpp
}

function install_abfs {
  install_azure-storage-sdk-cpp
}

function install_hdfs {
  brew install libxml2 gsasl
  install_hdfs_deps
}

function install_adapters {
  run_and_time install_s3
  run_and_time install_gcs
  run_and_time install_abfs
  run_and_time install_hdfs
}

function install_velox_deps {
  run_and_time install_velox_deps_from_brew
  run_and_time install_ranges_v3
  run_and_time install_double_conversion
  run_and_time install_re2
  run_and_time install_boost
  run_and_time install_fmt
  run_and_time install_fast_float
  run_and_time install_folly
  run_and_time install_fizz
  run_and_time install_wangle
  run_and_time install_mvfst
  run_and_time install_fbthrift
  run_and_time install_xsimd
  run_and_time install_duckdb
  run_and_time install_stemmer
# We allow arrow to bundle thrift on MacOS due to issues with bison and flex.
# See https://github.com/facebook/fbthrift/pull/317 for an explanation.
# run_and_time install_thrift
  run_and_time install_arrow
  run_and_time install_geos
}

(return 2> /dev/null) && return # If script was sourced, don't run commands.

(
  update_brew
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
    echo "All deps for Velox installed! Now try \"make\""
  fi
)

echo "To reuse the installed dependencies for subsequent builds, consider adding this to your ~/.zshrc"
echo "export INSTALL_PREFIX=$INSTALL_PREFIX"
