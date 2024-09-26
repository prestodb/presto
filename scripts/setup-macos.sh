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
source $SCRIPTDIR/setup-helper-functions.sh
PYTHON_VENV=${PYHTON_VENV:-"${SCRIPTDIR}/../.venv"}
# Allow installed package headers to be picked up before brew package headers
# by tagging the brew packages to be system packages.
# This is used during package builds.
export OS_CXXFLAGS=" -isystem $(brew --prefix)/include "
NPROC=$(getconf _NPROCESSORS_ONLN)

BUILD_DUCKDB="${BUILD_DUCKDB:-true}"
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}
MACOS_VELOX_DEPS="bison flex gflags glog googletest icu4c libevent libsodium lz4 lzo openssl protobuf@21 snappy xz zstd"
MACOS_BUILD_DEPS="ninja cmake"
FB_OS_VERSION="v2024.09.16.00"
FMT_VERSION="10.1.1"
FAST_FLOAT_VERSION="v6.1.6"
STEMMER_VERSION="2.2.0"

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
  if [ ! -f /usr/local/bin/ccache ]; then
    curl -L https://github.com/ccache/ccache/releases/download/v4.10.2/ccache-4.10.2-darwin.tar.gz > ccache.tar.gz
    tar -xf ccache.tar.gz
    mv ccache-4.10.2-darwin/ccache /usr/local/bin/
    rm -rf ccache-4.10.2-darwin ccache.tar.gz
  fi
}

function install_velox_deps_from_brew {
  for pkg in ${MACOS_VELOX_DEPS}
  do
    install_from_brew ${pkg}
  done
}

function install_fmt {
  wget_and_untar https://github.com/fmtlib/fmt/archive/${FMT_VERSION}.tar.gz fmt
  cmake_install_dir fmt -DFMT_TEST=OFF
}

function install_folly {
  wget_and_untar https://github.com/facebook/folly/archive/refs/tags/${FB_OS_VERSION}.tar.gz folly
  cmake_install_dir folly -DBUILD_TESTS=OFF -DFOLLY_HAVE_INT128_T=ON
}

function install_fizz {
  wget_and_untar https://github.com/facebookincubator/fizz/archive/refs/tags/${FB_OS_VERSION}.tar.gz fizz
  cmake_install_dir fizz/fizz -DBUILD_TESTS=OFF
}

function install_wangle {
  wget_and_untar https://github.com/facebook/wangle/archive/refs/tags/${FB_OS_VERSION}.tar.gz wangle
  cmake_install_dir wangle/wangle -DBUILD_TESTS=OFF
}

function install_mvfst {
  wget_and_untar https://github.com/facebook/mvfst/archive/refs/tags/${FB_OS_VERSION}.tar.gz mvfst
  cmake_install_dir mvfst -DBUILD_TESTS=OFF
}

function install_fbthrift {
  wget_and_untar https://github.com/facebook/fbthrift/archive/refs/tags/${FB_OS_VERSION}.tar.gz fbthrift
  cmake_install_dir fbthrift -Denable_tests=OFF -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF
}

function install_double_conversion {
  wget_and_untar https://github.com/google/double-conversion/archive/refs/tags/v3.1.5.tar.gz double-conversion
  cmake_install_dir double-conversion -DBUILD_TESTING=OFF
}

function install_ranges_v3 {
  wget_and_untar https://github.com/ericniebler/range-v3/archive/refs/tags/0.12.0.tar.gz ranges_v3
  cmake_install_dir ranges_v3 -DRANGES_ENABLE_WERROR=OFF -DRANGE_V3_TESTS=OFF -DRANGE_V3_EXAMPLES=OFF
}

function install_re2 {
  wget_and_untar https://github.com/google/re2/archive/refs/tags/2022-02-01.tar.gz re2
  cmake_install_dir re2 -DRE2_BUILD_TESTING=OFF
}

function install_fast_float {
  # Dependency of folly.
  wget_and_untar https://github.com/fastfloat/fast_float/archive/refs/tags/${FAST_FLOAT_VERSION}.tar.gz fast_float
  cmake_install_dir fast_float
}

function install_duckdb {
  if $BUILD_DUCKDB ; then
    echo 'Building DuckDB'
    wget_and_untar https://github.com/duckdb/duckdb/archive/refs/tags/v0.8.1.tar.gz duckdb
    cmake_install_dir duckdb -DBUILD_UNITTESTS=OFF -DENABLE_SANITIZER=OFF -DENABLE_UBSAN=OFF -DBUILD_SHELL=OFF -DEXPORT_DLL_SYMBOLS=OFF -DCMAKE_BUILD_TYPE=Release
  fi
}

function install_stemmer {
  wget_and_untar https://snowballstem.org/dist/libstemmer_c-${STEMMER_VERSION}.tar.gz stemmer
  (
    cd ${DEPENDENCY_DIR}/stemmer
    sed -i '/CPPFLAGS=-Iinclude/ s/$/ -fPIC/' Makefile
    make clean && make "-j${NPROC}"
    ${SUDO} cp libstemmer.a ${INSTALL_PREFIX}/lib/
    ${SUDO} cp include/libstemmer.h ${INSTALL_PREFIX}/include/
  )
}

function install_velox_deps {
  run_and_time install_velox_deps_from_brew
  run_and_time install_ranges_v3
  run_and_time install_double_conversion
  run_and_time install_re2
  run_and_time install_fmt
  run_and_time install_fast_float
  run_and_time install_folly
  run_and_time install_fizz
  run_and_time install_wangle
  run_and_time install_mvfst
  run_and_time install_fbthrift
  run_and_time install_duckdb
  run_and_time install_stemmer
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
