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
source $SCRIPTDIR/setup-helper-functions.sh

NPROC=$(getconf _NPROCESSORS_ONLN)

DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}
MACOS_VELOX_DEPS="flex bison protobuf@21 icu4c boost gflags glog libevent lz4 lzo snappy xz zstd openssl libsodium"
MACOS_BUILD_DEPS="ninja cmake ccache"
FB_OS_VERSION="v2024.05.20.00"
FMT_VERSION="10.1.1"

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
  pip3 install --user cmake-format regex pyyaml
}

function install_velox_deps_from_brew {
  for pkg in ${MACOS_VELOX_DEPS}
  do
    install_from_brew ${pkg}
  done
}

function install_fmt {
  wget_and_untar https://github.com/fmtlib/fmt/archive/${FMT_VERSION}.tar.gz fmt
  cmake_install fmt -DFMT_TEST=OFF
}

function install_folly {
  wget_and_untar https://github.com/facebook/folly/archive/refs/tags/${FB_OS_VERSION}.tar.gz folly
  cmake_install folly -DBUILD_TESTS=OFF -DFOLLY_HAVE_INT128_T=ON
}

function install_fizz {
  wget_and_untar https://github.com/facebookincubator/fizz/archive/refs/tags/${FB_OS_VERSION}.tar.gz fizz
  cmake_install fizz/fizz -DBUILD_TESTS=OFF
}

function install_wangle {
  wget_and_untar https://github.com/facebook/wangle/archive/refs/tags/${FB_OS_VERSION}.tar.gz wangle
  cmake_install wangle/wangle -DBUILD_TESTS=OFF
}

function install_mvfst {
  wget_and_untar https://github.com/facebook/mvfst/archive/refs/tags/${FB_OS_VERSION}.tar.gz mvfst
  cmake_install mvfst -DBUILD_TESTS=OFF
}

function install_fbthrift {
  wget_and_untar https://github.com/facebook/fbthrift/archive/refs/tags/${FB_OS_VERSION}.tar.gz fbthrift
  cmake_install fbthrift -Denable_tests=OFF -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF
}

function install_double_conversion {
  wget_and_untar https://github.com/google/double-conversion/archive/refs/tags/v3.1.5.tar.gz double-conversion
  cmake_install double-conversion -DBUILD_TESTING=OFF
}

function install_ranges_v3 {
  wget_and_untar https://github.com/ericniebler/range-v3/archive/refs/tags/0.12.0.tar.gz ranges_v3
  cmake_install ranges_v3 -DRANGES_ENABLE_WERROR=OFF -DRANGE_V3_TESTS=OFF -DRANGE_V3_EXAMPLES=OFF
}

function install_re2 {
  wget_and_untar https://github.com/google/re2/archive/refs/tags/2022-02-01.tar.gz re2
  cmake_install re2 -DRE2_BUILD_TESTING=OFF
}

function install_velox_deps {
  run_and_time install_velox_deps_from_brew
  run_and_time install_ranges_v3
  run_and_time install_double_conversion
  run_and_time install_re2
  run_and_time install_fmt
  run_and_time install_folly
  run_and_time install_fizz
  run_and_time install_wangle
  run_and_time install_mvfst
  run_and_time install_fbthrift
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

echo 'To add cmake-format bin to your $PATH, consider adding this to your ~/.profile:'
echo 'export PATH=$HOME/bin:$HOME/Library/Python/3.7/bin:$PATH'
