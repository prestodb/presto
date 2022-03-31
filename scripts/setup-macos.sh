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

# This script documents setting up a macOS host for presto_cpp
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

FB_OS_VERSION=v2021.05.10.00
NPROC=$(getconf _NPROCESSORS_ONLN)
COMPILER_FLAGS="-mavx2 -mfma -mavx -mf16c -masm=intel -mlzcnt"
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}
MACOS_DEPS="ninja cmake ccache protobuf icu4c boost gflags glog libevent lz4 lzo snappy xz zstd openssl@1.1"

function run_and_time {
  time "$@"
  { echo "+ Finished running $*"; } 2> /dev/null
}

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

# github_checkout $REPO $VERSION clones or re-uses an existing clone of the
# specified repo, checking out the requested version.
function github_checkout {
  local REPO=$1
  local VERSION=$2
  local DIRNAME=$(basename "$1")

  cd "${DEPENDENCY_DIR}"
  if [ -z "${DIRNAME}" ]; then
    echo "Failed to get repo name from $1"
    exit 1
  fi
  if [ -d "${DIRNAME}" ] && prompt "${DIRNAME} already exists. Delete?"; then
    rm -rf "${DIRNAME}"
  fi
  if [ ! -d "${DIRNAME}" ]; then
    git clone -q "https://github.com/${REPO}.git"
  fi
  cd "${DIRNAME}"
  git fetch -q
  git checkout "${VERSION}"
}

function cmake_install {
  local NAME=$(basename "$(pwd)")
  local BINARY_DIR=_build
  if [ -d "${BINARY_DIR}" ] && prompt "Do you want to rebuild ${NAME}?"; then
    rm -rf "${BINARY_DIR}"
  fi
  mkdir -p "${BINARY_DIR}"
  cmake -Wno-dev -B"${BINARY_DIR}" \
    -GNinja \
    -DCMAKE_CXX_STANDARD=17 \
    "${INSTALL_PREFIX+-DCMAKE_PREFIX_PATH=}${INSTALL_PREFIX-}" \
    "${INSTALL_PREFIX+-DCMAKE_INSTALL_PREFIX=}${INSTALL_PREFIX-}" \
    -DCMAKE_CXX_FLAGS="${COMPILER_FLAGS}" \
    -DBUILD_TESTING=OFF \
    "$@"
  ninja -C "${BINARY_DIR}" install
}

function update_brew {
  /usr/local/bin/brew update --force --quiet
}

function install_build_prerequisites {
  for pkg in ${MACOS_DEPS}
  do
    if [[ "${pkg}" =~ ^([0-9a-z-]*):([0-9](\.[0-9\])*)$ ]];
    then
      pkg=${BASH_REMATCH[1]}
      ver=${BASH_REMATCH[2]}
      echo "Installing '${pkg}' at '${ver}'"
      tap="velox/local-${pkg}"
      brew tap-new "${tap}"
      brew extract "--version=${ver}" "${pkg}" "${tap}"
      brew install "${tap}/${pkg}@${ver}"
    else
      brew install --formula "${pkg}" && echo "Installation of ${pkg} is successful" || brew upgrade --formula "$pkg"
    fi
  done

  pip3 install --user cmake-format regex
}

function install_googletest {
  github_checkout google/googletest release-1.11.0
  cmake_install
}

function install_fmt {
  github_checkout fmtlib/fmt 7.1.3
  cmake_install -DFMT_TEST=OFF
}

function install_double_conversion {
  github_checkout google/double-conversion v3.1.5
  cmake_install -DBUILD_TESTING=OFF
}

function install_folly {
  github_checkout facebook/folly "${FB_OS_VERSION}"
  OPENSSL_ROOT_DIR=$(brew --prefix openssl@1.1) \
    cmake_install -DBUILD_TESTS=OFF
}

function install_ranges_v3 {
  github_checkout ericniebler/range-v3 master
  cmake_install -DRANGES_ENABLE_WERROR=OFF -DRANGE_V3_TESTS=OFF -DRANGE_V3_EXAMPLES=OFF
}

function install_re2 {
  github_checkout google/re2 2021-04-01
  cmake_install -DRE2_BUILD_TESTING=OFF
}

function install_velox_deps {
  if [ "${INSTALL_PREREQUISITES:-Y}" == "Y" ]; then
    run_and_time install_build_prerequisites
  fi
  run_and_time install_ranges_v3
  run_and_time install_googletest
  run_and_time install_fmt
  run_and_time install_double_conversion
  run_and_time install_folly
  run_and_time install_re2
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

echo "All deps for Velox installed! Now try \"make\""
echo 'To add cmake-format bin to your $PATH, consider adding this to your ~/.profile:'
echo 'export PATH=$HOME/bin:$HOME/Library/Python/3.7/bin:$PATH'
