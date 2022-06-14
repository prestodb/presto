#!/bin/bash
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

set -eufx -o pipefail

# Run the velox setup script first.
source "$(dirname "${BASH_SOURCE}")/../velox/scripts/setup-macos.sh"

MACOS_DEPS="${MACOS_DEPS} bison gperf libsodium"

function install_six {
  pip3 install six
}

export PATH=$(brew --prefix bison)/bin:$PATH

function install_fizz {
  github_checkout facebookincubator/fizz "${FB_OS_VERSION}"
  OPENSSL_ROOT_DIR=$(brew --prefix openssl@1.1) \
    cmake_install -DBUILD_TESTS=OFF -S fizz
}

function install_wangle {
  github_checkout facebook/wangle "${FB_OS_VERSION}"
  OPENSSL_ROOT_DIR=$(brew --prefix openssl@1.1) \
    cmake_install -DBUILD_TESTS=OFF -S wangle
}

function install_fbthrift {
  github_checkout facebook/fbthrift "${FB_OS_VERSION}"
  OPENSSL_ROOT_DIR=$(brew --prefix openssl@1.1) \
    cmake_install -DBUILD_TESTS=OFF
}

function install_proxygen {
  github_checkout facebook/proxygen "${FB_OS_VERSION}"
  OPENSSL_ROOT_DIR=$(brew --prefix openssl@1.1) \
    cmake_install -DBUILD_TESTS=OFF
}

function install_antlr {
  github_checkout antlr/antlr4 "4.9.3"
  cd runtime/Cpp
  cmake_install -DBUILD_TESTS=OFF
}

function install_presto_deps {
  install_velox_deps
  run_and_time install_antlr
  run_and_time install_six
  run_and_time install_fizz
  run_and_time install_wangle
  run_and_time install_fbthrift
  run_and_time install_proxygen
}

if [[ $# -ne 0 ]]; then
  for cmd in "$@"; do
    run_and_time "${cmd}"
  done
else
  install_presto_deps
fi
