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

# Minimal setup for Ubuntu 20.04.
set -eufx -o pipefail

# Run the velox setup script first.
source "$(dirname "${BASH_SOURCE}")/../velox/scripts/setup-ubuntu.sh"
export FB_OS_VERSION=v2022.07.11.00
sudo apt install -y gperf uuid-dev libsodium-dev

function install_six {
  pip3 install six
}

function install_fizz {
  github_checkout facebookincubator/fizz "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF -S fizz
}

function install_wangle {
  github_checkout facebook/wangle "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF -S wangle
}

function install_fbthrift {
  github_checkout facebook/fbthrift "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF
}

function install_proxygen {
  github_checkout facebook/proxygen "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF
}

function install_antlr4 {
    cd "${DEPENDENCY_DIR}"
      if [ -d "antlr4-cpp-runtime-4.9.3-source" ]; then
        rm -rf antlr4-cpp-runtime-4.9.3-source
      fi
    wget https://www.antlr.org/download/antlr4-cpp-runtime-4.9.3-source.zip -O antlr4-cpp-runtime-4.9.3-source.zip
    mkdir antlr4-cpp-runtime-4.9.3-source && cd antlr4-cpp-runtime-4.9.3-source
    unzip ../antlr4-cpp-runtime-4.9.3-source.zip
    mkdir build && mkdir run && cd build
    cmake .. && make "-j${NPROC}" install
}

function install_presto_deps {
  install_velox_deps
  run_and_time install_six
  run_and_time install_fizz
  run_and_time install_wangle
  run_and_time install_fbthrift
  run_and_time install_proxygen
  run_and_time install_antlr4
}

if [[ $# -ne 0 ]]; then
  for cmd in "$@"; do
    run_and_time "${cmd}"
  done
else
  install_presto_deps
fi
