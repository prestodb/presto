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

# source the velox setup script first.
source "$(dirname "${BASH_SOURCE}")/../velox/scripts/setup-centos8.sh"
export FB_OS_VERSION=v2023.12.04.00
export nproc=$(getconf _NPROCESSORS_ONLN)
export CC=/opt/rh/gcc-toolset-9/root/bin/gcc
export CXX=/opt/rh/gcc-toolset-9/root/bin/g++

dnf install -y maven java python3-devel clang-tools-extra jq perl-XML-XPath
python3 -m pip install regex pyyaml chevron black

function install_gperf {
  wget_and_untar http://ftp.gnu.org/pub/gnu/gperf/gperf-3.1.tar.gz gperf
  (
   cd gperf &&
   ./configure --prefix=/usr/local/gperf/3_1 &&
   make "-j$(nproc)" &&
   make install &&
   ln -s /usr/local/gperf/3_1/bin/gperf /usr/local/bin/
  )
}

function install_proxygen {
  github_checkout facebook/proxygen "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF
}

function install_presto_deps {
  install_velox_deps
  run_and_time install_proxygen
  run_and_time install_gperf
}

if [[ $# -ne 0 ]]; then
  for cmd in "$@"; do
    run_and_time "${cmd}"
  done
else
  install_presto_deps
fi

dnf clean all
