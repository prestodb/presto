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

set -e
set -x

export FB_OS_VERSION=v2024.04.01.00
export RE2_VERSION=2021-04-01
export nproc=$(getconf _NPROCESSORS_ONLN)

dnf install -y maven java python3-devel clang-tools-extra jq perl-XML-XPath

python3 -m pip install regex pyyaml chevron black

export CC=/opt/rh/gcc-toolset-9/root/bin/gcc
export CXX=/opt/rh/gcc-toolset-9/root/bin/g++

CPU_TARGET="${CPU_TARGET:-avx}"
SCRIPT_DIR=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")
if [ -f "${SCRIPT_DIR}/setup-helper-functions.sh" ]
then
  source "${SCRIPT_DIR}/setup-helper-functions.sh"
else
  source "${SCRIPT_DIR}/../velox/scripts/setup-helper-functions.sh"
fi

export COMPILER_FLAGS=$(echo -n $(get_cxx_flags $CPU_TARGET))

(
  wget http://ftp.gnu.org/pub/gnu/gperf/gperf-3.1.tar.gz &&
  tar xvfz gperf-3.1.tar.gz &&
  cd gperf-3.1 &&
  ./configure --prefix=/usr/local/gperf/3_1 &&
  make "-j$(nproc)" &&
  make install &&
  ln -s /usr/local/gperf/3_1/bin/gperf /usr/local/bin/
)


(
  git clone https://github.com/facebook/proxygen &&
  cd proxygen &&
  git checkout $FB_OS_VERSION &&
  cmake_install -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON
)


dnf clean all
