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

set -ex
export SCRIPT_DIR=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")

source "${SCRIPT_DIR}/../velox/scripts/setup-helper-functions.sh" || \
source "${SCRIPT_DIR}/setup-helper-functions.sh"

dnf install -y tar jq wget git
dnf install -y maven java
dnf install -y python3
dnf install -y clang-tools-extra
dnf install -y perl-XML-XPath
dnf install -y libuuid-devel

python3 -m pip install regex pyyaml chevron black six

export FB_OS_VERSION=${FB_OS_VERSION:-'v2022.11.14.00'}
export nproc=${nproc:-"$(getconf _NPROCESSORS_ONLN)"}
export CC=${CC:-/opt/rh/gcc-toolset-9/root/bin/gcc}
export CXX=${CXX:-/opt/rh/gcc-toolset-9/root/bin/g++}
export CPU_TARGET="${CPU_TARGET:-avx}"
export COMPILER_FLAGS=${COMPILER_FLAGS:-"$(echo -n $(get_cxx_flags $CPU_TARGET))"}

(
  wget --max-redirect 3 https://download.libsodium.org/libsodium/releases/LATEST.tar.gz &&
  tar -xzvf LATEST.tar.gz &&
  cd libsodium-stable &&
  ./configure &&
  make "-j$(nproc)" &&
  make install
)
git clone --branch <tag> <repo>
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
  git clone --branch $FB_OS_VERSION https://github.com/facebook/folly &&
  cd folly &&
  cmake_install -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON
)

(
  git clone --branch $FB_OS_VERSION https://github.com/facebookincubator/fizz &&
  cd fizz &&
  cmake_install -DBUILD_EXAMPLES=OFF -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON fizz
)

(
  git clone --branch $FB_OS_VERSION https://github.com/facebook/wangle &&
  cd wangle &&
  cmake_install -DBUILD_EXAMPLES=OFF -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON wangle
)

(
  git clone --branch $FB_OS_VERSION https://github.com/facebook/proxygen &&
  cd proxygen &&
  cmake_install -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON
)

(
  git clone https://github.com/google/re2 &&
  cd re2 &&
  cmake_install -DBUILD_SHARED_LIBS=ON
)

(
  wget https://www.antlr.org/download/antlr4-cpp-runtime-4.9.3-source.zip &&
  mkdir antlr4-cpp-runtime-4.9.3-source &&
  cd antlr4-cpp-runtime-4.9.3-source &&
  unzip ../antlr4-cpp-runtime-4.9.3-source.zip &&
  cmake_install -DBUILD_SHARED_LIBS=ON
  ldconfig
)

(
  git clone --branch $FB_OS_VERSION https://github.com/facebook/fbthrift &&
  cd fbthrift &&
  cmake_install -DBUILD_EXAMPLES=OFF -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON
)

dnf clean all
