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

export FB_OS_VERSION=v2022.11.14.00
export RE2_VERSION=2021-04-01
export nproc=$(getconf _NPROCESSORS_ONLN)

dnf install -y maven
dnf install -y java
dnf install -y python3-devel
dnf install -y clang-tools-extra
dnf install -y jq
dnf install -y perl-XML-XPath

python3 -m pip install regex pyyaml chevron black six

# Required for Antlr4
dnf install -y libuuid-devel

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

function check_git_checkout()
{ 
  [[ -d "$(pwd)/$2" ]] || git clone "$1" "$2"
  cd "$2" &&
  git checkout $FB_OS_VERSION
}

(
  wget --max-redirect 3 https://download.libsodium.org/libsodium/releases/LATEST.tar.gz &&
  tar -xzvf LATEST.tar.gz &&
  cd libsodium-stable &&
  ./configure &&
  make "-j$(nproc)" &&
  make install
)

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
  check_git_checkout https://github.com/facebook/folly folly
  cmake_install -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON -DFOLLY_HAVE_INT128_T=ON
)

(
  check_git_checkout https://github.com/facebookincubator/fizz fizz
  cmake_install -DBUILD_EXAMPLES=OFF -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON fizz
)

(
  check_git_checkout https://github.com/facebook/wangle wangle
  cmake_install -DBUILD_EXAMPLES=OFF -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON wangle
)

(
  check_git_checkout https://github.com/facebook/proxygen proxygen
  cmake_install -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON
)

(
  check_git_checkout https://github.com/google/re2 re2
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
  check_git_checkout https://github.com/facebook/fbthrift fbthrift
  cmake_install -DBUILD_EXAMPLES=OFF -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON
)

dnf clean all
