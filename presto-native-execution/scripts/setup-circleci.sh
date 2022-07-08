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

export FB_OS_VERSION=v2022.03.14.00
export nproc=$(getconf _NPROCESSORS_ONLN)

dnf install -y maven
dnf install -y java
dnf install -y python3
dnf install -y clang-tools-extra
dnf install -y jq
dnf install -y perl-XML-XPath

python3 -m pip install regex pyyaml chevron black

# Required for Antlr4
dnf install -y libuuid-devel

export CC=/opt/rh/gcc-toolset-9/root/bin/gcc
export CXX=/opt/rh/gcc-toolset-9/root/bin/g++

CPU_TARGET="${CPU_TARGET:-avx}"
SOURCE_FILE="$(dirname "${BASH_SOURCE}")/setup-helper-functions.sh"
export COMPILER_FLAGS=$(source "$SOURCE_FILE" && echo -n $(get_cxx_flags $CPU_TARGET))

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
  git clone https://github.com/facebookincubator/fizz &&
  cd fizz &&
  git checkout $FB_OS_VERSION &&
  mkdir _build && cd _build &&
  cmake -DCMAKE_CXX_FLAGS="$COMPILER_FLAGS" -DBUILD_TESTS=OFF ../fizz &&
  make "-j$(nproc)" &&
  make install
)

(
  git clone https://github.com/facebook/wangle &&
  cd wangle &&
  git checkout $FB_OS_VERSION &&
  cd wangle &&
  mkdir _build && cd _build &&
  cmake -DCMAKE_CXX_FLAGS="$COMPILER_FLAGS" -DBUILD_TESTS=OFF ../ &&
  make "-j$(nproc)" &&
  make install
)

(
  git clone https://github.com/facebook/proxygen &&
  cd proxygen &&
  git checkout $FB_OS_VERSION &&
  mkdir _build && cd _build &&
  cmake                                     \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo       \
    -DCMAKE_INSTALL_PREFIX="$PREFIX"        \
    -DCMAKE_CXX_FLAGS="$COMPILER_FLAGS"     \
    -DCMAKE_INSTALL_PREFIX=/usr/local       \
    -DBUILD_TESTS=OFF                       \
    .. &&
  make "-j$(nproc)" &&
  make install
)

(
  git clone https://github.com/google/re2 &&
  cd re2 &&
  make "-j$(nproc)" &&
  make install
)

(
  wget https://www.antlr.org/download/antlr4-cpp-runtime-4.9.3-source.zip &&
  mkdir antlr4-cpp-runtime-4.9.3-source &&
  cd antlr4-cpp-runtime-4.9.3-source &&
  unzip ../antlr4-cpp-runtime-4.9.3-source.zip &&
  mkdir build && mkdir run && cd build &&
  cmake .. &&
  DESTDIR=../run make "-j$(nproc)" install
  cp -r ../run/usr/local/include/antlr4-runtime  /usr/local/include/. &&
  cp ../run/usr/local/lib/*  /usr/local/lib/. &&
  ldconfig
)

(
  git clone https://github.com/facebook/fbthrift &&
  cd fbthrift &&
  git checkout $FB_OS_VERSION &&
  cd build &&
  cmake -DCMAKE_CXX_FLAGS="$COMPILER_FLAGS" -DBUILD_TESTS=OFF .. &&
  make "-j$(nproc)" &&
  make install
)

dnf clean all
