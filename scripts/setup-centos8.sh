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

set -efx -o pipefail
# Some of the packages must be build with the same compiler flags
# so that some low level types are the same size. Also, disable warnings.
SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
source $SCRIPTDIR/setup-helper-functions.sh
CPU_TARGET="${CPU_TARGET:-avx}"
NPROC=$(getconf _NPROCESSORS_ONLN)
export CFLAGS=$(get_cxx_flags $CPU_TARGET)  # Used by LZO.
export CXXFLAGS=$CFLAGS  # Used by boost.
export CPPFLAGS=$CFLAGS  # Used by LZO.
CMAKE_BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_DUCKDB="${BUILD_DUCKDB:-true}"

function dnf_install {
  dnf install -y -q --setopt=install_weak_deps=False "$@"
}

dnf update -y
dnf_install epel-release dnf-plugins-core # For ccache, ninja
dnf config-manager --set-enabled powertools
dnf update -y
dnf_install ninja-build cmake curl ccache gcc-toolset-9 git wget which libevent-devel \
  openssl-devel re2-devel libzstd-devel lz4-devel double-conversion-devel \
  libdwarf-devel curl-devel libicu-devel

dnf_install autoconf automake libtool bison flex python3 libsodium-devel

# install sphinx for doc gen
pip3 install sphinx sphinx-tabs breathe sphinx_rtd_theme

# Activate gcc9; enable errors on unset variables afterwards.
source /opt/rh/gcc-toolset-9/enable || exit 1
set -u

function install_conda {
  dnf_install conda
}

function install_gflags {
  # Remove an older version if present.
  dnf remove -y gflags
  wget_and_untar https://github.com/gflags/gflags/archive/v2.2.2.tar.gz gflags
  (
    cd gflags
    cmake_install -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DBUILD_gflags_LIB=ON -DLIB_SUFFIX=64
  )
}

function install_glog {
  wget_and_untar https://github.com/google/glog/archive/v0.6.0.tar.gz glog
  (
    cd glog
    cmake_install -DBUILD_SHARED_LIBS=ON
  )
}

function install_lzo {
  wget_and_untar http://www.oberhumer.com/opensource/lzo/download/lzo-2.10.tar.gz lzo
  (
    cd lzo
    ./configure --prefix=/usr --enable-shared --disable-static --docdir=/usr/share/doc/lzo-2.10
    make "-j$(nproc)"
    make install
  )
}

function install_boost {
  wget_and_untar https://github.com/boostorg/boost/releases/download/boost-1.84.0/boost-1.84.0.tar.gz boost
  (
   cd boost
   ./bootstrap.sh --prefix=/usr/local
   ./b2 "-j$(nproc)" -d0 install threading=multi
  )
}

function install_snappy {
  wget_and_untar https://github.com/google/snappy/archive/1.1.8.tar.gz snappy
  (
    cd snappy
    cmake_install -DSNAPPY_BUILD_TESTS=OFF
  )
}

function install_fmt {
  wget_and_untar https://github.com/fmtlib/fmt/archive/10.1.1.tar.gz fmt
  (
    cd fmt
    cmake_install -DFMT_TEST=OFF
  )
}

function install_protobuf {
  wget_and_untar https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protobuf-all-21.4.tar.gz protobuf
  (
    cd protobuf
    ./configure --prefix=/usr
    make "-j${NPROC}"
    make install
    ldconfig
  )
}

FB_OS_VERSION="v2023.12.04.00"

function install_fizz {
  wget_and_untar https://github.com/facebookincubator/fizz/archive/refs/tags/${FB_OS_VERSION}.tar.gz fizz
  (
    cd fizz/fizz
    cmake_install -DBUILD_TESTS=OFF
  )
}

function install_folly {
  wget_and_untar https://github.com/facebook/folly/archive/refs/tags/${FB_OS_VERSION}.tar.gz folly
  (
    cd folly
    cmake_install -DFOLLY_HAVE_INT128_T=ON
  )
}

function install_wangle {
  wget_and_untar https://github.com/facebook/wangle/archive/refs/tags/${FB_OS_VERSION}.tar.gz wangle
  (
    cd wangle/wangle
    cmake_install -DBUILD_TESTS=OFF
  )
}

function install_fbthrift {
  wget_and_untar https://github.com/facebook/fbthrift/archive/refs/tags/${FB_OS_VERSION}.tar.gz fbthrift
  (
    cd fbthrift
    cmake_install -Denable_tests=OFF
  )
}

function install_mvfst {
  wget_and_untar https://github.com/facebook/mvfst/archive/refs/tags/${FB_OS_VERSION}.tar.gz mvfst
  (
   cd mvfst
   cmake_install -DBUILD_TESTS=OFF
  )
}

function install_duckdb {
  if $BUILD_DUCKDB ; then
    echo 'Building DuckDB'
    wget_and_untar https://github.com/duckdb/duckdb/archive/refs/tags/v0.8.1.tar.gz duckdb
    (
      cd duckdb
      cmake_install -DBUILD_UNITTESTS=OFF -DENABLE_SANITIZER=OFF -DENABLE_UBSAN=OFF -DBUILD_SHELL=OFF -DEXPORT_DLL_SYMBOLS=OFF -DCMAKE_BUILD_TYPE=Release
    )
  fi
}

function install_velox_deps {
  run_and_time install_conda
  run_and_time install_gflags
  run_and_time install_glog
  run_and_time install_lzo
  run_and_time install_snappy
  run_and_time install_boost
  run_and_time install_protobuf
  run_and_time install_fmt
  run_and_time install_folly
  run_and_time install_fizz
  run_and_time install_wangle
  run_and_time install_mvfst
  run_and_time install_fbthrift
  run_and_time install_duckdb
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

echo "All dependencies for Velox installed!"

dnf clean all
