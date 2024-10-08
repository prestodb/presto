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

# This script documents setting up a Centos9 host for Velox
# development.  Running it should make you ready to compile.
#
# Environment variables:
# * INSTALL_PREREQUISITES="N": Skip installation of packages for build.
# * PROMPT_ALWAYS_RESPOND="n": Automatically respond to interactive prompts.
#     Use "n" to never wipe directories.
#
# You can also run individual functions below by specifying them as arguments:
# $ scripts/setup-centos9.sh install_googletest install_fmt
#

set -efx -o pipefail
# Some of the packages must be build with the same compiler flags
# so that some low level types are the same size. Also, disable warnings.
SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
source $SCRIPTDIR/setup-helper-functions.sh
NPROC=$(getconf _NPROCESSORS_ONLN)
export CXXFLAGS=$(get_cxx_flags) # Used by boost.
export CFLAGS=${CXXFLAGS//"-std=c++17"/} # Used by LZO.
CMAKE_BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_DUCKDB="${BUILD_DUCKDB:-true}"
USE_CLANG="${USE_CLANG:-false}"
export INSTALL_PREFIX=${INSTALL_PREFIX:-"/usr/local"}
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)/deps-download}

FB_OS_VERSION="v2024.07.01.00"
FMT_VERSION="10.1.1"
BOOST_VERSION="boost-1.84.0"
ARROW_VERSION="15.0.0"
STEMMER_VERSION="2.2.0"

function dnf_install {
  dnf install -y -q --setopt=install_weak_deps=False "$@"
}

function install_clang15 {
  dnf_install clang15 gcc-toolset-13-libatomic-devel
}

# Install packages required for build.
function install_build_prerequisites {
  dnf update -y
  dnf_install epel-release dnf-plugins-core # For ccache, ninja
  dnf config-manager --set-enabled crb
  dnf update -y
  dnf_install ninja-build cmake ccache gcc-toolset-12 git wget which
  dnf_install autoconf automake python3-devel pip libtool

  pip install cmake==3.28.3

  if [[ ${USE_CLANG} != "false" ]]; then
    install_clang15
  fi
}

# Install dependencies from the package managers.
function install_velox_deps_from_dnf {
  dnf_install libevent-devel \
    openssl-devel re2-devel libzstd-devel lz4-devel double-conversion-devel \
    libdwarf-devel elfutils-libelf-devel curl-devel libicu-devel bison flex \
    libsodium-devel zlib-devel

  # install sphinx for doc gen
  pip install sphinx sphinx-tabs breathe sphinx_rtd_theme
}

function install_conda {
  dnf_install conda
}

function install_gflags {
  # Remove an older version if present.
  dnf remove -y gflags
  wget_and_untar https://github.com/gflags/gflags/archive/v2.2.2.tar.gz gflags
  cmake_install_dir gflags -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DBUILD_gflags_LIB=ON -DLIB_SUFFIX=64
}

function install_glog {
  wget_and_untar https://github.com/google/glog/archive/v0.6.0.tar.gz glog
  cmake_install_dir glog -DBUILD_SHARED_LIBS=ON
}

function install_lzo {
  wget_and_untar http://www.oberhumer.com/opensource/lzo/download/lzo-2.10.tar.gz lzo
  (
    cd ${DEPENDENCY_DIR}/lzo
    ./configure --prefix=${INSTALL_PREFIX} --enable-shared --disable-static --docdir=/usr/share/doc/lzo-2.10
    make "-j$(nproc)"
    make install
  )
}

function install_boost {
  wget_and_untar https://github.com/boostorg/boost/releases/download/${BOOST_VERSION}/${BOOST_VERSION}.tar.gz boost
  (
    cd ${DEPENDENCY_DIR}/boost
    if [[ ${USE_CLANG} != "false" ]]; then
      ./bootstrap.sh --prefix=${INSTALL_PREFIX} --with-toolset="clang-15"
      # Switch the compiler from the clang-15 toolset which doesn't exist (clang-15.jam) to
      # clang of version 15 when toolset clang-15 is used.
      # This reconciles the project-config.jam generation with what the b2 build system allows for customization.
      sed -i 's/using clang-15/using clang : 15/g' project-config.jam
      ${SUDO} ./b2 "-j$(nproc)" -d0 install threading=multi toolset=clang-15 --without-python
    else
      ./bootstrap.sh --prefix=${INSTALL_PREFIX}
      ${SUDO} ./b2 "-j$(nproc)" -d0 install threading=multi --without-python
    fi
  )
}

function install_snappy {
  wget_and_untar https://github.com/google/snappy/archive/1.1.8.tar.gz snappy
  cmake_install_dir snappy -DSNAPPY_BUILD_TESTS=OFF
}

function install_fmt {
  wget_and_untar https://github.com/fmtlib/fmt/archive/${FMT_VERSION}.tar.gz fmt
  cmake_install_dir fmt -DFMT_TEST=OFF
}

function install_protobuf {
  wget_and_untar https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protobuf-all-21.8.tar.gz protobuf
  (
    cd ${DEPENDENCY_DIR}/protobuf
    ./configure --prefix=${INSTALL_PREFIX}
    make "-j${NPROC}"
    make install
    ldconfig
  )
}

function install_fizz {
  wget_and_untar https://github.com/facebookincubator/fizz/archive/refs/tags/${FB_OS_VERSION}.tar.gz fizz
  cmake_install_dir fizz/fizz -DBUILD_TESTS=OFF
}

function install_folly {
  wget_and_untar https://github.com/facebook/folly/archive/refs/tags/${FB_OS_VERSION}.tar.gz folly
  cmake_install_dir folly -DBUILD_TESTS=OFF -DFOLLY_HAVE_INT128_T=ON
}

function install_wangle {
  wget_and_untar https://github.com/facebook/wangle/archive/refs/tags/${FB_OS_VERSION}.tar.gz wangle
  cmake_install_dir wangle/wangle -DBUILD_TESTS=OFF
}

function install_fbthrift {
  wget_and_untar https://github.com/facebook/fbthrift/archive/refs/tags/${FB_OS_VERSION}.tar.gz fbthrift
  cmake_install_dir fbthrift -Denable_tests=OFF -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF
}

function install_mvfst {
  wget_and_untar https://github.com/facebook/mvfst/archive/refs/tags/${FB_OS_VERSION}.tar.gz mvfst
  cmake_install_dir mvfst -DBUILD_TESTS=OFF
}

function install_duckdb {
  if $BUILD_DUCKDB ; then
    echo 'Building DuckDB'
    wget_and_untar https://github.com/duckdb/duckdb/archive/refs/tags/v0.8.1.tar.gz duckdb
    cmake_install_dir duckdb -DBUILD_UNITTESTS=OFF -DENABLE_SANITIZER=OFF -DENABLE_UBSAN=OFF -DBUILD_SHELL=OFF -DEXPORT_DLL_SYMBOLS=OFF -DCMAKE_BUILD_TYPE=Release
  fi
}

function install_stemmer {
  wget_and_untar https://snowballstem.org/dist/libstemmer_c-${STEMMER_VERSION}.tar.gz stemmer
  (
    cd ${DEPENDENCY_DIR}/stemmer
    sed -i '/CPPFLAGS=-Iinclude/ s/$/ -fPIC/' Makefile
    make clean && make "-j${NPROC}"
    ${SUDO} cp libstemmer.a ${INSTALL_PREFIX}/lib/
    ${SUDO} cp include/libstemmer.h ${INSTALL_PREFIX}/include/
  )
}

function install_arrow {
  wget_and_untar https://archive.apache.org/dist/arrow/arrow-${ARROW_VERSION}/apache-arrow-${ARROW_VERSION}.tar.gz arrow
  cmake_install_dir arrow/cpp \
    -DARROW_PARQUET=OFF \
    -DARROW_WITH_THRIFT=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_ZSTD=ON \
    -DARROW_JEMALLOC=OFF \
    -DARROW_SIMD_LEVEL=NONE \
    -DARROW_RUNTIME_SIMD_LEVEL=NONE \
    -DARROW_WITH_UTF8PROC=OFF \
    -DARROW_TESTING=ON \
    -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} \
    -DCMAKE_BUILD_TYPE=Release \
    -DARROW_BUILD_STATIC=ON \
    -DThrift_SOURCE=BUNDLED \
    -DBOOST_ROOT=${INSTALL_PREFIX}

  (
    # Install thrift.
    cd ${DEPENDENCY_DIR}/arrow/cpp/_build/thrift_ep-prefix/src/thrift_ep-build
    cmake --install ./ --prefix ${INSTALL_PREFIX}
  )
}

function install_cuda {
  # See https://developer.nvidia.com/cuda-downloads
  dnf config-manager --add-repo https://developer.download.nvidia.com/compute/cuda/repos/rhel9/x86_64/cuda-rhel9.repo
  dnf install -y cuda-nvcc-$(echo $1 | tr '.' '-') cuda-cudart-devel-$(echo $1 | tr '.' '-')
}

function install_velox_deps {
  run_and_time install_velox_deps_from_dnf
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
  run_and_time install_stemmer
  run_and_time install_arrow
}

(return 2> /dev/null) && return # If script was sourced, don't run commands.

(
  if [[ $# -ne 0 ]]; then
    if [[ ${USE_CLANG} != "false" ]]; then
      export CC=/usr/bin/clang-15
      export CXX=/usr/bin/clang++-15
    else
      # Activate gcc12; enable errors on unset variables afterwards.
      source /opt/rh/gcc-toolset-12/enable || exit 1
      set -u
    fi

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
    if [[ ${USE_CLANG} != "false" ]]; then
      export CC=/usr/bin/clang-15
      export CXX=/usr/bin/clang++-15
    else
      # Activate gcc12; enable errors on unset variables afterwards.
      source /opt/rh/gcc-toolset-12/enable || exit 1
      set -u
    fi
    install_velox_deps
    echo "All dependencies for Velox installed!"
    if [[ ${USE_CLANG} != "false" ]]; then
      echo "To use clang for the Velox build set the CC and CXX environment variables in your session."
      echo "  export CC=/usr/bin/clang-15"
      echo "  export CXX=/usr/bin/clang++-15"
    fi
    dnf clean all
  fi
)

