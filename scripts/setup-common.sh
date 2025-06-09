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
# trigger reinstall

SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
source $SCRIPTDIR/setup-helper-functions.sh
source $SCRIPTDIR/setup-versions.sh

VELOX_BUILD_SHARED=${VELOX_BUILD_SHARED:-"OFF"} #Build folly and gflags shared for use in libvelox.so.
CMAKE_BUILD_TYPE="${BUILD_TYPE:-Release}"
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}
BUILD_GEOS="${BUILD_GEOS:-true}"
BUILD_DUCKDB="${BUILD_DUCKDB:-true}"
EXTRA_ARROW_OPTIONS=${EXTRA_ARROW_OPTIONS:-""}

USE_CLANG="${USE_CLANG:-false}"

MACHINE=$(uname -m)

WGET_OPTIONS=${WGET_OPTIONS:-""}

mkdir -p "${DEPENDENCY_DIR}"

function install_fmt {
  wget_and_untar https://github.com/fmtlib/fmt/archive/${FMT_VERSION}.tar.gz fmt
  cmake_install_dir fmt -DFMT_TEST=OFF
}

function install_folly {
  # Folly Portability.h being used to decide whether or not support coroutines
  # causes issues (build, link) if the selection is not consistent across users of folly.
  EXTRA_PKG_CXXFLAGS=" -DFOLLY_CFG_NO_COROUTINES"
  wget_and_untar https://github.com/facebook/folly/archive/refs/tags/${FB_OS_VERSION}.tar.gz folly
  cmake_install_dir folly -DBUILD_SHARED_LIBS="$VELOX_BUILD_SHARED" -DBUILD_TESTS=OFF -DFOLLY_HAVE_INT128_T=ON
}

function install_fizz {
  # Folly Portability.h being used to decide whether or not support coroutines
  # causes issues (build, link) if the selection is not consistent across users of folly.
  EXTRA_PKG_CXXFLAGS=" -DFOLLY_CFG_NO_COROUTINES"
  wget_and_untar https://github.com/facebookincubator/fizz/archive/refs/tags/${FB_OS_VERSION}.tar.gz fizz
  cmake_install_dir fizz/fizz -DBUILD_TESTS=OFF
}

function install_fast_float {
  wget_and_untar https://github.com/fastfloat/fast_float/archive/refs/tags/${FAST_FLOAT_VERSION}.tar.gz fast_float
  cmake_install_dir fast_float -DBUILD_TESTS=OFF
}

function install_wangle {
  # Folly Portability.h being used to decide whether or not support coroutines
  # causes issues (build, link) if the selection is not consistent across users of folly.
  EXTRA_PKG_CXXFLAGS=" -DFOLLY_CFG_NO_COROUTINES"
  wget_and_untar https://github.com/facebook/wangle/archive/refs/tags/${FB_OS_VERSION}.tar.gz wangle
  cmake_install_dir wangle/wangle -DBUILD_TESTS=OFF
}

function install_mvfst {
  # Folly Portability.h being used to decide whether or not support coroutines
  # causes issues (build, link) if the selection is not consistent across users of folly.
  EXTRA_PKG_CXXFLAGS=" -DFOLLY_CFG_NO_COROUTINES"
  wget_and_untar https://github.com/facebook/mvfst/archive/refs/tags/${FB_OS_VERSION}.tar.gz mvfst
  cmake_install_dir mvfst -DBUILD_TESTS=OFF
}

function install_fbthrift {
  # Folly Portability.h being used to decide whether or not support coroutines
  # causes issues (build, link) if the selection is not consistent across users of folly.
  EXTRA_PKG_CXXFLAGS=" -DFOLLY_CFG_NO_COROUTINES"
  wget_and_untar https://github.com/facebook/fbthrift/archive/refs/tags/${FB_OS_VERSION}.tar.gz fbthrift
  cmake_install_dir fbthrift -Denable_tests=OFF -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF
}

function install_duckdb {
  if $BUILD_DUCKDB ; then
    wget_and_untar https://github.com/duckdb/duckdb/archive/refs/tags/${DUCKDB_VERSION}.tar.gz duckdb
    cmake_install_dir duckdb -DBUILD_UNITTESTS=OFF -DENABLE_SANITIZER=OFF -DENABLE_UBSAN=OFF -DBUILD_SHELL=OFF -DEXPORT_DLL_SYMBOLS=OFF -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
  fi
}

function install_boost {
  wget_and_untar https://github.com/boostorg/boost/releases/download/${BOOST_VERSION}/${BOOST_VERSION}.tar.gz boost
  (
    cd ${DEPENDENCY_DIR}/boost
    if [[ "$(uname)" == "Linux" && ${USE_CLANG} != "false" ]]; then
      ./bootstrap.sh --prefix=${INSTALL_PREFIX} --with-toolset="clang-15"
      # Switch the compiler from the clang-15 toolset which doesn't exist (clang-15.jam) to
      # clang of version 15 when toolset clang-15 is used.
      # This reconciles the project-config.jam generation with what the b2 build system allows for customization.
      sed -i 's/using clang-15/using clang : 15/g' project-config.jam
      ${SUDO} ./b2 "-j${NPROC}" -d0 install threading=multi toolset=clang-15 --without-python
    else
      ./bootstrap.sh --prefix=${INSTALL_PREFIX}
      ${SUDO} ./b2 "-j${NPROC}" -d0 install threading=multi --without-python
    fi
  )
}

function install_protobuf {
  wget_and_untar https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-all-${PROTOBUF_VERSION}.tar.gz protobuf
  cmake_install_dir protobuf -Dprotobuf_BUILD_TESTS=OFF -Dprotobuf_ABSL_PROVIDER=package
}

function install_double_conversion {
  wget_and_untar https://github.com/google/double-conversion/archive/refs/tags/${DOUBLE_CONVERSION_VERSION}.tar.gz double-conversion
  cmake_install_dir double-conversion -DBUILD_TESTING=OFF
}

function install_ranges_v3 {
  wget_and_untar https://github.com/ericniebler/range-v3/archive/refs/tags/${RANGE_V3_VERSION}.tar.gz ranges_v3
  cmake_install_dir ranges_v3 -DRANGES_ENABLE_WERROR=OFF -DRANGE_V3_TESTS=OFF -DRANGE_V3_EXAMPLES=OFF
}

function install_re2 {
  wget_and_untar https://github.com/google/re2/archive/refs/tags/${RE2_VERSION}.tar.gz re2
  cmake_install_dir re2 -DRE2_BUILD_TESTING=OFF
}

function install_glog {
  wget_and_untar https://github.com/google/glog/archive/${GLOG_VERSION}.tar.gz glog
  cmake_install_dir glog -DBUILD_SHARED_LIBS=ON
}

function install_lzo {
  wget_and_untar http://www.oberhumer.com/opensource/lzo/download/lzo-${LZO_VERSION}.tar.gz lzo
  (
    cd ${DEPENDENCY_DIR}/lzo
    ./configure --prefix=${INSTALL_PREFIX} --enable-shared --disable-static --docdir=/usr/share/doc/lzo-${LZO_VERSION}
    make "-j${NPROC}"
    ${SUDO} make install
  )
}

function install_snappy {
  wget_and_untar https://github.com/google/snappy/archive/${SNAPPY_VERSION}.tar.gz snappy
  cmake_install_dir snappy -DSNAPPY_BUILD_TESTS=OFF
}

function install_xsimd {
  wget_and_untar https://github.com/xtensor-stack/xsimd/archive/refs/tags/${XSIMD_VERSION}.tar.gz xsimd
  cmake_install_dir xsimd
}

function install_simdjson {
  wget_and_untar https://github.com/simdjson/simdjson/archive/refs/tags/v${SIMDJSON_VERSION}.tar.gz simdjson
  cmake_install_dir simdjson
}

function install_arrow {
  wget_and_untar https://github.com/apache/arrow/archive/apache-arrow-${ARROW_VERSION}.tar.gz arrow
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
    -DBOOST_ROOT=${INSTALL_PREFIX}
}

function install_thrift {
   wget_and_untar https://github.com/apache/thrift/archive/${THRIFT_VERSION}.tar.gz thrift

   EXTRA_CXXFLAGS="-O3 -fPIC"
   # Clang will generate warnings and they need to be suppressed, otherwise the build will fail.
   if [[ ${USE_CLANG} != "false" ]]; then
     EXTRA_CXXFLAGS="-O3 -fPIC -Wno-inconsistent-missing-override -Wno-unused-but-set-variable"
   fi

   CXX_FLAGS="$EXTRA_CXXFLAGS" cmake_install_dir thrift \
     -DBUILD_SHARED_LIBS=OFF \
     -DBUILD_COMPILER=ON \
     -DBUILD_EXAMPLES=OFF \
     -DBUILD_TUTORIALS=OFF \
     -DCMAKE_DEBUG_POSTFIX= \
     -DWITH_AS3=OFF \
     -DWITH_CPP=ON \
     -DWITH_C_GLIB=OFF \
     -DWITH_JAVA=OFF \
     -DWITH_JAVASCRIPT=OFF \
     -DWITH_LIBEVENT=OFF \
     -DWITH_NODEJS=OFF \
     -DWITH_PYTHON=OFF \
     -DWITH_QT5=OFF \
     -DWITH_ZLIB=OFF \
     ${EXTRA_ARROW_OPTIONS}
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

function install_geos {
  if [[ "$BUILD_GEOS" == "true" ]]; then
    wget_and_untar https://github.com/libgeos/geos/archive/${GEOS_VERSION}.tar.gz geos
    cmake_install_dir geos -DBUILD_TESTING=OFF
  fi
}

# Adapters that can be installed.

function install_aws_deps {
  local AWS_REPO_NAME="aws/aws-sdk-cpp"

  github_checkout $AWS_REPO_NAME $AWS_SDK_VERSION --depth 1 --recurse-submodules
  cmake_install -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_SHARED_LIBS:BOOL=OFF -DMINIMIZE_SIZE:BOOL=ON -DENABLE_TESTING:BOOL=OFF -DBUILD_ONLY:STRING="s3;identity-management"
}

function install_minio {
  local MINIO_OS=${1:-darwin}
  local MINIO_ARCH

  if [[ $MACHINE == aarch64 ]]; then
    MINIO_ARCH="arm64"
  elif [[ $MACHINE == x86_64 ]]; then
    MINIO_ARCH="amd64"
  else
    echo "Unsupported Minio platform"
  fi

  wget ${WGET_OPTIONS} https://dl.min.io/server/minio/release/${MINIO_OS}-${MINIO_ARCH}/archive/minio.RELEASE.${MINIO_VERSION} -O ${MINIO_BINARY_NAME}
  chmod +x ./${MINIO_BINARY_NAME}
  ${SUDO} mv ./${MINIO_BINARY_NAME} /usr/local/bin/
}

function install_gcs-sdk-cpp {
  # Install gcs dependencies
  # https://github.com/googleapis/google-cloud-cpp/blob/main/doc/packaging.md#required-libraries

  # abseil-cpp
  github_checkout abseil/abseil-cpp ${ABSEIL_VERSION} --depth 1
  cmake_install \
    -DABSL_BUILD_TESTING=OFF \
    -DCMAKE_CXX_STANDARD=17 \
    -DABSL_PROPAGATE_CXX_STD=ON \
    -DABSL_ENABLE_INSTALL=ON

  # protobuf
  github_checkout protocolbuffers/protobuf v${PROTOBUF_VERSION} --depth 1
  cmake_install \
    -Dprotobuf_BUILD_TESTS=OFF \
    -Dprotobuf_ABSL_PROVIDER=package

  # grpc
  github_checkout grpc/grpc ${GRPC_VERSION} --depth 1
  cmake_install \
    -DgRPC_BUILD_TESTS=OFF \
    -DgRPC_ABSL_PROVIDER=package \
    -DgRPC_ZLIB_PROVIDER=package \
    -DgRPC_CARES_PROVIDER=package \
    -DgRPC_RE2_PROVIDER=package \
    -DgRPC_SSL_PROVIDER=package \
    -DgRPC_PROTOBUF_PROVIDER=package \
    -DgRPC_INSTALL=ON

  # crc32
  github_checkout google/crc32c ${CRC32_VERSION} --depth 1
  cmake_install \
    -DCRC32C_BUILD_TESTS=OFF \
    -DCRC32C_BUILD_BENCHMARKS=OFF \
    -DCRC32C_USE_GLOG=OFF

  # nlohmann json
  github_checkout nlohmann/json ${NLOHMAN_JSON_VERSION} --depth 1
  cmake_install \
    -DJSON_BuildTests=OFF

  # google-cloud-cpp
  github_checkout googleapis/google-cloud-cpp ${GOOGLE_CLOUD_CPP_VERSION} --depth 1
  cmake_install \
    -DGOOGLE_CLOUD_CPP_ENABLE_EXAMPLES=OFF \
    -DGOOGLE_CLOUD_CPP_ENABLE=storage
}

function install_azure-storage-sdk-cpp {
  # Disable VCPKG to install additional static dependencies under the VCPKG installed path
  # instead of using system pre-installed dependencies.
  export AZURE_SDK_DISABLE_AUTO_VCPKG=ON
  vcpkg_commit_id=7a6f366cefd27210f6a8309aed10c31104436509
  github_checkout azure/azure-sdk-for-cpp azure-storage-files-datalake_${AZURE_SDK_VERSION}
  sed -i "s/set(VCPKG_COMMIT_STRING .*)/set(VCPKG_COMMIT_STRING $vcpkg_commit_id)/" cmake-modules/AzureVcpkg.cmake

  azure_core_dir="sdk/core/azure-core"
  if ! grep -q "baseline" $azure_core_dir/vcpkg.json; then
    # build and install azure-core with the version compatible with system pre-installed openssl
    openssl_version=$(openssl version -v | awk '{print $2}')
    if [[ "$openssl_version" == 1.1.1* ]]; then
      openssl_version="1.1.1n"
    fi
    sed -i "s/\"version-string\"/\"builtin-baseline\": \"$vcpkg_commit_id\",\"version-string\"/" $azure_core_dir/vcpkg.json
    sed -i "s/\"version-string\"/\"overrides\": [{ \"name\": \"openssl\", \"version-string\": \"$openssl_version\" }],\"version-string\"/" $azure_core_dir/vcpkg.json
  fi
  (
    cd $azure_core_dir
    cmake_install  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_SHARED_LIBS=OFF
  )
  # install azure-identity
  (
    cd sdk/identity/azure-identity
    cmake_install -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_SHARED_LIBS=OFF
  )
  # install azure-storage-common
  (
    cd sdk/storage/azure-storage-common
    cmake_install -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_SHARED_LIBS=OFF
  )
  # install azure-storage-blobs
  (
    cd sdk/storage/azure-storage-blobs
    cmake_install -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_SHARED_LIBS=OFF
  )
  # install azure-storage-files-datalake
  (
    cd sdk/storage/azure-storage-files-datalake
    cmake_install -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_SHARED_LIBS=OFF
  )
}

function install_hdfs_deps {
  # Dependencies for Hadoop testing
  wget_and_untar https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz hadoop
  cp -a ${DEPENDENCY_DIR}/hadoop /usr/local/
  wget ${WGET_OPTIONS} -P /usr/local/hadoop/share/hadoop/common/lib/ https://repo1.maven.org/maven2/junit/junit/4.11/junit-4.11.jar
}
