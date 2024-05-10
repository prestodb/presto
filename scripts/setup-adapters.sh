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

SCRIPTDIR=$(dirname "$0")
source $SCRIPTDIR/setup-helper-functions.sh

# Propagate errors and improve debugging.
set -eufx -o pipefail

SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
source $SCRIPTDIR/setup-helper-functions.sh
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}
CMAKE_BUILD_TYPE="${BUILD_TYPE:-Release}"
MACHINE=$(uname -m)

function install_aws_deps {
  local AWS_REPO_NAME="aws/aws-sdk-cpp"
  local AWS_SDK_VERSION="1.11.321"

  github_checkout $AWS_REPO_NAME $AWS_SDK_VERSION --depth 1 --recurse-submodules
  cmake_install -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_SHARED_LIBS:BOOL=OFF -DMINIMIZE_SIZE:BOOL=ON -DENABLE_TESTING:BOOL=OFF -DBUILD_ONLY:STRING="s3;identity-management"
  # Dependencies for S3 testing
  # We need this specific version of Minio for testing.
  if [[ "$OSTYPE" == linux-gnu* ]]; then
    wget https://dl.min.io/server/minio/release/linux-amd64/archive/minio-20220526054841.0.0.x86_64.rpm
    rpm -i minio-20220526054841.0.0.x86_64.rpm
    rm minio-20220526054841.0.0.x86_64.rpm
  fi
  # minio will have to approved under the Privacy & Security on MacOS on first use.
  if [[ "$OSTYPE" == darwin* ]]; then
    if [ "$MACHINE" = "x86_64" ]; then
      wget https://dl.min.io/server/minio/release/darwin-arm64/archive/minio.RELEASE.2022-05-26T05-48-41Z -O minio
      chmod +x ./minio
      sudo mv ./minio /usr/local/bin/
    fi
    if [ "$MACHINE" = "arm64" ]; then
      wget https://dl.min.io/server/minio/release/darwin-arm64/archive/minio.RELEASE.2022-05-26T05-48-41Z -O minio
      chmod +x ./minio
      sudo mv ./minio /usr/local/bin/
    fi
  fi
}

function install_gcs-sdk-cpp {
  # Install gcs dependencies
  # https://github.com/googleapis/google-cloud-cpp/blob/main/doc/packaging.md#required-libraries

  # abseil-cpp
  github_checkout abseil/abseil-cpp 20240116.1 --depth 1
  sed -i 's/^#define ABSL_OPTION_USE_\(.*\) 2/#define ABSL_OPTION_USE_\1 0/' "absl/base/options.h"
  cmake_install -DBUILD_SHARED_LIBS=OFF \
    -DABSL_BUILD_TESTING=OFF

  # crc32
  github_checkout google/crc32c 1.1.2 --depth 1
  cmake_install -DBUILD_SHARED_LIBS=OFF \
    -DCRC32C_BUILD_TESTS=OFF \
    -DCRC32C_BUILD_BENCHMARKS=OFF \
    -DCRC32C_USE_GLOG=OFF

  # nlohmann json
  github_checkout nlohmann/json v3.11.3 --depth 1
  cmake_install -DBUILD_SHARED_LIBS=OFF \
    -DJSON_BuildTests=OFF

  # google-cloud-cpp
  github_checkout googleapis/google-cloud-cpp v2.22.0 --depth 1
  cmake_install -DBUILD_SHARED_LIBS=OFF \
    -DCMAKE_INSTALL_MESSAGE=NEVER \
    -DGOOGLE_CLOUD_CPP_ENABLE_EXAMPLES=OFF \
    -DGOOGLE_CLOUD_CPP_ENABLE=storage
}

function install_azure-storage-sdk-cpp {
  # Disable VCPKG to install additional static dependencies under the VCPKG installed path
  # instead of using system pre-installed dependencies.
  export AZURE_SDK_DISABLE_AUTO_VCPKG=ON
  vcpkg_commit_id=7a6f366cefd27210f6a8309aed10c31104436509
  github_checkout azure/azure-sdk-for-cpp azure-storage-files-datalake_12.8.0
  sed -i "s/set(VCPKG_COMMIT_STRING .*)/set(VCPKG_COMMIT_STRING $vcpkg_commit_id)/" cmake-modules/AzureVcpkg.cmake

  cd sdk/core/azure-core
  if ! grep -q "baseline" vcpkg.json; then
    # build and install azure-core with the version compatible with system pre-installed openssl
    openssl_version=$(openssl version -v | awk '{print $2}')
    if [[ "$openssl_version" == 1.1.1* ]]; then
      openssl_version="1.1.1n"
    fi
    sed -i "s/\"version-string\"/\"builtin-baseline\": \"$vcpkg_commit_id\",\"version-string\"/" vcpkg.json
    sed -i "s/\"version-string\"/\"overrides\": [{ \"name\": \"openssl\", \"version-string\": \"$openssl_version\" }],\"version-string\"/" vcpkg.json
  fi
  cmake_install -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_SHARED_LIBS=OFF

  cd -
  # install azure-storage-common
  cd sdk/storage/azure-storage-common
  cmake_install -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_SHARED_LIBS=OFF

  cd -
  # install azure-storage-blobs
  cd sdk/storage/azure-storage-blobs
  cmake_install -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_SHARED_LIBS=OFF

  cd -
  # install azure-storage-files-datalake
  cd sdk/storage/azure-storage-files-datalake
  cmake_install -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_SHARED_LIBS=OFF
}

function install_hdfs_deps {
  github_checkout apache/hawq master
  cd $DEPENDENCY_DIR/hawq/depends/libhdfs3
  if [[ "$OSTYPE" == darwin* ]]; then
     sed -i '' -e "/FIND_PACKAGE(GoogleTest REQUIRED)/d" ./CMakeLists.txt
     sed -i '' -e "s/dumpversion/dumpfullversion/" ./CMakeLists.txt
  fi

  if [[ "$OSTYPE" == linux-gnu* ]]; then
    sed -i "/FIND_PACKAGE(GoogleTest REQUIRED)/d" ./CMakeLists.txt
    sed -i "s/dumpversion/dumpfullversion/" ./CMake/Platform.cmake
    # Dependencies for Hadoop testing
    wget_and_untar https://archive.apache.org/dist/hadoop/common/hadoop-2.10.1/hadoop-2.10.1.tar.gz hadoop
    cp -a hadoop /usr/local/
  fi
  cmake_install
}

cd "${DEPENDENCY_DIR}" || exit
# aws-sdk-cpp missing dependencies

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
   # /etc/os-release is a standard way to query various distribution
   # information and is available everywhere
   LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})
   if [[ "$LINUX_DISTRIBUTION" == "ubuntu" || "$LINUX_DISTRIBUTION" == "debian" ]]; then
      apt install -y --no-install-recommends libxml2-dev libgsasl7-dev uuid-dev
      # Dependencies of GCS, probably a workaround until the docker image is rebuilt
      apt install -y --no-install-recommends libc-ares-dev libcurl4-openssl-dev
      # Dependencies of Azure Storage Blob cpp
      apt install -y openssl
   else # Assume Fedora/CentOS
      yum -y install libxml2-devel libgsasl-devel libuuid-devel
      # Dependencies of GCS, probably a workaround until the docker image is rebuilt
      yum -y install curl-devel c-ares-devel
      # Dependencies of Azure Storage Blob Cpp
      yum -y install perl-IPC-Cmd
      yum -y install openssl
   fi
fi

if [[ "$OSTYPE" == darwin* ]]; then
   brew install libxml2 gsasl
fi

install_aws=0
install_gcs=0
install_hdfs=0
install_abfs=0

if [ "$#" -eq 0 ]; then
    # Install all adapters by default
    install_aws=1
    install_gcs=1
    install_hdfs=1
    install_abfs=1
fi

while [[ $# -gt 0 ]]; do
  case $1 in
    gcs)
      install_gcs=1
      shift # past argument
      ;;
    aws)
      install_aws=1
      shift # past argument
      ;;
    hdfs)
      install_hdfs=1
      shift # past argument
      ;;
    abfs)
      install_abfs=1
      shift # past argument
      ;;
    *)
      echo "ERROR: Unknown option $1! will be ignored!"
      shift
      ;;
  esac
done

if [ $install_gcs -eq 1 ]; then
  install_gcs-sdk-cpp
fi
if [ $install_aws -eq 1 ]; then
  install_aws_deps
fi
if [ $install_hdfs -eq 1 ]; then
  install_hdfs_deps
fi
if [ $install_abfs -eq 1 ]; then
  install_azure-storage-sdk-cpp
fi

_ret=$?
if [ $_ret -eq 0 ] ; then
   echo "All deps for Velox adapters installed!"
fi
