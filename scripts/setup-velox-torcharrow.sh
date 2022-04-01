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
export CFLAGS="-mavx2 -mfma -mavx -mf16c -masm=intel -mlzcnt -std=c++17"
export CXXFLAGS=$CFLAGS  # Used by boost.

yum -y install ccache
yum -y install ninja-build
yum -y install git
yum -y install openssl-devel
yum -y install double-conversion-devel
yum -y install glog-devel
yum -y install bzip2-devel
yum -y install gflags-devel
yum -y install gtest-devel
yum -y install libevent-devel
yum -y install lz4-devel
yum -y install libzstd-devel
yum -y install re2-devel
yum -y install snappy-devel
yum -y install lzo-devel
yum -y install wget
yum -y install python3-devel.x86_64
yum -y install fmt-devel

#Install conda
rpm --import https://repo.anaconda.com/pkgs/misc/gpgkeys/anaconda.asc

# Add the Anaconda repository
cat <<EOF > /etc/yum.repos.d/conda.repo
[conda]
name=Conda
baseurl=https://repo.anaconda.com/pkgs/misc/rpmrepo/conda
enabled=1
gpgcheck=1
gpgkey=https://repo.anaconda.com/pkgs/misc/gpgkeys/anaconda.asc
EOF

yum -y install conda

function cmake_install {
  cmake -B "$1-build" -GNinja -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_CXX_FLAGS="${CFLAGS}" -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_BUILD_TYPE=Release -Wno-dev "$@"
  ninja -C "$1-build" install
}

function wget_and_untar {
  local URL=$1
  local DIR=$2
  mkdir -p "${DIR}"
  wget -q --max-redirect 3 -O - "${URL}" | tar -xz -C "${DIR}" --strip-components=1
}


wget_and_untar https://boostorg.jfrog.io/artifactory/main/release/1.69.0/source/boost_1_69_0.tar.gz boost &
wget_and_untar https://github.com/facebook/folly/archive/v2022.03.14.00.tar.gz folly &

wait

(
  cd boost
  ls
  ./bootstrap.sh --prefix=/usr/local
  CPLUS_INCLUDE_PATH=/usr/include/python3.6m  ./b2 "-j$(nproc)" -d0 install threading=multi
)

# Folly fails to build in release-mode due
# AtomicUtil-inl.h:202: Error: operand type mismatch for `bts'
cmake_install folly -DCMAKE_BUILD_TYPE=Debug
