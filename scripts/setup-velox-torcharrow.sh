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
CONDA=${1:-true}
export CFLAGS=$(get_cxx_flags $CPU_TARGET)
export CXXFLAGS=$CFLAGS  # Used by boost.

yum -y install bzip2-devel \
  ccache \
  double-conversion-devel \
  flex \
  gflags-devel \
  git \
  glog-devel \
  libevent-devel \
  libicu-devel \
  libzstd-devel \
  lz4-devel \
  lzo-devel \
  ninja-build \
  pcre-devel \
  perl-core \
  python3-devel.x86_64 \
  re2-devel \
  snappy-devel \
  wget \
  zlib-devel

if [ "$CONDA" = true ]; then
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
fi

function cmake_install {
  cmake -B "$1-build" -GNinja -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_CXX_FLAGS="${CFLAGS}" -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_BUILD_TYPE=Release -Wno-dev "$@"
  ninja -C "$1-build" install
}

function wget_and_untar {
  local URL=$1
  local DIR=$2
  mkdir -p "${DIR}"
  wget --no-check-certificate -q --max-redirect 3 -O - "${URL}" | tar -xz -C "${DIR}" --strip-components=1
}

wget_and_untar https://github.com/gflags/gflags/archive/refs/tags/v2.2.2.tar.gz gflags
wget_and_untar https://ftp.openssl.org/source/openssl-1.1.1k.tar.gz openssl &
wget_and_untar https://boostorg.jfrog.io/artifactory/main/release/1.69.0/source/boost_1_69_0.tar.gz boost &
wget_and_untar https://github.com/facebook/folly/archive/v2022.11.14.00.tar.gz folly &
wget_and_untar https://github.com/fmtlib/fmt/archive/refs/tags/10.1.1.tar.gz fmt &

wait

(
  cd openssl
  ./config --prefix=/usr --openssldir=/etc/ssl --libdir=lib no-shared zlib-dynamic
  make install
)

(
  cd boost
  ls
  ./bootstrap.sh --prefix=/usr/local
  CPLUS_INCLUDE_PATH=/usr/include/python3.6m  ./b2 "-j$(nproc)" -d0 install threading=multi
)

cmake_install gflags -DBUILD_SHARED_LIBS=ON
cmake_install fmt -DFMT_TEST=OFF
cmake_install folly -DFOLLY_HAVE_INT128_T=ON
