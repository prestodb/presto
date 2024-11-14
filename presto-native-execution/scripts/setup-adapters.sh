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

# Propagate errors and improve debugging.
set -eufx -o pipefail

SCRIPT_DIR=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")
if [ -f "${SCRIPT_DIR}/setup-helper-functions.sh" ]
then
  source "${SCRIPT_DIR}/setup-helper-functions.sh"
else
  source "${SCRIPT_DIR}/../velox/scripts/setup-helper-functions.sh"
fi
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}

function install_jwt_cpp {
  github_checkout Thalhammer/jwt-cpp v0.6.0 --depth 1
  cmake_install -DBUILD_TESTS=OFF -DJWT_BUILD_EXAMPLES=OFF -DJWT_DISABLE_PICOJSON=ON -DJWT_CMAKE_FILES_INSTALL_DIR="${DEPENDENCY_DIR}/jwt-cpp"
}

function install_prometheus_cpp {
  github_checkout jupp0r/prometheus-cpp v1.2.4 --depth 1
  git submodule init
  git submodule update
  cmake_install -DBUILD_SHARED_LIBS=ON -DENABLE_PUSH=OFF -DENABLE_COMPRESSION=OFF
}

function install_abseil {
  # abseil-cpp
  github_checkout abseil/abseil-cpp 20240116.2 --depth 1
  cmake_install \
    -DABSL_BUILD_TESTING=OFF \
    -DCMAKE_CXX_STANDARD=17 \
    -DABSL_PROPAGATE_CXX_STD=ON \
    -DABSL_ENABLE_INSTALL=ON
}

function install_grpc {
  # grpc
  github_checkout grpc/grpc v1.48.1 --depth 1
  cmake_install \
    -DgRPC_BUILD_TESTS=OFF \
    -DgRPC_ABSL_PROVIDER=package \
    -DgRPC_ZLIB_PROVIDER=package \
    -DgRPC_CARES_PROVIDER=package \
    -DgRPC_RE2_PROVIDER=package \
    -DgRPC_SSL_PROVIDER=package \
    -DgRPC_PROTOBUF_PROVIDER=package \
    -DgRPC_INSTALL=ON
}

function install_arrow_flight {
  ARROW_VERSION="${ARROW_VERSION:-15.0.0}"
  if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    export INSTALL_PREFIX=${INSTALL_PREFIX:-"/usr/local"}
    LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})
    if [[ "$LINUX_DISTRIBUTION" == "ubuntu" || "$LINUX_DISTRIBUTION" == "debian" ]]; then
      SUDO="${SUDO:-"sudo --preserve-env"}"
      ${SUDO} apt install -y libc-ares-dev
      ${SUDO} ldconfig -v 2>/dev/null | grep "${INSTALL_PREFIX}/lib" || \
        echo "${INSTALL_PREFIX}/lib" | ${SUDO} tee /etc/ld.so.conf.d/local-libraries.conf > /dev/null \
        && ${SUDO} ldconfig
    else
      dnf -y install c-ares-devel
      ldconfig -v 2>/dev/null | grep "${INSTALL_PREFIX}/lib" || \
        echo "${INSTALL_PREFIX}/lib" | tee /etc/ld.so.conf.d/local-libraries.conf > /dev/null \
        && ldconfig
    fi
  else
    # The installation script for the Arrow Flight connector currently works only on Linux distributions.
    return 0
  fi

  install_abseil
  install_grpc

  # NOTE: benchmarks are on due to a compilation error with v15.0.0, once updated that can be removed
  # see https://github.com/apache/arrow/issues/41617
  wget_and_untar https://github.com/apache/arrow/archive/apache-arrow-${ARROW_VERSION}.tar.gz arrow
  cmake_install_dir arrow/cpp \
    -DARROW_FLIGHT=ON \
    -DARROW_BUILD_BENCHMARKS=ON \
    -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
}

cd "${DEPENDENCY_DIR}" || exit

install_jwt=0
install_prometheus_cpp=0
install_arrow_flight=0

if [ "$#" -eq 0 ]; then
    # Install all adapters by default
    install_jwt=1
    install_prometheus_cpp=1
    install_arrow_flight=1
fi

while [[ $# -gt 0 ]]; do
  case $1 in
    jwt)
      install_jwt=1
      shift # past argument
      ;;
    prometheus)
      install_prometheus_cpp=1;
      shift
          ;;
    arrow_flight)
      install_arrow_flight=1;
      shift
      ;;
    *)
      echo "ERROR: Unknown option $1! will be ignored!"
      shift
      ;;

  esac
done

if [ $install_jwt -eq 1 ]; then
  install_jwt_cpp
fi

if [ $install_prometheus_cpp -eq 1 ]; then
  install_prometheus_cpp
fi

if [ $install_arrow_flight -eq 1 ]; then
  install_arrow_flight
fi

_ret=$?
if [ $_ret -eq 0 ] ; then
   echo "All deps for Presto adapters installed!"
fi
