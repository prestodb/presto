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
if [ -f "${SCRIPT_DIR}/setup-common.sh" ]
then
  source "${SCRIPT_DIR}/setup-common.sh"
else
  source "${SCRIPT_DIR}/../velox/scripts/setup-common.sh"
fi
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}

OS=$(uname)
if [ "$OS" = "Darwin" ]; then
  export INSTALL_PREFIX=${INSTALL_PREFIX:-"$(pwd)/deps-install"}
else
  export INSTALL_PREFIX=${INSTALL_PREFIX:-"/usr/local"}
fi

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

function install_arrow_flight {
  # Velox provides an install for the Arrow library. Rebuild with the original Velox options and
  # Arrow Flight enabled. The Velox version of Arrow is used.
  # NOTE: benchmarks are on due to a compilation error with v15.0.0, once updated that can be removed
  # see https://github.com/apache/arrow/issues/41617
  EXTRA_ARROW_OPTIONS=" -DARROW_FLIGHT=ON -DARROW_BUILD_BENCHMARKS=ON "
  install_arrow
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
