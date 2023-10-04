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

cd "${DEPENDENCY_DIR}" || exit

install_jwt=0

if [ "$#" -eq 0 ]; then
    # Install all adapters by default
    install_jwt=1
fi

while [[ $# -gt 0 ]]; do
  case $1 in
    jwt)
      install_jwt=1
      shift # past argument
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

_ret=$?
if [ $_ret -eq 0 ] ; then
   echo "All deps for Presto adapters installed!"
fi
