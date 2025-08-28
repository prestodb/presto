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

export CC=/opt/rh/gcc-toolset-12/root/bin/gcc
export CXX=/opt/rh/gcc-toolset-12/root/bin/g++

WGET_OPTIONS=${WGET_OPTIONS:-""}

CPU_TARGET="${CPU_TARGET:-avx}"
SCRIPT_DIR=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")
if [ -f "${SCRIPT_DIR}/setup-centos9.sh" ]
then
  source "${SCRIPT_DIR}/setup-centos9.sh"
else
  source "${SCRIPT_DIR}/../velox/scripts/setup-centos9.sh"
fi

# NPROC is normally sourced from the Velox setup scripts.
export NPROC=${NPROC:-$(getconf _NPROCESSORS_ONLN)}

function install_presto_deps_from_package_managers {
  dnf install -y maven java clang-tools-extra jq perl-XML-XPath
  # This python version is installed by the Velox setup scripts
  pip install regex pyyaml chevron black
}

function install_gperf {
  wget ${WGET_OPTIONS} https://ftp.gnu.org/pub/gnu/gperf/gperf-3.1.tar.gz ||
  wget ${WGET_OPTIONS} https://mirrors.ocf.berkeley.edu/gnu/gperf/gperf-3.1.tar.gz &&
  tar -xzf gperf-3.1.tar.gz &&
  cd gperf-3.1 &&
  ./configure --prefix=/usr/local/gperf/3_1 &&
  make "-j${NPROC}" &&
  make install
  if [ -f /usr/local/bin/gperf ]; then
    echo "Did not create '/usr/local/bin/gperf' symlink as file already exists."
  else
    ln -s /usr/local/gperf/3_1/bin/gperf /usr/local/bin/
  fi
}

function install_proxygen {
  git clone https://github.com/facebook/proxygen
  cd proxygen
  git checkout $FB_OS_VERSION
  # Folly Portability.h being used to decide whether or not support coroutines
  # causes issues (build, lin) if the selection is not consistent across users of folly.
  EXTRA_PKG_CXXFLAGS=" -DFOLLY_CFG_NO_COROUTINES"
  cmake_install -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON
}

function install_presto_deps {
  run_and_time install_presto_deps_from_package_managers
  run_and_time install_gperf
  run_and_time install_proxygen
}

if [[ $# -ne 0 ]]; then
  # Activate gcc12; enable errors on unset variables afterwards.
  source /opt/rh/gcc-toolset-12/enable || exit 1
  set -u
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
  # Activate gcc12; enable errors on unset variables afterwards.
  source /opt/rh/gcc-toolset-12/enable || exit 1
  set -u
  install_velox_deps
  install_presto_deps
  echo "All dependencies for Prestissimo installed!"
fi

dnf clean all
