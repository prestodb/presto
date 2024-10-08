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

set -eufx -o pipefail

SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
PYTHON_VENV=${PYTHON_VENV:-"${SCRIPTDIR}/../.venv"}
# Prestissimo fails to build DuckDB with error
# "math cannot parse the expression" when this
# script is invoked under the Presto git project.
# Set DEPENDENCY_DIR to a directory outside of Presto
# to build DuckDB.
BUILD_DUCKDB="${BUILD_DUCKDB:-false}"
source "$(dirname "${BASH_SOURCE}")/../velox/scripts/setup-macos.sh"

function install_proxygen {
  github_checkout facebook/proxygen "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF
}

function install_presto_deps {
  install_from_brew "gperf"
  run_and_time install_proxygen
}

if [[ $# -ne 0 ]]; then
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
  install_velox_deps
  install_presto_deps
  echo "All dependencies for Prestissimo installed!"
  echo "To reuse the installed dependencies for subsequent builds, consider adding this to your ~/.zshrc"
  echo "export INSTALL_PREFIX=$INSTALL_PREFIX"
fi
