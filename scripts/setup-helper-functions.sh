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

# github_checkout $REPO $VERSION $GIT_CLONE_PARAMS clones or re-uses an existing clone of the
# specified repo, checking out the requested version.

DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)/deps-download}
OS_CXXFLAGS=""

function run_and_time {
  time "$@" || (echo "Failed to run $* ." ; exit 1 )
  { echo "+ Finished running $*"; } 2> /dev/null
}

function prompt {
  (
    while true; do
      local input="${PROMPT_ALWAYS_RESPOND:-}"
      echo -n "$(tput bold)$* [Y, n]$(tput sgr0) "
      [[ -z "${input}" ]] && read input
      if [[ "${input}" == "Y" || "${input}" == "y" || "${input}" == "" ]]; then
        return 0
      elif [[ "${input}" == "N" || "${input}" == "n" ]]; then
        return 1
      fi
    done
  ) 2> /dev/null
}

function github_checkout {
  local REPO=$1
  shift
  local VERSION=$1
  shift
  local GIT_CLONE_PARAMS=$@
  local DIRNAME=$(basename $REPO)
  SUDO="${SUDO:-""}"
  cd "${DEPENDENCY_DIR}"
  if [ -z "${DIRNAME}" ]; then
    echo "Failed to get repo name from ${REPO}"
    exit 1
  fi
  if [ -d "${DIRNAME}" ] && prompt "${DIRNAME} already exists. Delete?"; then
    ${SUDO} rm -rf "${DIRNAME}"
  fi
  if [ ! -d "${DIRNAME}" ]; then
    git clone -q -b $VERSION $GIT_CLONE_PARAMS "https://github.com/${REPO}.git"
  fi
  cd "${DIRNAME}"
}

# get_cxx_flags [$CPU_ARCH]
# Echos appropriate compiler flags.
# If $CPU_ARCH is set then we use that else we determine best possible set of flags
# to use based on current cpu architecture.
# The goal of this function is to consolidate all architecture specific flags to one
# location.
# The values that CPU_ARCH can take are as follows:
#   arm64  : Target Apple silicon.
#   aarch64: Target general 64 bit arm cpus.
#   avx:     Target Intel CPUs with AVX.
#   sse:     Target Intel CPUs with sse.
# Echo's the appropriate compiler flags which can be captured as so
# CXX_FLAGS=$(get_cxx_flags) or
# CXX_FLAGS=$(get_cxx_flags "avx")

function get_cxx_flags {
  local CPU_ARCH=${1:-""}
  local OS=$(uname)
  local MACHINE=$(uname -m)

  if [[ -z "$CPU_ARCH" ]]; then
   if [ "$OS" = "Darwin" ]; then
     if [ "$MACHINE" = "arm64" ]; then
       CPU_ARCH="arm64"
     else # x86_64
       local CPU_CAPABILITIES=$(sysctl -a | grep machdep.cpu.features | awk '{print tolower($0)}')
       if [[ $CPU_CAPABILITIES =~ "avx" ]]; then
         CPU_ARCH="avx"
       else
         CPU_ARCH="sse"
       fi
     fi
   elif [ "$OS" = "Linux" ]; then
     if [ "$MACHINE" = "aarch64" ]; then
       CPU_ARCH="aarch64"
     else # x86_64
       local CPU_CAPABILITIES=$(cat /proc/cpuinfo | grep flags | head -n 1| awk '{print tolower($0)}')
       if [[ $CPU_CAPABILITIES =~ "avx" ]]; then
           CPU_ARCH="avx"
       elif [[ $CPU_CAPABILITIES =~ "sse" ]]; then
           CPU_ARCH="sse"
       fi
     fi
   else
     echo "Unsupported platform $OS"; exit 1;
   fi
  fi
  case $CPU_ARCH in

    "arm64")
      echo -n "-mcpu=apple-m1+crc -std=c++17 -fvisibility=hidden"
    ;;

    "avx")
      echo -n "-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17 -mbmi2"
    ;;

    "sse")
      echo -n "-msse4.2 -std=c++17"
    ;;

    "aarch64")
      # Read Arm MIDR_EL1 register to detect Arm cpu.
      # https://developer.arm.com/documentation/100616/0301/register-descriptions/aarch64-system-registers/midr-el1--main-id-register--el1
      ARM_CPU_FILE="/sys/devices/system/cpu/cpu0/regs/identification/midr_el1"

      # https://gitlab.arm.com/telemetry-solution/telemetry-solution/-/blob/main/data/pmu/cpu/neoverse/neoverse-n1.json#L13
      # N1:d0c; N2:d49; V1:d40;
      Neoverse_N1="d0c"
      Neoverse_N2="d49"
      Neoverse_V1="d40"
      if [ -f "$ARM_CPU_FILE" ]; then
        hex_ARM_CPU_DETECT=`cat $ARM_CPU_FILE`
        # PartNum, [15:4]: The primary part number such as Neoverse N1/N2 core.
        ARM_CPU_PRODUCT=${hex_ARM_CPU_DETECT: -4:3}

        if [ "$ARM_CPU_PRODUCT" = "$Neoverse_N1" ]; then
          echo -n "-mcpu=neoverse-n1 -std=c++17"
        elif [ "$ARM_CPU_PRODUCT" = "$Neoverse_N2" ]; then
          echo -n "-mcpu=neoverse-n2 -std=c++17"
        elif [ "$ARM_CPU_PRODUCT" = "$Neoverse_V1" ]; then
          echo -n "-mcpu=neoverse-v1 -std=c++17"
        else
          echo -n "-march=armv8-a+crc+crypto -std=c++17"
        fi
      else
        echo -n "-std=c++17"
      fi
    ;;
  *)
    echo -n "Architecture not supported!"
  esac

}

function wget_and_untar {
  local URL=$1
  local DIR=$2
  mkdir -p "${DEPENDENCY_DIR}"
  pushd "${DEPENDENCY_DIR}"
  SUDO="${SUDO:-""}"
  if [ -d "${DIR}" ]; then
    if prompt "${DIR} already exists. Delete?"; then
      ${SUDO} rm -rf "${DIR}"
    else
      popd
      return
    fi
  fi
  mkdir -p "${DIR}"
  pushd "${DIR}"
  curl -L "${URL}" > $2.tar.gz
  tar -xz --strip-components=1 -f $2.tar.gz
  popd
  popd
}

function cmake_install_dir {
  pushd "${DEPENDENCY_DIR}/$1"
  # remove the directory argument
  shift
  cmake_install $@
  popd
}

function cmake_install {
  local NAME=$(basename "$(pwd)")
  local BINARY_DIR=_build
  SUDO="${SUDO:-""}"
  if [ -d "${BINARY_DIR}" ]; then
    if prompt "Do you want to rebuild ${NAME}?"; then
      ${SUDO} rm -rf "${BINARY_DIR}"
    else
      return
    fi
  fi

  mkdir -p "${BINARY_DIR}"
  COMPILER_FLAGS=$(get_cxx_flags)
  # Add platform specific CXX flags if any
  COMPILER_FLAGS+=${OS_CXXFLAGS}

  # CMAKE_POSITION_INDEPENDENT_CODE is required so that Velox can be built into dynamic libraries \
  cmake -Wno-dev -B"${BINARY_DIR}" \
    -GNinja \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_CXX_STANDARD=17 \
    "${INSTALL_PREFIX+-DCMAKE_PREFIX_PATH=}${INSTALL_PREFIX-}" \
    "${INSTALL_PREFIX+-DCMAKE_INSTALL_PREFIX=}${INSTALL_PREFIX-}" \
    -DCMAKE_CXX_FLAGS="$COMPILER_FLAGS" \
    -DBUILD_TESTING=OFF \
    "$@"
  # Exit if the build fails.
  cmake --build "${BINARY_DIR}" || { echo 'build failed' ; exit 1; }
  ${SUDO} cmake --install "${BINARY_DIR}"
}

