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

include_guard(GLOBAL)

# 3.30.4 is the minimum version required by cudf
cmake_minimum_required(VERSION 3.30.4)

set(VELOX_rapids_cmake_VERSION 25.08)
set(VELOX_rapids_cmake_BUILD_SHA256_CHECKSUM
    38f9e374e726b9c30098c723ba97cde4574f411228453f0d247457406143ae20)
set(VELOX_rapids_cmake_SOURCE_URL
    "https://github.com/rapidsai/rapids-cmake/archive/a0349d5b0eff1c68c399526b512aae754420a5b0.tar.gz"
)
velox_resolve_dependency_url(rapids_cmake)

set(VELOX_rmm_VERSION 25.08)
set(VELOX_rmm_BUILD_SHA256_CHECKSUM
    f2d7a64a3dcfe9b49231375b6df72b3be8a10e0828404463fd5176a9d75a1d4f)
set(VELOX_rmm_SOURCE_URL
    "https://github.com/rapidsai/rmm/archive/29dd32302eb7c3e16fb837a1cfe4baac98071512.tar.gz"
)
velox_resolve_dependency_url(rmm)

set(VELOX_kvikio_VERSION 25.08)
set(VELOX_kvikio_BUILD_SHA256_CHECKSUM
    456126c106830666398b49fbeca572717c0b7f8f612ad7a6c96c7f63a6a1ad98)
set(VELOX_kvikio_SOURCE_URL
    "https://github.com/rapidsai/kvikio/archive/a2bbfeb0de49c29245da15d9df0ae7619c0a7531.tar.gz"
)
velox_resolve_dependency_url(kvikio)

set(VELOX_cudf_VERSION
    25.08
    CACHE STRING "cudf version")

set(VELOX_cudf_BUILD_SHA256_CHECKSUM
    a7a33283d58c4ec56b5183d334445ee5cfccf95d0752899a7d59579e5ee28abe)
set(VELOX_cudf_SOURCE_URL
    "https://github.com/rapidsai/cudf/archive/f366b7f82966ba0a75aba8f806c6c5b58202d483.tar.gz"
)
velox_resolve_dependency_url(cudf)

# Use block so we don't leak variables
block(SCOPE_FOR VARIABLES)
# Setup libcudf build to not have testing components
set(BUILD_TESTS OFF)
set(CUDF_BUILD_TESTUTIL OFF)
set(BUILD_SHARED_LIBS ON)

FetchContent_Declare(
  rapids-cmake
  URL ${VELOX_rapids_cmake_SOURCE_URL}
  URL_HASH ${VELOX_rapids_cmake_BUILD_SHA256_CHECKSUM}
  UPDATE_DISCONNECTED 1)

FetchContent_Declare(
  rmm
  URL ${VELOX_rmm_SOURCE_URL}
  URL_HASH ${VELOX_rmm_BUILD_SHA256_CHECKSUM}
  UPDATE_DISCONNECTED 1)

FetchContent_Declare(
  kvikio
  URL ${VELOX_kvikio_SOURCE_URL}
  URL_HASH ${VELOX_kvikio_BUILD_SHA256_CHECKSUM}
  SOURCE_SUBDIR cpp
  UPDATE_DISCONNECTED 1)

FetchContent_Declare(
  cudf
  URL ${VELOX_cudf_SOURCE_URL}
  URL_HASH ${VELOX_cudf_BUILD_SHA256_CHECKSUM}
  SOURCE_SUBDIR cpp
  UPDATE_DISCONNECTED 1)

FetchContent_MakeAvailable(cudf)

# cudf sets all warnings as errors, and therefore fails to compile with velox
# expanded set of warnings. We selectively disable problematic warnings just for
# cudf
target_compile_options(
  cudf PRIVATE -Wno-non-virtual-dtor -Wno-missing-field-initializers
               -Wno-deprecated-copy -Wno-restrict)

unset(BUILD_SHARED_LIBS)
endblock()
