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

set(VELOX_rapids_cmake_VERSION 25.04)
set(VELOX_rapids_cmake_BUILD_SHA256_CHECKSUM
    458c14eaff9000067b32d65c8c914f4521090ede7690e16eb57035ce731386db)
set(VELOX_rapids_cmake_SOURCE_URL
    "https://github.com/rapidsai/rapids-cmake/archive/7828fc8ff2e9f4fa86099f3c844505c2f47ac672.tar.gz"
)
velox_resolve_dependency_url(rapids_cmake)

set(VELOX_rmm_VERSION 25.04)
set(VELOX_rmm_BUILD_SHA256_CHECKSUM
    294905094213a2d1fd8e024500359ff871bc52f913a3fbaca3514727c49f62de)
set(VELOX_rmm_SOURCE_URL
    "https://github.com/rapidsai/rmm/archive/d8b7dacdeda302d2e37313c02d14ef5e1d1e98ea.tar.gz"
)
velox_resolve_dependency_url(rmm)

set(VELOX_kvikio_VERSION 25.04)
set(VELOX_kvikio_BUILD_SHA256_CHECKSUM
    4a0b15295d0a397433930bf9a309e4ad2361b25dc7a7b3e6a35d0c9419d0cb62)
set(VELOX_kvikio_SOURCE_URL
    "https://github.com/rapidsai/kvikio/archive/5c710f37236bda76e447e929e17b1efbc6c632c3.tar.gz"
)
velox_resolve_dependency_url(kvikio)

set(VELOX_cudf_VERSION 25.04)
set(VELOX_cudf_BUILD_SHA256_CHECKSUM
    e5a1900dfaf23dab2c5808afa17a2d04fa867d2892ecec1cb37908f3b73715c2)
set(VELOX_cudf_SOURCE_URL
    "https://github.com/rapidsai/cudf/archive/4c1c99011da2c23856244e05adda78ba66697105.tar.gz"
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
               -Wno-deprecated-copy)

unset(BUILD_SHARED_LIBS)
endblock()
