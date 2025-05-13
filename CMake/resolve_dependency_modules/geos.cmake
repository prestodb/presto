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
# GEOS Configuration
set(VELOX_GEOS_BUILD_VERSION 3.10.2)
set(VELOX_GEOS_BUILD_SHA256_CHECKSUM
    50bbc599ac386b4c2b3962dcc411f0040a61f204aaef4eba7225ecdd0cf45715)
string(CONCAT VELOX_GEOS_SOURCE_URL "https://download.osgeo.org/geos/"
              "geos-${VELOX_GEOS_BUILD_VERSION}.tar.bz2")

velox_resolve_dependency_url(GEOS)

FetchContent_Declare(
  geos
  URL ${VELOX_GEOS_SOURCE_URL}
  URL_HASH ${VELOX_GEOS_BUILD_SHA256_CHECKSUM}
  PATCH_COMMAND
    git apply "${CMAKE_CURRENT_LIST_DIR}/geos/geos-cmakelists.patch" && git
    apply "${CMAKE_CURRENT_LIST_DIR}/geos/geos-build.patch")

list(APPEND CMAKE_MODULE_PATH "${geos_SOURCE_DIR}/cmake")
set(BUILD_SHARED_LIBS ${VELOX_BUILD_SHARED})
set(CMAKE_BUILD_TYPE Release)
set(PREVIOUS_CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -Wno-nonnull ")

# This option defaults to on and adds warning flags that fail the build.
set(GEOS_BUILD_DEVELOPER OFF)

if("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -Wno-dangling-pointer")
endif()

FetchContent_MakeAvailable(geos)

add_library(GEOS::geos ALIAS geos)

unset(BUILD_SHARED_LIBS)
set(CMAKE_CXX_FLAGS ${PREVIOUS_CMAKE_CXX_FLAGS})
set(CMAKE_BUILD_TYPE ${PREVIOUS_BUILD_TYPE})
