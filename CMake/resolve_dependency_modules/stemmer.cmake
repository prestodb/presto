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

set(VELOX_STEMMER_VERSION 2.2.0)
set(VELOX_STEMMER_BUILD_SHA256_CHECKSUM
    b941d9fe9cf36b4e2f8d3873cd4d8b8775bd94867a1df8d8c001bb8b688377c3)
set(VELOX_STEMMER_SOURCE_URL
    "https://snowballstem.org/dist/libstemmer_c-${VELOX_STEMMER_VERSION}.tar.gz"
)

resolve_dependency_url(STEMMER)

message(STATUS "Building stemmer from source")
find_program(MAKE_PROGRAM make REQUIRED)

set(STEMMER_PREFIX "${CMAKE_BINARY_DIR}/_deps/libstemmer")
set(STEMMER_INCLUDE_PATH ${STEMMER_PREFIX}/src/libstemmer/include)

# We can not use FetchContent as libstemmer does not use cmake
ExternalProject_Add(
  libstemmer
  PREFIX ${STEMMER_PREFIX}
  SOURCE_DIR ${STEMMER_PREFIX}/src/libstemmer
  URL ${VELOX_STEMMER_SOURCE_URL}
  URL_HASH ${VELOX_STEMMER_BUILD_SHA256_CHECKSUM}
  BUILD_IN_SOURCE TRUE
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${MAKE_PROGRAM}
  INSTALL_COMMAND ""
  PATCH_COMMAND git apply ${CMAKE_CURRENT_LIST_DIR}/libstemmer/Makefile.patch
  BUILD_BYPRODUCTS
    ${STEMMER_PREFIX}/src/libstemmer/${CMAKE_STATIC_LIBRARY_PREFIX}stemmer${CMAKE_STATIC_LIBRARY_SUFFIX}
)

add_library(stemmer STATIC IMPORTED GLOBAL)
add_library(stemmer::stemmer ALIAS stemmer)
file(MAKE_DIRECTORY ${STEMMER_INCLUDE_PATH})
set_target_properties(
  stemmer
  PROPERTIES
    IMPORTED_LOCATION
    ${STEMMER_PREFIX}/src/libstemmer/${CMAKE_STATIC_LIBRARY_PREFIX}stemmer${CMAKE_STATIC_LIBRARY_SUFFIX}
    INTERFACE_INCLUDE_DIRECTORIES ${STEMMER_INCLUDE_PATH})

add_dependencies(stemmer libstemmer)
