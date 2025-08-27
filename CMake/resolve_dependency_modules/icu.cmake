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

set(VELOX_ICU4C_BUILD_VERSION 72)
set(
  VELOX_ICU4C_BUILD_SHA256_CHECKSUM
  a2d2d38217092a7ed56635e34467f92f976b370e20182ad325edea6681a71d68
)
string(
  CONCAT
  VELOX_ICU4C_SOURCE_URL
  "https://github.com/unicode-org/icu/releases/download/"
  "release-${VELOX_ICU4C_BUILD_VERSION}-1/"
  "icu4c-${VELOX_ICU4C_BUILD_VERSION}_1-src.tgz"
)

velox_resolve_dependency_url(ICU4C)

message(STATUS "Building ICU4C from source")

ProcessorCount(NUM_JOBS)
velox_set_with_default(NUM_JOBS NUM_THREADS ${NUM_JOBS})
find_program(MAKE_PROGRAM make REQUIRED)

set(ICU_CFG --disable-tests --disable-samples)
set(
  HOST_ENV_CMAKE
  ${CMAKE_COMMAND}
  -E
  env
  CC="${CMAKE_C_COMPILER}"
  CXX="${CMAKE_CXX_COMPILER}"
  CFLAGS="${CMAKE_C_FLAGS}"
  CXXFLAGS="${CMAKE_CXX_FLAGS} -w"
  LDFLAGS="${CMAKE_MODULE_LINKER_FLAGS}"
)
set(ICU_DIR ${CMAKE_CURRENT_BINARY_DIR}/_deps/icu)
set(ICU_INCLUDE_DIRS ${ICU_DIR}/include)
set(ICU_LIBRARIES ${ICU_DIR}/lib)

# We can not use FetchContent as ICU does not use cmake
ExternalProject_Add(
  ICU
  URL ${VELOX_ICU4C_SOURCE_URL}
  URL_HASH ${VELOX_ICU4C_BUILD_SHA256_CHECKSUM}
  SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR}/icu-src
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/icu-bld
  CONFIGURE_COMMAND
    <SOURCE_DIR>/source/configure --prefix=${ICU_DIR} --libdir=${ICU_LIBRARIES} ${ICU_CFG}
  BUILD_COMMAND ${MAKE_PROGRAM} -j ${NUM_JOBS}
  INSTALL_COMMAND ${HOST_ENV_CMAKE} ${MAKE_PROGRAM} install
)

add_library(ICU::ICU UNKNOWN IMPORTED)
add_dependencies(ICU::ICU ICU-build)

# We have to manually create these files and folders so that the checks for the
# files made at configure time (before icu is built) pass
file(MAKE_DIRECTORY ${ICU_INCLUDE_DIRS})
file(MAKE_DIRECTORY ${ICU_LIBRARIES})

# Create a target for each component
set(
  icu_components
  data
  i18n
  io
  uc
  tu
)

foreach(component ${icu_components})
  add_library(ICU::${component} SHARED IMPORTED)
  string(CONCAT ICU_${component}_LIBRARY ${ICU_LIBRARIES} "/libicu" ${component} ".so")
  file(TOUCH ${ICU_${component}_LIBRARY})
  set_target_properties(
    ICU::${component}
    PROPERTIES
      IMPORTED_LOCATION ${ICU_${component}_LIBRARY}
      INTERFACE_SYSTEM_INCLUDE_DIRECTORIES ${ICU_INCLUDE_DIRS}
  )
  target_link_libraries(ICU::ICU INTERFACE ICU::${component})
endforeach()

# We have to keep the FindICU.cmake in a subfolder to prevent it from overriding
# the system provided one when ICU_SOURCE=SYSTEM
list(PREPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/icu)
