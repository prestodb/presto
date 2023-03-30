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

if(DEFINED ENV{VELOX_RE2_URL})
  set(VELOX_RE2_SOURCE_URL "$ENV{VELOX_RE2_URL}")
else()
  set(VELOX_RE2_VERSION 2022-02-01)
  set(VELOX_RE2_SOURCE_URL
      "https://github.com/google/re2/archive/refs/tags/${VELOX_RE2_VERSION}.tar.gz"
  )
  set(VELOX_RE2_BUILD_SHA256_CHECKSUM
      9c1e6acfd0fed71f40b025a7a1dabaf3ee2ebb74d64ced1f9ee1b0b01d22fd27)
endif()

message(STATUS "Building re2 from source")
FetchContent_Declare(
  re2
  URL ${VELOX_RE2_SOURCE_URL}
  URL_HASH SHA256=${VELOX_RE2_BUILD_SHA256_CHECKSUM})

set(RE2_USE_ICU ON)
set(RE2_BUILD_TESTING OFF)

FetchContent_MakeAvailable(re2)

if(${ICU_SOURCE} STREQUAL "BUNDLED")
  # build re2 after icu so the files are available
  add_dependencies(re2 ICU ICU::uc)
endif()

set(re2_LIBRARIES ${re2_BINARY_DIR}/libre2.a)
set(re2_INCLUDE_DIRS ${re2_SOURCE_DIR})
add_library(re2::re2 ALIAS re2)

set(RE2_ROOT ${re2_BINARY_DIR})
set(re2_ROOT ${re2_BINARY_DIR})
