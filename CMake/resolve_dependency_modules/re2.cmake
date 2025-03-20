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
  set(VELOX_RE2_VERSION 2024-07-02)
  set(VELOX_RE2_SOURCE_URL
      "https://github.com/google/re2/archive/refs/tags/${VELOX_RE2_VERSION}.tar.gz"
  )
  set(VELOX_RE2_BUILD_SHA256_CHECKSUM
      eb2df807c781601c14a260a507a5bb4509be1ee626024cb45acbd57cb9d4032b)
endif()

message(STATUS "Building re2 from source")
FetchContent_Declare(
  re2
  URL ${VELOX_RE2_SOURCE_URL}
  URL_HASH SHA256=${VELOX_RE2_BUILD_SHA256_CHECKSUM})

set(RE2_USE_ICU ON)
set(RE2_BUILD_TESTING OFF)

# RE2 needs Abseil.
velox_set_source(absl)
velox_resolve_dependency(absl)

FetchContent_MakeAvailable(re2)
if("${absl_SOURCE}" STREQUAL "SYSTEM")
  if(DEFINED absl_VERSION AND "${absl_VERSION}" VERSION_LESS "20240116")
    message(
      FATAL_ERROR
        "Abseil 20240116 or later is required for bundled RE2: ${absl_VERSION}")
  endif()
elseif("${absl_SOURCE}" STREQUAL "BUNDLED")
  # Build RE2 after Abseil so the files are available
  add_dependencies(re2 absl::base)
endif()
if("${ICU_SOURCE}" STREQUAL "BUNDLED")
  # Build RE2 after ICU so the files are available
  add_dependencies(re2 ICU ICU::uc)
endif()

set(re2_LIBRARIES ${re2_BINARY_DIR}/libre2.a)
set(re2_INCLUDE_DIRS ${re2_SOURCE_DIR})

set(RE2_ROOT ${re2_BINARY_DIR})
set(re2_ROOT ${re2_BINARY_DIR})
