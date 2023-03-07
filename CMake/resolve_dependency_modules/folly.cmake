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

if(DEFINED ENV{VELOX_FOLLY_URL})
  set(FOLLY_SOURCE_URL "$ENV{VELOX_FOLLY_URL}")
else()
  set(VELOX_FOLLY_BUILD_VERSION v2022.11.14.00)
  set(FOLLY_SOURCE_URL
      "https://github.com/facebook/folly/archive/${VELOX_FOLLY_BUILD_VERSION}.tar.gz"
  )
  set(VELOX_FOLLY_BUILD_SHA256_CHECKSUM
      b249436cb61b6dfd5288093565438d8da642b07ae021191a4042b221bc1bdc0e)
endif()

message(STATUS "Building Folly from source")
# FOLLY_CXX_FLAGS is used internally on folly to define some extra
# CMAKE_CXX_FLAGS for some known warnings to avoid possible errors on some
# OS/archs
set(EXTRA_CXX_FLAGS -Wno-deprecated-declarations)
check_cxx_compiler_flag(-Wnullability-completeness
                        COMPILER_HAS_W_NULLABILITY_COMPLETENESS)
if(COMPILER_HAS_W_NULLABILITY_COMPLETENESS)
  list(APPEND EXTRA_CXX_FLAGS -Wno-nullability-completeness)
endif()
check_cxx_compiler_flag(-Wstringop-overflow COMPILER_HAS_W_STRINGOP_OVERFLOW)
if(COMPILER_HAS_W_STRINGOP_OVERFLOW)
  list(APPEND EXTRA_CXX_FLAGS -Wno-stringop-overflow)
endif()
check_cxx_compiler_flag(-Wundef-prefix COMPILER_HAS_W_UNDEF_PREFIX)
if(COMPILER_HAS_W_UNDEF_PREFIX)
  list(APPEND EXTRA_CXX_FLAGS -Wno-undef-prefix)
endif()
set(FOLLY_CXX_FLAGS -Wno-unused -Wno-unused-parameter -Wno-overloaded-virtual
                    ${EXTRA_CXX_FLAGS})

if(gflags_SOURCE STREQUAL "BUNDLED")
  set(glog_patch && git apply
                 ${CMAKE_CURRENT_LIST_DIR}/folly/folly-gflags-glog.patch)
endif()

FetchContent_Declare(
  folly
  URL ${FOLLY_SOURCE_URL}
  URL_HASH SHA256=${VELOX_FOLLY_BUILD_SHA256_CHECKSUM}
  PATCH_COMMAND git apply ${CMAKE_CURRENT_LIST_DIR}/folly/folly-no-export.patch
                ${glog_patch})

if(ON_APPLE_M1)
  # folly will wrongly assume x86_64 if this is not set
  set(CMAKE_LIBRARY_ARCHITECTURE aarch64)
endif()
FetchContent_MakeAvailable(folly)

# Avoid possible errors for known warnings
target_compile_options(folly PUBLIC ${FOLLY_CXX_FLAGS})

# Folly::folly is not valid for FC but we want to match FindFolly
add_library(Folly::folly ALIAS folly)

if(${gflags_SOURCE} STREQUAL "BUNDLED")
  add_dependencies(folly glog gflags)
endif()

set(FOLLY_BENCHMARK_STATIC_LIB
    ${folly_BINARY_DIR}/folly/libfollybenchmark${CMAKE_STATIC_LIBRARY_SUFFIX})
set(FOLLY_LIBRARIES folly)
