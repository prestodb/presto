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

set(VELOX_FOLLY_BUILD_VERSION v2022.11.14.00)
set(VELOX_FOLLY_BUILD_SHA256_CHECKSUM
    b249436cb61b6dfd5288093565438d8da642b07ae021191a4042b221bc1bdc0e)
set(VELOX_FOLLY_SOURCE_URL
    "https://github.com/facebook/folly/archive/${VELOX_FOLLY_BUILD_VERSION}.tar.gz"
)

resolve_dependency_url(FOLLY)

message(STATUS "Building Folly from source")

# Suppress warnings when compiling folly.
set(FOLLY_CXX_FLAGS -w)

if(gflags_SOURCE STREQUAL "BUNDLED")
  set(glog_patch && git apply
                 ${CMAKE_CURRENT_LIST_DIR}/folly/folly-gflags-glog.patch)
endif()

FetchContent_Declare(
  folly
  URL ${VELOX_FOLLY_SOURCE_URL}
  URL_HASH ${VELOX_FOLLY_BUILD_SHA256_CHECKSUM}
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
