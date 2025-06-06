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
project(Folly)
cmake_minimum_required(VERSION 3.28)

velox_set_source(fastfloat)
velox_resolve_dependency(fastfloat CONFIG REQUIRED)

set(VELOX_FOLLY_BUILD_VERSION v2025.04.28.00)
set(VELOX_FOLLY_BUILD_SHA256_CHECKSUM
    ccbb7eac662023f9f5beba94e51350d527f33d8a7a036eb5e3d8a5cf1b49d3bc)
set(VELOX_FOLLY_SOURCE_URL
    "https://github.com/facebook/folly/releases/download/${VELOX_FOLLY_BUILD_VERSION}/folly-${VELOX_FOLLY_BUILD_VERSION}.tar.gz"
)

velox_resolve_dependency_url(FOLLY)

message(STATUS "Building Folly from source")

if(gflags_SOURCE STREQUAL "BUNDLED")
  set(glog_patch && git apply ${CMAKE_CURRENT_LIST_DIR}/folly-gflags-glog.patch)
  # Together with the patch applied above prevents folly from test compiling a
  # snippet to find the right namespace (which would fail because gflags isn't
  # built yet)
  set(FOLLY_UNUSUAL_GFLAGS_NAMESPACE OFF)
  set(FOLLY_GFLAGS_NAMESPACE gflags)
endif()

FetchContent_Declare(
  folly
  URL ${VELOX_FOLLY_SOURCE_URL}
  URL_HASH ${VELOX_FOLLY_BUILD_SHA256_CHECKSUM}
  PATCH_COMMAND git apply ${CMAKE_CURRENT_LIST_DIR}/folly-no-export.patch
                ${glog_patch} OVERRIDE_FIND_PACKAGE SYSTEM EXCLUDE_FROM_ALL)

set(BUILD_SHARED_LIBS ${VELOX_BUILD_SHARED})

# Enable INT128 support
set(FOLLY_HAVE_INT128_T ON)
set(PREVIOUS_CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DFOLLY_CFG_NO_COROUTINES")

FetchContent_MakeAvailable(folly)

# Folly::folly is not valid for FC but we want to match FindFolly
add_library(Folly::folly ALIAS folly)
add_library(Folly::follybenchmark ALIAS follybenchmark)

if(gflags_SOURCE STREQUAL "BUNDLED")
  add_dependencies(folly glog::glog gflags::gflags fmt::fmt)
endif()
set(CMAKE_CXX_FLAGS ${PREVIOUS_CMAKE_CXX_FLAGS})
