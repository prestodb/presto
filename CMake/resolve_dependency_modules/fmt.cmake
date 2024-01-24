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

set(VELOX_FMT_VERSION 10.1.1)
set(VELOX_FMT_BUILD_SHA256_CHECKSUM
    78b8c0a72b1c35e4443a7e308df52498252d1cefc2b08c9a97bc9ee6cfe61f8b)
set(VELOX_FMT_SOURCE_URL
    "https://github.com/fmtlib/fmt/archive/${VELOX_FMT_VERSION}.tar.gz")

resolve_dependency_url(FMT)

message(STATUS "Building fmt from source")
FetchContent_Declare(
  fmt
  URL ${VELOX_FMT_SOURCE_URL}
  URL_HASH ${VELOX_FMT_BUILD_SHA256_CHECKSUM})
# Force fmt to create fmt-config.cmake which can be found by other dependecies
# (e.g. folly)
set(FMT_INSTALL ON)
set(fmt_BUILD_TESTS OFF)
FetchContent_MakeAvailable(fmt)
list(APPEND CMAKE_PREFIX_PATH ${fmt_BINARY_DIR})
