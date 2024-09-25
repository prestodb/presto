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

set(VELOX_FAST_FLOAT_VERSION 6.1.6)
set(VELOX_FAST_FLOAT_BUILD_SHA256_CHECKSUM
    4458aae4b0eb55717968edda42987cabf5f7fc737aee8fede87a70035dba9ab0)
set(VELOX_FAST_FLOAT_SOURCE_URL
    "https://github.com/fastfloat/fast_float/archive/refs/tags/v${VELOX_FAST_FLOAT_VERSION}.tar.gz"
)

resolve_dependency_url(FAST_FLOAT)

message(STATUS "Building fast_float from source")
FetchContent_Declare(
  fast_float
  URL ${VELOX_FAST_FLOAT_SOURCE_URL}
  URL_HASH ${VELOX_FAST_FLOAT_BUILD_SHA256_CHECKSUM})

FetchContent_MakeAvailable(fast_float)
# Folly searches for the header path directly so need to make sure to search in
# the dependency path.
list(APPEND CMAKE_PREFIX_PATH "${fast_float_SOURCE_DIR}")
