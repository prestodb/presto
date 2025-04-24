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

set(VELOX_FASTFLOAT_VERSION v8.0.2)
set(VELOX_FASTFLOAT_BUILD_SHA256_CHECKSUM
    e14a33089712b681d74d94e2a11362643bd7d769ae8f7e7caefe955f57f7eacd)
set(VELOX_FASTFLOAT_SOURCE_URL
    "https://github.com/fastfloat/fast_float/archive/refs/tags/${VELOX_FASTFLOAT_VERSION}.tar.gz"
)

velox_resolve_dependency_url(FASTFLOAT)

message(STATUS "Building fast_float from source")
FetchContent_Declare(
  fastfloat
  URL ${VELOX_FASTFLOAT_SOURCE_URL}
  URL_HASH ${VELOX_FASTFLOAT_BUILD_SHA256_CHECKSUM})

set(fastfloat_BUILD_TESTS OFF)
FetchContent_MakeAvailable(fastfloat)
