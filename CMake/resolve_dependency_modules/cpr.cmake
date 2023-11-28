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

set(VELOX_CPR_VERSION 1.10.5)
set(VELOX_CPR_BUILD_SHA256_CHECKSUM
    c8590568996cea918d7cf7ec6845d954b9b95ab2c4980b365f582a665dea08d8)
set(VELOX_CPR_SOURCE_URL
    "https://github.com/libcpr/cpr/archive/refs/tags/${VELOX_CPR_VERSION}.tar.gz"
)

resolve_dependency_url(CPR)

message(STATUS "Building cpr from source")
FetchContent_Declare(
  cpr
  URL ${VELOX_CPR_SOURCE_URL}
  URL_HASH ${VELOX_CPR_BUILD_SHA256_CHECKSUM}
  PATCH_COMMAND git apply
                ${CMAKE_CURRENT_LIST_DIR}/cpr/cpr-libcurl-compatible.patch)
set(BUILD_SHARED_LIBS OFF)
set(CPR_USE_SYSTEM_CURL OFF)
FetchContent_MakeAvailable(cpr)
