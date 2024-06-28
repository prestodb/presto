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

set(VELOX_PROTOBUF_BUILD_VERSION 21.8)
set(VELOX_PROTOBUF_BUILD_SHA256_CHECKSUM
    83ad4faf95ff9cbece7cb9c56eb3ca9e42c3497b77001840ab616982c6269fb6)
if(${VELOX_PROTOBUF_BUILD_VERSION} LESS 22.0)
  string(
    CONCAT
      VELOX_PROTOBUF_SOURCE_URL
      "https://github.com/protocolbuffers/protobuf/releases/download/"
      "v${VELOX_PROTOBUF_BUILD_VERSION}/protobuf-all-${VELOX_PROTOBUF_BUILD_VERSION}.tar.gz"
  )
else()
  set_source(absl)
  resolve_dependency(absl CONFIG REQUIRED)
  string(CONCAT VELOX_PROTOBUF_SOURCE_URL
                "https://github.com/protocolbuffers/protobuf/archive/"
                "v${VELOX_PROTOBUF_BUILD_VERSION}.tar.gz")
endif()

resolve_dependency_url(PROTOBUF)

message(STATUS "Building Protobuf from source")

FetchContent_Declare(
  protobuf
  URL ${VELOX_PROTOBUF_SOURCE_URL}
  URL_HASH ${VELOX_PROTOBUF_BUILD_SHA256_CHECKSUM}
  OVERRIDE_FIND_PACKAGE EXCLUDE_FROM_ALL SYSTEM)

set(protobuf_BUILD_TESTS OFF)
set(protobuf_ABSL_PROVIDER
    "package"
    CACHE STRING "Provider of absl library")
FetchContent_MakeAvailable(protobuf)
set(Protobuf_INCLUDE_DIRS ${protobuf_SOURCE_DIR}/src)
