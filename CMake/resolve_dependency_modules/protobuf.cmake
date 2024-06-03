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

set(VELOX_PROTOBUF_BUILD_VERSION 21.4)
set(VELOX_PROTOBUF_BUILD_SHA256_CHECKSUM
    6c5e1b0788afba4569aeebb2cfe205cb154aa01deacaba0cd26442f3b761a836)
string(
  CONCAT
    VELOX_PROTOBUF_SOURCE_URL
    "https://github.com/protocolbuffers/protobuf/releases/download/"
    "v${VELOX_PROTOBUF_BUILD_VERSION}/protobuf-all-${VELOX_PROTOBUF_BUILD_VERSION}.tar.gz"
)

resolve_dependency_url(PROTOBUF)

message(STATUS "Building Protobuf from source")

FetchContent_Declare(
  protobuf
  URL ${VELOX_PROTOBUF_SOURCE_URL}
  URL_HASH ${VELOX_PROTOBUF_BUILD_SHA256_CHECKSUM}
  OVERRIDE_FIND_PACKAGE EXCLUDE_FROM_ALL SYSTEM)

set(protobuf_BUILD_TESTS OFF)
FetchContent_MakeAvailable(protobuf)
set(Protobuf_INCLUDE_DIRS ${protobuf_SOURCE_DIR}/src)
set(Protobuf_PROTOC_EXECUTABLE "${protobuf_BINARY_DIR}/protoc")
