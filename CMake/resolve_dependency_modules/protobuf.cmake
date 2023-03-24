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
  URL_HASH ${VELOX_PROTOBUF_BUILD_SHA256_CHECKSUM})

if(NOT protobuf_POPULATED)
  # We don't want to build tests.
  set(protobuf_BUILD_TESTS
      OFF
      CACHE BOOL "Disable protobuf tests" FORCE)
  set(CMAKE_CXX_FLAGS_BKP "${CMAKE_CXX_FLAGS}")

  # Disable warnings that would fail protobuf compilation.
  string(APPEND CMAKE_CXX_FLAGS " -Wno-missing-field-initializers")

  check_cxx_compiler_flag("-Wstringop-overflow"
                          COMPILER_HAS_W_STRINGOP_OVERFLOW)
  if(COMPILER_HAS_W_STRINGOP_OVERFLOW)
    string(APPEND CMAKE_CXX_FLAGS " -Wno-stringop-overflow")
  endif()

  check_cxx_compiler_flag("-Winvalid-noreturn" COMPILER_HAS_W_INVALID_NORETURN)

  if(COMPILER_HAS_W_INVALID_NORETURN)
    string(APPEND CMAKE_CXX_FLAGS " -Wno-invalid-noreturn")
  else()
    # Currently reproduced on Ubuntu 22.04 with clang 14
    string(APPEND CMAKE_CXX_FLAGS " -Wno-error")
  endif()

  # Fetch the content using previously declared details
  FetchContent_Populate(protobuf)

  # Set right path to libprotobuf-dev include files.
  set(Protobuf_INCLUDE_DIRS "${protobuf_SOURCE_DIR}/src/")
  set(Protobuf_PROTOC_EXECUTABLE "${protobuf_BINARY_DIR}/protoc")
  if(CMAKE_BUILD_TYPE MATCHES Debug)
    set(Protobuf_LIBRARIES "${protobuf_BINARY_DIR}/libprotobufd.a")
  else()
    set(Protobuf_LIBRARIES "${protobuf_BINARY_DIR}/libprotobuf.a")
  endif()
  add_subdirectory(${protobuf_SOURCE_DIR} ${protobuf_BINARY_DIR})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_BKP}")
endif()
