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

set_source(absl)
resolve_dependency(absl CONFIG REQUIRED)

set(VELOX_GRPC_BUILD_VERSION 1.48.1)
set(VELOX_GRPC_BUILD_SHA256_CHECKSUM
    320366665d19027cda87b2368c03939006a37e0388bfd1091c8d2a96fbc93bd8)
string(
  CONCAT VELOX_GRPC_SOURCE_URL
         "https://github.com/grpc/grpc/archive/refs/tags/"
         "v${VELOX_GRPC_BUILD_VERSION}.tar.gz")

resolve_dependency_url(GRPC)

message(STATUS "Building gRPC from source")

FetchContent_Declare(
  gRPC
  URL ${VELOX_GRPC_SOURCE_URL}
  URL_HASH ${VELOX_GRPC_BUILD_SHA256_CHECKSUM}
  OVERRIDE_FIND_PACKAGE EXCLUDE_FROM_ALL)

set(gRPC_ABSL_PROVIDER
    "package"
    CACHE STRING "Provider of absl library")
set(gRPC_ZLIB_PROVIDER
    "package"
    CACHE STRING "Provider of zlib library")
set(gRPC_CARES_PROVIDER
    "package"
    CACHE STRING "Provider of c-ares library")
set(gRPC_RE2_PROVIDER
    "package"
    CACHE STRING "Provider of re2 library")
set(gRPC_SSL_PROVIDER
    "package"
    CACHE STRING "Provider of ssl library")
set(gRPC_PROTOBUF_PROVIDER
    "package"
    CACHE STRING "Provider of protobuf library")
set(gRPC_INSTALL
    ON
    CACHE BOOL "Generate installation target")
FetchContent_MakeAvailable(gRPC)
add_library(gRPC::grpc ALIAS grpc)
add_library(gRPC::grpc++ ALIAS grpc++)
add_executable(gRPC::grpc_cpp_plugin ALIAS grpc_cpp_plugin)
