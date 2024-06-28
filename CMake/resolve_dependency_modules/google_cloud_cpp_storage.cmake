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

set_source(gRPC)
resolve_dependency(gRPC CONFIG 1.48.1 REQUIRED)

set(VELOX_GOOGLE_CLOUD_CPP_BUILD_VERSION 2.22.0)
set(VELOX_GOOGLE_CLOUD_CPP_BUILD_SHA256_CHECKSUM
    0c68782e57959c82e0c81def805c01460a042c1aae0c2feee905acaa2a2dc9bf)
string(
  CONCAT VELOX_GOOGLE_CLOUD_CPP_SOURCE_URL
         "https://github.com/googleapis/google-cloud-cpp/archive/refs/tags/"
         "v${VELOX_GOOGLE_CLOUD_CPP_BUILD_VERSION}.tar.gz")

resolve_dependency_url(GOOGLE_CLOUD_CPP)

message(STATUS "Building Google Cloud CPP storage from source")

FetchContent_Declare(
  google_cloud_cpp
  URL ${VELOX_GOOGLE_CLOUD_CPP_SOURCE_URL}
  URL_HASH ${VELOX_GOOGLE_CLOUD_CPP_BUILD_SHA256_CHECKSUM}
  OVERRIDE_FIND_PACKAGE EXCLUDE_FROM_ALL SYSTEM)

set(GOOGLE_CLOUD_CPP_ENABLE_EXAMPLES OFF)
set(GOOGLE_CLOUD_CPP_ENABLE
    "storage"
    CACHE STRING "The list of libraries to build.")
FetchContent_MakeAvailable(google_cloud_cpp)
