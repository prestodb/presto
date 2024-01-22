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

set(VELOX_SIMDJSON_VERSION 3.2.0)
set(VELOX_SIMDJSON_BUILD_SHA256_CHECKSUM
    75a684dbbe38cf72b8b3bdbdc430764813f3615899a6029931c26ddd89812da4)
set(VELOX_SIMDJSON_SOURCE_URL
    "https://github.com/simdjson/simdjson/archive/refs/tags/v${VELOX_SIMDJSON_VERSION}.tar.gz"
)

resolve_dependency_url(SIMDJSON)

message(STATUS "Building simdjson from source")

FetchContent_Declare(
  simdjson
  URL ${VELOX_SIMDJSON_SOURCE_URL}
  URL_HASH ${VELOX_SIMDJSON_BUILD_SHA256_CHECKSUM})

FetchContent_MakeAvailable(simdjson)
