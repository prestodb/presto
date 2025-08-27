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

set(VELOX_PYBIND11_BUILD_VERSION 2.10.0)
set(
  VELOX_PYBIND11_BUILD_SHA256_CHECKSUM
  eacf582fa8f696227988d08cfc46121770823839fe9e301a20fbce67e7cd70ec
)
string(
  CONCAT
  VELOX_PYBIND11_SOURCE_URL
  "https://github.com/pybind/pybind11/archive/refs/tags/"
  "v${VELOX_PYBIND11_BUILD_VERSION}.tar.gz"
)

velox_resolve_dependency_url(PYBIND11)

message(STATUS "Building Pybind11 from source")

FetchContent_Declare(
  pybind11
  URL ${VELOX_PYBIND11_SOURCE_URL}
  URL_HASH ${VELOX_PYBIND11_BUILD_SHA256_CHECKSUM}
)

FetchContent_MakeAvailable(pybind11)
