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

set(VELOX_<PACKAGE>_VERSION x.y.z)
# release artifacts are tough (except the auto generated ones)
set(VELOX_<PACKAGE>_BUILD_SHA256_CHECKSUM 123)
# ideally don't use github archive links as they are not guaranteed to be hash stable
set(VELOX_<PACKAGE>_SOURCE_URL "")

velox_resolve_dependency_url(<PACKAGE>)

message(STATUS "Building <PACKAGE> from source")
FetchContent_Declare(
  <package>
  URL ${VELOX_<PACKAGE>_SOURCE_URL}
  URL_HASH ${VELOX_<PACKAGE>_BUILD_SHA256_CHECKSUM}
)

FetchContent_MakeAvailable(<package>)
