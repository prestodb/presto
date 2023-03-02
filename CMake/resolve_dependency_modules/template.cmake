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

if(DEFINED ENV{VELOX_<PACKAGE>_URL})
  set(VELOX_<PACKAGE>_SOURCE_URL "$ENV{VELOX_<PACKAGE>_URL}")
else()
  set(VELOX_<PACKAGE>_VERSION x.y.z)
  set(VELOX_<PACKAGE>_SOURCE_URL "") # ideally don't use github archive links as
                                     # they are not guranteed to be hash stable
  # release artifacts are tough (except the auto generated ones)
  set(VELOX_<PACKAGE>_BUILD_SHA256_CHECKSUM 123)
endif()

message(STATUS "Building <PACKAGE> from source")
FetchContent_Declare(
  <package>
  URL ${VELOX_<PACKAGE>_SOURCE_URL}
  URL_HASH SHA256=${VELOX_<PACKAGE>_BUILD_SHA256_CHECKSUM})

FetchContent_MakeAvailable(<package>)
