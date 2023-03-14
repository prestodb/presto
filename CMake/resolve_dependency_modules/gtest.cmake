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

if(DEFINED ENV{VELOX_GTEST_URL})
  set(VELOX_GTEST_SOURCE_URL "$ENV{VELOX_GTEST_URL}")
else()
  set(VELOX_GTEST_VERSION 1.13.0)
  set(VELOX_GTEST_SOURCE_URL
      "https://github.com/google/googletest/archive/refs/tags/v${VELOX_GTEST_VERSION}.tar.gz"
  )
  set(VELOX_GTEST_BUILD_SHA256_CHECKSUM
      ad7fdba11ea011c1d925b3289cf4af2c66a352e18d4c7264392fead75e919363)
endif()

message(STATUS "Building gtest from source")
FetchContent_Declare(
  gtest
  URL ${VELOX_GTEST_SOURCE_URL}
  URL_HASH SHA256=${VELOX_GTEST_BUILD_SHA256_CHECKSUM})

FetchContent_MakeAvailable(gtest)
