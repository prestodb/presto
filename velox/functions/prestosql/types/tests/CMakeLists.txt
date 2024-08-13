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

add_executable(
  velox_presto_types_test
  HyperLogLogTypeTest.cpp
  JsonTypeTest.cpp
  TimestampWithTimeZoneTypeTest.cpp
  TypeTestBase.cpp
  UuidTypeTest.cpp
  IPAddressTypeTest.cpp)

add_test(velox_presto_types_test velox_presto_types_test)

target_link_libraries(
  velox_presto_types_test
  velox_presto_types
  Folly::folly
  GTest::gtest
  GTest::gtest_main
  GTest::gmock
  gflags::gflags
  glog::glog)
