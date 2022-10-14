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
include(GoogleTest)
add_executable(
  velox_memory_test
  ByteStreamTest.cpp
  CompactDoubleListTest.cpp
  HashStringAllocatorTest.cpp
  MemoryHeaderTest.cpp
  MemoryManagerTest.cpp
  MemoryPoolTest.cpp
  MemoryUsageTest.cpp
  MemoryUsageTrackerTest.cpp
  MappedMemoryTest.cpp)

target_link_libraries(
  velox_memory_test
  velox_memory
  velox_exception
  velox_test_util
  gtest
  gtest_main
  ${gflags_LIBRARIES}
  gmock
  glog::glog)

gtest_add_tests(velox_memory_test "" AUTO)

add_executable(velox_fragmentation_benchmark FragmentationBenchmark.cpp)

target_link_libraries(
  velox_fragmentation_benchmark
  velox_memory
  glog::glog
  gtest
  gtest_main
  ${gflags_LIBRARIES}
  pthread)
