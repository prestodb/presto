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
  AllocationPoolTest.cpp
  AllocationTest.cpp
  ByteStreamTest.cpp
  CompactDoubleListTest.cpp
  HashStringAllocatorTest.cpp
  MemoryAllocatorTest.cpp
  MemoryArbitratorTest.cpp
  MemoryCapExceededTest.cpp
  MemoryManagerTest.cpp
  MemoryPoolTest.cpp
  MockSharedArbitratorTest.cpp
  SharedArbitratorTest.cpp
  StreamArenaTest.cpp)

target_link_libraries(
  velox_memory_test
  PRIVATE
    velox_caching
    velox_common_base
    velox_exception
    velox_exec
    velox_exec_test_lib
    velox_memory
    velox_temp_path
    velox_test_util
    velox_vector_fuzzer
    Folly::folly
    fmt::fmt
    gflags::gflags
    glog::glog
    GTest::gmock
    GTest::gtest
    GTest::gtest_main
    re2::re2)

gtest_add_tests(velox_memory_test "" AUTO)

add_executable(velox_fragmentation_benchmark FragmentationBenchmark.cpp)

target_link_libraries(
  velox_fragmentation_benchmark
  PRIVATE velox_memory Folly::folly gflags::gflags glog::glog)

add_executable(velox_concurrent_allocation_benchmark
               ConcurrentAllocationBenchmark.cpp)

target_link_libraries(
  velox_concurrent_allocation_benchmark
  PRIVATE velox_memory velox_time)
