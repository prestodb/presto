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
  velox_dwio_common_test
  ChainedBufferTests.cpp DataBufferTests.cpp RetryTests.cpp
  TestColumnSelector.cpp TypeTests.cpp LoggedExceptionTest.cpp)

add_test(velox_dwio_common_test velox_dwio_common_test)

target_link_libraries(
  velox_dwio_common_test
  velox_memory
  velox_dwio_common
  velox_dwio_common_exception
  velox_dwio_type_fbhive
  velox_exception
  velox_process
  velox_type
  Boost::regex
  ${gflags_LIBRARIES}
  gtest
  gtest_main
  gmock
  glog::glog
  ${FMT})

add_executable(velox_dwio_common_data_buffer_benchmark DataBufferBenchmark.cpp)

target_link_libraries(
  velox_dwio_common_data_buffer_benchmark velox_dwio_common velox_memory
  velox_dwio_common_exception ${FOLLY} ${FOLLY_BENCHMARK})
