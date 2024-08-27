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

add_subdirectory(utils)

add_executable(velox_wave_exec_test FilterProjectTest.cpp TableScanTest.cpp
                                    AggregationTest.cpp Main.cpp)

target_link_libraries(
  velox_wave_exec_test
  velox_wave_exec
  velox_wave_mock_reader
  velox_aggregates
  velox_dwio_common
  velox_dwio_common_exception
  velox_dwio_common_test_utils
  velox_dwio_parquet_reader
  velox_dwio_parquet_writer
  velox_exec
  velox_exec_test_lib
  velox_functions_json
  velox_functions_lib
  velox_functions_prestosql
  velox_functions_test_lib
  velox_hive_connector
  velox_memory
  velox_serialization
  velox_test_util
  velox_type
  velox_vector
  velox_vector_fuzzer
  Boost::atomic
  Boost::context
  Boost::date_time
  Boost::filesystem
  Boost::program_options
  Boost::regex
  Boost::thread
  Boost::system
  gtest
  gtest_main
  gmock
  Folly::folly
  gflags::gflags
  glog::glog
  fmt::fmt
  ${FILESYSTEM})

add_test(velox_wave_exec_test velox_wave_exec_test)

if(${VELOX_ENABLE_BENCHMARKS})
  add_executable(velox_wave_benchmark WaveBenchmark.cpp)

  target_link_libraries(
    velox_wave_benchmark
    velox_query_benchmark
    velox_wave_exec
    velox_wave_mock_reader
    velox_aggregates
    velox_dwio_common
    velox_dwio_common_exception
    velox_dwio_common_test_utils
    velox_dwio_parquet_reader
    velox_dwio_parquet_writer
    velox_exec
    velox_exec_test_lib
    velox_functions_json
    velox_functions_lib
    velox_functions_prestosql
    velox_functions_test_lib
    velox_hive_connector
    velox_memory
    velox_serialization
    velox_test_util
    velox_type
    velox_vector
    velox_vector_fuzzer
    Boost::atomic
    Boost::context
    Boost::date_time
    Boost::filesystem
    Boost::program_options
    Boost::regex
    Boost::thread
    Boost::system
    gtest
    gtest_main
    gmock
    ${FOLLY_BENCHMARK}
    Folly::folly
    gflags::gflags
    glog::glog
    fmt::fmt
    ${FILESYSTEM})
endif()
