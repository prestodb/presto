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
add_subdirectory(basic)

set(velox_benchmark_deps
    velox_type
    velox_vector
    velox_vector_fuzzer
    velox_expression
    velox_parse_parser
    velox_parse_utils
    velox_parse_expression
    velox_serialization
    Folly::folly
    ${FOLLY_BENCHMARK}
    ${DOUBLE_CONVERSION}
    gflags::gflags
    glog::glog)

add_library(velox_benchmark_builder ExpressionBenchmarkBuilder.cpp)
target_link_libraries(
  velox_benchmark_builder ${velox_benchmark_deps})
# This is a workaround for the use of VectorTestBase.h which includes gtest.h
target_link_libraries(
  velox_benchmark_builder GTest::gtest)

if(${VELOX_ENABLE_BENCHMARKS})
  add_subdirectory(tpch)
  add_subdirectory(filesystem)
endif()

add_library(velox_query_benchmark QueryBenchmarkBase.cpp)
target_link_libraries(
  velox_query_benchmark
  velox_aggregates
  velox_exec
  velox_exec_test_lib
  velox_dwio_common
  velox_dwio_common_exception
  velox_dwio_parquet_reader
  velox_dwio_common_test_utils
  velox_hive_connector
  velox_exception
  velox_memory
  velox_process
  velox_serialization
  velox_encode
  velox_type
  velox_type_fbhive
  velox_caching
  velox_vector_test_lib
  ${FOLLY_BENCHMARK}
  Folly::folly
  fmt::fmt)
