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

add_library(
  velox_fuzzer_util
  ReferenceQueryRunner.cpp
  DuckQueryRunner.cpp
  PrestoQueryRunner.cpp
  PrestoQueryRunnerIntermediateTypeTransforms.cpp
  PrestoQueryRunnerTimestampWithTimeZoneTransform.cpp
  FuzzerUtil.cpp
  PrestoSql.cpp)

target_link_libraries(
  velox_fuzzer_util
  velox_common_fuzzer_util
  velox_vector_fuzzer
  velox_core
  velox_exec_test_lib
  velox_expression_functions
  velox_presto_types
  cpr::cpr
  Boost::regex
  velox_type_parser
  Folly::folly
  velox_hive_connector
  velox_dwio_dwrf_writer
  velox_dwio_catalog_fbhive
  velox_dwio_faulty_file_sink)

add_library(velox_aggregation_fuzzer_base AggregationFuzzerBase.cpp)

target_link_libraries(
  velox_aggregation_fuzzer_base
  velox_exec_test_lib
  velox_temp_path
  velox_common_base
  velox_file
  velox_hive_connector
  velox_dwio_dwrf_reader
  velox_dwio_dwrf_writer
  velox_type
  velox_vector_fuzzer
  velox_fuzzer_util
  velox_expression_test_utility
  velox_vector
  velox_core)

add_library(velox_aggregation_fuzzer AggregationFuzzer.cpp)

target_link_libraries(
  velox_aggregation_fuzzer
  velox_type
  velox_vector_fuzzer
  velox_exec_test_lib
  velox_expression_test_utility
  velox_aggregation_fuzzer_base
  velox_fuzzer_util)

add_library(velox_window_fuzzer WindowFuzzer.cpp)

target_link_libraries(
  velox_window_fuzzer
  velox_fuzzer_util
  velox_type
  velox_vector_fuzzer
  velox_exec_test_lib
  velox_expression_test_utility
  velox_aggregation_fuzzer_base
  velox_temp_path)

add_library(velox_row_number_fuzzer_base_lib RowNumberFuzzerBase.cpp)

target_link_libraries(
  velox_row_number_fuzzer_base_lib
  velox_dwio_dwrf_reader
  velox_fuzzer_util
  velox_vector_fuzzer
  velox_exec_test_lib)

add_library(velox_row_number_fuzzer_lib RowNumberFuzzer.cpp)

target_link_libraries(
  velox_row_number_fuzzer_lib velox_row_number_fuzzer_base_lib velox_type
  velox_expression_test_utility)

# RowNumber Fuzzer.
add_executable(velox_row_number_fuzzer RowNumberFuzzerRunner.cpp)

target_link_libraries(
  velox_row_number_fuzzer velox_row_number_fuzzer_lib)

add_library(velox_topn_row_number_fuzzer_lib TopNRowNumberFuzzer.cpp)

target_link_libraries(
  velox_topn_row_number_fuzzer_lib velox_row_number_fuzzer_base_lib velox_type
  velox_expression_test_utility)

# TopNRowNumber Fuzzer.
add_executable(velox_topn_row_number_fuzzer TopNRowNumberFuzzerRunner.cpp)

target_link_libraries(
  velox_topn_row_number_fuzzer velox_topn_row_number_fuzzer_lib)

# Join Fuzzer.
add_executable(velox_join_fuzzer JoinFuzzerRunner.cpp JoinFuzzer.cpp)

target_link_libraries(
  velox_join_fuzzer
  velox_type
  velox_vector_fuzzer
  velox_fuzzer_util
  velox_exec_test_lib
  velox_expression_test_utility)

add_library(velox_writer_fuzzer WriterFuzzer.cpp)

target_link_libraries(
  velox_writer_fuzzer
  velox_fuzzer_util
  velox_type
  velox_vector_fuzzer
  velox_exec_test_lib
  velox_expression_test_utility
  velox_temp_path
  velox_vector_test_lib
  velox_dwio_faulty_file_sink
  velox_file_test_utils)

add_library(velox_memory_arbitration_fuzzer MemoryArbitrationFuzzer.cpp)

target_link_libraries(
  velox_memory_arbitration_fuzzer
  velox_common_fuzzer_util
  velox_fuzzer_util
  velox_type
  velox_vector_fuzzer
  velox_exec_test_lib
  velox_expression_test_utility
  velox_functions_prestosql
  velox_aggregates)

add_library(velox_cache_fuzzer_lib CacheFuzzer.cpp)

# Cache Fuzzer
add_executable(velox_cache_fuzzer CacheFuzzerRunner.cpp)

target_link_libraries(
  velox_cache_fuzzer velox_cache_fuzzer_lib velox_fuzzer_util)

target_link_libraries(
  velox_cache_fuzzer_lib velox_dwio_common velox_temp_path
  velox_vector_test_lib)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
