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

add_library(velox_fuzzer_util DuckQueryRunner.cpp PrestoQueryRunner.cpp
                              FuzzerUtil.cpp ToSQLUtil.cpp)

target_link_libraries(
  velox_fuzzer_util
  velox_core
  velox_exec_test_lib
  cpr::cpr
  Boost::regex
  velox_type_parser
  Folly::folly
  velox_hive_connector
  velox_dwio_dwrf_reader
  velox_dwio_dwrf_writer
  velox_dwio_catalog_fbhive)

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

add_library(velox_row_number_fuzzer RowNumberFuzzer.cpp)

target_link_libraries(
  velox_row_number_fuzzer
  velox_fuzzer_util
  velox_type
  velox_vector_fuzzer
  velox_exec_test_lib
  velox_expression_test_utility)

add_library(velox_join_fuzzer JoinFuzzer.cpp)

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
  velox_vector_test_lib)

add_library(velox_memory_arbitration_fuzzer MemoryArbitrationFuzzer.cpp)

target_link_libraries(
  velox_memory_arbitration_fuzzer
  velox_fuzzer_util
  velox_type
  velox_vector_fuzzer
  velox_exec_test_lib
  velox_expression_test_utility
  velox_functions_prestosql
  velox_aggregates)

add_library(velox_cache_fuzzer CacheFuzzer.cpp)

target_link_libraries(
  velox_cache_fuzzer velox_dwio_common velox_temp_path velox_vector_test_lib)
