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

add_library(velox_aggregation_result_verifier MinMaxByResultVerifier.cpp)

target_link_libraries(
  velox_aggregation_result_verifier velox_core velox_aggregation_fuzzer_base
  velox_exec_test_lib velox_vector velox_vector_test_lib)

add_executable(velox_aggregation_fuzzer_test AggregationFuzzerTest.cpp)

target_link_libraries(
  velox_aggregation_fuzzer_test
  velox_aggregation_fuzzer
  velox_aggregation_fuzzer_base
  velox_aggregation_result_verifier
  velox_aggregates
  velox_window
  velox_vector_test_lib
  gtest
  gtest_main)

# Window Fuzzer
add_executable(velox_window_fuzzer_test WindowFuzzerTest.cpp)

target_link_libraries(
  velox_window_fuzzer_test
  velox_window_fuzzer
  velox_fuzzer_util
  velox_aggregates
  velox_window
  velox_expression_test_utility
  velox_functions_prestosql
  velox_vector_test_lib
  gtest
  gtest_main)

# Writer Fuzzer
add_executable(velox_writer_fuzzer_test WriterFuzzerTest.cpp)

target_link_libraries(velox_writer_fuzzer_test velox_writer_fuzzer
                      velox_fuzzer_util gtest gtest_main)
