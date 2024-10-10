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

add_library(velox_expression_test_utility ArgumentTypeFuzzer.cpp
                                          FuzzerToolkit.cpp)

target_link_libraries(
  velox_expression_test_utility
  velox_type
  velox_expression_functions
  velox_fuzzer_util
  GTest::gtest)

add_library(
  velox_expression_fuzzer
  ArgumentTypeFuzzer.cpp
  DecimalArgGeneratorBase.cpp
  ExpressionFuzzer.cpp
  FuzzerRunner.cpp
  ExpressionFuzzerVerifier.cpp)

target_link_libraries(
  velox_expression_fuzzer
  velox_expression_verifier
  velox_type
  velox_vector_fuzzer
  velox_vector_test_lib
  velox_function_registry
  velox_expression_test_utility
  velox_file
  velox_hive_connector
  velox_fuzzer_util)

add_executable(velox_expression_fuzzer_test ExpressionFuzzerTest.cpp)

target_link_libraries(
  velox_expression_fuzzer_test
  velox_expression_fuzzer
  velox_functions_prestosql
  velox_fuzzer_util
  velox_hive_connector
  GTest::gtest
  GTest::gtest_main)

add_executable(spark_expression_fuzzer_test SparkExpressionFuzzerTest.cpp)

target_link_libraries(
  spark_expression_fuzzer_test
  velox_expression_fuzzer
  velox_fuzzer_util
  velox_hive_connector
  velox_functions_spark
  GTest::gtest
  GTest::gtest_main)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
