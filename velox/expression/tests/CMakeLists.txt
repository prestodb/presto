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

# for generated headers
include_directories(${CMAKE_BINARY_DIR})

add_executable(
  velox_expression_test ExprTest.cpp CastExprTest.cpp EvalSimplifiedTest.cpp
                        SignatureBinderTest.cpp SimpleFunctionTest.cpp)

add_test(
  NAME velox_expression_test
  COMMAND velox_expression_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  velox_expression_test
  velox_aggregates
  velox_hive_connector
  velox_dwio_common
  velox_exec_test_util
  velox_expression
  velox_functions_lib
  velox_functions_prestosql
  velox_functions_test_lib
  velox_parse_parser
  velox_parse_expression
  velox_presto_serializer
  velox_vector_test_lib
  velox_vector_fuzzer
  ${GTEST_BOTH_LIBRARIES}
  ${FOLLY_WITH_DEPENDENCIES}
  ${gflags_LIBRARIES}
  ${GLOG}
  ${FMT})

add_library(velox_vector_fuzzer VectorFuzzer.cpp)

target_link_libraries(velox_vector_fuzzer velox_type velox_vector)

add_library(velox_expression_fuzzer ExpressionFuzzer.cpp)

target_link_libraries(velox_expression_fuzzer velox_type velox_vector_fuzzer
                      velox_vector_test_lib)

add_executable(velox_expression_fuzzer_main ExpressionFuzzerMain.cpp)

target_link_libraries(velox_expression_fuzzer_main velox_expression_fuzzer
                      velox_functions_prestosql)
