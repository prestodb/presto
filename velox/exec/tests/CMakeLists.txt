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

add_executable(
  aggregate_companion_functions_test
  AggregateCompanionAdapterTest.cpp AggregateCompanionSignaturesTest.cpp
  DummyAggregateFunction.cpp)

add_test(
  NAME aggregate_companion_functions_test
  COMMAND aggregate_companion_functions_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(aggregate_companion_functions_test velox_exec
                      velox_function_registry gtest gtest_main)

add_executable(
  velox_exec_test
  AggregationTest.cpp
  AggregateFunctionRegistryTest.cpp
  ArrowStreamTest.cpp
  AssignUniqueIdTest.cpp
  AsyncConnectorTest.cpp
  CustomJoinTest.cpp
  EnforceSingleRowTest.cpp
  FilterProjectTest.cpp
  FunctionResolutionTest.cpp
  HashJoinBridgeTest.cpp
  HashJoinTest.cpp
  HashBitRangeTest.cpp
  HashPartitionFunctionTest.cpp
  HashTableTest.cpp
  LimitTest.cpp
  LocalPartitionTest.cpp
  Main.cpp
  MergeJoinTest.cpp
  MergeTest.cpp
  MultiFragmentTest.cpp
  NestedLoopJoinTest.cpp
  OrderByTest.cpp
  PartitionedOutputBufferManagerTest.cpp
  PlanNodeSerdeTest.cpp
  PlanNodeToStringTest.cpp
  PrintPlanWithStatsTest.cpp
  ProbeOperatorStateTest.cpp
  RoundRobinPartitionFunctionTest.cpp
  RowContainerTest.cpp
  MemoryCapExceededTest.cpp
  SpillTest.cpp
  SpillOperatorGroupTest.cpp
  SpillerTest.cpp
  SqlTest.cpp
  StreamingAggregationTest.cpp
  TableScanTest.cpp
  TableWriteTest.cpp
  TaskListenerTest.cpp
  TopNTest.cpp
  UnorderedStreamReaderTest.cpp
  UnnestTest.cpp
  VectorHasherTest.cpp
  ValuesTest.cpp
  WindowFunctionRegistryTest.cpp)

add_executable(
  velox_exec_infra_test
  AssertQueryBuilderTest.cpp
  DriverTest.cpp
  FunctionSignatureBuilderTest.cpp
  GroupedExecutionTest.cpp
  Main.cpp
  OperatorUtilsTest.cpp
  ParseTypeSignatureTest.cpp
  PlanBuilderTest.cpp
  QueryAssertionsTest.cpp
  TaskTest.cpp
  TreeOfLosersTest.cpp)

add_test(
  NAME velox_exec_test
  COMMAND velox_exec_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(
  NAME velox_exec_infra_test
  COMMAND velox_exec_infra_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  velox_exec_test
  velox_aggregates
  velox_dwio_common
  velox_dwio_common_exception
  velox_dwio_common_test_utils
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

target_link_libraries(
  velox_exec_infra_test
  velox_dwio_common
  velox_dwio_common_exception
  velox_dwio_common_test_utils
  velox_exec
  velox_exec_test_lib
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

add_executable(velox_in_10_min_demo VeloxIn10MinDemo.cpp)

target_link_libraries(
  velox_in_10_min_demo
  velox_aggregates
  velox_type
  velox_vector
  velox_vector_test_lib
  velox_exec
  velox_exec_test_lib
  velox_tpch_connector
  velox_memory)

# Aggregation Fuzzer.

add_library(velox_aggregation_fuzzer AggregationFuzzer.cpp)

target_link_libraries(velox_aggregation_fuzzer velox_type velox_vector_fuzzer
                      velox_exec_test_lib velox_expression_test_utility)

add_executable(velox_aggregation_fuzzer_test AggregationFuzzerTest.cpp)

target_link_libraries(
  velox_aggregation_fuzzer_test
  velox_aggregation_fuzzer
  velox_aggregates
  velox_window
  velox_vector_test_lib
  gtest
  gtest_main)

add_executable(spark_aggregation_fuzzer_test SparkAggregationFuzzerTest.cpp)

target_link_libraries(
  spark_aggregation_fuzzer_test
  velox_aggregation_fuzzer
  velox_functions_spark_aggregates
  velox_window
  velox_vector_test_lib
  gtest
  gtest_main)

# Join Fuzzer.

add_library(velox_join_fuzzer JoinFuzzer.cpp)

target_link_libraries(velox_join_fuzzer velox_type velox_vector_fuzzer
                      velox_exec_test_lib velox_expression_test_utility)

add_executable(velox_join_fuzzer_test JoinFuzzerTest.cpp)

target_link_libraries(velox_join_fuzzer_test velox_join_fuzzer gtest gtest_main)
