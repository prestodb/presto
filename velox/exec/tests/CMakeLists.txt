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
  AddressableNonNullValueListTest.cpp
  AggregationTest.cpp
  AggregateFunctionRegistryTest.cpp
  ArrowStreamTest.cpp
  AssignUniqueIdTest.cpp
  AsyncConnectorTest.cpp
  ContainerRowSerdeTest.cpp
  CustomJoinTest.cpp
  EnforceSingleRowTest.cpp
  ExchangeClientTest.cpp
  ExpandTest.cpp
  FilterProjectTest.cpp
  FunctionResolutionTest.cpp
  HashBitRangeTest.cpp
  HashJoinBridgeTest.cpp
  HashJoinTest.cpp
  HashPartitionFunctionTest.cpp
  HashTableTest.cpp
  LimitTest.cpp
  LocalPartitionTest.cpp
  Main.cpp
  MarkDistinctTest.cpp
  MemoryReclaimerTest.cpp
  MergeJoinTest.cpp
  MergeTest.cpp
  MultiFragmentTest.cpp
  NestedLoopJoinTest.cpp
  OrderByTest.cpp
  OutputBufferManagerTest.cpp
  PlanNodeSerdeTest.cpp
  PlanNodeToStringTest.cpp
  PrefixSortTest.cpp
  PrintPlanWithStatsTest.cpp
  ProbeOperatorStateTest.cpp
  RoundRobinPartitionFunctionTest.cpp
  RowContainerTest.cpp
  RowNumberTest.cpp
  SortBufferTest.cpp
  SpillerTest.cpp
  SpillTest.cpp
  SplitToStringTest.cpp
  SqlTest.cpp
  StreamingAggregationTest.cpp
  TableScanTest.cpp
  TableWriteTest.cpp
  TaskListenerTest.cpp
  ThreadDebugInfoTest.cpp
  TopNRowNumberTest.cpp
  TopNTest.cpp
  UnnestTest.cpp
  UnorderedStreamReaderTest.cpp
  ValuesTest.cpp
  VectorHasherTest.cpp
  WindowFunctionRegistryTest.cpp
  WindowTest.cpp)

add_executable(
  velox_exec_infra_test
  AssertQueryBuilderTest.cpp
  DriverTest.cpp
  FunctionSignatureBuilderTest.cpp
  GroupedExecutionTest.cpp
  Main.cpp
  OperatorUtilsTest.cpp
  PlanBuilderTest.cpp
  PrestoQueryRunnerTest.cpp
  QueryAssertionsTest.cpp
  TaskTest.cpp
  TreeOfLosersTest.cpp)

add_test(
  NAME velox_exec_test
  COMMAND velox_exec_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

set_tests_properties(velox_exec_test PROPERTIES TIMEOUT 3000)

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
  velox_window
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
  velox_fuzzer_util
  velox_functions_lib
  velox_functions_prestosql
  velox_functions_test_lib
  velox_window
  velox_hive_connector
  velox_memory
  velox_serialization
  velox_test_util
  velox_type
  velox_vector
  velox_vector_fuzzer
  velox_presto_serializer
  velox_dwio_dwrf_writer
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

# Join Fuzzer.
add_library(velox_join_fuzzer JoinFuzzer.cpp)

target_link_libraries(velox_join_fuzzer velox_type velox_vector_fuzzer
                      velox_exec_test_lib velox_expression_test_utility)

add_executable(velox_join_fuzzer_test JoinFuzzerTest.cpp)

target_link_libraries(velox_join_fuzzer_test velox_join_fuzzer gtest gtest_main)

add_executable(velox_exchange_fuzzer_test ExchangeFuzzer.cpp)

target_link_libraries(
  velox_exchange_fuzzer_test
  velox_exec_test_lib
  velox_aggregates
  velox_vector_test_lib
  velox_vector_fuzzer
  gtest
  gtest
  gtest_main)

add_executable(velox_aggregation_runner_test AggregationRunnerTest.cpp)

target_link_libraries(
  velox_aggregation_runner_test
  velox_aggregation_fuzzer
  velox_fuzzer_util
  velox_aggregates
  velox_vector_test_lib
  gtest
  gtest_main)

add_library(velox_simple_aggregate SimpleAverageAggregate.cpp
                                   SimpleArrayAggAggregate.cpp)

target_link_libraries(velox_simple_aggregate velox_exec velox_expression
                      velox_expression_functions velox_aggregates)

add_executable(velox_simple_aggregate_test SimpleAggregateAdapterTest.cpp
                                           Main.cpp)

target_link_libraries(
  velox_simple_aggregate_test velox_simple_aggregate velox_exec
  velox_functions_aggregates_test_lib gtest gtest_main)

add_library(velox_spiller_join_benchmark_base JoinSpillInputBenchmarkBase.cpp
                                              SpillerBenchmarkBase.cpp)
target_link_libraries(
  velox_spiller_join_benchmark_base
  velox_exec
  velox_exec_test_lib
  velox_memory
  velox_vector_fuzzer
  glog::glog
  gflags::gflags
  Folly::folly
  pthread)

add_executable(velox_spiller_join_benchmark SpillerJoinInputBenchmarkTest.cpp)
target_link_libraries(velox_spiller_join_benchmark velox_exec
                      velox_exec_test_lib velox_spiller_join_benchmark_base)

add_library(velox_spiller_aggregate_benchmark_base
            AggregateSpillBenchmarkBase.cpp SpillerBenchmarkBase.cpp)
target_link_libraries(
  velox_spiller_aggregate_benchmark_base
  velox_exec
  velox_exec_test_lib
  velox_memory
  velox_vector_fuzzer
  glog::glog
  gflags::gflags
  Folly::folly
  pthread)

add_executable(velox_spiller_aggregate_benchmark
               SpillerAggregateBenchmarkTest.cpp)
target_link_libraries(
  velox_spiller_aggregate_benchmark velox_exec velox_exec_test_lib
  velox_spiller_aggregate_benchmark_base)

add_executable(cpr_http_client_test CprHttpClientTest.cpp)
add_test(
  NAME cpr_http_client_test
  COMMAND cpr_http_client_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(cpr_http_client_test cpr::cpr gtest gtest_main)
