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
  velox_exec_test
  AssertQueryBuilderTest.cpp
  AsyncConnectorTest.cpp
  AggregationTest.cpp
  AggregateFunctionRegistryTest.cpp
  AssignUniqueIdTest.cpp
  CrossJoinTest.cpp
  CustomJoinTest.cpp
  DriverTest.cpp
  EnforceSingleRowTest.cpp
  FilterProjectTest.cpp
  FunctionResolutionTest.cpp
  FunctionSignatureBuilderTest.cpp
  HashJoinTest.cpp
  HashPartitionFunctionTest.cpp
  HashTableTest.cpp
  LimitTest.cpp
  LocalPartitionTest.cpp
  MultiFragmentTest.cpp
  MergeJoinTest.cpp
  MergeTest.cpp
  OperatorUtilsTest.cpp
  OrderByTest.cpp
  ParseTypeSignatureTest.cpp
  PartitionedOutputBufferManagerTest.cpp
  PlanBuilderTest.cpp
  PlanNodeToStringTest.cpp
  PrintPlanWithStatsTest.cpp
  RoundRobinPartitionFunctionTest.cpp
  RowContainerTest.cpp
  MemoryCapExceededTest.cpp
  SpillTest.cpp
  SpillerTest.cpp
  StreamingAggregationTest.cpp
  TableScanTest.cpp
  TableWriteTest.cpp
  TaskListenerTest.cpp
  TaskTest.cpp
  TopNTest.cpp
  TreeOfLosersTest.cpp
  UnnestTest.cpp
  VectorHasherTest.cpp
  WindowFunctionRegistryTest.cpp)

add_test(
  NAME velox_exec_test
  COMMAND velox_exec_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  velox_exec_test
  velox_functions_test_lib
  velox_exec
  velox_dwio_common
  velox_dwio_common_test_utils
  velox_aggregates
  velox_aggregates_test_lib
  velox_functions_lib
  velox_functions_prestosql
  velox_hive_connector
  velox_test_util
  velox_type
  velox_serialization
  velox_exec_test_util
  velox_functions_json
  velox_vector
  velox_memory
  velox_dwio_common_exception
  ${Boost_ATOMIC_LIBRARIES}
  ${Boost_CONTEXT_LIBRARIES}
  ${Boost_DATE_TIME_LIBRARIES}
  ${Boost_FILESYSTEM_LIBRARIES}
  ${Boost_PROGRAM_OPTIONS_LIBRARIES}
  ${Boost_REGEX_LIBRARIES}
  ${Boost_THREAD_LIBRARIES}
  ${Boost_SYSTEM_LIBRARIES}
  gtest
  gtest_main
  gmock
  ${FOLLY_WITH_DEPENDENCIES}
  ${DOUBLE_CONVERSION}
  ${gflags_LIBRARIES}
  glog::glog
  ${FMT}
  ${FILESYSTEM})

add_executable(velox_in_10_min_demo VeloxIn10MinDemo.cpp)

target_link_libraries(
  velox_in_10_min_demo
  velox_aggregates
  velox_type
  velox_vector
  velox_vector_test_lib
  velox_exec
  velox_exec_test_util
  velox_tpch_connector
  velox_memory)
