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
  CrossJoinTest.cpp
  CustomJoinTest.cpp
  DriverTest.cpp
  EnforceSingleRowTest.cpp
  FilterProjectTest.cpp
  TableScanTest.cpp
  TaskTest.cpp
  AggregationTest.cpp
  AggregateFunctionRegistryTest.cpp
  RowContainerTest.cpp
  HashTableTest.cpp
  TreeOfLosersTest.cpp
  VectorHasherTest.cpp
  LocalPartitionTest.cpp
  MultiFragmentTest.cpp
  ParseTypeSignatureTest.cpp
  PartitionedOutputBufferManagerTest.cpp
  RoundRobinPartitionFunctionTest.cpp
  StreamingAggregationTest.cpp
  TableWriteTest.cpp
  TaskListenerTest.cpp
  TopNTest.cpp
  LimitTest.cpp
  OrderByTest.cpp
  OperatorUtilsTest.cpp
  MergeTest.cpp
  MergeJoinTest.cpp
  HashJoinTest.cpp
  PlanNodeToStringTest.cpp
  FunctionSignatureBuilderTest.cpp
  SimpleFunctionResolutionTest.cpp
  SpillTest.cpp
  "SpillerTest.cpp"
  UnnestTest.cpp
  AssignUniqueIdTest.cpp)

add_test(
  NAME velox_exec_test
  COMMAND velox_exec_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  velox_exec_test
  velox_functions_test_lib
  velox_exec
  velox_dwio_common
  velox_aggregates
  velox_aggregates_test_lib
  velox_functions_lib
  velox_functions_prestosql
  velox_hive_connector
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
  ${FOLLY_WITH_DEPENDENCIES}
  ${DOUBLE_CONVERSION}
  ${gflags_LIBRARIES}
  glog::glog
  ${FMT}
  ${FILESYSTEM})
