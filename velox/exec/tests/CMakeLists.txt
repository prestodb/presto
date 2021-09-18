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

add_library(
  velox_exec_test_lib
  PlanBuilder.cpp QueryAssertions.cpp Cursor.cpp OperatorTestBase.cpp
  HiveConnectorTestBase.cpp TempFilePath.cpp)

target_link_libraries(
  velox_exec_test_lib
  velox_core
  duckdb
  velox_parse_parser
  velox_vector_test_lib
  velox_duckdb_conversion
  velox_dwio_common
  velox_dwio_type_fbhive
  velox_dwrf_test_utils
  velox_presto_serializer)

add_executable(
  velox_exec_test
  DriverTest.cpp
  EnforceSingleRowTest.cpp
  FilterProjectTest.cpp
  TableScanTest.cpp
  TaskTest.cpp
  AggregationTest.cpp
  RowContainerTest.cpp
  HashTableTest.cpp
  HashStringAllocatorTest.cpp
  CompactDoubleListTest.cpp
  TreeOfLosersTest.cpp
  VectorHasherTest.cpp
  LocalPartitionTest.cpp
  MultiFragmentTest.cpp
  ParseTypeSignatureTest.cpp
  PartitionedOutputBufferManagerTest.cpp
  TableWriteTest.cpp
  TopNTest.cpp
  LimitTest.cpp
  OrderByTest.cpp
  MergeTest.cpp
  HashJoinTest.cpp
  PlanNodeToStringTest.cpp
  FunctionSignatureBuilderTest.cpp
  UnnestTest.cpp)

add_test(
  NAME velox_exec_test
  COMMAND velox_exec_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  velox_exec_test
  velox_exec_test_lib
  velox_functions_test_lib
  velox_exec
  velox_dwio_dwrf_reader
  velox_dwio_dwrf_writer
  velox_dwio_common
  velox_aggregates
  velox_aggregates_test_lib
  velox_functions_lib
  velox_functions_prestosql
  velox_hive_connector
  velox_core
  velox_type
  velox_serialization
  velox_exec_test_util
  velox_functions_json
  velox_vector
  velox_memory
  velox_parse_parser
  velox_dwio_type_fbhive
  velox_dwio_common_exception
  velox_dwrf_test_utils
  velox_presto_serializer
  ${Boost_ATOMIC_LIBRARIES}
  ${Boost_CONTEXT_LIBRARIES}
  ${Boost_DATE_TIME_LIBRARIES}
  ${Boost_FILESYSTEM_LIBRARIES}
  ${Boost_PROGRAM_OPTIONS_LIBRARIES}
  ${Boost_REGEX_LIBRARIES}
  ${Boost_THREAD_LIBRARIES}
  ${Boost_SYSTEM_LIBRARIES}
  ${GTEST_BOTH_LIBRARIES}
  ${FOLLY_WITH_DEPENDENCIES}
  ${DOUBLE_CONVERSION}
  ${gflags_LIBRARIES}
  ${GLOG}
  ${FMT}
  ${FILESYSTEM})
