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

add_library(presto_type_test_utils OBJECT TestUtils.cpp)

add_executable(presto_velox_split_test PrestoToVeloxSplitTest.cpp)

add_test(presto_velox_split_test presto_velox_split_test)

target_link_libraries(
  presto_velox_split_test
  GTest::gtest
  GTest::gtest_main
  presto_connectors
  presto_operators
  presto_protocol
  velox_dwio_common
  velox_dwio_orc_reader
  velox_hive_connector
  velox_tpch_connector
  velox_exec
  velox_dwio_common_exception
  presto_type_converter
  presto_types
  velox_type_fbhive
  velox_hive_partition_function)

add_executable(presto_expressions_test RowExpressionTest.cpp ValuesPipeTest.cpp
                                       PlanConverterTest.cpp)

add_test(
  NAME presto_expressions_test
  COMMAND presto_expressions_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_dependencies(presto_expressions_test presto_operators presto_protocol
                 presto_types)

target_link_libraries(
  presto_expressions_test
  GTest::gtest
  GTest::gtest_main
  presto_connectors
  $<TARGET_OBJECTS:presto_protocol>
  $<TARGET_OBJECTS:presto_type_converter>
  $<TARGET_OBJECTS:presto_types>
  presto_operators
  presto_mutable_configs
  presto_type_test_utils
  velox_core
  velox_dwio_common_exception
  velox_encode
  velox_exec
  velox_exec_test_lib
  velox_functions_prestosql
  velox_functions_lib
  velox_hive_connector
  velox_tpch_connector
  velox_hive_partition_function
  velox_presto_serializer
  velox_presto_type_parser
  velox_serialization
  velox_type
  Boost::filesystem
  ${RE2}
  ${FOLLY_WITH_DEPENDENCIES}
  ${GLOG}
  ${GFLAGS_LIBRARIES}
  pthread)

set_property(TARGET presto_expressions_test PROPERTY JOB_POOL_LINK
                                                     presto_link_job_pool)

add_executable(presto_to_velox_connector_test PrestoToVeloxConnectorTest.cpp)

add_test(
  NAME presto_to_velox_connector_test
  COMMAND presto_to_velox_connector_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  presto_to_velox_connector_test
  presto_connectors
  presto_protocol
  presto_operators
  presto_type_converter
  presto_types
  velox_dwio_common
  velox_hive_connector
  velox_tpch_connector
  GTest::gtest
  GTest::gtest_main)

add_executable(presto_function_metadata_test FunctionMetadataTest.cpp)

add_test(
  NAME presto_function_metadata_test
  COMMAND presto_function_metadata_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  presto_function_metadata_test
  presto_function_metadata
  presto_protocol
  presto_type_test_utils
  velox_aggregates
  velox_functions_prestosql
  velox_window
  GTest::gtest
  GTest::gtest_main)

add_executable(presto_to_velox_query_plan_test PrestoToVeloxQueryPlanTest.cpp)

add_test(
  NAME presto_to_velox_query_plan_test
  COMMAND presto_to_velox_query_plan_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  presto_to_velox_query_plan_test
  presto_connectors
  presto_operators
  presto_protocol
  presto_type_converter
  presto_types
  velox_dwio_common
  velox_exec_test_lib
  velox_hive_connector
  velox_tpch_connector
  GTest::gtest
  GTest::gtest_main)
