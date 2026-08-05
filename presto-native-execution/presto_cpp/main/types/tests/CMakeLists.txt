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
  presto_type_converter
  presto_types
  velox
)

add_executable(
  presto_expressions_test
  RowExpressionTest.cpp
  ValuesPipeTest.cpp
  PlanConverterTest.cpp
  TypeParserTest.cpp
)

add_test(
  NAME presto_expressions_test
  COMMAND presto_expressions_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_dependencies(presto_expressions_test presto_operators presto_protocol presto_types)

target_link_libraries(
  presto_expressions_test
  GTest::gtest
  GTest::gtest_main
  presto_connectors
  presto_protocol
  presto_type_converter
  presto_types
  presto_operators
  presto_mutable_configs
  presto_type_test_utils
  presto_session_properties
  velox
  Boost::filesystem
)

set_property(TARGET presto_expressions_test PROPERTY JOB_POOL_LINK presto_link_job_pool)

add_executable(presto_to_velox_connector_test PrestoToVeloxConnectorTest.cpp)

add_test(
  NAME presto_to_velox_connector_test
  COMMAND presto_to_velox_connector_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  presto_to_velox_connector_test
  presto_connectors
  presto_protocol
  presto_operators
  presto_type_converter
  presto_types
  velox
  GTest::gtest
  GTest::gtest_main
)

add_executable(
  presto_to_velox_query_plan_test
  PrestoToVeloxQueryPlanTest.cpp
  NativeFunctionHandleTest.cpp
)

add_test(
  NAME presto_to_velox_query_plan_test
  COMMAND presto_to_velox_query_plan_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  presto_to_velox_query_plan_test
  presto_connectors
  presto_operators
  presto_protocol
  presto_type_converter
  presto_types
  velox
  GTest::gtest
  GTest::gtest_main
)

# RemoteFunctionHandleTest.cpp only contains tests cases related to
# RestFunctionHandle, therefore it is only enabled when remote functions are
# enabled.
if(PRESTO_ENABLE_REMOTE_FUNCTIONS)
  add_executable(presto_to_rest_function_test RestFunctionHandleTest.cpp)

  add_test(
    NAME presto_to_rest_function_test
    COMMAND presto_to_rest_function_test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  )

  target_link_libraries(
    presto_to_rest_function_test
    presto_velox_expr_conversion
    presto_protocol
    presto_types
    GTest::gtest
    GTest::gtest_main
  )
endif()
