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
add_executable(
  presto_server_test
  AnnouncerTest.cpp
  CoordinatorDiscovererTest.cpp
  HttpServerWrapper.cpp
  PeriodicMemoryCheckerTest.cpp
  PrestoExchangeSourceTest.cpp
  PrestoTaskTest.cpp
  PrestoToVeloxQueryConfigTest.cpp
  QueryContextCacheTest.cpp
  ServerOperationTest.cpp
  SessionPropertiesTest.cpp
  TaskManagerTest.cpp
  QueryContextManagerTest.cpp
  TaskInfoTest.cpp
  TaskStatusTest.cpp
  TaskUpdateRequestTest.cpp)

if(DEFINED PRESTO_MEMORY_CHECKER_TYPE AND PRESTO_MEMORY_CHECKER_TYPE STREQUAL
                                          "LINUX_MEMORY_CHECKER")
  target_sources(presto_server_test PRIVATE LinuxMemoryCheckerTest.cpp)
endif()

add_test(
  NAME presto_server_test
  COMMAND presto_server_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  presto_server_test
  velox_caching
  velox_exec_test_lib
  presto_server_lib
  velox_dwio_common_test_utils
  $<TARGET_OBJECTS:presto_type_converter>
  $<TARGET_OBJECTS:presto_types>
  velox_hive_connector
  velox_tpch_connector
  velox_presto_serializer
  velox_functions_prestosql
  velox_aggregates
  velox_hive_partition_function
  presto_mutable_configs
  ${RE2}
  GTest::gmock
  GTest::gtest
  GTest::gtest_main)

# Clang requires explicit linking with libatomic.
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang"
   AND "${CMAKE_CXX_COMPILER_VERSION}" VERSION_GREATER_EQUAL 15)
  target_link_libraries(presto_server_test atomic)
endif()

set_property(TARGET presto_server_test PROPERTY JOB_POOL_LINK
                                                presto_link_job_pool)

if(PRESTO_ENABLE_REMOTE_FUNCTIONS)
  add_executable(presto_server_remote_function_test
                 JsonSignatureParserTest.cpp RemoteFunctionRegistererTest.cpp)

  add_test(
    NAME presto_server_remote_function_test
    COMMAND presto_server_remote_function_test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  target_link_libraries(
    presto_server_remote_function_test
    presto_server_remote_function
    velox_expression
    velox_temp_path
    GTest::gmock
    GTest::gtest
    GTest::gtest_main)
endif()
