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

add_executable(presto_function_metadata_test FunctionMetadataTest.cpp)

add_test(
  NAME presto_function_metadata_test
  COMMAND presto_function_metadata_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  presto_function_metadata_test
  presto_function_metadata
  GTest::gtest
  GTest::gtest_main
  velox_function_registry
  presto_common
)

add_dependencies(presto_function_metadata_test presto_types)

target_link_libraries(
  presto_function_metadata_test
  presto_type_test_utils
  presto_types
  $<TARGET_OBJECTS:presto_type_converter>
  $<TARGET_OBJECTS:presto_types>
  velox_functions_prestosql
  velox_aggregates
  velox_window
  velox_caching
  velox_exec_test_lib
  presto_server_lib
  velox_dwio_common_test_utils
  velox_hive_connector
  velox_tpch_connector
  velox_presto_serializer
  velox_hive_partition_function
  presto_mutable_configs
  ${RE2}
  velox_dwio_common
  velox_dwio_common_exception
)

if(PRESTO_ENABLE_CUDF)
  add_executable(presto_cudf_function_metadata_test CudfFunctionMetadataTest.cpp)

  add_test(
    NAME presto_cudf_function_metadata_test
    COMMAND presto_cudf_function_metadata_test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  )

  target_link_libraries(
    presto_cudf_function_metadata_test
    presto_function_metadata
    presto_cudf_function_metadata
    GTest::gtest
    GTest::gtest_main
    velox_function_registry
    presto_common
  )
  target_link_libraries(
    presto_cudf_function_metadata_test
    presto_type_test_utils
    presto_types
    $<TARGET_OBJECTS:presto_type_converter>
    $<TARGET_OBJECTS:presto_types>
    velox_functions_prestosql
    velox_aggregates
    velox_window
    velox_caching
    velox_exec_test_lib
    presto_server_lib
    velox_dwio_common_test_utils
    velox_dwio_common
    velox_dwio_common_exception
    velox_hive_connector
    velox_tpch_connector
    velox_presto_serializer
    velox_hive_partition_function
    presto_mutable_configs
    ${RE2}
    velox_connector
  )
endif()
