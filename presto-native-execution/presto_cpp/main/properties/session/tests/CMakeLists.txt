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

add_executable(presto_session_properties_test SessionPropertiesTest.cpp)

add_test(NAME presto_session_properties_test COMMAND presto_session_properties_test)

target_link_libraries(
  presto_session_properties_test
  presto_session_properties
  $<TARGET_OBJECTS:presto_protocol>
  velox_core
  velox_exec
  velox_functions_prestosql
  GTest::gmock
  GTest::gtest
  GTest::gtest_main
  ${FOLLY_WITH_DEPENDENCIES}
  velox_dwio_common
  velox_dwio_common_exception
)

if(PRESTO_ENABLE_CUDF)
  add_executable(presto_cudf_session_properties_test CudfSessionPropertiesTest.cpp)

  add_test(
    NAME presto_cudf_session_properties_test
    COMMAND presto_cudf_session_properties_test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  )

  target_link_libraries(
    presto_cudf_session_properties_test
    presto_cudf_session_properties
    $<TARGET_OBJECTS:presto_protocol>
    velox_core
    velox_cudf_exec
    GTest::gmock
    GTest::gtest
    GTest::gtest_main
    ${FOLLY_WITH_DEPENDENCIES}
    velox_dwio_common
    velox_dwio_common_exception
    velox_hive_connector
  )
endif()
