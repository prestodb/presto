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

add_executable(presto_velox_split_test PrestoToVeloxSplitTest.cpp)

add_test(presto_velox_split_test presto_velox_split_test)

target_link_libraries(
  presto_velox_split_test
  gtest
  gtest_main
  presto_protocol
  velox_hive_connector
  velox_tpch_connector
  velox_exec
  velox_dwio_common_exception
  presto_type_converter
  presto_types
  ${ANTLR4_RUNTIME}
  velox_hive_partition_function)

add_executable(
  presto_expressions_test RowExpressionTest.cpp TypeSignatureTest.cpp
                          ValuesPipeTest.cpp PlanConverterTest.cpp)

add_test(
  NAME presto_expressions_test
  COMMAND presto_expressions_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_dependencies(presto_expressions_test presto_protocol presto_types)

# There is not really a better way at this point.  Several posts have complained
# about the parser generator not producing a file that finds the includes on the
# default install path. :(
#
target_include_directories(presto_expressions_test SYSTEM
                           PRIVATE "/usr/local/include/antlr4-runtime")

target_link_libraries(
  presto_expressions_test
  gtest
  gtest_main
  $<TARGET_OBJECTS:presto_protocol>
  $<TARGET_OBJECTS:presto_type_converter>
  presto_types
  velox_core
  velox_dwio_common_exception
  velox_encode
  velox_exec
  velox_functions_prestosql
  velox_functions_lib
  velox_hive_connector
  velox_tpch_connector
  velox_hive_partition_function
  velox_presto_serializer
  velox_serialization
  velox_type
  Boost::filesystem
  ${RE2}
  ${ANTLR4_RUNTIME}
  ${FOLLY_WITH_DEPENDENCIES}
  ${GLOG}
  ${GFLAGS_LIBRARIES}
  pthread)
