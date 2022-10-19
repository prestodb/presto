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
  PrestoExchangeSourceTest.cpp TaskManagerTest.cpp HttpServerWrapper.cpp
  PrestoTaskTest.cpp AnnouncerTest.cpp QueryContextCacheTest.cpp)

add_test(presto_server_test presto_server_test)

target_link_libraries(
  presto_server_test
  presto_server_lib
  $<TARGET_OBJECTS:presto_type_converter>
  presto_types
  velox_exec_test_lib
  velox_dwio_common_test_utils
  velox_hive_connector
  velox_tpch_connector
  velox_presto_serializer
  velox_functions_prestosql
  velox_aggregates
  velox_hive_partition_function
  ${RE2}
  gtest
  gtest_main)
