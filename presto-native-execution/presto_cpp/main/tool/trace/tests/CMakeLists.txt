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

add_executable(presto_partition_and_serialize_replayer_test PartitionAndSerializeReplayerTest.cpp)

target_link_libraries(
  presto_partition_and_serialize_replayer_test
  presto_tool_trace_replayers
  presto_operators
  presto_plan_builder
  velox_core
  velox_exec
  velox_exec_test_lib
  velox_hive_connector
  velox_hive_iceberg_splitreader
  velox_query_trace_replayer_base
  gtest
  gtest_main
  ${FOLLY_WITH_DEPENDENCIES}
  ${GFLAGS_LIBRARIES}
)

set_property(
  TARGET presto_partition_and_serialize_replayer_test
  PROPERTY JOB_POOL_LINK presto_link_job_pool
)

add_test(
  NAME presto_partition_and_serialize_replayer_test
  COMMAND presto_partition_and_serialize_replayer_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
