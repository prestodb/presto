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

add_library(presto_tool_trace_replayers PartitionAndSerializeReplayer.cpp)

target_link_libraries(
  presto_tool_trace_replayers
  presto_operators
  velox_core
  velox_exec
  velox_query_trace_replayer_base
)

add_executable(presto_trace_replayer TraceReplayerMain.cpp)

target_link_libraries(
  presto_trace_replayer
  presto_tool_trace_replayers
  presto_operators
  presto_type_converter
  velox_core
  velox_exec
  velox_query_trace_replayer_base
  ${FOLLY_WITH_DEPENDENCIES}
  ${GFLAGS_LIBRARIES}
)

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(tests)
endif()
