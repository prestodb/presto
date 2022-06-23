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
add_subdirectory(types)
add_subdirectory(http)
add_subdirectory(common)
add_subdirectory(thrift)
add_subdirectory(connectors)

add_library(
  presto_server_lib
  Announcer.cpp
  CPUMon.cpp
  PeriodicTaskManager.cpp
  PrestoExchangeSource.cpp
  PrestoServer.cpp
  PrestoTask.cpp
  QueryContextManager.cpp
  SignalHandler.cpp
  TaskManager.cpp
  TaskResource.cpp)

add_dependencies(presto_server_lib presto_protocol presto_types
                 presto_thrift-cpp2 presto_thrift_extra)

target_include_directories(presto_server_lib PRIVATE ${presto_thrift_INCLUDES})
target_link_libraries(
  presto_server_lib
  $<TARGET_OBJECTS:presto_type_converter>
  $<TARGET_OBJECTS:presto_types>
  $<TARGET_OBJECTS:presto_protocol>
  presto_adapters
  presto_common
  presto_thrift-cpp2
  presto_thrift_extra
  presto_exception
  presto_http
  velox_core
  velox_vector
  velox_exec
  velox_time
  velox_type
  velox_type_tz
  velox_serialization
  velox_encode
  velox_dwio_common_exception
  velox_functions_lib
  velox_hive_partition_function
  ${THRIFT_LIBRARY}
  ${RE2}
  ${FOLLY_WITH_DEPENDENCIES}
  ${ANTLR4_RUNTIME}
  ${GLOG}
  ${GFLAGS_LIBRARIES}
  pthread)

add_executable(presto_server PrestoMain.cpp)

target_link_libraries(
  presto_server
  presto_server_lib
  velox_functions_prestosql
  velox_aggregates
  velox_hive_connector
  velox_presto_serializer
  velox_dwio_dwrf_reader)

if(PRESTO_ENABLE_PARQUET)
  target_link_libraries(presto_server velox_dwio_parquet_reader)
endif()

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(tests)
endif()
