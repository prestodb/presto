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
add_subdirectory(operators)
add_subdirectory(types)
add_subdirectory(http)
add_subdirectory(common)
add_subdirectory(thrift)

add_library(
  presto_server_lib
  Announcer.cpp
  CPUMon.cpp
  CoordinatorDiscoverer.cpp
  LinuxMemoryChecker.cpp
  PeriodicMemoryChecker.cpp
  PeriodicTaskManager.cpp
  PrestoExchangeSource.cpp
  PrestoServer.cpp
  PrestoServerOperations.cpp
  PrestoTask.cpp
  QueryContextManager.cpp
  ServerOperation.cpp
  SignalHandler.cpp
  SystemConnector.cpp
  TaskManager.cpp
  TaskResource.cpp
  PeriodicHeartbeatManager.cpp
  PeriodicServiceInventoryManager.cpp)

add_dependencies(presto_server_lib presto_operators presto_protocol
                 presto_types presto_thrift-cpp2 presto_thrift_extra)

target_include_directories(presto_server_lib PRIVATE ${presto_thrift_INCLUDES})
target_link_libraries(
  presto_server_lib
  $<TARGET_OBJECTS:presto_type_converter>
  $<TARGET_OBJECTS:presto_types>
  $<TARGET_OBJECTS:presto_protocol>
  presto_common
  presto_exception
  presto_http
  presto_operators
  velox_aggregates
  velox_caching
  velox_common_base
  velox_core
  velox_dwio_common_exception
  velox_encode
  velox_exec
  velox_functions_lib
  velox_functions_prestosql
  velox_hive_connector
  velox_hive_iceberg_splitreader
  velox_hive_partition_function
  velox_presto_serializer
  velox_serialization
  velox_time
  velox_type_parser
  velox_type
  velox_type_fbhive
  velox_type_tz
  velox_vector
  velox_window
  ${RE2}
  ${FOLLY_WITH_DEPENDENCIES}
  ${GLOG}
  ${GFLAGS_LIBRARIES}
  pthread)

# Enabling Parquet causes build errors with missing symbols on MacOS. This is
# likely due to a conflict between Arrow Thrift from velox_hive_connector and
# FBThrift libraries. The build issue is fixed by linking velox_hive_connector
# dependencies first followed by FBThrift.
target_link_libraries(presto_server_lib presto_thrift-cpp2 presto_thrift_extra
                      ${THRIFT_LIBRARY})

set_property(TARGET presto_server_lib PROPERTY JOB_POOL_LINK
                                               presto_link_job_pool)

add_executable(presto_server PrestoMain.cpp)

# Moving velox_hive_connector and velox_tpch_connector to presto_server_lib
# results in multiple link errors similar to the one below only on GCC.
# "undefined reference to `vtable for velox::connector::tpch::TpchTableHandle`"
# TODO: Fix these errors.
target_link_libraries(presto_server presto_server_lib velox_hive_connector
                      velox_tpch_connector)

if(PRESTO_ENABLE_REMOTE_FUNCTIONS)
  add_library(presto_server_remote_function JsonSignatureParser.cpp
                                            RemoteFunctionRegisterer.cpp)

  target_link_libraries(presto_server_remote_function velox_expression
                        velox_functions_remote ${FOLLY_WITH_DEPENDENCIES})
  target_link_libraries(presto_server_lib presto_server_remote_function)
endif()

set_property(TARGET presto_server PROPERTY JOB_POOL_LINK presto_link_job_pool)

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(tests)
endif()
