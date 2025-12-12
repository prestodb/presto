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
add_subdirectory(connectors)
add_subdirectory(functions)
add_subdirectory(tool)

add_library(presto_session_properties SessionProperties.cpp)

target_link_libraries(presto_session_properties ${FOLLY_WITH_DEPENDENCIES})

# if(PRESTO_ENABLE_TABLE_FUNCTIONS) add_subdirectory(tvf)
add_subdirectory(tvf)
# endif()

add_library(
  presto_server_lib
  Announcer.cpp
  CPUMon.cpp
  CoordinatorDiscoverer.cpp
  PeriodicMemoryChecker.cpp
  PeriodicTaskManager.cpp
  PrestoExchangeSource.cpp
  PrestoServer.cpp
  PrestoServerOperations.cpp
  PrestoTask.cpp
  PrestoToVeloxQueryConfig.cpp
  QueryContextManager.cpp
  ServerOperation.cpp
  SignalHandler.cpp
  TaskManager.cpp
  TaskResource.cpp
  PeriodicHeartbeatManager.cpp
  PeriodicServiceInventoryManager.cpp
)

add_dependencies(
  presto_server_lib
  presto_operators
  presto_protocol
  presto_types
  presto_thrift-cpp2
  presto_thrift_extra
)

target_include_directories(presto_server_lib PRIVATE ${presto_thrift_INCLUDES})

target_link_libraries(
  presto_server_lib
  $<TARGET_OBJECTS:presto_type_converter>
  $<TARGET_OBJECTS:presto_types>
  $<TARGET_OBJECTS:presto_protocol>
  presto_common
  presto_exception
  presto_expression_optimizer
  presto_function_metadata
  presto_connectors
  presto_http
  presto_operators
  presto_session_properties
  presto_velox_plan_conversion
  presto_hive_functions
  velox_abfs
  velox_aggregates
  velox_caching
  velox_common_base
  velox_core
  velox_dwio_common_exception
  velox_dwio_dwrf_reader
  velox_dwio_dwrf_writer
  velox_dwio_orc_reader
  velox_dwio_parquet_reader
  velox_dwio_parquet_writer
  velox_dwio_text_reader_register
  velox_dwio_text_writer_register
  velox_dynamic_library_loader
  velox_encode
  velox_exec
  velox_file
  velox_functions_lib
  velox_functions_prestosql
  velox_gcs
  velox_hdfs
  velox_hive_connector
  velox_hive_iceberg_splitreader
  velox_hive_partition_function
  velox_presto_serializer
  velox_presto_type_parser
  velox_s3fs
  velox_serialization
  velox_time
  velox_type
  velox_type_fbhive
  velox_type_tz
  velox_vector
  velox_window
  ${RE2}
  ${FOLLY_WITH_DEPENDENCIES}
  ${GLOG}
  ${GFLAGS_LIBRARIES}
  pthread
)

target_link_libraries(presto_server_lib presto_tvf_exec presto_tvf_functions)

if(PRESTO_ENABLE_CUDF)
  target_link_libraries(presto_server_lib velox_cudf_exec)
endif()

# Enabling Parquet causes build errors with missing symbols on MacOS. This is
# likely due to a conflict between Arrow Thrift from velox_hive_connector and
# FBThrift libraries. The build issue is fixed by linking velox_hive_connector
# dependencies first followed by FBThrift.
target_link_libraries(presto_server_lib presto_thrift-cpp2 presto_thrift_extra ${THRIFT_LIBRARY})

if(PRESTO_ENABLE_REMOTE_FUNCTIONS)
  add_library(presto_server_remote_function JsonSignatureParser.cpp RemoteFunctionRegisterer.cpp)

  target_link_libraries(
    presto_server_remote_function
    velox_expression
    velox_functions_remote
    ${FOLLY_WITH_DEPENDENCIES}
  )
  target_link_libraries(presto_server_lib presto_server_remote_function)
endif()

set_property(TARGET presto_server_lib PROPERTY JOB_POOL_LINK presto_link_job_pool)

add_executable(presto_server PrestoMain.cpp)

# The below additional flags are necessary for resolving dependencies for
# loading dynamic libraries.
if(APPLE)
  target_link_options(presto_server BEFORE PUBLIC "-Wl,-undefined,dynamic_lookup")
else()
  target_link_options(presto_server BEFORE PUBLIC "-Wl,-export-dynamic")
endif()

# velox_tpch_connector is an OBJECT target in Velox and so needs to be linked to
# the executable or use TARGET_OBJECT linkage for the presto_server_lib target.
# However, we also would need to add its dependencies (tpch_gen etc). TODO
# change the target in Velox to a library target then we can move this to the
# presto_server_lib.
target_link_libraries(presto_server presto_server_lib velox_tpch_connector velox_tpcds_connector)

# Clang requires explicit linking with libatomic.
if(
  "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang"
  AND "${CMAKE_CXX_COMPILER_VERSION}" VERSION_GREATER_EQUAL 15
)
  target_link_libraries(presto_server atomic)
endif()

set_property(TARGET presto_server PROPERTY JOB_POOL_LINK presto_link_job_pool)

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(tests)
endif()

if(PRESTO_STATS_REPORTER_TYPE)
  add_compile_definitions(PRESTO_STATS_REPORTER_TYPE)
  if(PRESTO_STATS_REPORTER_TYPE STREQUAL "PROMETHEUS")
    add_subdirectory(runtime-metrics)
    target_link_libraries(presto_server presto_prometheus_reporter)
  else()
    message(FATAL_ERROR "${PRESTO_STATS_REPORTER_TYPE} is not a valid stats reporter name")
  endif()
endif()

if(PRESTO_MEMORY_CHECKER_TYPE)
  add_compile_definitions(PRESTO_MEMORY_CHECKER_TYPE)
  # Check if the current platform is Linux and the memory checker type is
  # LINUX_MEMORY_CHECKER.
  if(UNIX AND NOT APPLE AND (PRESTO_MEMORY_CHECKER_TYPE STREQUAL "LINUX_MEMORY_CHECKER"))
    add_library(presto_linux_memory_checker OBJECT LinuxMemoryChecker.cpp)
    target_link_libraries(presto_server presto_linux_memory_checker)
  else()
    message(FATAL_ERROR "${PRESTO_MEMORY_CHECKER_TYPE} is not a valid memory checker name")
  endif()
endif()
