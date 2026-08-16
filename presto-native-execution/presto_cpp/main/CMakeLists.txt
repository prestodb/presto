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
add_subdirectory(properties)

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
  presto_theta_sketch_functions
  velox
  ${RE2}
  ${FOLLY_WITH_DEPENDENCIES}
  ${GLOG}
  ${GFLAGS_LIBRARIES}
  pthread
)

if(PRESTO_ENABLE_CUDF)
  # The symbols from velox_cudf_exec are not included in the Velox mono library
  # and need to be linked separately.
  target_link_libraries(presto_server_lib velox_cudf_exec)
endif()

if(PRESTO_ENABLE_REMOTE_FUNCTIONS)
  add_library(presto_server_remote_function JsonSignatureParser.cpp RemoteFunctionRegisterer.cpp)

  target_link_libraries(presto_server_remote_function velox ${FOLLY_WITH_DEPENDENCIES})
  target_link_libraries(presto_server_lib presto_server_remote_function)
endif()

target_link_libraries(presto_server_lib presto_thrift-cpp2 presto_thrift_extra ${THRIFT_LIBRARY})

set_property(TARGET presto_server_lib PROPERTY JOB_POOL_LINK presto_link_job_pool)

add_executable(presto_server PrestoMain.cpp)

# The below additional flags are necessary for resolving dependencies for
# loading dynamic libraries.
if(APPLE)
  target_link_options(presto_server BEFORE PUBLIC "-Wl,-undefined,dynamic_lookup")
else()
  target_link_options(presto_server BEFORE PUBLIC "-Wl,-export-dynamic")
endif()

target_link_libraries(presto_server presto_server_lib)

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
