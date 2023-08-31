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
cmake_minimum_required(VERSION 3.10)

# set the project name
project(PrestoCpp)

# Note: these are not the same when doing out-of-tree build from src root dir,
# like `cmake -B _build/release`
set(VELOX_ROOT_SRC ${CMAKE_SOURCE_DIR}/velox)
set(VELOX_ROOT_BIN ${CMAKE_BINARY_DIR}/velox)

execute_process(
  COMMAND
    bash -c
    "( source ${CMAKE_CURRENT_SOURCE_DIR}/velox/scripts/setup-helper-functions.sh && echo -n $(get_cxx_flags $ENV{CPU_TARGET}))"
  OUTPUT_VARIABLE SCRIPT_CXX_FLAGS
  RESULT_VARIABLE COMMAND_STATUS)

# Include velox's dependency macros, see:
# velox/CMake/resolve_dependency_modules/README.md
list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake"
     "${VELOX_ROOT_SRC}/CMake" "${VELOX_ROOT_SRC}/CMake/third-party")
include(ResolveDependency)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
message("Appending CMAKE_CXX_FLAGS with ${SCRIPT_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS}")
if("${TREAT_WARNINGS_AS_ERRORS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()

# Known warnings that are benign can be disabled.
set(DISABLED_WARNINGS
    "-Wno-nullability-completeness -Wno-deprecated-declarations")

# Important warnings that must be explicitly enabled.
set(ENABLE_WARNINGS "-Wreorder")

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} ${DISABLED_WARNINGS} ${ENABLE_WARNINGS}")

# Add all Presto options below.

# Forwards user input to VELOX_ENABLE_S3.
option(PRESTO_ENABLE_S3 "Build S3 connector" OFF)

# Forwards user input to VELOX_ENABLE_HDFS.
option(PRESTO_ENABLE_HDFS "Build HDFS connector" OFF)

# Forwards user input to VELOX_ENABLE_PARQUET.
option(PRESTO_ENABLE_PARQUET "Enable Parquet support" OFF)

# Forwards user input to VELOX_ENABLE_REMOTE_FUNCTIONS.
option(PRESTO_ENABLE_REMOTE_FUNCTIONS "Enable remote function support" OFF)

option(PRESTO_ENABLE_TESTING "Enable tests" ON)

# Set all Velox options below
if(PRESTO_ENABLE_S3)
  set(VELOX_ENABLE_S3
      ON
      CACHE BOOL "Build S3 connector")
endif()

if(PRESTO_ENABLE_HDFS)
  set(VELOX_ENABLE_HDFS
      ON
      CACHE BOOL "Build HDFS Connector")
endif()

if(PRESTO_ENABLE_PARQUET)
  set(VELOX_ENABLE_PARQUET
      ON
      CACHE BOOL "Enable Parquet support")
endif()

if(PRESTO_ENABLE_REMOTE_FUNCTIONS)
  set(VELOX_ENABLE_REMOTE_FUNCTIONS
      ON
      CACHE BOOL "Enable remote function support in Velox")
  add_compile_definitions(PRESTO_ENABLE_REMOTE_FUNCTIONS)
endif()

set(VELOX_BUILD_TESTING
    OFF
    CACHE BOOL "Enable Velox tests")

set(VELOX_ENABLE_SPARK_FUNCTIONS
    OFF
    CACHE BOOL "Enable Velox Spark functions")

set(VELOX_ENABLE_EXAMPLES
    OFF
    CACHE BOOL "Enable Velox examples")

set(VELOX_BUILD_TEST_UTILS
    ${PRESTO_ENABLE_TESTING}
    CACHE BOOL "Enable Velox test utils")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set this for backwards compatibility, will be overwritten in velox/
set(VELOX_GTEST_INCUDE_DIR "velox/third_party/googletest/googletest/include")
include_directories(.)
include_directories(${CMAKE_BINARY_DIR})
include_directories(${VELOX_ROOT_SRC})
include_directories(${VELOX_ROOT_BIN})
# Before doing anything else, we add velox and any includes it might need. This
# allows us to bundle the same Find modules that are shipped with velox, but
# with our own modifications. For instance, our modules can detect if the target
# dependency is already defined.
add_subdirectory(${VELOX_ROOT_SRC})

set(BOOST_INCLUDE_LIBRARIES
    atomic
    context
    date_time
    filesystem
    program_options
    regex
    system
    thread)
set(Boost_USE_MULTITHREADED ON)
set_source(Boost)
resolve_dependency(Boost 1.66.0 COMPONENTS ${BOOST_INCLUDE_LIBRARIES})

set_source(gflags)
resolve_dependency(gflags COMPONENTS shared)

set_source(antlr4-runtime)
resolve_dependency(antlr4-runtime)

set_source(glog)
resolve_dependency(glog)

set_source(fmt)
resolve_dependency(fmt)

add_compile_definitions(FOLLY_HAVE_INT128_T=1)
set_source(folly)
resolve_dependency(folly)

# Include third party header files
find_path(OPT_OPENSSL_DIR NAMES opt/openssl@1.1)
set(OPENSSL_ROOT_DIR "${OPT_OPENSSL_DIR}/opt/openssl@1.1")
set_source(OpenSSL)
resolve_dependency(OpenSSL REQUIRED)

# Note: the uppercase name here is correct
set_source(Sodium)
resolve_dependency(Sodium)
set_source(fizz)
resolve_dependency(fizz)
set_source(wangle)
resolve_dependency(wangle)
set_source(proxygen)
resolve_dependency(proxygen)
set_source(re2)
resolve_dependency(re2)
set_source(FBThrift)
resolve_dependency(FBThrift)

include_directories(SYSTEM ${FBTHRIFT_INCLUDE_DIR} ${OPENSSL_INCLUDE_DIR}
                    ${PROXYGEN_INCLUDE_DIR} ${ANTLR4_RUNTIME_INCLUDE_DIR})

if(PRESTO_ENABLE_TESTING)
  include(CTest) # include after project() but before add_subdirectory()
  include_directories(${VELOX_GTEST_INCUDE_DIR})
else()
  add_definitions(-DVELOX_DISABLE_GOOGLETEST)
endif()

add_subdirectory(presto_cpp)
