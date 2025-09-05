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

set(VELOX_ROOT ${CMAKE_BINARY_DIR}/velox)
list(PREPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/velox/CMake")

execute_process(
  COMMAND
    bash -c
    "( source ${CMAKE_CURRENT_SOURCE_DIR}/velox/scripts/setup-helper-functions.sh && echo -n $(get_cxx_flags $ENV{CPU_TARGET}))"
  OUTPUT_VARIABLE SCRIPT_CXX_FLAGS
  RESULT_VARIABLE COMMAND_STATUS)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
message("Appending CMAKE_CXX_FLAGS with ${SCRIPT_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS}")

# Known warnings that are benign can be disabled.
set(DISABLED_WARNINGS
    "-Wno-nullability-completeness -Wno-deprecated-declarations")

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "14.0.0")
    string(APPEND DISABLED_WARNINGS " -Wno-error=template-id-cdtor")
  endif()
endif()

# Important warnings that must be explicitly enabled.
set(ENABLE_WARNINGS "-Wreorder")

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} ${DISABLED_WARNINGS} ${ENABLE_WARNINGS}")

# Add all Presto options below.

# Forwards user input to VELOX_ENABLE_S3.
option(PRESTO_ENABLE_S3 "Build S3 support" OFF)

# Forwards user input to VELOX_ENABLE_HDFS.
option(PRESTO_ENABLE_HDFS "Build HDFS support" OFF)

# Forwards user input to VELOX_ENABLE_GCS.
option(PRESTO_ENABLE_GCS "Build GCS support" OFF)

# Forwards user input to VELOX_ENABLE_ABFS.
option(PRESTO_ENABLE_ABFS "Build ABFS support" OFF)

# Forwards user input to VELOX_ENABLE_PARQUET.
option(PRESTO_ENABLE_PARQUET "Enable Parquet support" ON)

# Forwards user input to VELOX_ENABLE_CUDF.
option(PRESTO_ENABLE_CUDF "Enable cuDF support" OFF)

# Forwards user input to VELOX_ENABLE_REMOTE_FUNCTIONS.
option(PRESTO_ENABLE_REMOTE_FUNCTIONS "Enable remote function support" OFF)

option(PRESTO_ENABLE_EXAMPLES "Enable Presto examples" OFF)

option(PRESTO_ENABLE_TESTING "Enable tests" ON)

option(PRESTO_ENABLE_JWT "Enable JWT (JSON Web Token) authentication" OFF)

option(PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR "Enable Arrow Flight connector" OFF)

option(PRESTO_ENABLE_SPATIAL "Enable spatial support" ON)

# Set all Velox options below and make sure that if we include folly headers or
# other dependency headers that include folly headers we turn off the coroutines
# and turn on int128.
add_compile_definitions(FOLLY_HAVE_INT128_T=1 FOLLY_CFG_NO_COROUTINES)

if(PRESTO_ENABLE_S3)
  set(VELOX_ENABLE_S3
      ON
      CACHE BOOL "Build S3 support")
endif()

if(PRESTO_ENABLE_HDFS)
  set(VELOX_ENABLE_HDFS
      ON
      CACHE BOOL "Build HDFS support")
endif()

if(PRESTO_ENABLE_GCS)
  set(VELOX_ENABLE_GCS
      ON
      CACHE BOOL "Build GCS support")
endif()

if(PRESTO_ENABLE_ABFS)
  set(VELOX_ENABLE_ABFS
      ON
      CACHE BOOL "Build ABFS support")
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

if(PRESTO_ENABLE_CUDF)
  set(VELOX_ENABLE_CUDF
      ON
      CACHE BOOL "Enable cuDF support")
  add_compile_definitions(PRESTO_ENABLE_CUDF)
  enable_language(CUDA)
  # Determine CUDA_ARCHITECTURES automatically.
  cmake_policy(SET CMP0104 NEW)
endif()

if(PRESTO_ENABLE_SPATIAL)
  set(VELOX_ENABLE_GEO
      ON
      CACHE BOOL "Enable Velox Geometry (aka spatial) support")
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

if(DEFINED ENV{INSTALL_PREFIX})
  message(STATUS "Dependency install directory set to: $ENV{INSTALL_PREFIX}")
  list(APPEND CMAKE_PREFIX_PATH "$ENV{INSTALL_PREFIX}")
endif()

set(Boost_USE_MULTITHREADED TRUE)
find_package(
  Boost
  1.66.0
  REQUIRED
  program_options
  context
  filesystem
  regex
  thread
  system
  date_time
  atomic)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})

find_package(gflags COMPONENTS shared)

find_library(GLOG glog)

find_library(FMT fmt)

find_library(EVENT event)

find_library(DOUBLE_CONVERSION double-conversion)

find_library(LZ4 lz4)
find_library(LZO lzo2)
find_library(ZSTD zstd)
find_package(ZLIB)
find_library(SNAPPY snappy)

find_package(folly CONFIG REQUIRED)
set(FOLLY_WITH_DEPENDENCIES
    ${FOLLY_LIBRARIES}
    ${DOUBLE_CONVERSION}
    Boost::context
    dl
    ${EVENT}
    ${SNAPPY}
    ${LZ4}
    ${ZSTD}
    ${ZLIB_LIBRARIES})

find_package(BZip2 MODULE)
if(BZIP2_FOUND)
  list(APPEND FOLLY_WITH_DEPENDENCIES ${BZIP2_LIBRARIES})
endif()
include_directories(SYSTEM ${FOLLY_INCLUDE_DIRS})

find_package(OpenSSL REQUIRED)

find_package(Sodium REQUIRED)
find_library(PROXYGEN proxygen)
find_library(PROXYGEN_HTTP_SERVER proxygenhttpserver)
find_library(FIZZ fizz)
find_library(WANGLE wangle)
find_library(MVFST_EXCEPTION mvfst_exception)

find_library(RE2 re2)

find_package(fizz CONFIG)
find_package(wangle CONFIG)
find_package(FBThrift)
include_directories(SYSTEM ${FBTHRIFT_INCLUDE_DIR})

set(PROXYGEN_LIBRARIES ${PROXYGEN_HTTP_SERVER} ${PROXYGEN} ${WANGLE} ${FIZZ}
                       ${MVFST_EXCEPTION})
find_path(PROXYGEN_DIR NAMES include/proxygen)
set(PROXYGEN_INCLUDE_DIR "${PROXYGEN_DIR}/include/")

include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR} ${PROXYGEN_INCLUDE_DIR})
include_directories(.)
include_directories(velox)
include_directories(velox/velox/external/xxhash)
include_directories(${VELOX_ROOT})
include_directories(${CMAKE_BINARY_DIR})

# set this for backwards compatibility, will be overwritten in velox/
set(VELOX_GTEST_INCUDE_DIR "velox/third_party/googletest/googletest/include")

add_subdirectory(velox)

if(PRESTO_ENABLE_TESTING)
  set(BUILD_TESTING ON) # some third-party dependency could have disabled this.
  include(CTest) # include after project() but before add_subdirectory()
  find_package(GTest)
  include_directories(${VELOX_GTEST_INCUDE_DIR})
else()
  add_definitions(-DVELOX_DISABLE_GOOGLETEST)
endif()

if(PRESTO_ENABLE_JWT)
  add_compile_definitions(PRESTO_ENABLE_JWT)
endif()

if("${MAX_LINK_JOBS}")
  set_property(GLOBAL APPEND PROPERTY JOB_POOLS
                                      "presto_link_job_pool=${MAX_LINK_JOBS}")
else()
  set_property(GLOBAL APPEND PROPERTY JOB_POOLS "presto_link_job_pool=8")
endif()
set(CMAKE_JOB_POOL_LINK presto_link_job_pool)

# Adding this down here prevents warnings in dependencies from stopping the
# build
if("${TREAT_WARNINGS_AS_ERRORS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()

if(DEFINED ENV{INSTALL_PREFIX})
  # Allow installed package headers to be picked up before brew/system package
  # headers. We set this after Velox since Velox handles INSTALL_PREFIX its own
  # way.
  include_directories(BEFORE "$ENV{INSTALL_PREFIX}/include")
endif()

add_subdirectory(presto_cpp)
