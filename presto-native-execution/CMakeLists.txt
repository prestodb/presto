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

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -mavx2 -mfma -mavx -mf16c -mlzcnt")
if("${TREAT_WARNINGS_AS_ERRORS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()

# Add all Presto options below
option(PRESTO_ENABLE_S3 "Build S3 connector" OFF)
option(PRESTO_ENABLE_PARQUET "Enable Parquet support" OFF)
option(PRESTO_ENABLE_TESTING "Enable tests" ON)

# Set all Velox options below
if(PRESTO_ENABLE_S3)
  set(VELOX_ENABLE_S3
      ON
      CACHE BOOL "Build S3 connector")
  add_definitions(-DPRESTO_ENABLE_S3)
endif()

if(PRESTO_ENABLE_PARQUET)
  set(VELOX_ENABLE_PARQUET
      ON
      CACHE BOOL "Enable Parquet support")
  add_definitions(-DPRESTO_ENABLE_PARQUET)
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

find_library(ANTLR4_RUNTIME antlr4-runtime)
find_path(
  ANTLR4_RUNTIME_INCLUDE_DIR
  NAMES antlr4-runtime.h
  PATH_SUFFIXES antlr4-runtime)
include_directories(SYSTEM ${ANTLR4_RUNTIME_INCLUDE_DIR})

find_library(GMock gmock)

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

# Include third party header files

find_package(Protobuf REQUIRED)

find_path(OPT_OPENSSL_DIR NAMES opt/openssl)
set(OPENSSL_ROOT_DIR "${OPT_OPENSSL_DIR}/opt/openssl")
find_package(OpenSSL REQUIRED)

find_library(LIBSODIUM_LIBRARY NAMES sodium)
find_library(PROXYGEN proxygen)
find_library(PROXYGEN_HTTP_SERVER proxygenhttpserver)
find_library(FIZZ fizz)
find_library(WANGLE wangle)

find_library(RE2 re2)

set(PROXYGEN_LIBRARIES ${PROXYGEN_HTTP_SERVER} ${PROXYGEN} ${WANGLE} ${FIZZ})
find_path(PROXYGEN_DIR NAMES include/proxygen)
set(PROXYGEN_INCLUDE_DIR "${PROXYGEN_DIR}/include/proxygen")

include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR} ${PROXYGEN_INCLUDE_DIR})
include_directories(.)
include_directories(velox)
include_directories(${VELOX_ROOT})

add_subdirectory(velox)

if(PRESTO_ENABLE_TESTING)
  include(CTest) # include after project() but before add_subdirectory()
  include_directories(velox/third_party/googletest/googletest/include)
  include_directories(veloxs/third_party/googletest/googlemock/include)
endif()

add_subdirectory(presto_cpp)
