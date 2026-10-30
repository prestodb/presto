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
cmake_minimum_required(VERSION 3.24)

# Use rapids-cmake to configure CUDA architectures
set(_cuda_lang "")
if(PRESTO_ENABLE_CUDF)
  # Declare rapids-cmake exactly as Velox's cudf.cmake does, including the
  # checksum and the VELOX_rapids_cmake_URL/VELOX_rapids_cmake_SHA256 environment
  # overrides. FetchContent keeps the first declaration of a name, so this one is
  # the one cuDF ends up using.
  file(
    READ
    "${CMAKE_SOURCE_DIR}/velox/CMake/resolve_dependency_modules/cudf.cmake"
    _dep_cmake_content
  )
  string(REGEX MATCH "VELOX_rapids_cmake_COMMIT ([0-9a-fA-F]+)" _match "${_dep_cmake_content}")
  set(VELOX_rapids_cmake_COMMIT "${CMAKE_MATCH_1}")
  string(
    REGEX MATCH
    "VELOX_rapids_cmake_BUILD_SHA256_CHECKSUM[ \t\r\n]+([0-9a-fA-F]+)"
    _match
    "${_dep_cmake_content}"
  )
  set(VELOX_rapids_cmake_BUILD_SHA256_CHECKSUM "${CMAKE_MATCH_1}")
  if(NOT VELOX_rapids_cmake_COMMIT OR NOT VELOX_rapids_cmake_BUILD_SHA256_CHECKSUM)
    message(
      FATAL_ERROR
      "Could not read the rapids-cmake commit and checksum from Velox's cudf.cmake"
    )
  endif()
  set(
    VELOX_rapids_cmake_SOURCE_URL
    "https://github.com/rapidsai/rapids-cmake/archive/${VELOX_rapids_cmake_COMMIT}.tar.gz"
  )
  include(${CMAKE_SOURCE_DIR}/velox/CMake/ResolveDependency.cmake)
  velox_resolve_dependency_url(rapids_cmake)
  message(STATUS "rapids-cmake: ${VELOX_rapids_cmake_SOURCE_URL}")

  FetchContent_Declare(
    rapids-cmake
    URL ${VELOX_rapids_cmake_SOURCE_URL}
    URL_HASH ${VELOX_rapids_cmake_BUILD_SHA256_CHECKSUM}
    UPDATE_DISCONNECTED 1
  )
  FetchContent_MakeAvailable(rapids-cmake)
  include(rapids-cuda)
  rapids_cuda_init_architectures(PrestoCpp)
  # Enable CUDA in project() itself: rapids_cuda_set_architectures, which runs
  # from project(), returns early unless the CUDA compiler is already known.
  set(_cuda_lang CUDA)
endif()

# set the project name
project(PrestoCpp LANGUAGES C CXX ${_cuda_lang})

set(VELOX_ROOT ${CMAKE_BINARY_DIR}/velox)
list(PREPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/velox/CMake")

execute_process(
  COMMAND
    bash -c
    "( source ${CMAKE_CURRENT_SOURCE_DIR}/velox/scripts/setup-helper-functions.sh && echo -n $(get_cxx_flags $ENV{CPU_TARGET}))"
  OUTPUT_VARIABLE SCRIPT_CXX_FLAGS
  RESULT_VARIABLE COMMAND_STATUS
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
message("Appending CMAKE_CXX_FLAGS with ${SCRIPT_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS}")

# Known warnings that are benign can be disabled.
set(DISABLED_WARNINGS "-Wno-nullability-completeness -Wno-deprecated-declarations")

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  # GCC -O3 -Wstringop-overflow false positive in the velox dwio dictionary
  # visitor: bounds checks added by velox 0166f69b changed inlining in
  # DictionaryColumnVisitor::processRun, tripping the warning on std::copy in
  # SelectiveIntegerDictionaryColumnReader.cpp. Keep the warning visible but
  # do not fail the build on it. Scoped to GNU: Clang has no such warning
  # group and would flag the unknown option under -Werror.
  string(APPEND DISABLED_WARNINGS " -Wno-error=stringop-overflow")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "14.0.0")
    string(APPEND DISABLED_WARNINGS " -Wno-error=template-id-cdtor")
  endif()
elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "AppleClang")
  # Handle Apple toolchain Clang change of name of variable.
  # Remove when upgrading to the latest FBOS version where this has been addressed.
  add_compile_options(-D_LIBCPP_HAS_NO_ASAN)
endif()

# Important warnings that must be explicitly enabled.
set(ENABLE_WARNINGS "-Wreorder")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${DISABLED_WARNINGS} ${ENABLE_WARNINGS}")

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

# Turn on folly int128 support.
add_compile_definitions(FOLLY_HAVE_INT128_T=1)

set(VELOX_ENABLE_S3 ${PRESTO_ENABLE_S3} CACHE BOOL "Build S3 support")

set(VELOX_ENABLE_HDFS ${PRESTO_ENABLE_HDFS} CACHE BOOL "Build HDFS support")

set(VELOX_ENABLE_GCS ${PRESTO_ENABLE_GCS} CACHE BOOL "Build GCS support")

set(VELOX_ENABLE_ABFS ${PRESTO_ENABLE_ABFS} CACHE BOOL "Build ABFS support")

set(VELOX_ENABLE_PARQUET ${PRESTO_ENABLE_PARQUET} CACHE BOOL "Enable Parquet support")

set(
  VELOX_ENABLE_REMOTE_FUNCTIONS
  ${PRESTO_ENABLE_REMOTE_FUNCTIONS}
  CACHE BOOL
  "Enable remote function support in Velox"
)
if(PRESTO_ENABLE_REMOTE_FUNCTIONS)
  add_compile_definitions(PRESTO_ENABLE_REMOTE_FUNCTIONS)
endif()

set(VELOX_ENABLE_CUDF ${PRESTO_ENABLE_CUDF} CACHE BOOL "Enable cuDF support")
if(PRESTO_ENABLE_CUDF)
  add_compile_definitions(PRESTO_ENABLE_CUDF)
  # Determine CUDA_ARCHITECTURES automatically.
  cmake_policy(SET CMP0104 NEW)
endif()

set(
  VELOX_ENABLE_GEO
  ${PRESTO_ENABLE_SPATIAL}
  CACHE BOOL
  "Enable Velox Geometry (aka spatial) support"
)

set(VELOX_BUILD_TESTING OFF CACHE BOOL "Enable Velox tests")

set(VELOX_ENABLE_SPARK_FUNCTIONS OFF CACHE BOOL "Enable Velox Spark functions")

set(VELOX_ENABLE_EXAMPLES OFF CACHE BOOL "Enable Velox examples")

set(VELOX_BUILD_TEST_UTILS ${PRESTO_ENABLE_TESTING} CACHE BOOL "Enable Velox test utils")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(DEFINED ENV{INSTALL_PREFIX})
  message(STATUS "Dependency install directory set to: $ENV{INSTALL_PREFIX}")
  list(APPEND CMAKE_PREFIX_PATH "$ENV{INSTALL_PREFIX}")
endif()

set(Boost_USE_MULTITHREADED TRUE)
find_package(
  Boost
  1.66.0
  REQUIRED program_options context filesystem regex thread system date_time url atomic
)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})

find_package(gflags COMPONENTS shared)

find_library(GLOG glog)

# Must precede find_package(folly), which transitively imports fmt via
# folly-config.cmake's find_dependency(fmt). Use GLOBAL so the imported
# target's directory scope doesn't trip (GPU) rapids-cmake's make_global() when
# cudf is later configured under add_subdirectory(velox).
find_package(fmt CONFIG REQUIRED GLOBAL)

find_library(EVENT event)
# On macOS, explicitly find and add libevent include directory to handle
# the case where there is a missing dependency on libevent.
# See issue https://github.com/prestodb/presto/issues/27856
if(APPLE)
  find_path(EVENT_INCLUDE_DIR event.h)
  if(EVENT_INCLUDE_DIR)
    include_directories(SYSTEM ${EVENT_INCLUDE_DIR})
  endif()
endif()

find_library(DOUBLE_CONVERSION double-conversion)

find_library(LZ4 lz4)
find_library(LZO lzo2)
find_library(ZSTD zstd)
find_package(ZLIB)
find_library(SNAPPY snappy)

find_package(folly CONFIG REQUIRED)
set(
  FOLLY_WITH_DEPENDENCIES
  ${FOLLY_LIBRARIES}
  ${DOUBLE_CONVERSION}
  Boost::context
  dl
  ${EVENT}
  ${SNAPPY}
  ${LZ4}
  ${ZSTD}
  ${ZLIB_LIBRARIES}
)

find_package(BZip2 MODULE)
if(BZIP2_FOUND)
  list(APPEND FOLLY_WITH_DEPENDENCIES ${BZIP2_LIBRARIES})
endif()
include_directories(SYSTEM ${FOLLY_INCLUDE_DIRS})

find_package(OpenSSL REQUIRED)

find_package(Sodium REQUIRED)
find_library(PROXYGEN proxygen)
find_library(PROXYGEN_HTTPSERVER proxygen_httpserver)
find_library(FIZZ fizz)
find_library(WANGLE wangle)
find_library(MVFST mvfst)
find_library(RE2 re2)
find_package(fizz CONFIG)
find_package(wangle CONFIG)
find_package(FBThrift CONFIG REQUIRED)
include_directories(SYSTEM ${FBTHRIFT_INCLUDE_DIR})

set(
  PROXYGEN_LIBRARIES
  ${PROXYGEN_HTTPSERVER}
  ${PROXYGEN}
  ${WANGLE}
  ${FIZZ}
  ${MVFST}
)
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

set(VELOX_MONO_LIBRARY ON CACHE BOOL "Build Velox mono library")
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

find_package(DataSketches)
add_compile_definitions(PRESTO_ENABLE_THETA_SKETCH)
add_compile_definitions(PRESTO_ENABLE_KLL_SKETCH)

if("${MAX_LINK_JOBS}")
  set_property(GLOBAL APPEND PROPERTY JOB_POOLS "presto_link_job_pool=${MAX_LINK_JOBS}")
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
