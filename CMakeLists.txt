# Copyright (c) Facebook, Inc. and its affiliates.
#
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
cmake_minimum_required(VERSION 3.28)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

# The policy allows us to change options without caching.
cmake_policy(SET CMP0077 NEW)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

# Sets new behavior for CMP0135, which controls how timestamps are extracted
# when using ExternalProject_Add():
# https://cmake.org/cmake/help/latest/policy/CMP0135.html
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
  set(CMAKE_POLICY_DEFAULT_CMP0135 NEW)
endif()

# Set the project name.
project(velox)

# If we are in an active conda env disable search in system paths and add env to
# prefix path
if(DEFINED ENV{CONDA_PREFIX})
  if(NOT DEFINED ENV{VELOX_DEPENDENCY_SOURCE} OR "$ENV{VELOX_DEPENDENCY_SOURCE}"
                                                 STREQUAL "CONDA")
    message(STATUS "Using Conda environment: $ENV{CONDA_PREFIX}")
    set(CMAKE_FIND_USE_SYSTEM_ENVIRONMENT_PATH FALSE)
    list(APPEND CMAKE_PREFIX_PATH "$ENV{CONDA_PREFIX}")
    # Override in case it was set to CONDA
    set(ENV{VELOX_DEPENDENCY_SOURCE} AUTO)
  elseif(DEFINED ENV{VELOX_DEPENDENCY_SOURCE}
         AND NOT "$ENV{VELOX_DEPENDENCY_SOURCE}" STREQUAL "CONDA")
    message(STATUS "Overriding Conda environment: $ENV{CONDA_PREFIX}")
  endif()
endif()

list(PREPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/CMake"
     "${PROJECT_SOURCE_DIR}/CMake/third-party")

# Include our ThirdPartyToolchain dependencies macros
include(ResolveDependency)

set_with_default(VELOX_DEPENDENCY_SOURCE_DEFAULT VELOX_DEPENDENCY_SOURCE AUTO)
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# Add all options below
option(
  VELOX_BUILD_TESTING
  "Enable Velox tests. This will enable all other build options automatically."
  ON)
option(
  VELOX_BUILD_MINIMAL
  "Build a minimal set of components only. This will override other build options."
  OFF)
option(
  VELOX_BUILD_MINIMAL_WITH_DWIO
  "Build a minimal set of components, including DWIO (file format readers/writers).
   This will override other build options."
  OFF)

# option() always creates a BOOL variable so we have to use a normal cache
# variable with STRING type for this option.
#
# * AUTO: Try SYSTEM first fall back to BUNDLED.
# * SYSTEM: Use installed dependencies via find_package.
# * BUNDLED: Build dependencies from source.
set(VELOX_DEPENDENCY_SOURCE
    ${VELOX_DEPENDENCY_SOURCE_DEFAULT}
    CACHE
      STRING
      "Default source for all dependencies with source builds enabled: AUTO SYSTEM BUNDLED."
)
set(VELOX_GFLAGS_TYPE
    "shared"
    CACHE
      STRING
      "Specify whether to find the gflags package as a shared or static package"
)
option(VELOX_ENABLE_EXEC "Build exec." ON)
option(VELOX_ENABLE_AGGREGATES "Build aggregates." ON)
option(VELOX_ENABLE_HIVE_CONNECTOR "Build Hive connector." ON)
option(VELOX_ENABLE_TPCH_CONNECTOR "Build TPC-H connector." ON)
option(VELOX_ENABLE_PRESTO_FUNCTIONS "Build Presto SQL functions." ON)
option(VELOX_ENABLE_SPARK_FUNCTIONS "Build Spark SQL functions." ON)
option(VELOX_ENABLE_EXPRESSION "Build expression." ON)
option(VELOX_ENABLE_EXAMPLES
       "Build examples. This will enable VELOX_ENABLE_EXPRESSION automatically."
       OFF)
option(VELOX_ENABLE_SUBSTRAIT "Build Substrait-to-Velox converter." OFF)
option(VELOX_ENABLE_BENCHMARKS "Enable Velox top level benchmarks." OFF)
option(VELOX_ENABLE_BENCHMARKS_BASIC "Enable Velox basic benchmarks." OFF)
option(VELOX_ENABLE_S3 "Build S3 Connector" OFF)
option(VELOX_ENABLE_GCS "Build GCS Connector" OFF)
option(VELOX_ENABLE_ABFS "Build Abfs Connector" OFF)
option(VELOX_ENABLE_HDFS "Build Hdfs Connector" OFF)
option(VELOX_ENABLE_PARQUET "Enable Parquet support" OFF)
option(VELOX_ENABLE_ARROW "Enable Arrow support" OFF)
option(VELOX_ENABLE_REMOTE_FUNCTIONS "Enable remote function support" OFF)
option(VELOX_ENABLE_CCACHE "Use ccache if installed." ON)

option(VELOX_BUILD_TEST_UTILS "Builds Velox test utilities" OFF)
option(VELOX_BUILD_VECTOR_TEST_UTILS "Builds Velox vector test utilities" OFF)
option(VELOX_BUILD_PYTHON_PACKAGE "Builds Velox Python bindings" OFF)
option(
  VELOX_ENABLE_INT64_BUILD_PARTITION_BOUND
  "make buildPartitionBounds_ a vector int64 instead of int32 to avoid integer overflow when the hashtable has billions of records"
  OFF)

# Explicitly force compilers to generate colored output. Compilers usually do
# this by default if they detect the output is a terminal, but this assumption
# is broken if you use ninja.
option(VELOX_FORCE_COLORED_OUTPUT
       "Always produce ANSI-colored output (GNU/Clang only)." OFF)

if(${VELOX_BUILD_MINIMAL} OR ${VELOX_BUILD_MINIMAL_WITH_DWIO})
  # Enable and disable components for velox base build
  set(VELOX_BUILD_TESTING OFF)
  set(VELOX_ENABLE_PRESTO_FUNCTIONS ON)
  set(VELOX_ENABLE_EXPRESSION ON)
  set(VELOX_ENABLE_EXEC OFF)
  set(VELOX_ENABLE_AGGREGATES OFF)
  set(VELOX_ENABLE_HIVE_CONNECTOR OFF)
  set(VELOX_ENABLE_TPCH_CONNECTOR OFF)
  set(VELOX_ENABLE_SPARK_FUNCTIONS OFF)
  set(VELOX_ENABLE_EXAMPLES OFF)
  set(VELOX_ENABLE_S3 OFF)
  set(VELOX_ENABLE_GCS OFF)
  set(VELOX_ENABLE_ABFS OFF)
  set(VELOX_ENABLE_SUBSTRAIT OFF)
endif()

if(${VELOX_BUILD_TESTING})
  # Enable all components to build testing binaries
  set(VELOX_ENABLE_PRESTO_FUNCTIONS ON)
  set(VELOX_ENABLE_EXPRESSION ON)
  set(VELOX_ENABLE_EXEC ON)
  set(VELOX_ENABLE_AGGREGATES ON)
  set(VELOX_ENABLE_HIVE_CONNECTOR ON)
  set(VELOX_ENABLE_TPCH_CONNECTOR ON)
  set(VELOX_ENABLE_SPARK_FUNCTIONS ON)
  set(VELOX_ENABLE_EXAMPLES ON)
endif()

if(${VELOX_ENABLE_BENCHMARKS})
  set(VELOX_ENABLE_BENCHMARKS_BASIC ON)
endif()

if(VELOX_ENABLE_BENCHMARKS_BASIC)
  set(VELOX_BUILD_TEST_UTILS ON)
endif()

if(VELOX_BUILD_TESTING OR VELOX_BUILD_TEST_UTILS)
  set(cpr_SOURCE BUNDLED)
  resolve_dependency(cpr)
  set(VELOX_ENABLE_DUCKDB ON)
  set(VELOX_ENABLE_PARSE ON)
endif()

if(${VELOX_ENABLE_EXAMPLES})
  set(VELOX_ENABLE_EXPRESSION ON)
endif()

if(${VELOX_BUILD_PYTHON_PACKAGE})
  set(VELOX_ENABLE_PRESTO_FUNCTIONS ON)
  set(VELOX_ENABLE_DUCKDB ON)
  set(VELOX_ENABLE_EXPRESSION ON)
  set(VELOX_ENABLE_PARSE ON)
  set(VELOX_ENABLE_EXEC ON)
  set(VELOX_ENABLE_AGGREGATES ON)
  set(VELOX_ENABLE_SPARK_FUNCTIONS ON)
endif()

# We look for OpenSSL here to cache the result enforce the version across our
# dependencies.
find_package(OpenSSL REQUIRED)

if(VELOX_ENABLE_CCACHE
   AND NOT CMAKE_C_COMPILER_LAUNCHER
   AND NOT CMAKE_CXX_COMPILER_LAUNCHER)

  find_program(CCACHE_FOUND ccache)

  if(CCACHE_FOUND)
    message(STATUS "Using ccache: ${CCACHE_FOUND}")
    set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE_FOUND})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_FOUND})
    # keep comments as they might matter to the compiler
    set(ENV{CCACHE_COMMENTS} "1")
  endif()
endif()

if(${VELOX_FORCE_COLORED_OUTPUT})
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    add_compile_options(-fdiagnostics-color=always)
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang"
         OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    add_compile_options(-fcolor-diagnostics)
  endif()
endif()

# At the moment we prefer static linking but by default cmake looks for shared
# libs first. This will still fallback to shared libs when static ones are not
# found
list(INSERT CMAKE_FIND_LIBRARY_SUFFIXES 0 a)
if(VELOX_ENABLE_S3)
  # Set AWS_ROOT_DIR if you have a custom install location of AWS SDK CPP.
  if(AWSSDK_ROOT_DIR)
    set(CMAKE_PREFIX_PATH ${AWSSDK_ROOT_DIR})
  endif()
  find_package(AWSSDK REQUIRED COMPONENTS s3;identity-management)
  add_definitions(-DVELOX_ENABLE_S3)
endif()

if(VELOX_ENABLE_ABFS)
  # Set AZURESDK_ROOT_DIR if you have a custom install location of Azure Storage
  # SDK CPP.
  if(AZURESDK_ROOT_DIR)
    list(APPEND CMAKE_PREFIX_PATH ${AZURESDK_ROOT_DIR})
  endif()
  # files-datalake is built on blobs
  find_package(azure-storage-files-datalake-cpp CONFIG REQUIRED)
  add_definitions(-DVELOX_ENABLE_ABFS)
endif()

if(VELOX_ENABLE_HDFS)
  find_library(
    LIBHDFS3
    NAMES libhdfs3.so libhdfs3.dylib
    HINTS "${CMAKE_SOURCE_DIR}/hawq/depends/libhdfs3/_build/src/" REQUIRED)
  add_definitions(-DVELOX_ENABLE_HDFS3)
endif()

if(VELOX_ENABLE_PARQUET)
  add_definitions(-DVELOX_ENABLE_PARQUET)
  # Native Parquet reader requires Apache Thrift and Arrow Parquet writer, which
  # are included in Arrow.
  set(VELOX_ENABLE_ARROW ON)
endif()

# make buildPartitionBounds_ a vector int64 instead of int32 to avoid integer
# overflow
if(${VELOX_ENABLE_INT64_BUILD_PARTITION_BOUND})
  add_compile_definitions(VELOX_ENABLE_INT64_BUILD_PARTITION_BOUND)
endif()

# MacOSX enables two-level namespace by default:
# http://mirror.informatimago.com/next/developer.apple.com/releasenotes/DeveloperTools/TwoLevelNamespaces.html
# Enables -flat_namespace so type_info can be deudplicated across .so boundaries
if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  add_link_options("-Wl,-flat_namespace")
endif()

if(UNIX AND NOT APPLE)
  # linker flags, -export-dynamic for rtti
  add_link_options("-Wl,-export-dynamic")
endif()

if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm64" AND ${CMAKE_SYSTEM_NAME} MATCHES
                                                 "Darwin")
  set(ON_APPLE_M1 True)
endif()

# Required so velox code can be used in a dynamic library
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

execute_process(
  COMMAND
    bash -c
    "( source ${CMAKE_CURRENT_SOURCE_DIR}/scripts/setup-helper-functions.sh && echo -n $(get_cxx_flags $ENV{CPU_TARGET}))"
  OUTPUT_VARIABLE SCRIPT_CXX_FLAGS
  RESULT_VARIABLE COMMAND_STATUS)

if(COMMAND_STATUS EQUAL "1")
  message(FATAL_ERROR "Unable to determine compiler flags!")
endif()
message("Setting CMAKE_CXX_FLAGS=${SCRIPT_CXX_FLAGS}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D USE_VELOX_COMMON_BASE")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D HAS_UNCAUGHT_EXCEPTIONS")
if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsigned-char")
endif()

# Under Ninja, we are able to designate certain targets large enough to require
# restricted parallelism.
if("${MAX_HIGH_MEM_JOBS}")
  set_property(GLOBAL PROPERTY JOB_POOLS
                               "high_memory_pool=${MAX_HIGH_MEM_JOBS}")
else()
  set_property(GLOBAL PROPERTY JOB_POOLS high_memory_pool=1000)
endif()

if("${MAX_LINK_JOBS}")
  set_property(GLOBAL APPEND PROPERTY JOB_POOLS
                                      "link_job_pool=${MAX_LINK_JOBS}")
  set(CMAKE_JOB_POOL_LINK link_job_pool)
endif()

if("${ENABLE_ALL_WARNINGS}")
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(KNOWN_COMPILER_SPECIFIC_WARNINGS
        "-Wno-range-loop-analysis \
         -Wno-mismatched-tags \
         -Wno-nullability-completeness")
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(KNOWN_COMPILER_SPECIFIC_WARNINGS
        "-Wno-implicit-fallthrough \
         -Wno-class-memaccess \
         -Wno-comment \
         -Wno-int-in-bool-context \
         -Wno-redundant-move \
         -Wno-array-bounds \
         -Wno-maybe-uninitialized \
         -Wno-unused-result \
         -Wno-format-overflow \
         -Wno-strict-aliasing")
  endif()

  set(KNOWN_WARNINGS
      "-Wno-unused \
       -Wno-unused-parameter \
       -Wno-sign-compare \
       -Wno-ignored-qualifiers \
       ${KNOWN_COMPILER_SPECIFIC_WARNINGS}")

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra ${KNOWN_WARNINGS}")
endif()

message("FINAL CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")

if(${VELOX_ENABLE_GPU})
  enable_language(CUDA)
  # Determine CUDA_ARCHITECTURES automatically.
  cmake_policy(SET CMP0104 NEW)
  if(NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
    # it will fail later in the build otherwise
    message(FATAL_ERROR "-DCMAKE_CUDA_ARCHITECTURES= must be set")
  endif()
  if(CMAKE_BUILD_TYPE MATCHES Debug)
    add_compile_options("$<$<COMPILE_LANGUAGE:CUDA>:-G>")
  endif()
  include_directories("${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES}")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(BOOST_INCLUDE_LIBRARIES
    atomic
    context
    date_time
    filesystem
    program_options
    regex
    system
    thread)

set_source(Boost)
resolve_dependency(Boost 1.77.0 COMPONENTS ${BOOST_INCLUDE_LIBRARIES})

# Range-v3 will be enable when the codegen code actually lands keeping it here
# for reference. find_package(range-v3)

set_source(gflags)
resolve_dependency(gflags COMPONENTS ${VELOX_GFLAGS_TYPE})
if(NOT TARGET gflags::gflags)
  # This is a bit convoluted, but we want to be able to use gflags::gflags as a
  # target even when velox is built as a subproject which uses
  # `find_package(gflags)` which does not create a globally imported target that
  # we can ALIAS.
  add_library(gflags_gflags INTERFACE)
  target_link_libraries(gflags_gflags INTERFACE gflags)
  add_library(gflags::gflags ALIAS gflags_gflags)
endif()

if(${gflags_SOURCE} STREQUAL "BUNDLED")
  # we force glog from source to avoid issues with a system version built
  # against another gflags version (which is likely)
  set(glog_SOURCE BUNDLED)
else()
  set(glog_SOURCE SYSTEM)
endif()
resolve_dependency(glog)

if(${VELOX_ENABLE_DUCKDB})
  set_source(DuckDB)
  resolve_dependency(DuckDB)
endif()

set_source(fmt)
resolve_dependency(fmt 9.0.0)

if(${VELOX_BUILD_MINIMAL_WITH_DWIO} OR ${VELOX_ENABLE_HIVE_CONNECTOR})
  # DWIO needs all sorts of stream compression libraries.
  #
  # TODO: make these optional and pluggable.
  find_package(ZLIB REQUIRED)
  find_package(lz4 REQUIRED)
  find_package(lzo2 REQUIRED)
  find_package(zstd REQUIRED)
  find_package(Snappy REQUIRED)
  if(NOT TARGET zstd::zstd)
    if(TARGET zstd::libzstd_static)
      set(ZSTD_TYPE static)
    else()
      set(ZSTD_TYPE shared)
    endif()
    add_library(zstd::zstd ALIAS zstd::libzstd_${ZSTD_TYPE})
  endif()
endif()

set_source(re2)
resolve_dependency(re2)

if(${VELOX_BUILD_PYTHON_PACKAGE})
  set_source(pybind11)
  resolve_dependency(pybind11 2.10.0)
  add_subdirectory(pyvelox)
endif()

# DWIO (ORC/DWRF) and Substrait depend on protobuf.
if(${VELOX_BUILD_MINIMAL_WITH_DWIO}
   OR ${VELOX_ENABLE_HIVE_CONNECTOR}
   OR ${VELOX_ENABLE_SUBSTRAIT}
   OR VELOX_ENABLE_GCS)

  # Locate or build protobuf.
  set_source(Protobuf)
  resolve_dependency(Protobuf 3.21.4 EXACT)
  include_directories(${Protobuf_INCLUDE_DIRS})
endif()

set_source(simdjson)
resolve_dependency(simdjson 3.9.3)

# Locate or build folly.
add_compile_definitions(FOLLY_HAVE_INT128_T=1)
set_source(folly)
resolve_dependency(folly)

if(VELOX_ENABLE_REMOTE_FUNCTIONS)
  # TODO: Move this to use resolve_dependency(). For some reason, FBThrift
  # requires clients to explicitly install fizz and wangle.
  find_package(fizz CONFIG REQUIRED)
  find_package(wangle CONFIG REQUIRED)
  find_package(FBThrift CONFIG REQUIRED)
endif()

if(DEFINED FOLLY_BENCHMARK_STATIC_LIB)
  set(FOLLY_BENCHMARK ${FOLLY_BENCHMARK_STATIC_LIB})
else()
  set(FOLLY_BENCHMARK Folly::follybenchmark)
endif()

if(VELOX_ENABLE_GCS)
  set_source(google_cloud_cpp_storage)
  resolve_dependency(google_cloud_cpp_storage 2.22.0 EXACT)
  add_definitions(-DVELOX_ENABLE_GCS)
endif()

# GCC needs to link a library to enable std::filesystem.
if("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  set(FILESYSTEM "stdc++fs")

  # Ensure we have gcc at least 9+.
  if(CMAKE_CXX_COMPILER_VERSION LESS 9.0)
    message(
      FATAL_ERROR "VELOX requires gcc > 9. Found ${CMAKE_CXX_COMPILER_VERSION}")
  endif()

  # Find Threads library
  find_package(Threads REQUIRED)
else()
  set(FILESYSTEM "")
endif()

if(VELOX_BUILD_TESTING AND NOT VELOX_ENABLE_DUCKDB)
  message(
    FATAL_ERROR
      "Unit tests require duckDB to be enabled (VELOX_ENABLE_DUCKDB=ON or VELOX_BUILD_TESTING=OFF)"
  )
endif()

set(VELOX_DISABLE_GOOGLETEST OFF)
if(NOT VELOX_BUILD_TEST_UTILS AND NOT VELOX_BUILD_TESTING)
  set(VELOX_DISABLE_GOOGLETEST ON)
  add_definitions(-DVELOX_DISABLE_GOOGLETEST)
endif()

# On macOS, search Homebrew for keg-only versions of Bison and Flex. Xcode does
# not provide new enough versions for us to use.
if(CMAKE_HOST_SYSTEM_NAME MATCHES "Darwin")
  execute_process(
    COMMAND brew --prefix bison
    RESULT_VARIABLE BREW_BISON
    OUTPUT_VARIABLE BREW_BISON_PREFIX
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(BREW_BISON EQUAL 0 AND EXISTS "${BREW_BISON_PREFIX}")
    message(
      STATUS "Found Bison keg installed by Homebrew at ${BREW_BISON_PREFIX}")
    set(BISON_EXECUTABLE "${BREW_BISON_PREFIX}/bin/bison")
  endif()

  execute_process(
    COMMAND brew --prefix flex
    RESULT_VARIABLE BREW_FLEX
    OUTPUT_VARIABLE BREW_FLEX_PREFIX
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(BREW_FLEX EQUAL 0 AND EXISTS "${BREW_FLEX_PREFIX}")
    message(
      STATUS "Found Flex keg installed by Homebrew at ${BREW_FLEX_PREFIX}")
    set(FLEX_EXECUTABLE "${BREW_FLEX_PREFIX}/bin/flex")
    set(FLEX_INCLUDE_DIR "${BREW_FLEX_PREFIX}/include")
  endif()
endif()
find_package(BISON 3.0.4 REQUIRED)
find_package(FLEX 2.5.13 REQUIRED)
find_package(double-conversion 3.1.5 REQUIRED)

include_directories(SYSTEM velox)
include_directories(SYSTEM velox/external)

# these were previously vendored in third-party/
if(NOT VELOX_DISABLE_GOOGLETEST)
  set(gtest_SOURCE AUTO)
  resolve_dependency(gtest)
  set(VELOX_GTEST_INCUDE_DIR
      "${gtest_SOURCE_DIR}/googletest/include"
      PARENT_SCOPE)
endif()

set_source(xsimd)
resolve_dependency(xsimd 10.0.0)

set(stemmer_SOURCE BUNDLED)
resolve_dependency(stemmer)

if(VELOX_BUILD_TESTING)
  set(BUILD_TESTING ON)
  include(CTest) # include after project() but before add_subdirectory()
endif()

include_directories(.)

# TODO: Include all other installation files. For now just making sure this
# generates an installable makefile.
install(FILES velox/type/Type.h DESTINATION "include/velox")

# Adding this down here prevents warnings in dependencies from stopping the
# build
if("${TREAT_WARNINGS_AS_ERRORS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()

add_subdirectory(third_party)
add_subdirectory(velox)
