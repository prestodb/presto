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
#
# Copyright (c) 2024 by Rivos Inc.
# Licensed under the Apache License, Version 2.0, see LICENSE for details.
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.28)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

cmake_policy(SET CMP0116 NEW)

# Sets new behavior for CMP0135, which controls how timestamps are extracted
# when using ExternalProject_Add():
# https://cmake.org/cmake/help/latest/policy/CMP0135.html
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
  set(CMAKE_POLICY_DEFAULT_CMP0135 NEW)
endif()

project(breeze)

option(BUILD_TESTING "Build testing." ON)
option(BUILD_CUDA "Build CUDA tests." OFF)
option(BUILD_HIP "Build HIP tests." OFF)
option(BUILD_SYCL "Build SYCL tests." OFF)
option(BUILD_OPENCL "Build OpenCL tests." OFF)
option(BUILD_OPENMP "Build OpenMP tests." OFF)
option(BUILD_METAL "Build Metal tests." OFF)
option(BUILD_TRACING "Build tracing." ON)
option(BUILD_GENERATE_TEST_FIXTURES "Generate test fixtures at build time." ON)

if(NOT DEFINED PERFTEST_EXT_TYPES)
  set(PERFTEST_EXT_TYPES 0 CACHE STRING "Extended test types for perf tests")
endif()

if(NOT DEFINED VELOX_PROJECT_SOURCE_DIR)
  set(VELOX_PROJECT_SOURCE_DIR "${PROJECT_SOURCE_DIR}/../../..")
endif()

list(
  PREPEND
  CMAKE_MODULE_PATH
  "${PROJECT_SOURCE_DIR}/cmake"
  "${VELOX_PROJECT_SOURCE_DIR}/CMake"
  "${VELOX_PROJECT_SOURCE_DIR}/CMake/third-party"
)

# Include Velox ThirdPartyToolchain dependencies macros
include(ResolveDependency)

set(OPT_FLAGS "-O0;-g")
set(BREEZE_BUILD_TYPE "${CMAKE_BUILD_TYPE}")
if(CMAKE_BUILD_TYPE MATCHES Release)
  set(NDEBUG_DEFINE "-DNDEBUG")
  set(OPT_FLAGS "-O2")
  set(LIB_OPT_FLAGS "-O2")
endif()
if(CMAKE_BUILD_TYPE MATCHES RelWithDebInfo)
  set(OPT_FLAGS "-O2;-g")
  # override build type for debug info to be limited to code built with
  # OPT_FLAGS
  set(CMAKE_BUILD_TYPE "Release")
endif()
if(CMAKE_BUILD_TYPE MATCHES Asan)
  set(OPT_FLAGS "-O1;-g")
  set(SANITIZE_COMPILE_FLAGS "-fsanitize=address;-fno-omit-frame-pointer")
  set(SANITIZE_LINK_FLAGS "-fsanitize=address;-fno-omit-frame-pointer")
endif()
if(CMAKE_BUILD_TYPE MATCHES Ubsan)
  set(OPT_FLAGS "-O1;-g")
  set(SANITIZE_COMPILE_FLAGS "-fsanitize=undefined;-fno-omit-frame-pointer")
  set(SANITIZE_LINK_FLAGS "-fsanitize=undefined;-fno-omit-frame-pointer")
endif()

set(WARN_FLAGS "-Wall;-Wextra;-Werror")
set(COMPILER_WARN_FLAGS "-Xcompiler=,-Wall,-Wextra,-Werror")

if(BUILD_CUDA)
  include(cuda)
endif()

if(BUILD_HIP)
  include(hip)
endif()

if(BUILD_SYCL)
  include(sycl)
endif()

if(BUILD_OPENCL)
  include(opencl)
endif()

if(BUILD_OPENMP)
  include(openmp)
endif()

if(BUILD_METAL)
  include(metal)
endif()

if(BUILD_TRACING)
  find_package(Threads REQUIRED)

  # static library for perfetto
  include_directories(external/perfetto/sdk)
  add_library(perfetto STATIC ../../external/perfetto/sdk/perfetto.cc)
  target_compile_features(perfetto PRIVATE cxx_std_17)
  target_compile_options(perfetto PRIVATE -fPIC)
  target_include_directories(perfetto INTERFACE ../../external/perfetto/sdk)
  target_compile_definitions(perfetto INTERFACE TRACING=1)
endif()

if(NOT DISABLE_GOOGLETEST)
  set(GTest_SOURCE AUTO)
  velox_resolve_dependency(GTest)
endif()

if(BUILD_TESTING)
  include(CTest)
  include(GoogleTest)
  add_subdirectory(test)
  add_subdirectory(perftest)
endif()

message("")
message("Platforms:")
message("  CUDA:   ${BUILD_CUDA}")
message("  HIP:    ${BUILD_HIP}")
message("  SYCL:   ${BUILD_SYCL}")
message("  OpenCL: ${BUILD_OPENCL}")
message("  OpenMP: ${BUILD_OPENMP}")
message("  Metal:  ${BUILD_METAL}")
message("")
message("Features:")
message("  Tracing: ${BUILD_TRACING}")
message("")
