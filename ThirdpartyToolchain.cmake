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

# MODULE:   ThirdpartyToolchain
#
# PROVIDES: resolve_dependency( DEPENDENCY_NAME dependencyName [REQUIRED_VERSION
# required version] ... )
#
# Provides the ability to resolve third party dependencies. If the dependency is
# already available in the system it will be used.
#
# The DEPENDENCY_NAME argument is required. The dependencyName value will be
# used to search for the installed dependencies Config file and thus this name
# should match find_package() standards.
#
# EXAMPLE USAGE: # Download and setup or use already installed dependency.
# include(ThirdpartyToolchain) resolve_dependency(folly)
#
# ========================================================================================

include(FetchContent)
include(CheckCXXCompilerFlag)

# =====================================FOLLY==============================================

if(DEFINED ENV{VELOX_FOLLY_URL})
  set(FOLLY_SOURCE_URL "$ENV{VELOX_FOLLY_URL}")
else()
  set(VELOX_FOLLY_BUILD_VERSION v2022.07.11.00)
  set(FOLLY_SOURCE_URL
      "https://github.com/facebook/folly/archive/${VELOX_FOLLY_BUILD_VERSION}.tar.gz"
  )
  set(VELOX_FOLLY_BUILD_SHA256_CHECKSUM
      b6cc4082afd1530fdb8d759bc3878c1ea8588f6d5bc9eddf8e1e8abe63f41735)
endif()

macro(build_folly)
  message(STATUS "Building Folly from source")
  # FOLLY_CXX_FLAGS is used internally on folly to define some extra
  # CMAKE_CXX_FLAGS for some known warnings to avoid possible errors on some
  # OS/archs
  set(EXTRA_CXX_FLAGS -Wno-deprecated-declarations)
  check_cxx_compiler_flag(-Wnullability-completeness
                          COMPILER_HAS_W_NULLABILITY_COMPLETENESS)
  if(COMPILER_HAS_W_NULLABILITY_COMPLETENESS)
    list(APPEND EXTRA_CXX_FLAGS -Wno-nullability-completeness)
  endif()
  check_cxx_compiler_flag(-Wstringop-overflow COMPILER_HAS_W_STRINGOP_OVERFLOW)
  if(COMPILER_HAS_W_STRINGOP_OVERFLOW)
    list(APPEND EXTRA_CXX_FLAGS -Wno-stringop-overflow)
  endif()
  check_cxx_compiler_flag(-Wundef-prefix COMPILER_HAS_W_UNDEF_PREFIX)
  if(COMPILER_HAS_W_UNDEF_PREFIX)
    list(APPEND EXTRA_CXX_FLAGS -Wno-undef-prefix)
  endif()
  set(FOLLY_CXX_FLAGS -Wno-unused -Wno-unused-parameter -Wno-overloaded-virtual
                      ${EXTRA_CXX_FLAGS})
  FetchContent_Declare(
    folly
    URL ${FOLLY_SOURCE_URL}
    URL_HASH SHA256=${VELOX_FOLLY_BUILD_SHA256_CHECKSUM})
  if(NOT folly_POPULATED)
    # Fetch the content using previously declared details
    FetchContent_Populate(folly)
    add_subdirectory(${folly_SOURCE_DIR} ${folly_BINARY_DIR})
    # Avoid possible errors for known warnings
    target_compile_options(folly PUBLIC ${EXTRA_CXX_FLAGS})
  endif()
  set(FOLLY_BENCHMARK_STATIC_LIB
      ${folly_BINARY_DIR}/folly/libfollybenchmark${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(FOLLY_LIBRARIES folly)
endmacro()
# ===============================END FOLLY================================

macro(build_dependency DEPENDENCY_NAME)
  if("${DEPENDENCY_NAME}" STREQUAL "folly")
    build_folly()
  else()
    message(
      FATAL_ERROR "Unknown thirdparty dependency to build: ${DEPENDENCY_NAME}")
  endif()
endmacro()

# * Macro to resolve thirparty dependencies.
#
# Provides the macro resolve_dependency(). This macro will allow us to find the
# dependency via the usage of find_package or use the custom
# build_dependency(DEPENDENCY_NAME) macro to download and build the third party
# dependency.
#
# resolve_dependency(DEPENDENCY_NAME [REQUIRED_VERSION <required_version>] )
#
# The resolve_dependency() macro can be used to define a thirdparty dependency.
#
# ${DEPENDENCY_NAME}_SOURCE is expected to be set to either AUTO, SYSTEM or
# BUNDLED. If ${DEPENDENCY_NAME}_SOURCE is SYSTEM it will try to find the
# corresponding package via find_package and if not found it will call the
# build_dependency macro to download and build the third party dependency. If
# ${DEPENDENCY_NAME}_SOURCE is SYSTEM it will force to find via find_package. If
# ${DEPENDENCY_NAME}_SOURCE is BUNDLED it will force to build from source.
#
# If REQUIRED_VERSION is provided it will be used as the VERSION to be used on
# the find_package(DEPENDENCY_NAME [version]) call. In the case of setting
# ${DEPENDENCY_NAME}_SOURCE to SYSTEM if the dependency is not found the build
# will fail and will not fall back to download and build from source.
macro(resolve_dependency DEPENDENCY_NAME)
  set(options)
  set(one_value_args REQUIRED_VERSION)
  set(multi_value_args)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}"
                        "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(
      SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()
  set(PACKAGE_NAME ${DEPENDENCY_NAME})
  set(FIND_PACKAGE_ARGUMENTS ${PACKAGE_NAME})
  if(ARG_REQUIRED_VERSION)
    list(APPEND FIND_PACKAGE_ARGUMENTS ${ARG_REQUIRED_VERSION})
  endif()
  if(${DEPENDENCY_NAME}_SOURCE STREQUAL "AUTO")
    find_package(${FIND_PACKAGE_ARGUMENTS} QUIET)
    if(${${PACKAGE_NAME}_FOUND})
      set(${DEPENDENCY_NAME}_SOURCE "SYSTEM")
    else()
      build_dependency(${DEPENDENCY_NAME})
      set(${DEPENDENCY_NAME}_SOURCE "BUNDLED")
    endif()
  elseif(${DEPENDENCY_NAME}_SOURCE STREQUAL "SYSTEM")
    find_package(${FIND_PACKAGE_ARGUMENTS} REQUIRED)
  elseif(${DEPENDENCY_NAME}_SOURCE STREQUAL "BUNDLED")
    build_dependency(${DEPENDENCY_NAME})
  endif()
endmacro()
