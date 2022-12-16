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
# ==============================================================================

include(FetchContent)
include(ExternalProject)
include(ProcessorCount)
include(CheckCXXCompilerFlag)

# Enable SSL certificate verification for file downloads
set(CMAKE_TLS_VERIFY true)

# ================================ FOLLY =======================================

if(DEFINED ENV{VELOX_FOLLY_URL})
  set(FOLLY_SOURCE_URL "$ENV{VELOX_FOLLY_URL}")
else()
  set(VELOX_FOLLY_BUILD_VERSION v2022.11.14.00)
  set(FOLLY_SOURCE_URL
      "https://github.com/facebook/folly/archive/${VELOX_FOLLY_BUILD_VERSION}.tar.gz"
  )
  set(VELOX_FOLLY_BUILD_SHA256_CHECKSUM
      b249436cb61b6dfd5288093565438d8da642b07ae021191a4042b221bc1bdc0e)
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
# ================================= END FOLLY ==================================

# ================================== PROTOBUF ==================================

if(DEFINED ENV{VELOX_PROTOBUF_URL})
  set(PROTOBUF_SOURCE_URL "$ENV{VELOX_PROTOBUF_URL}")
else()
  set(VELOX_PROTOBUF_BUILD_VERSION 21.4)
  string(
    CONCAT
      PROTOBUF_SOURCE_URL
      "https://github.com/protocolbuffers/protobuf/releases/download/"
      "v${VELOX_PROTOBUF_BUILD_VERSION}/protobuf-all-${VELOX_PROTOBUF_BUILD_VERSION}.tar.gz"
  )
  set(VELOX_PROTOBUF_BUILD_SHA256_CHECKSUM
      6c5e1b0788afba4569aeebb2cfe205cb154aa01deacaba0cd26442f3b761a836)
endif()

macro(build_protobuf)
  message(STATUS "Building Protobuf from source")

  FetchContent_Declare(
    protobuf
    URL ${PROTOBUF_SOURCE_URL}
    URL_HASH SHA256=${VELOX_PROTOBUF_BUILD_SHA256_CHECKSUM})

  if(NOT protobuf_POPULATED)
    # We don't want to build tests.
    set(protobuf_BUILD_TESTS
        OFF
        CACHE BOOL "Disable protobuf tests" FORCE)
    set(CMAKE_CXX_FLAGS_BKP "${CMAKE_CXX_FLAGS}")

    # Disable warnings that would fail protobuf compilation.
    string(APPEND CMAKE_CXX_FLAGS " -Wno-missing-field-initializers")

    check_cxx_compiler_flag("-Wstringop-overflow"
                            COMPILER_HAS_W_STRINGOP_OVERFLOW)
    if(COMPILER_HAS_W_STRINGOP_OVERFLOW)
      string(APPEND CMAKE_CXX_FLAGS " -Wno-stringop-overflow")
    endif()

    check_cxx_compiler_flag("-Winvalid-noreturn"
                            COMPILER_HAS_W_INVALID_NORETURN)

    if(COMPILER_HAS_W_INVALID_NORETURN)
      string(APPEND CMAKE_CXX_FLAGS " -Wno-invalid-noreturn")
    else()
      # Currently reproduced on Ubuntu 22.04 with clang 14
      string(APPEND CMAKE_CXX_FLAGS " -Wno-error")
    endif()

    # Fetch the content using previously declared details
    FetchContent_Populate(protobuf)

    # Set right path to libprotobuf-dev include files.
    set(Protobuf_INCLUDE_DIR "${protobuf_SOURCE_DIR}/src/")
    set(Protobuf_PROTOC_EXECUTABLE "${protobuf_BINARY_DIR}/protoc")
    if(CMAKE_BUILD_TYPE MATCHES Debug)
      set(Protobuf_LIBRARIES "${protobuf_BINARY_DIR}/libprotobufd.a")
    else()
      set(Protobuf_LIBRARIES "${protobuf_BINARY_DIR}/libprotobuf.a")
    endif()
    include_directories("${protobuf_SOURCE_DIR}/src/")
    add_subdirectory(${protobuf_SOURCE_DIR} ${protobuf_BINARY_DIR})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_BKP}")
  endif()
endmacro()
# ================================ END PROTOBUF ================================

# ================================== PYBIND11 ==================================
if(DEFINED ENV{VELOX_PYBIND11_URL})
  set(PYBIND11_SOURCE_URL "$ENV{VELOX_PYBIND11_URL}")
else()
  set(VELOX_PYBIND11_BUILD_VERSION 2.10.0)
  string(CONCAT PYBIND11_SOURCE_URL
                "https://github.com/pybind/pybind11/archive/refs/tags/"
                "v${VELOX_PYBIND11_BUILD_VERSION}.tar.gz")
  set(VELOX_PYBIND11_BUILD_SHA256_CHECKSUM
      eacf582fa8f696227988d08cfc46121770823839fe9e301a20fbce67e7cd70ec)
endif()

macro(build_pybind11)
  message(STATUS "Building Pybind11 from source")

  FetchContent_Declare(
    pybind11
    URL ${PYBIND11_SOURCE_URL}
    URL_HASH SHA256=${VELOX_PYBIND11_BUILD_SHA256_CHECKSUM})

  if(NOT pybind11_POPULATED)

    # Fetch the content using previously declared details
    FetchContent_Populate(pybind11)

    add_subdirectory(${pybind11_SOURCE_DIR})
  endif()
endmacro()

# ================================ END PYBIND11 ================================
# ================================ FMT =========================================
if(DEFINED ENV{VELOX_FMT_URL})
  set(VELOX_FMT_SOURCE_URL "$ENV{VELOX_FMT_URL}")
else()
  set(VELOX_FMT_VERSION 8.0.1)
  set(VELOX_FMT_SOURCE_URL
      "https://github.com/fmtlib/fmt/archive/${VELOX_FMT_VERSION}.tar.gz")
  set(VELOX_FMT_BUILD_SHA256_CHECKSUM
      b06ca3130158c625848f3fb7418f235155a4d389b2abc3a6245fb01cb0eb1e01)
endif()

macro(build_fmt)
  message(STATUS "Building fmt from source")
  FetchContent_Declare(
    fmt
    URL ${VELOX_FMT_SOURCE_URL}
    URL_HASH SHA256=${VELOX_FMT_BUILD_SHA256_CHECKSUM})

  # Force fmt to create fmt-config.cmake which can be found by other dependecies
  # (e.g. folly)
  set(FMT_INSTALL ON)
  set(fmt_BUILD_TESTS OFF)
  FetchContent_MakeAvailable(fmt)
endmacro()
# ================================ END FMT ================================

# ================================== ICU4C ==================================
if(DEFINED ENV{VELOX_ICU4C_URL})
  set(ICU4C_SOURCE_URL "$ENV{VELOX_ICU4C_URL}")
else()
  set(VELOX_ICU4C_BUILD_VERSION 72)
  string(
    CONCAT ICU4C_SOURCE_URL
           "https://github.com/unicode-org/icu/releases/download/"
           "release-${VELOX_ICU4C_BUILD_VERSION}-1/"
           "icu4c-${VELOX_ICU4C_BUILD_VERSION}_1-src.tgz")

  set(VELOX_ICU4C_BUILD_SHA256_CHECKSUM
      a2d2d38217092a7ed56635e34467f92f976b370e20182ad325edea6681a71d68)
endif()

macro(build_icu4c)

  message(STATUS "Building ICU4C from source")

  ProcessorCount(NUM_JOBS)
  set_with_default(NUM_JOBS NUM_THREADS ${NUM_JOBS})
  find_program(MAKE_PROGRAM make)

  set(ICU_CFG --disable-tests --disable-samples)
  set(HOST_ENV_CMAKE
      ${CMAKE_COMMAND}
      -E
      env
      CC="${CMAKE_C_COMPILER}"
      CXX="${CMAKE_CXX_COMPILER}"
      CFLAGS="${CMAKE_C_FLAGS}"
      CXXFLAGS="${CMAKE_CXX_FLAGS}"
      LDFLAGS="${CMAKE_MODULE_LINKER_FLAGS}")
  set(ICU_DIR ${CMAKE_CURRENT_BINARY_DIR}/icu)
  set(ICU_INCLUDE_DIRS ${ICU_DIR}/include)
  set(ICU_LIBRARIES ${ICU_DIR}/lib)

  # We can not use FetchContent as ICU does not use cmake
  ExternalProject_Add(
    ICU
    URL ${ICU4C_SOURCE_URL}
    URL_HASH SHA256=${VELOX_ICU4C_BUILD_SHA256_CHECKSUM}
    SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR}/icu-src
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/icu-bld
    CONFIGURE_COMMAND <SOURCE_DIR>/source/configure --prefix=${ICU_DIR}
                      --libdir=${ICU_LIBRARIES} ${ICU_CFG}
    BUILD_COMMAND ${MAKE_PROGRAM} -j ${NUM_JOBS}
    INSTALL_COMMAND ${HOST_ENV_CMAKE} ${MAKE_PROGRAM} install)
endmacro()

# ================================ END ICU4C ================================

macro(build_dependency DEPENDENCY_NAME)
  if("${DEPENDENCY_NAME}" STREQUAL "folly")
    build_folly()
  elseif("${DEPENDENCY_NAME}" STREQUAL "Protobuf")
    build_protobuf()
  elseif("${DEPENDENCY_NAME}" STREQUAL "pybind11")
    build_pybind11()
  elseif("${DEPENDENCY_NAME}" STREQUAL "fmt")
    build_fmt()
  elseif("${DEPENDENCY_NAME}" STREQUAL "ICU")
    build_icu4c()
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
  else()
    message(
      FATAL_ERROR
        "Invalid source for ${DEPENDENCY_NAME}: ${${DEPENDENCY_NAME}_SOURCE}")
  endif()
endmacro()

# By using a macro we don't need to propagate the value into the parent scope.
macro(set_source DEPENDENCY_NAME)
  set_with_default(${DEPENDENCY_NAME}_SOURCE ${DEPENDENCY_NAME}_SOURCE
                   ${VELOX_DEPENDENCY_SOURCE})
  message(
    STATUS "Setting ${DEPENDENCY_NAME} source to ${${DEPENDENCY_NAME}_SOURCE}")
endmacro()

# Set a variable to the value of $ENV{envvar_name} if defined, set to ${DEFAULT}
# if not defined. If called from within a nested scope the variable will not
# propagate into outer scopes automatically! Use PARENT_SCOPE.
function(set_with_default var_name envvar_name default)
  if(DEFINED ENV{${envvar_name}})
    set(${var_name}
        $ENV{${envvar_name}}
        PARENT_SCOPE)
  else()
    set(${var_name}
        ${default}
        PARENT_SCOPE)
  endif()
endfunction()
