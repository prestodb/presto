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
# PROVIDES: resolve_dependency( dependency_name dependencyName [...] )
#
# Provides the ability to resolve third party dependencies. If the dependency is
# already available in the system it will be used.
#
# The dependency_name argument is required. The dependencyName value will be
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
    add_library(Folly::folly ALIAS folly)
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
    set(Protobuf_INCLUDE_DIRS "${protobuf_SOURCE_DIR}/src/")
    set(Protobuf_PROTOC_EXECUTABLE "${protobuf_BINARY_DIR}/protoc")
    if(CMAKE_BUILD_TYPE MATCHES Debug)
      set(Protobuf_LIBRARIES "${protobuf_BINARY_DIR}/libprotobufd.a")
    else()
      set(Protobuf_LIBRARIES "${protobuf_BINARY_DIR}/libprotobuf.a")
    endif()
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

  # We have to keep the FindICU.cmake in a subfolder to prevent it from
  # overriding the system provided one when ICU_SOURCE=SYSTEM
  list(PREPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/CMake/icu)
endmacro()

# ================================ END ICU4C ================================

# ================================== BOOST ==================================
if(DEFINED ENV{VELOX_BOOST_URL})
  set(BOOST_SOURCE_URL "$ENV{VELOX_BOOST_URL}")
else()
  # We need to use boost > 1.70 to build it with CMake
  set(VELOX_BOOST_BUILD_VERSION 1.80.0)
  string(REPLACE "." "_" VELOX_BOOST_UNDERSCORE_VERSION
                 ${VELOX_BOOST_BUILD_VERSION})
  string(
    CONCAT BOOST_SOURCE_URL
           "https://boostorg.jfrog.io/artifactory/main/release/"
           "${VELOX_BOOST_BUILD_VERSION}/source/boost_"
           "${VELOX_BOOST_UNDERSCORE_VERSION}.tar.gz")
  set(VELOX_BOOST_BUILD_SHA256_CHECKSUM
      4b2136f98bdd1f5857f1c3dea9ac2018effe65286cf251534b6ae20cc45e1847)
endif()

macro(build_boost)
  message(STATUS "Building boost from source")

  # required for Boost::thread
  find_package(Threads)

  # Make download progress visible
  set(FETCHCONTENT_QUIET OFF)

  FetchContent_Declare(
    Boost
    GIT_REPOSITORY https://github.com/boostorg/boost.git
    GIT_TAG boost-1.80.0
    GIT_SHALLOW TRUE)
  FetchContent_Populate(Boost)

  # Boost cmake uses the global option
  set(BUILD_SHARED_LIBS ON)
  add_subdirectory(${boost_SOURCE_DIR} ${boost_BINARY_DIR})
  unset(BUILD_SHARED_LIBS)

  # Manually construct include dirs. This is only necessary until we switch to
  # properly using targets.
  list_subdirs(boost_INCLUDE_DIRS ${boost_SOURCE_DIR}/libs)
  list(TRANSFORM boost_INCLUDE_DIRS APPEND /include)

  # numeric contains subdirs with their own include dir
  list_subdirs(numeric_subdirs ${boost_SOURCE_DIR}/libs/numeric)
  list(TRANSFORM numeric_subdirs APPEND /include)
  include_directories(${boost_INCLUDE_DIRS} ${numeric_subdirs})

  # This prevents system boost from leaking in
  set(Boost_NO_SYSTEM_PATHS ON)
  # We have to keep the FindBoost.cmake in an subfolder to prevent it from
  # overriding the system provided one when Boost_SOURCE=SYSTEM
  list(PREPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/CMake/boost)
  set(FETCHCONTENT_QUIET ON)
endmacro()

# ================================ END BOOST ================================

macro(build_dependency dependency_name)
  if("${dependency_name}" STREQUAL "folly")
    build_folly()
  elseif("${dependency_name}" STREQUAL "Protobuf")
    build_protobuf()
  elseif("${dependency_name}" STREQUAL "pybind11")
    build_pybind11()
  elseif("${dependency_name}" STREQUAL "fmt")
    build_fmt()
  elseif("${dependency_name}" STREQUAL "ICU")
    build_icu4c()
  elseif("${dependency_name}" STREQUAL "Boost")
    build_boost()
  else()
    message(
      FATAL_ERROR "Unknown thirdparty dependency to build: ${dependency_name}")
  endif()
endmacro()

# * Macro to resolve thirparty dependencies.
#
# Provides the macro resolve_dependency(). This macro will allow us to find the
# dependency via the usage of find_package or use the custom
# build_dependency(dependency_name) macro to download and build the third party
# dependency.
#
# resolve_dependency(dependency_name [...] )
#
# The resolve_dependency() macro can be used to define a thirdparty dependency.
#
# [...]: the macro will pass all arguments after DELPENDENCY_NAME on to
# find_package. ${dependency_name}_SOURCE is expected to be set to either AUTO,
# SYSTEM or BUNDLED. If ${dependency_name}_SOURCE is SYSTEM it will try to find
# the corresponding package via find_package and if not found it will call the
# build_dependency macro to download and build the third party dependency. If
# ${dependency_name}_SOURCE is SYSTEM it will force to find via find_package. If
# ${dependency_name}_SOURCE is BUNDLED it will force to build from source.
#
# In the case of setting ${dependency_name}_SOURCE to SYSTEM if the dependency
# is not found the build will fail and will not fall back to download and build
# from source.
macro(resolve_dependency dependency_name)
  set(find_package_args ${dependency_name} ${ARGN})
  list(REMOVE_ITEM find_package_args REQUIRED QUIET)
  if(${dependency_name}_SOURCE STREQUAL "AUTO")
    find_package(${find_package_args} QUIET)
    if(${${dependency_name}_FOUND})
      set(${dependency_name}_SOURCE "SYSTEM")
    else()
      set(${dependency_name}_SOURCE "BUNDLED")
      build_dependency(${dependency_name})
    endif()
    message(STATUS "Using ${${dependency_name}_SOURCE} ${dependency_name}")
  elseif(${dependency_name}_SOURCE STREQUAL "SYSTEM")
    find_package(${find_package_args} REQUIRED)
  elseif(${dependency_name}_SOURCE STREQUAL "BUNDLED")
    build_dependency(${dependency_name})
  else()
    message(
      FATAL_ERROR
        "Invalid source for ${dependency_name}: ${${dependency_name}_SOURCE}")
  endif()
endmacro()

# By using a macro we don't need to propagate the value into the parent scope.
macro(set_source dependency_name)
  set_with_default(${dependency_name}_SOURCE ${dependency_name}_SOURCE
                   ${VELOX_DEPENDENCY_SOURCE})
  message(
    STATUS "Setting ${dependency_name} source to ${${dependency_name}_SOURCE}")
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

# List subdirectories of ${dir}
function(list_subdirs var dir)
  if(NOT IS_DIRECTORY ${dir})
    message(FATAL_ERROR "${dir} is not a directory!")
  endif()

  # finds files & dirs
  file(GLOB children ${dir}/*)
  set(dirs "")

  foreach(child ${children})
    if(IS_DIRECTORY ${child})
      list(APPEND dirs ${child})
    endif()
  endforeach()

  set(${var}
      ${dirs}
      PARENT_SCOPE)
endfunction()
