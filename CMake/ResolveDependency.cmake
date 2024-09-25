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
list(APPEND CMAKE_MODULE_PATH
     ${CMAKE_CURRENT_LIST_DIR}/resolve_dependency_modules)

# Enable SSL certificate verification for file downloads
set(CMAKE_TLS_VERIFY true)

macro(build_dependency dependency_name)
  string(TOLOWER ${dependency_name} dependency_name_lower)
  include(${dependency_name_lower})
endmacro()

# * Macro to resolve third-party dependencies.
#
# Provides the macro resolve_dependency(). This macro will allow us to find the
# dependency via the usage of find_package or use the custom
# build_dependency(dependency_name) macro to download and build the third party
# dependency.
#
# resolve_dependency(dependency_name [...] )
#
# [...]: the macro will pass all arguments after DEPENDENCY_NAME on to
# find_package. ${dependency_name}_SOURCE is expected to be set to either AUTO,
# SYSTEM or BUNDLED. If ${dependency_name}_SOURCE is AUTO it will try to find
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
    find_package(${find_package_args})
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

# Set var_name to the value of $ENV{envvar_name} if ENV is defined. If neither
# ENV or var_name is defined then set var_name to ${DEFAULT}. If called from
# within a nested scope the variable will not propagate into outer scopes
# automatically! Use PARENT_SCOPE.
function(set_with_default var_name envvar_name default)
  if(DEFINED ENV{${envvar_name}})
    set(${var_name}
        $ENV{${envvar_name}}
        PARENT_SCOPE)
  elseif(NOT DEFINED ${var_name})
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

# Set custom source url with a optional sha256 checksum.
macro(resolve_dependency_url dependency_name)
  # Prepend prefix for default checksum.
  string(PREPEND VELOX_${dependency_name}_BUILD_SHA256_CHECKSUM "SHA256=")

  set_with_default(
    VELOX_${dependency_name}_SOURCE_URL VELOX_${dependency_name}_URL
    ${VELOX_${dependency_name}_SOURCE_URL})
  message(VERBOSE "Set VELOX_${dependency_name}_SOURCE_URL to "
          "${VELOX_${dependency_name}_SOURCE_URL}")
  if(DEFINED ENV{VELOX_${dependency_name}_URL})
    set_with_default(VELOX_${dependency_name}_BUILD_SHA256_CHECKSUM
                     VELOX_${dependency_name}_SHA256 "")
    if(DEFINED ENV{VELOX_${dependency_name}_SHA256})
      string(PREPEND VELOX_${dependency_name}_BUILD_SHA256_CHECKSUM "SHA256=")
    endif()
  endif()
endmacro()
