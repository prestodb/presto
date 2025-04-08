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
include_guard(GLOBAL)
function(get_rpath_origin VAR)
  if(APPLE)
    set(_origin @loader_path)
  else()
    set(_origin "\$ORIGIN")
  endif()
  set(${VAR}
      ${_origin}
      PARENT_SCOPE)
endfunction()

function(pyvelox_add_module TARGET)
  pybind11_add_module(${TARGET} ${ARGN})

  if(DEFINED SKBUILD_PROJECT_VERSION_FULL)
    target_compile_definitions(
      ${TARGET} PRIVATE PYVELOX_VERSION=${SKBUILD_PROJECT_VERSION_FULL})
  else()
    target_compile_definitions(${TARGET} PRIVATE PYVELOX_VERSION=dev)
  endif()

  # Set the rpath so linker looks within pyvelox package for libs
  get_rpath_origin(_origin)
  set_target_properties(
    ${TARGET} PROPERTIES INSTALL_RPATH "${_origin}/;${CMAKE_BINARY_DIR}/lib"
                         INSTALL_RPATH_USE_LINK_PATH TRUE)
  install(TARGETS ${TARGET} LIBRARY DESTINATION pyvelox
                                    COMPONENT pyvelox_libraries)
endfunction()

# TODO use file sets
function(velox_install_library_headers)
  # Find any headers and install them relative to the source tree in include.
  file(GLOB _hdrs "*.h")
  if(NOT "${_hdrs}" STREQUAL "")
    cmake_path(
      RELATIVE_PATH
      CMAKE_CURRENT_SOURCE_DIR
      BASE_DIRECTORY
      "${CMAKE_SOURCE_DIR}"
      OUTPUT_VARIABLE
      _hdr_dir)
    install(FILES ${_hdrs} DESTINATION include/${_hdr_dir})
  endif()
endfunction()

# Base add velox library call to add a library and install it.
function(velox_base_add_library TARGET)
  add_library(${TARGET} ${ARGN})
  install(TARGETS ${TARGET} DESTINATION lib/velox)
  velox_install_library_headers()
endfunction()

# This is extremely hackish but presents an easy path to installation.
function(velox_add_library TARGET)
  set(options OBJECT STATIC SHARED INTERFACE)
  set(oneValueArgs)
  set(multiValueArgs)
  cmake_parse_arguments(
    VELOX
    "${options}"
    "${oneValueArgs}"
    "${multiValueArgs}"
    ${ARGN})

  # Remove library type specifiers from ARGN
  set(library_type)
  if(VELOX_OBJECT)
    set(library_type OBJECT)
  elseif(VELOX_STATIC)
    set(library_type STATIC)
  elseif(VELOX_SHARED)
    set(library_type SHARED)
  elseif(VELOX_INTERFACE)
    set(library_type INTERFACE)
  endif()

  list(REMOVE_ITEM ARGN OBJECT)
  list(REMOVE_ITEM ARGN STATIC)
  list(REMOVE_ITEM ARGN SHARED)
  list(REMOVE_ITEM ARGN INTERFACE)
  # Propagate to the underlying add_library and then install the target.
  if(VELOX_MONO_LIBRARY)
    if(TARGET velox)
      # Target already exists, append sources to it.
      target_sources(velox PRIVATE ${ARGN})
      install(TARGETS velox LIBRARY DESTINATION pyvelox
                                    COMPONENT pyvelox_libraries)
    else()
      set(_type STATIC)
      if(VELOX_BUILD_SHARED)
        set(_type SHARED)
      endif()
      # Create the target if this is the first invocation.
      add_library(velox ${_type} ${ARGN})
      set_target_properties(velox PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                                             ${PROJECT_BINARY_DIR}/lib)
      set_target_properties(velox PROPERTIES ARCHIVE_OUTPUT_DIRECTORY
                                             ${PROJECT_BINARY_DIR}/lib)
      install(TARGETS velox DESTINATION lib/velox)
    endif()
    # create alias for compatability
    if(NOT TARGET ${TARGET})
      add_library(${TARGET} ALIAS velox)
    endif()
  else()
    # Create a library for each invocation.
    velox_base_add_library(${TARGET} ${library_type} ${ARGN})
  endif()
  velox_install_library_headers()
endfunction()

function(velox_link_libraries TARGET)
  # TODO(assignUser): Handle scope keywords (they currently are empty calls ala
  # target_link_libraries(target PRIVATE))
  if(VELOX_MONO_LIBRARY)
    # These targets follow the velox_* name for consistency but are NOT actually
    # aliases to velox when building the mono lib and need to be linked
    # explicitly (this is a hack)
    set(explicit_targets
        velox_exec_test_lib
        # see velox/experimental/wave/README.md
        velox_wave_common
        velox_wave_decode
        velox_wave_dwio
        velox_wave_exec
        velox_wave_stream
        velox_wave_vector)

    foreach(_arg ${ARGN})
      list(FIND explicit_targets ${_arg} _explicit)
      if(_explicit EQUAL -1 AND "${_arg}" MATCHES "^velox_*")
        message(DEBUG "\t\tDROP: ${_arg}")
      else()
        message(DEBUG "\t\tADDING: ${_arg}")
        target_link_libraries(velox ${_arg})
      endif()
    endforeach()
  else()
    target_link_libraries(${TARGET} ${ARGN})
  endif()
endfunction()

function(velox_include_directories TARGET)
  if(VELOX_MONO_LIBRARY)
    target_include_directories(velox ${ARGN})
  else()
    target_include_directories(${TARGET} ${ARGN})
  endif()
endfunction()

function(velox_compile_definitions TARGET)
  if(VELOX_MONO_LIBRARY)
    target_compile_definitions(velox ${ARGN})
  else()
    target_compile_definitions(${TARGET} ${ARGN})
  endif()
endfunction()

function(velox_sources TARGET)
  if(VELOX_MONO_LIBRARY)
    target_sources(velox ${ARGN})
  else()
    target_sources(${TARGET} ${ARGN})
  endif()
endfunction()
