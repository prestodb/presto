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

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
message("Appending CMAKE_CXX_FLAGS with ${SCRIPT_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS}")

# Known warnings that are benign can be disabled.
set(DISABLED_WARNINGS
    "-Wno-nullability-completeness -Wno-deprecated-declarations")

# Important warnings that must be explicitly enabled.
set(ENABLE_WARNINGS "-Wreorder")

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} ${DISABLED_WARNINGS} ${ENABLE_WARNINGS}")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(DEFINED ENV{INSTALL_PREFIX})
  message(STATUS "Dependency install directory set to: $ENV{INSTALL_PREFIX}")
  list(APPEND CMAKE_PREFIX_PATH "$ENV{INSTALL_PREFIX}")
endif()

find_package(gflags REQUIRED)

find_library(GLOG glog)

find_package(fmt REQUIRED)

if(NOT TARGET gflags::gflags)
  # This is a bit convoluted, but we want to be able to use gflags::gflags as a
  # target even when velox is built as a subproject which uses
  # `find_package(gflags)` which does not create a globally imported target that
  # we can ALIAS.
  add_library(gflags_gflags INTERFACE)
  target_link_libraries(gflags_gflags INTERFACE gflags)
  add_library(gflags::gflags ALIAS gflags_gflags)
endif()
include_directories(.)

include_directories(${CMAKE_BINARY_DIR})

# Adding this down here prevents warnings in dependencies from stopping the
# build.
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
