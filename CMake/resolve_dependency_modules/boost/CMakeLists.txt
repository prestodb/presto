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

project(Boost)
cmake_minimum_required(VERSION 3.14)

# Suppress all warnings (in this subdir)
add_compile_options(-w)

# We need to use boost > 1.70 to build it with CMake 1.81 was the first to be
# released as a github release INCLUDING the cmake files (which are not in the
# officale releases for some reason)
set(VELOX_BOOST_BUILD_VERSION 1.84.0)
string(
  CONCAT
  VELOX_BOOST_SOURCE_URL
  "https://github.com/boostorg/boost/releases/download/"
  "boost-${VELOX_BOOST_BUILD_VERSION}/"
  "boost-${VELOX_BOOST_BUILD_VERSION}.tar.gz"
)
set(
  VELOX_BOOST_BUILD_SHA256_CHECKSUM
  4d27e9efed0f6f152dc28db6430b9d3dfb40c0345da7342eaa5a987dde57bd95
)

velox_resolve_dependency_url(BOOST)
message(STATUS "Building boost from source")

# required for Boost::thread
if(NOT TARGET Threads::Threads)
  set(THREADS_PREFER_PTHREAD_FLAG ON)
  find_package(Threads REQUIRED)
endif()

FetchContent_Declare(
  Boost
  URL ${VELOX_BOOST_SOURCE_URL}
  URL_HASH ${VELOX_BOOST_BUILD_SHA256_CHECKSUM}
)

# Configure the file before adding the header only libs
configure_file(
  ${CMAKE_CURRENT_LIST_DIR}/FindBoost.cmake.in
  ${CMAKE_CURRENT_LIST_DIR}/FindBoost.cmake
  @ONLY
)

set(
  BOOST_HEADER_ONLY
  crc
  circular_buffer
  math
  multi_index
  multiprecision
  numeric_conversion
  random
  uuid
  variant
)
list(APPEND BOOST_INCLUDE_LIBRARIES ${BOOST_HEADER_ONLY})

# The `headers` target is not created by Boost cmake and leads to a warning
list(REMOVE_ITEM BOOST_INCLUDE_LIBRARIES headers)
set(BUILD_SHARED_LIBS OFF)
FetchContent_MakeAvailable(Boost)

list(TRANSFORM BOOST_HEADER_ONLY PREPEND Boost::)
target_link_libraries(boost_headers INTERFACE ${BOOST_HEADER_ONLY})
add_library(Boost::headers ALIAS boost_headers)
