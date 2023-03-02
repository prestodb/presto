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

message(STATUS "Building boost from source")

# required for Boost::thread
if(NOT TARGET Threads::Threads)
  set(THREADS_PREFER_PTHREAD_FLAG ON)
  find_package(Threads REQUIRED)
endif()

# Make download progress visible
set(fc_quiet_state ${FETCHCONTENT_QUIET})
set(FETCHCONTENT_QUIET OFF)

FetchContent_Declare(
  Boost
  GIT_REPOSITORY https://github.com/boostorg/boost.git
  GIT_TAG boost-1.80.0
  GIT_SHALLOW TRUE)
FetchContent_Populate(Boost)

# Boost cmake uses the global option
set(shared_libs ${BUILD_SHARED_LIBS})
set(BUILD_SHARED_LIBS ON)
add_subdirectory(${boost_SOURCE_DIR} ${boost_BINARY_DIR})
set(BUILD_SHARED_LIBS ${shared_libs})

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
list(PREPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/boost)
set(FETCHCONTENT_QUIET ${fc_quiet_state})
