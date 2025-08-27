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

set(VELOX_GLOG_VERSION 0.6.0)
set(
  VELOX_GLOG_BUILD_SHA256_CHECKSUM
  8a83bf982f37bb70825df71a9709fa90ea9f4447fb3c099e1d720a439d88bad6
)
set(
  VELOX_GLOG_SOURCE_URL
  "https://github.com/google/glog/archive/refs/tags/v${VELOX_GLOG_VERSION}.tar.gz"
)

velox_resolve_dependency_url(GLOG)

message(STATUS "Building glog from source")
FetchContent_Declare(
  glog
  URL ${VELOX_GLOG_SOURCE_URL}
  URL_HASH ${VELOX_GLOG_BUILD_SHA256_CHECKSUM}
  PATCH_COMMAND
    git apply ${CMAKE_CURRENT_LIST_DIR}/glog/glog-no-export.patch && git apply
    ${CMAKE_CURRENT_LIST_DIR}/glog/glog-config.patch
  SYSTEM
  OVERRIDE_FIND_PACKAGE
  EXCLUDE_FROM_ALL
)

set(BUILD_SHARED_LIBS ${VELOX_BUILD_SHARED})
set(WITH_UNWIND OFF)
set(gflags_NAMESPACE google)
set(BUILD_TESTING OFF)
FetchContent_MakeAvailable(glog)
unset(BUILD_TESTING)
unset(BUILD_SHARED_LIBS)

# Folly uses variables instead of targets
set(glog_LIBRARY glog::glog)

add_dependencies(glog gflags::gflags)

# The default target has the glog-src as an include dir but this causes issues
# with folly due to an internal glog 'demangle.h' being mistaken for a system
# header so we remove glog_SOURCE_DIR by overwriting INTERFACE_INCLUDE_DIRECTORIES

# Can't set properties on ALIAS targets
get_target_property(_glog_target glog::glog ALIASED_TARGET)

set_target_properties(${_glog_target} PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${glog_BINARY_DIR})

# These headers are missing from glog_BINARY_DIR
file(COPY ${glog_SOURCE_DIR}/src/glog/platform.h DESTINATION ${glog_BINARY_DIR}/glog)
file(COPY ${glog_SOURCE_DIR}/src/glog/log_severity.h DESTINATION ${glog_BINARY_DIR}/glog)
