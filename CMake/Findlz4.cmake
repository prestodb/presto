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
# - Try to find lz4
# Once done, this will define
#
# LZ4_FOUND - system has Glog
# LZ4_INCLUDE_DIRS - deprecated
# LZ4_LIBRARIES -  deprecated
# lz4::lz4 will be defined based on CMAKE_FIND_LIBRARY_SUFFIXES priority

include(FindPackageHandleStandardArgs)
include(SelectLibraryConfigurations)

find_library(LZ4_LIBRARY_RELEASE lz4 PATHS $LZ4_LIBRARYDIR})
find_library(LZ4_LIBRARY_DEBUG lz4d PATHS ${LZ4_LIBRARYDIR})

find_path(LZ4_INCLUDE_DIR lz4.h PATHS ${LZ4_INCLUDEDIR})

select_library_configurations(LZ4)

find_package_handle_standard_args(lz4 DEFAULT_MSG LZ4_LIBRARY LZ4_INCLUDE_DIR)

mark_as_advanced(LZ4_LIBRARY LZ4_INCLUDE_DIR)

get_filename_component(liblz4_ext ${LZ4_LIBRARY} EXT)
if(liblz4_ext STREQUAL ".a")
  set(liblz4_type STATIC)
else()
  set(liblz4_type SHARED)
endif()

if(NOT TARGET lz4::lz4)
  add_library(lz4::lz4 ${liblz4_type} IMPORTED)
  set_target_properties(lz4::lz4 PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                            "${LZ4_INCLUDE_DIR}")
  set_target_properties(
    lz4::lz4 PROPERTIES IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                        IMPORTED_LOCATION "${LZ4_LIBRARIES}")
endif()
