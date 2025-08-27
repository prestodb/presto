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

find_package(c-ares CONFIG)
if(c-ares_FOUND)
  if(TARGET c-ares::cares)
    return()
  endif()
endif()

find_path(C_ARES_INCLUDE_DIR NAMES ares.h PATH_SUFFIXES c-ares)
find_library(C_ARES_LIBRARY NAMES c-ares)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(c-ares DEFAULT_MSG C_ARES_LIBRARY C_ARES_INCLUDE_DIR)

if(c-ares_FOUND AND NOT TARGET c-ares::cares)
  add_library(c-ares::cares UNKNOWN IMPORTED)
  set_target_properties(
    c-ares::cares
    PROPERTIES
      IMPORTED_LOCATION "${C_ARES_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${C_ARES_INCLUDE_DIR}"
  )
endif()

mark_as_advanced(C_ARES_INCLUDE_DIR C_ARES_LIBRARY)
