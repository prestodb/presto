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
find_library(re2_lib re2)
find_path(re2_include re2/re2.h)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(re2 REQUIRED_VARS re2_lib re2_include)

if(re2_FOUND)
  set(re2_LIBRARIES ${re2_lib})
  set(re2_INCLUDE_DIRS ${re2_include})

  add_library(re2::re2 UNKNOWN IMPORTED)
  target_include_directories(re2::re2 INTERFACE ${re2_INCLUDE_DIRS})
  target_link_libraries(re2::re2 INTERFACE ${re2_LIBRARIES})
  set_target_properties(re2::re2 PROPERTIES IMPORTED_LOCATION
                                            "${re2_LIBRARIES}")
endif()
