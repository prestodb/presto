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

include(FindPackageHandleStandardArgs)

find_library(ARROW_LIB libarrow.a)
find_library(ARROW_TESTING_LIB libarrow_testing.a)
find_path(ARROW_INCLUDE_PATH arrow/api.h)
find_package(Thrift)

find_package_handle_standard_args(
  Arrow
  DEFAULT_MSG
  ARROW_LIB
  ARROW_TESTING_LIB
  ARROW_INCLUDE_PATH
  Thrift_FOUND
)

# Only add the libraries once.
if(Arrow_FOUND AND NOT TARGET arrow)
  add_library(arrow STATIC IMPORTED GLOBAL)
  add_library(arrow_testing STATIC IMPORTED GLOBAL)
  add_library(thrift ALIAS thrift::thrift)

  set_target_properties(
    arrow
    arrow_testing
    PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_PATH}
  )
  set_target_properties(
    arrow
    PROPERTIES IMPORTED_LOCATION ${ARROW_LIB} INTERFACE_LINK_LIBRARIES thrift
  )
  set_target_properties(arrow_testing PROPERTIES IMPORTED_LOCATION ${ARROW_TESTING_LIB})
endif()
