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

set(find_package_args)
if(Arrow_FIND_VERSION)
  list(APPEND find_package_args ${Arrow_FIND_VERSION})
endif()
if(Arrow_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(Arrow ${find_package_args} CONFIG)
if(Arrow_VERSION VERSION_LESS_EQUAL 21.0.0)
  # Workaround for https://github.com/apache/arrow/issues/46386 .
  # ArrowTestingConfig.cmake may call this file via find_dependency(Arrow). It
  # causes an infinite loop.
  set(CMAKE_FIND_PACKAGE_PREFER_CONFIG_KEEP ${CMAKE_FIND_PACKAGE_PREFER_CONFIG})
  set(CMAKE_FIND_PACKAGE_PREFER_CONFIG TRUE)
endif()
find_package(ArrowTesting ${find_package_args} CONFIG)
if(Arrow_VERSION VERSION_LESS_EQUAL 21.0.0)
  set(CMAKE_FIND_PACKAGE_PREFER_CONFIG ${CMAKE_FIND_PACKAGE_PREFER_CONFIG_KEEP})
endif()

# Only add the libraries once.
if(Arrow_FOUND
   AND ArrowTesting_FOUND
   AND NOT TARGET arrow)
  add_library(arrow ALIAS Arrow::arrow_static)
  add_library(arrow_testing ALIAS ArrowTesting::arrow_testing_static)
endif()
