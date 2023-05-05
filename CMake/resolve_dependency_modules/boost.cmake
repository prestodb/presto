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
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/boost)

if(${ICU_SOURCE} STREQUAL "BUNDLED")
  # ensure ICU is built before Boost
  add_dependencies(boost_regex ICU ICU::i18n)
endif()

# This prevents system boost from leaking in
set(Boost_NO_SYSTEM_PATHS ON)
# We have to keep the FindBoost.cmake in an subfolder to prevent it from
# overriding the system provided one when Boost_SOURCE=SYSTEM
list(PREPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/boost)
