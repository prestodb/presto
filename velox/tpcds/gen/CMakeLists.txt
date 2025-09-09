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

# Add subdirectories
add_subdirectory(utils)
add_subdirectory(dsdgen)

velox_add_library(velox_tpcds_gen TpcdsGen.cpp DSDGenIterator.cpp)
velox_include_directories(
  velox_tpcds_gen
  PUBLIC "$<BUILD_LOCAL_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/dsdgen/include>"
)
velox_link_libraries(
  velox_tpcds_gen
  PUBLIC velox_memory velox_tpcds_append_info
  PRIVATE velox_vector velox_tpcds_dsdgen fmt::fmt
)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
