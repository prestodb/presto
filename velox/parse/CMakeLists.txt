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
velox_add_library(velox_parse_expression Expressions.cpp)
velox_link_libraries(velox_parse_expression velox_type velox_parse_utils velox_expression)

velox_add_library(velox_parse_parser ExpressionsParser.cpp QueryPlanner.cpp)
velox_link_libraries(velox_parse_parser velox_parse_expression velox_type velox_duckdb_parser)

velox_add_library(velox_parse_utils TypeResolver.cpp)
velox_link_libraries(
  velox_parse_utils
  velox_buffer
  velox_exec
  velox_type
  velox_vector
  velox_function_registry
)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
