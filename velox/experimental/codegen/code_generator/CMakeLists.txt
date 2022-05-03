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

add_library(velox_all_link STATIC)
target_link_libraries(
  velox_all_link
  velox_codegen_code_generator
  velox_codegen_utils_resource_path
  velox_core
  velox_parse_parser
  velox_exec_test_util
  velox_functions_lib
  velox_functions_prestosql_impl
  ${FOLLY_WITH_DEPENDENCIES}
  gtest
  gtest_main
  ${GFLAGS_LIBRARIES}
  glog::glog
  ${FMT})

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
add_library(velox_codegen_code_generator ExprCodeGenerator.cpp)
target_link_libraries(velox_codegen_code_generator velox_core velox_codegen_ast
                      velox_codegen_udf_manager velox_memory)
target_link_libraries(velox_codegen_code_generator velox_codegen_proto)
