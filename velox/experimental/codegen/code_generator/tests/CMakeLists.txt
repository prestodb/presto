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

# write all cmake var in a file for post-processing

set(CODEGEN_LINK_PATH "$<TARGET_FILE:velox_all_link>")
get_directory_property(DEFINED_VARIABLE VARIABLES)
foreach(VR IN LISTS DEFINED_VARIABLE)
  set(CMAKE_VARS "${CMAKE_VARS}\n${VR}=${${VR}}")
endforeach()

add_executable(
  velox_codegen_expression_test
  BasicExprCodeGeneratorTest.cpp
  resources/UDFTest.h
  LogicalExpressionsTest.cpp
  CastExpressionTest.cpp
  ArithmeticFunctionsTest.cpp
  StringExpressionsTest.cpp
  UDFCallTest.cpp)

target_link_libraries(
  velox_codegen_expression_test
  velox_codegen_code_generator
  gtest
  gtest_main
  ${GFLAGS_LIBRARIES}
  glog::glog
  ${FMT}
  velox_core
  velox_parse_parser
  velox_exec_test_lib
  velox_functions_lib
  velox_functions_prestosql
  velox_all_link)

add_test(velox_codegen_expression_test velox_codegen_expression_test)

add_executable(velox_codegen_ast_analysis_test ASTAnalysisTest.cpp)

target_link_libraries(
  velox_codegen_ast_analysis_test
  velox_codegen_code_generator
  gtest
  gtest_main
  ${GFLAGS_LIBRARIES}
  glog::glog
  ${FMT}
  velox_core
  velox_parse_parser
  velox_exec_test_lib
  velox_functions_lib
  velox_functions_prestosql
  velox_all_link)

add_test(velox_codegen_ast_analysis_test velox_codegen_ast_analysis_test)
