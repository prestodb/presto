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

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()

bison_target(
  TypeParser TypeParser.yy ${CMAKE_CURRENT_BINARY_DIR}/TypeParser.yy.cc
  DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/TypeParser.yy.h
  COMPILE_FLAGS "-Werror -Wno-deprecated")

flex_target(
  TypeParserScanner TypeParser.ll ${CMAKE_CURRENT_BINARY_DIR}/Scanner.cpp
  COMPILE_FLAGS "-Cf --prefix=veloxtp")

add_flex_bison_dependency(TypeParserScanner TypeParser)

if(VELOX_MONO_LIBRARY)
  add_custom_target(
    velox_type_parser_gen_src DEPENDS ${BISON_TypeParser_OUTPUTS}
                                      ${FLEX_TypeParserScanner_OUTPUTS})
  add_dependencies(velox velox_type_parser_gen_src)
endif()

include_directories(${PROJECT_BINARY_DIR})
include_directories(${FLEX_INCLUDE_DIRS})
velox_add_library(velox_type_parser ${BISON_TypeParser_OUTPUTS}
                  ${FLEX_TypeParserScanner_OUTPUTS} ParserUtil.cpp)
velox_link_libraries(velox_type_parser velox_common_base)
