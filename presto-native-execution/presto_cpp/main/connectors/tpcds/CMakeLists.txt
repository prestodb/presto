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

cmake_policy(SET CMP0079 NEW)

project(TPCDS)

add_library(presto_tpcds_connector OBJECT TpcdsConnector.cpp)
target_link_libraries(presto_tpcds_connector velox_connector tpcds_gen fmt::fmt)

# Without this hack, there are multiple link errors similar to the one below
# only on GCC. "undefined reference to `vtable for
# velox::connector::tpcds::TpcdsTableHandle`. TODO: Fix this hack.
target_link_libraries(velox_exec_test_lib presto_tpcds_connector)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wno-deprecated-declarations -Wno-writable-strings
                      -Wno-missing-field-initializers)
endif()

# This stringop-overflow warning seems to have lots of false positives and has
# been the source of a lot of compiler bug reports (e.g.
# https://gcc.gnu.org/bugzilla/show_bug.cgi?id=99578), which causes
# parquet-amalgamation.cpp to fail to compile. For now, we disable this warning
# on the affected compiler (GCC).
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  add_compile_options(-Wno-stringop-overflow -Wno-write-strings)
endif()

# Add subdirectories
add_subdirectory(${CMAKE_SOURCE_DIR}/presto_cpp/external/dsdgen/dsdgen-c build)

# Generate tpcds_idx.hpp
set(INPUT_FILE tpcds.idx)
set(OUTPUT_FILE tpcds_idx.hpp)
set(WORKING_DIR ${CMAKE_SOURCE_DIR}/presto_cpp/external/dsdgen/include/)

add_custom_command(
  OUTPUT ${WORKING_DIR}/${OUTPUT_FILE}
  COMMAND xxd -i ${INPUT_FILE} > ${OUTPUT_FILE}
  DEPENDS ${WORKING_DIR}/${INPUT_FILE}
  COMMENT "Generating ${OUTPUT_FILE} from ${INPUT_FILE}"
  WORKING_DIRECTORY ${WORKING_DIR})

add_custom_target(generate_tpcds_idx ALL DEPENDS ${WORKING_DIR}/${OUTPUT_FILE})
add_dependencies(dsdgen_c generate_tpcds_idx)

add_library(append_info OBJECT utils/append_info-c.cpp)
target_link_libraries(append_info velox_vector_test_lib Folly::folly xsimd)
target_link_libraries(dsdgen_c append_info)

add_library(tpcds_gen TpcdsGen.cpp DSDGenIterator.cpp)
target_include_directories(tpcds_gen PUBLIC dsdgen/include)
target_link_libraries(tpcds_gen velox_memory velox_vector dsdgen_c append_info
                      fmt::fmt)
