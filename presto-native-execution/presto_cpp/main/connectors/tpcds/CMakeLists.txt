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

cmake_minimum_required(VERSION 3.14)
cmake_policy(SET CMP0063 NEW)

project(TPCDS)

# Certain Clang compiler versions throw "deprecated declarations" warnings for
# DuckDB's parquet-amalgamation.h. This warning is from the Thrift library that
# DuckDB embeds and is also on Thrift master. The workaround, for now, is to
# disable this warning if the compiler is Clang.
check_cxx_compiler_flag(-Wno-unqualified-std-cast-call UNQUALIFIED_STD_CAST)
check_cxx_compiler_flag(-Wno-bitwise-instead-of-logical
                        BITWISE_INSTEAD_OF_LOGICAL)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wno-deprecated-declarations -Wno-writable-strings
                      -Wno-missing-field-initializers)
  if(UNQUALIFIED_STD_CAST)
    add_compile_options(-Wno-unqualified-std-cast-call)
  endif()
  if(BITWISE_INSTEAD_OF_LOGICAL)
    add_compile_options(-Wno-bitwise-instead-of-logical)
  endif()
endif()

# This stringop-overflow warning seems to have lots of false positives and has
# been the source of a lot of compiler bug reports (e.g.
# https://gcc.gnu.org/bugzilla/show_bug.cgi?id=99578), which causes
# parquet-amalgamation.cpp to fail to compile. For now, we disable this warning
# on the affected compiler (GCC).
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  add_compile_options(-Wno-stringop-overflow)
endif()

include_directories(dsdgen/include)
include_directories(dsdgen/include/dsdgen-c)
include_directories(include)
include_directories(..)

add_subdirectory(dsdgen/dsdgen-c)

add_library(dsdgen STATIC include/append_info-c.cpp)
target_link_libraries(dsdgen dsdgen_c velox_vector_test_lib Folly::folly xsimd)

add_library(tpcds_gen TpcdsGen.cpp DSDGenIterator.cpp)
target_include_directories(tpcds_gen PRIVATE dsdgen/include)
target_link_libraries(tpcds_gen velox_memory velox_vector dsdgen fmt::fmt)

# if(${VELOX_BUILD_TESTING}) add_subdirectory(tests) endif()
