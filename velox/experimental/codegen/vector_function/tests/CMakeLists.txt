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

add_executable(
  velox_codegen_vector_function_test CodegenVectorFunctionTest.cpp
                                     TempStringTest.cpp VectorReaderTest.cpp)
add_test(velox_codegen_vector_function_test velox_codegen_vector_function_test)

target_link_libraries(
  velox_codegen_vector_function_test
  velox_codegen_ast
  velox_codegen_external_process
  velox_exec
  velox_dwio_dwrf_reader
  velox_dwio_dwrf_writer
  velox_dwio_common
  velox_aggregates
  velox_functions_lib
  velox_functions_prestosql
  velox_hive_connector
  velox_core
  velox_type
  velox_serialization
  velox_exec_test_lib
  velox_functions_json
  velox_vector
  velox_memory
  velox_dwio_type_fbhive
  velox_dwio_common_exception
  velox_dwio_common_test_utils
  velox_presto_serializer
  velox_transform_utils
  ${ANTLR4_RUNTIME}
  ${Boost_ATOMIC_LIBRARIES}
  ${Boost_CONTEXT_LIBRARIES}
  ${Boost_DATE_TIME_LIBRARIES}
  ${Boost_FILESYSTEM_LIBRARIES}
  ${Boost_PROGRAM_OPTIONS_LIBRARIES}
  ${Boost_REGEX_LIBRARIES}
  ${Boost_THREAD_LIBRARIES}
  ${Boost_SYSTEM_LIBRARIES}
  ${gflags_LIBRARIES}
  gtest
  gtest_main
  ${FOLLY_WITH_DEPENDENCIES}
  ${DOUBLE_CONVERSION}
  ${GFLAGS_LIBRARIES}
  glog::glog
  ${FMT}
  dl)
