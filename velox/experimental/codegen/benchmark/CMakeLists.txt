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
  velox_codegen_benchmark_single_output
  SingleOutputDefaultNullsBenchmark.cpp
  SingleOutputNotDefaultNullsBenchmark.cpp BooleanBenchmarks.cpp)

target_link_libraries(
  velox_codegen_benchmark_single_output
  velox_exec_test_util
  velox_codegen_utils_resource_path
  velox_codegen_code_generator
  velox_hive_connector
  ${FOLLY_BENCHMARK}
  ${FOLLY_WITH_DEPENDENCIES}
  ${Boost_FILESYSTEM_LIBRARIES}
  ${GTEST_BOTH_LIBRARIES}
  ${GFLAGS_LIBRARIES}
  ${FMT}
  velox_core
  velox_exec
  velox_exec_test_util
  velox_functions_lib
  velox_functions_prestosql
  velox_dwrf_test_utils)

add_test(velox_codegen_benchmark_single_output
         velox_codegen_benchmark_single_output)

add_executable(velox_codegen_benchmark CodegenBenchmarks.cpp)
target_link_libraries(velox_codegen_benchmark velox_hive_connector)

add_compile_definitions(velox_codegen_test BASEFOLDER="${PROJECT_SOURCE_DIR}")
add_compile_definitions(velox_codegen_test COMPFLAGS="${CMAKE_CXX_FLAGS}")
add_compile_definitions(velox_codegen_test MYFOLLY="${GFLAGS_LIBRARIES}")

# find a better way to do this
add_compile_definitions(velox_codegen_test
                        VELOX_CORE="$<TARGET_LINKER_FILE:velox_core>")
add_compile_definitions(
  velox_codegen_test
  CODEGEN="$<TARGET_LINKER_FILE:velox_codegen_code_generator>")

add_compile_definitions(
  velox_codegen_test
  VELOX_FUNCTIONS_LIB="$<TARGET_LINKER_FILE:velox_functions_lib>")
add_compile_definitions(velox_codegen_test
                        CODEGEN_AST="$<TARGET_LINKER_FILE:velox_codegen_ast>")
add_compile_definitions(velox_codegen_test
                        VELOX_EXEC="$<TARGET_LINKER_FILE:velox_exec>")
add_compile_definitions(velox_codegen_test
                        VELOX_VECTOR="$<TARGET_LINKER_FILE:velox_vector>")
add_compile_definitions(velox_codegen_test
                        VELOX_TYPE="$<TARGET_LINKER_FILE:velox_type>")
add_compile_definitions(velox_codegen_test
                        CODEGEN_AST="$<TARGET_LINKER_FILE:velox_codegen_ast>")
add_compile_definitions(velox_codegen_test
                        VELOX_EXCEPTION="$<TARGET_LINKER_FILE:velox_exception>")
add_compile_definitions(
  velox_codegen_test
  VELOX_SERIALIZATION="$<TARGET_LINKER_FILE:velox_serialization>")
add_compile_definitions(velox_codegen_test
                        VELOX_PROCESS="$<TARGET_LINKER_FILE:velox_process>")
add_compile_definitions(velox_codegen_test
                        VELOX_MEMORY="$<TARGET_LINKER_FILE:velox_memory>")
add_compile_definitions(velox_codegen_test
                        VELOX_ENCODE="$<TARGET_LINKER_FILE:velox_encode>")
add_compile_definitions(velox_codegen_test
                        VELOX_CACHING="$<TARGET_LINKER_FILE:velox_caching>")

add_compile_definitions(velox_codegen_test GFLAGS_LI="${GFLAGS_INCLUDES}")

target_link_libraries(
  velox_codegen_benchmark
  velox_exec_test_util
  velox_codegen_code_generator
  ${FOLLY_BENCHMARK}
  ${FOLLY_WITH_DEPENDENCIES}
  ${Boost_FILESYSTEM_LIBRARIES}
  ${GTEST_BOTH_LIBRARIES}
  ${GFLAGS_LIBRARIES}
  glog::glog
  ${FMT}
  velox_core
  velox_exec
  velox_codegen_utils_resource_path
  velox_exec_test_util
  velox_functions_lib
  velox_functions_prestosql
  velox_dwrf_test_utils)
