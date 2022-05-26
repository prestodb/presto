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

set(BENCHMARK_DEPENDENCIES
    velox_functions_test_lib velox_exec velox_exec_test_util
    ${gflags_LIBRARIES} ${FOLLY_WITH_DEPENDENCIES} ${FOLLY_BENCHMARK})

add_executable(velox_benchmark_call_null_free_no_nulls
               CallNullFreeBenchmark.cpp)
target_link_libraries(velox_benchmark_call_null_free_no_nulls
                      ${BENCHMARK_DEPENDENCIES})
target_compile_definitions(
  velox_benchmark_call_null_free_no_nulls PUBLIC WITH_NULL_ARRAYS=false
                                                 WITH_NULL_ELEMENTS=false)

add_executable(velox_benchmark_call_null_free_null_arrays
               CallNullFreeBenchmark.cpp)
target_link_libraries(velox_benchmark_call_null_free_null_arrays
                      ${BENCHMARK_DEPENDENCIES})
target_compile_definitions(
  velox_benchmark_call_null_free_null_arrays PUBLIC WITH_NULL_ARRAYS=true
                                                    WITH_NULL_ELEMENTS=false)

add_executable(velox_benchmark_call_null_free_null_elements
               CallNullFreeBenchmark.cpp)
target_link_libraries(velox_benchmark_call_null_free_null_elements
                      ${BENCHMARK_DEPENDENCIES})
target_compile_definitions(
  velox_benchmark_call_null_free_null_elements PUBLIC WITH_NULL_ARRAYS=false
                                                      WITH_NULL_ELEMENTS=true)

add_executable(velox_benchmark_call_null_free_null_arrays_and_null_elements
               CallNullFreeBenchmark.cpp)
target_link_libraries(
  velox_benchmark_call_null_free_null_arrays_and_null_elements
  ${BENCHMARK_DEPENDENCIES})
target_compile_definitions(
  velox_benchmark_call_null_free_null_arrays_and_null_elements
  PUBLIC WITH_NULL_ARRAYS=true WITH_NULL_ELEMENTS=true)

add_executable(velox_benchmark_variadic VariadicBenchmark.cpp)
target_link_libraries(velox_benchmark_variadic ${BENCHMARK_DEPENDENCIES})
