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
add_executable(velox_exec_vector_hasher_benchmark VectorHasherBenchmark.cpp)

target_link_libraries(velox_exec_vector_hasher_benchmark velox_exec
                      velox_vector_test_lib ${FOLLY_BENCHMARK})

add_executable(velox_filter_project_benchmark FilterProjectBenchmark.cpp)

target_link_libraries(
  velox_filter_project_benchmark velox_exec velox_vector_test_lib
  velox_exec_test_lib ${FOLLY_BENCHMARK})

add_executable(velox_exchange_benchmark ExchangeBenchmark.cpp)

target_link_libraries(velox_exchange_benchmark velox_exec velox_exec_test_lib
                      velox_vector_test_lib ${FOLLY_BENCHMARK})

add_executable(velox_merge_benchmark MergeBenchmark.cpp)

target_link_libraries(velox_merge_benchmark velox_exec velox_vector_test_lib
                      ${FOLLY_BENCHMARK} gtest gtest_main)

add_executable(velox_hash_benchmark HashTableBenchmark.cpp)

target_link_libraries(velox_hash_benchmark velox_exec velox_exec_test_lib
                      velox_vector_test_lib ${FOLLY_BENCHMARK})

add_executable(velox_hash_join_list_result_benchmark
               HashJoinListResultBenchmark.cpp)

target_link_libraries(
  velox_hash_join_list_result_benchmark velox_exec velox_exec_test_lib
  velox_vector_test_lib ${FOLLY_BENCHMARK})

if(${VELOX_ENABLE_PARQUET})
  add_executable(velox_sort_benchmark RowContainerSortBenchmark.cpp)

  target_link_libraries(
    velox_sort_benchmark
    velox_exec
    velox_exec_test_lib
    velox_vector_test_lib
    ${FOLLY_BENCHMARK}
    arrow
    thrift)
endif()

add_executable(velox_prefixsort_benchmark PrefixSortBenchmark.cpp)

target_link_libraries(velox_prefixsort_benchmark velox_exec velox_vector_fuzzer
                      velox_vector_test_lib ${FOLLY_BENCHMARK})
