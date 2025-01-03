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
add_executable(velox_common_base_benchmarks BitUtilBenchmark.cpp)

target_link_libraries(
  velox_common_base_benchmarks
  PUBLIC Folly::follybenchmark
  PRIVATE velox_common_base Folly::folly)

add_executable(velox_common_stringsearch_benchmarks StringSearchBenchmark.cpp)

target_link_libraries(
  velox_common_stringsearch_benchmarks
  PUBLIC Folly::follybenchmark
  PRIVATE velox_common_base Folly::folly)

add_executable(velox_common_indexed_priority_queue_benchmark
               IndexedPriorityQueueBenchmark.cpp)

target_link_libraries(
  velox_common_indexed_priority_queue_benchmark
  PUBLIC Folly::follybenchmark
  PRIVATE velox_common_base Folly::folly)

add_executable(velox_common_sorting_network_benchmark
               SortingNetworkBenchmark.cpp)

target_link_libraries(
  velox_common_sorting_network_benchmark
  PUBLIC Folly::follybenchmark
  PRIVATE velox_common_base Folly::folly)
