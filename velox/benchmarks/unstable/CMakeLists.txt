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
set(velox_benchmark_deps
    velox_type
    velox_vector
    velox_vector_fuzzer
    velox_expression
    velox_parse_parser
    velox_parse_utils
    velox_parse_expression
    velox_serialization
    Folly::folly
    ${FOLLY_BENCHMARK}
    ${DOUBLE_CONVERSION}
    gflags::gflags
    glog::glog)

add_executable(velox_memory_alloc_benchmark MemoryAllocationBenchmark.cpp)
target_link_libraries(
  velox_memory_alloc_benchmark ${velox_benchmark_deps} velox_memory pthread)
