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
    ${FOLLY}
    ${FOLLY_BENCHMARK}
    ${DOUBLE_CONVERSION}
    ${GTEST_BOTH_LIBRARIES}
    ${gflags_LIBRARIES}
    glog::glog)

add_executable(velox_benchmark_basic_simple_arithmetic SimpleArithmetic.cpp)
target_link_libraries(velox_benchmark_basic_simple_arithmetic
                      ${velox_benchmark_deps})

add_executable(velox_benchmark_basic_comparison_conjunct ComparisonConjunct.cpp)
target_link_libraries(velox_benchmark_basic_comparison_conjunct
                      ${velox_benchmark_deps})

add_executable(velox_benchmark_basic_decoded_vector DecodedVector.cpp)
target_link_libraries(velox_benchmark_basic_decoded_vector
                      ${velox_benchmark_deps})
