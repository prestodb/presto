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

add_executable(velox_cudf_order_by_test Main.cpp OrderByTest.cpp)
add_executable(velox_cudf_aggregation_test Main.cpp AggregationTest.cpp)

add_test(
  NAME velox_cudf_order_by_test
  COMMAND velox_cudf_order_by_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(
  NAME velox_cudf_aggregation_test
  COMMAND velox_cudf_aggregation_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

set_tests_properties(velox_cudf_order_by_test PROPERTIES LABELS cuda_driver
                                                         TIMEOUT 3000)
set_tests_properties(velox_cudf_aggregation_test PROPERTIES LABELS cuda_driver
                                                            TIMEOUT 3000)

target_link_libraries(
  velox_cudf_order_by_test
  velox_cudf_exec
  velox_exec
  velox_exec_test_lib
  velox_test_util
  gtest
  gtest_main
  fmt::fmt)

target_link_libraries(
  velox_cudf_aggregation_test
  velox_cudf_exec
  velox_exec
  velox_exec_test_lib
  velox_test_util
  velox_vector_fuzzer
  gtest
  gtest_main
  fmt::fmt)
