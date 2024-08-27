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

add_executable(velox_exec_trace_test QueryTraceTest.cpp)

add_test(
  NAME velox_exec_trace_test
  COMMAND velox_exec_trace_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

set_tests_properties(velox_exec_trace_test PROPERTIES TIMEOUT 3000)

target_link_libraries(
  velox_exec_trace_test
  velox_exec
  velox_exec_test_lib
  velox_memory
  velox_query_trace_exec
  velox_query_trace_retrieve
  velox_vector_fuzzer
  GTest::gtest_main
  GTest::gmock
  Folly::folly
  gflags::gflags
  glog::glog
  fmt::fmt
  ${FILESYSTEM})
