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
  velox_row_test UnsafeRowSerializerTest.cpp UnsafeRowDeserializerTest.cpp
                 UnsafeRowFuzzTests.cpp)

add_test(velox_row_test velox_row_test)

target_link_libraries(
  velox_row_test
  velox_row
  velox_exec
  velox_exec_test_util
  velox_functions_lib
  velox_functions_prestosql
  velox_type
  velox_vector
  velox_vector_fuzzer
  velox_vector_test_lib
  ${FOLLY}
  ${DOUBLE_CONVERSION}
  gtest
  gtest_main
  ${GFLAGS_LIBRARIES}
  glog::glog)
