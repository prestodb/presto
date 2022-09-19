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
add_executable(presto_operators_test UnsaferowShuffleTest.cpp)

add_test(presto_operators_test presto_operators_test)

target_link_libraries(
  presto_operators_test
  presto_operators
  velox_vector_fuzzer
  velox_exec_test_lib
  velox_vector_test_lib
  velox_type
  velox_vector
  velox_exec
  velox_memory
  velox_exec
  gtest
  gtest_main)
