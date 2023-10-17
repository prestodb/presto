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

add_executable(presto_functions_test KeySamplingPercentFunctionTests.cpp)

add_test(presto_functions_test presto_functions_test)

target_link_libraries(
  presto_functions_test
  presto_functions
  velox_type
  velox_exec
  velox_expression
  velox_functions_lib
  velox_exec_test_lib
  velox_functions_test_lib
  velox_dwio_common_test_utils
  velox_vector_fuzzer
  gtest
  gtest_main
  gflags::gflags
  gmock
  gmock_main
  simdjson::simdjson
  presto_type_converter
  ${ANTLR4_RUNTIME})

  set_property(TARGET presto_functions_test PROPERTY JOB_POOL_COMPILE presto_link_job_pool)


