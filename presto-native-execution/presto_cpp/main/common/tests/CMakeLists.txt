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
add_library(presto_mutable_configs MutableConfigs.cpp)

target_link_libraries(
  presto_mutable_configs
  presto_common
  velox_file)

add_executable(presto_common_test CommonTest.cpp ConfigTest.cpp)

add_test(presto_common_test presto_common_test)

target_link_libraries(
  presto_common_test
  presto_common
  presto_exception
  velox_aggregates
  velox_exception
  velox_file
  velox_functions_prestosql
  velox_function_registry
  velox_presto_types
  velox_window
  ${RE2}
  GTest::gtest
  GTest::gtest_main)

set_property(TARGET presto_common_test PROPERTY JOB_POOL_LINK
                                                presto_link_job_pool)
