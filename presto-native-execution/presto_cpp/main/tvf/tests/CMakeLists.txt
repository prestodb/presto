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
add_library(presto_tvf_plan_builder PlanBuilder.cpp)

target_link_libraries(presto_tvf_plan_builder presto_tvf_core
                      presto_tvf_functions presto_tvf_spi velox_core)

add_executable(presto_tvf_test ExcludeColumnsTest.cpp PlanNodeSerdeTest.cpp
                               SequenceTest.cpp)

add_test(presto_tvf_test presto_tvf_test)

target_link_libraries(
  presto_tvf_test
  presto_tvf_core
  presto_tvf_exec
  presto_tvf_functions
  presto_tvf_plan_builder
  presto_tvf_spi
  velox_vector_fuzzer
  velox_exec_test_lib
  velox_vector_test_lib
  velox_type
  velox_vector
  velox_exec
  velox_memory
  velox_exec
  GTest::gmock
  GTest::gtest
  GTest::gtest_main)

set_property(TARGET presto_tvf_test PROPERTY JOB_POOL_LINK presto_link_job_pool)
