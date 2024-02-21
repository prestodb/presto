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
add_executable(presto_metrics_test PrometheusStatsReporterTest.cpp)

add_test(
        NAME presto_server_test
        COMMAND presto_server_test
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
        presto_metrics_test
        presto_metrics
        velox_vector_fuzzer
        velox_exec_test_lib
        velox_hive_partition_function
        velox_vector_test_lib
        velox_type
        velox_vector
        velox_exec
        velox_memory
        velox_exec
        gmock
        gtest
        gtest_main)
