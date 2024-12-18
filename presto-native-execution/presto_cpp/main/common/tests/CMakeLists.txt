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

add_executable(presto_common_BaseVeloxQueryConfigTest BaseVeloxQueryConfigTest.cpp)
add_executable(presto_common_CommonTest CommonTest.cpp)
add_executable(presto_common_ConfigTest ConfigTest.cpp)

add_test(presto_common_BaseVeloxQueryConfigTest presto_common_BaseVeloxQueryConfigTest)
add_test(presto_common_CommonTest presto_common_CommonTest)
add_test(presto_common_ConfigTest presto_common_ConfigTest)

target_link_libraries(
        presto_common_CommonTest
        presto_common_Utils
        presto_exception
        ${RE2}
        velox_exception
        GTest::gtest
        GTest::gtest_main)

target_link_libraries(
        presto_common_ConfigTest
        presto_common_ConfigReader
        presto_common_Configs
        ${RE2}
        velox_exception
        velox_file
        GTest::gtest
        GTest::gtest_main)

target_link_libraries(
        presto_common_BaseVeloxQueryConfigTest
        presto_common_ConfigReader
        presto_common_Configs
        ${RE2}
        velox_core
        velox_exception
        velox_file
        GTest::gtest
        GTest::gtest_main)
