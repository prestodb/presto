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

if(${PRESTO_ENABLE_TESTING})
  add_subdirectory(server)
endif()

add_executable(presto_functions_rest_test RemoteFunctionRestTest.cpp)

add_test(presto_functions_rest_test presto_functions_rest_test)

target_link_libraries(
  presto_functions_rest_test
  presto_functions_rest_client
  presto_functions_rest_server
  presto_functions_remote
  velox_functions_test_lib
  GTest::gmock
  GTest::gtest
  GTest::gtest_main
)

add_executable(presto_rest_function_registration_test PrestoRestFunctionRegistrationTest.cpp)

add_test(presto_rest_function_registration_test presto_rest_function_registration_test)

target_link_libraries(
  presto_rest_function_registration_test
  presto_to_velox_remote_functions
  presto_functions_remote
  presto_protocol
  GTest::gtest
  GTest::gtest_main
)
