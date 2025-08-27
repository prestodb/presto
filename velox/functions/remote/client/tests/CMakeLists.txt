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

add_executable(velox_functions_remote_client_test RemoteFunctionTest.cpp)

add_test(velox_functions_remote_client_test velox_functions_remote_client_test)

target_link_libraries(
  velox_functions_remote_client_test
  remote_function_thrift
  velox_functions_remote_server
  velox_functions_remote
  velox_function_registry
  velox_functions_test_lib
  velox_exec_test_lib
  velox_functions_remote_utils
  GTest::gmock
  GTest::gtest
  GTest::gtest_main
)
