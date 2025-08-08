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

add_executable(velox_local_runner_test LocalRunnerTest.cpp Main.cpp)

add_test(velox_local_runner_test velox_local_runner_test)

target_link_libraries(
  velox_local_runner_test
  velox_exec_runner_test_util
  velox_exec_test_lib
  velox_parse_parser
  velox_parse_expression
  GTest::gtest)

add_library(velox_presto_query_replay_runner PrestoQueryReplayRunner.cpp)

target_link_libraries(
  velox_presto_query_replay_runner velox_local_runner velox_hive_connector
  velox_exec_test_lib)

add_executable(velox_presto_query_replay_runner_test
               PrestoQueryReplayRunnerTest.cpp Main.cpp)

add_test(velox_presto_query_replay_runner_test
         velox_presto_query_replay_runner_test)

target_link_libraries(
  velox_presto_query_replay_runner_test
  velox_presto_query_replay_runner
  velox_exec_test_lib
  velox_exec
  GTest::gtest)
