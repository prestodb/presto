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

set(CMAKE_WINDOW_TEST_LINK_LIBRARIES
    velox_core
    velox_exec
    velox_exec_test_lib
    velox_functions_spark_window
    velox_functions_window_test_lib
    velox_vector_fuzzer
    velox_vector_test_lib
    gflags::gflags
    gtest
    gtest_main)

set(CMAKE_WINDOW_TEST_MAIN_FILES Main.cpp)

add_executable(velox_spark_windows_test SparkWindowTest.cpp
                                        ${CMAKE_WINDOW_TEST_MAIN_FILES})

add_test(
  NAME velox_spark_windows_test
  COMMAND velox_spark_windows_test
  WORKING_DIRECTORY .)

target_link_libraries(
  velox_spark_windows_test ${CMAKE_WINDOW_TEST_LINK_LIBRARIES})
