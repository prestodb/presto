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

add_executable(
  velox_functions_spark_test
  ArithmeticTest.cpp
  ArraySortTest.cpp
  CompareNullSafeTests.cpp
  HashTest.cpp
  InTest.cpp
  LeastGreatestTest.cpp
  MapTest.cpp
  RegexFunctionsTest.cpp
  SizeTest.cpp
  SortArrayTest.cpp
  SplitFunctionsTest.cpp
  StringTest.cpp
  SubscriptTest.cpp)

add_test(velox_functions_spark_test velox_functions_spark_test)

target_link_libraries(
  velox_functions_spark_test
  velox_functions_spark
  velox_functions_test_lib
  velox_exec_test_lib
  ${FMT}
  gtest
  gtest_main
  gmock
  ${gflags_LIBRARIES})
