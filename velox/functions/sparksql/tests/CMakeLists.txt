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
  ArrayFlattenTest.cpp
  ArrayMaxTest.cpp
  ArrayMinTest.cpp
  ArraySizeTest.cpp
  ArraySortTest.cpp
  BitwiseTest.cpp
  ComparisonsTest.cpp
  DateTimeFunctionsTest.cpp
  DecimalArithmeticTest.cpp
  DecimalCompareTest.cpp
  DecimalRoundTest.cpp
  DecimalUtilTest.cpp
  ElementAtTest.cpp
  HashTest.cpp
  InTest.cpp
  LeastGreatestTest.cpp
  MakeDecimalTest.cpp
  MakeTimestampTest.cpp
  MapTest.cpp
  MightContainTest.cpp
  MonotonicallyIncreasingIdTest.cpp
  RandTest.cpp
  RegexFunctionsTest.cpp
  SizeTest.cpp
  SortArrayTest.cpp
  SparkCastExprTest.cpp
  SparkPartitionIdTest.cpp
  SplitFunctionsTest.cpp
  StringTest.cpp
  StringToMapTest.cpp
  UnscaledValueFunctionTest.cpp
  UuidTest.cpp
  XxHash64Test.cpp)

add_test(velox_functions_spark_test velox_functions_spark_test)

target_link_libraries(
  velox_functions_spark_test
  velox_functions_spark
  velox_functions_test_lib
  velox_exec_test_lib
  fmt::fmt
  gtest
  gtest_main
  gmock
  gflags::gflags)
