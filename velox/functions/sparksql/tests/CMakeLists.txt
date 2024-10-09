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
  ArrayGetTest.cpp
  ArrayInsertTest.cpp
  ArrayMaxTest.cpp
  ArrayMinTest.cpp
  ArraySortTest.cpp
  ArrayShuffleTest.cpp
  ArgGeneratorTest.cpp
  AtLeastNNonNullsTest.cpp
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
  JsonObjectKeysTest.cpp
  LeastGreatestTest.cpp
  MakeDecimalTest.cpp
  MakeTimestampTest.cpp
  MapTest.cpp
  MaskTest.cpp
  MightContainTest.cpp
  MonotonicallyIncreasingIdTest.cpp
  RaiseErrorTest.cpp
  RandTest.cpp
  RegexFunctionsTest.cpp
  SliceTest.cpp
  SizeTest.cpp
  SortArrayTest.cpp
  SparkCastExprTest.cpp
  SparkPartitionIdTest.cpp
  SplitTest.cpp
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
  velox_expression_fuzzer_test_utility
  velox_expression_fuzzer
  fmt::fmt
  GTest::gtest
  GTest::gtest_main
  GTest::gmock
  gflags::gflags)

add_executable(velox_spark_function_registry_test RegisterTest.cpp)

add_test(velox_spark_function_registry_test velox_spark_function_registry_test)

target_link_libraries(
  velox_spark_function_registry_test
  velox_expression
  velox_functions_spark
  GTest::gtest
  GTest::gtest_main)
