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

add_subdirectory(utils)

add_executable(
  velox_functions_test
  ArithmeticTest.cpp
  ArrayAllMatchTest.cpp
  ArrayAnyMatchTest.cpp
  ArrayAverageTest.cpp
  ArrayCombinationsTest.cpp
  ArrayConcatTest.cpp
  ArrayConstructorTest.cpp
  ArrayContainsTest.cpp
  ArrayDistinctTest.cpp
  ArrayDuplicatesTest.cpp
  ArrayExceptTest.cpp
  ArrayFilterTest.cpp
  ArrayFrequencyTest.cpp
  ArrayFlattenTest.cpp
  ArrayJoinTest.cpp
  ArrayHasDuplicatesTest.cpp
  ArrayIntersectTest.cpp
  ArrayMaxTest.cpp
  ArrayMinTest.cpp
  ArrayNGramsTest.cpp
  ArrayNoneMatchTest.cpp
  ArrayNormalizeTest.cpp
  ArrayPositionTest.cpp
  ArrayRemoveTest.cpp
  ArrayRemoveNullsTest.cpp
  ArraySortTest.cpp
  ArrayShuffleTest.cpp
  ArraysOverlapTest.cpp
  ArraySumTest.cpp
  ArrayTrimTest.cpp
  ArrayUnionTest.cpp
  BinaryFunctionsTest.cpp
  BitwiseTest.cpp
  CardinalityTest.cpp
  CeilFloorTest.cpp
  ComparisonsTest.cpp
  DateTimeFunctionsTest.cpp
  DecimalArithmeticTest.cpp
  ElementAtTest.cpp
  FindFirstTest.cpp
  FromUtf8Test.cpp
  HyperLogLogCastTest.cpp
  HyperLogLogFunctionsTest.cpp
  InPredicateTest.cpp
  JsonCastTest.cpp
  JsonExtractScalarTest.cpp
  JsonFunctionsTest.cpp
  MapEntriesTest.cpp
  MapFilterTest.cpp
  MapFromEntriesTest.cpp
  MultimapFromEntriesTest.cpp
  MapKeysAndValuesTest.cpp
  MapMatchTest.cpp
  MapTest.cpp
  MapZipWithTest.cpp
  NotTest.cpp
  ProbabilityTest.cpp
  RandTest.cpp
  ReduceTest.cpp
  RegexpReplaceTest.cpp
  ReverseTest.cpp
  RoundTest.cpp
  ScalarFunctionRegTest.cpp
  SimpleComparisonMatcherTest.cpp
  SliceTest.cpp
  SequenceTest.cpp
  SplitTest.cpp
  SplitToMapTest.cpp
  StringFunctionsTest.cpp
  TimestampWithTimeZoneCastTest.cpp
  TransformTest.cpp
  TransformKeysTest.cpp
  TransformValuesTest.cpp
  TrimFunctionsTest.cpp
  URLFunctionsTest.cpp
  Utf8Test.cpp
  WidthBucketArrayTest.cpp
  GreatestLeastTest.cpp
  ZipTest.cpp
  ZipWithTest.cpp)

add_test(velox_functions_test velox_functions_test)

target_link_libraries(
  velox_functions_test
  velox_functions_test_lib
  velox_exec
  velox_expression
  velox_functions_prestosql
  velox_functions_lib
  velox_exec_test_lib
  velox_dwio_common_test_utils
  velox_vector_fuzzer
  gtest
  gtest_main
  gflags::gflags
  gmock
  gmock_main)
