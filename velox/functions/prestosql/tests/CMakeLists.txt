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

add_library(velox_functions_test_lib FunctionBaseTest.cpp)

target_link_libraries(
  velox_functions_test_lib velox_exec velox_functions_prestosql
  velox_exec_test_util velox_vector_test_lib velox_parse_parser)

add_executable(
  velox_functions_test
  ArithmeticTest.cpp
  ArrayConstructorTest.cpp
  ArrayContainsTest.cpp
  ArrayFilterTest.cpp
  ArrayIntersectTest.cpp
  ArrayMaxTest.cpp
  ArrayMinTest.cpp
  CoalesceTest.cpp
  ComparisonsTest.cpp
  IsNullTest.cpp
  NotTest.cpp
  InPredicateTest.cpp
  CeilFloorTest.cpp
  JsonExtractScalarTest.cpp
  StringFunctionsTest.cpp
  DateTimeFunctionsTest.cpp
  ReduceTest.cpp
  RoundTest.cpp
  CardinalityTest.cpp
  MapKeysAndValuesTest.cpp
  MapTest.cpp
  MapConcatTest.cpp
  MapEntriesTest.cpp
  MapFilterTest.cpp
  TransformTest.cpp
  ElementAtTest.cpp
  WidthBucketArrayTest.cpp)

add_test(velox_functions_test velox_functions_test)

target_link_libraries(
  velox_functions_test
  velox_functions_test_lib
  velox_exec
  velox_functions_prestosql
  velox_functions_lib
  velox_exec_test_util
  velox_dwrf_test_utils
  ${GTEST_BOTH_LIBRARIES}
  ${gflags_LIBRARIES})
