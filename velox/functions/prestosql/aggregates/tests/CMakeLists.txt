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

add_library(velox_aggregates_test_lib AggregationTestBase.cpp)

target_link_libraries(velox_aggregates_test_lib velox_exec_test_util)

add_executable(
  velox_aggregates_test
  ApproxDistinctTest.cpp
  ApproxMostFrequentTest.cpp
  ApproxPercentileTest.cpp
  ArbitraryTest.cpp
  ArrayAggTest.cpp
  AverageAggregationTest.cpp
  BitwiseAggregationTest.cpp
  BoolAndOrTest.cpp
  ChecksumAggregateTest.cpp
  CountAggregationTest.cpp
  CountIfAggregationTest.cpp
  CovarianceAggregationTest.cpp
  HistogramTest.cpp
  Main.cpp
  MinMaxByAggregationTest.cpp
  MinMaxTest.cpp
  PrestoHasherTest.cpp
  SumTest.cpp
  MapAggTest.cpp
  MapUnionAggregationTest.cpp
  ValueListTest.cpp
  VarianceAggregationTest.cpp
  MaxSizeForStatsTest.cpp)

add_test(
  NAME velox_aggregates_test
  COMMAND velox_aggregates_test
  WORKING_DIRECTORY .)

target_link_libraries(
  velox_aggregates_test
  velox_aggregates
  velox_aggregates_test_lib
  velox_core
  velox_dwio_common_test_utils
  velox_exec
  velox_exec_test_util
  velox_file
  velox_functions_test_lib
  velox_functions_prestosql
  velox_functions_lib
  velox_hive_connector
  velox_type
  ${gflags_LIBRARIES}
  gtest
  gtest_main)
