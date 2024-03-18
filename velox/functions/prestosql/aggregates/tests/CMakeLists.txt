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
  velox_aggregates_test
  AggregationFunctionRegTest.cpp
  ApproxDistinctTest.cpp
  ApproxMostFrequentTest.cpp
  ApproxPercentileTest.cpp
  ArbitraryTest.cpp
  ArrayAggTest.cpp
  AverageAggregationTest.cpp
  BitwiseAggregationTest.cpp
  BoolAndOrTest.cpp
  CentralMomentsAggregationTest.cpp
  ChecksumAggregateTest.cpp
  CountAggregationTest.cpp
  CountDistinctTest.cpp
  CountIfAggregationTest.cpp
  CovarianceAggregationTest.cpp
  EntropyAggregationTest.cpp
  GeometricMeanTest.cpp
  HistogramTest.cpp
  Main.cpp
  MapAccumulatorTest.cpp
  MinMaxByAggregationTest.cpp
  MinMaxTest.cpp
  MultiMapAggTest.cpp
  PrestoHasherTest.cpp
  SetAggTest.cpp
  SetUnionTest.cpp
  SumTest.cpp
  MapAggTest.cpp
  MapUnionAggregationTest.cpp
  MapUnionSumTest.cpp
  ReduceAggTest.cpp
  ValueListTest.cpp
  VarianceAggregationTest.cpp
  MaxSizeForStatsTest.cpp
  SumDataSizeForStatsTest.cpp)

add_test(
  NAME velox_aggregates_test
  COMMAND velox_aggregates_test
  WORKING_DIRECTORY .)

target_link_libraries(
  velox_aggregates_test
  velox_aggregates
  velox_core
  velox_dwio_common_test_utils
  velox_exec
  velox_exec_test_lib
  velox_file
  velox_functions_aggregates
  velox_functions_aggregates_test_lib
  velox_functions_window_test_lib
  velox_functions_test_lib
  velox_functions_prestosql
  velox_functions_lib
  velox_hive_connector
  velox_simple_aggregate
  velox_type
  velox_vector_fuzzer
  velox_temp_path
  gflags::gflags
  gtest
  gtest_main)
