/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::functions::aggregate::sparksql::test {

namespace {

class CollectListAggregateTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("spark_");
  }
};

TEST_F(CollectListAggregateTest, groupBy) {
  std::vector<RowVectorPtr> batches;
  // Creating 3 batches of input data.
  // 0: {0, null} {0, 1}    {0, 2}
  // 1: {1, 1}    {1, null} {1, 3}
  // 2: {2, 2}    {2, 3}    {2, null}
  // 3: {3, 3}    {3, 4}    {3, 5}
  // 4: {4, 4}    {4, 5}    {4, 6}
  for (auto i = 0; i < 3; i++) {
    RowVectorPtr data = makeRowVector(
        {makeFlatVector<int32_t>({0, 1, 2, 3, 4}),
         makeFlatVector<int64_t>(
             5,
             [&i](const vector_size_t& row) { return i + row; },
             [&i](const auto& row) { return i == row; })});
    batches.push_back(data);
  }

  auto expected = makeRowVector(
      {makeFlatVector<int32_t>({0, 1, 2, 3, 4}),
       makeArrayVectorFromJson<int64_t>(
           {"[1, 2]", "[1, 3]", "[2, 3]", "[3, 4, 5]", "[4, 5, 6]"})});

  testAggregations(
      batches,
      {"c0"},
      {"spark_collect_list(c1)"},
      {"c0", "array_sort(a0)"},
      {expected});
  testAggregationsWithCompanion(
      batches,
      [](auto& /*builder*/) {},
      {"c0"},
      {"spark_collect_list(c1)"},
      {{BIGINT()}},
      {"c0", "array_sort(a0)"},
      {expected},
      {});
}

TEST_F(CollectListAggregateTest, global) {
  auto data = makeRowVector({makeNullableFlatVector<int32_t>(
      {std::nullopt, 1, 2, std::nullopt, 4, 5})});
  auto expected =
      makeRowVector({makeArrayVectorFromJson<int32_t>({"[1, 2, 4, 5]"})});

  testAggregations(
      {data}, {}, {"spark_collect_list(c0)"}, {"array_sort(a0)"}, {expected});
  testAggregationsWithCompanion(
      {data},
      [](auto& /*builder*/) {},
      {},
      {"spark_collect_list(c0)"},
      {{INTEGER()}},
      {"array_sort(a0)"},
      {expected});
}

TEST_F(CollectListAggregateTest, ignoreNulls) {
  auto input = makeRowVector({makeNullableFlatVector<int32_t>(
      {1, 2, std::nullopt, 4, std::nullopt, 6})});
  // Spark will ignore all null values in the input.
  auto expected =
      makeRowVector({makeArrayVectorFromJson<int32_t>({"[1, 2, 4, 6]"})});
  testAggregations(
      {input}, {}, {"spark_collect_list(c0)"}, {"array_sort(a0)"}, {expected});
  testAggregationsWithCompanion(
      {input},
      [](auto& /*builder*/) {},
      {},
      {"spark_collect_list(c0)"},
      {{INTEGER()}},
      {"array_sort(a0)"},
      {expected},
      {});
}

TEST_F(CollectListAggregateTest, allNullsInput) {
  auto input = makeRowVector({makeAllNullFlatVector<int64_t>(100)});
  // If all input data is null, Spark will output an empty array.
  auto expected = makeRowVector({makeArrayVectorFromJson<int32_t>({"[]"})});
  testAggregations({input}, {}, {"spark_collect_list(c0)"}, {expected});
  testAggregationsWithCompanion(
      {input},
      [](auto& /*builder*/) {},
      {},
      {"spark_collect_list(c0)"},
      {{BIGINT()}},
      {},
      {expected},
      {});
}
} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test
