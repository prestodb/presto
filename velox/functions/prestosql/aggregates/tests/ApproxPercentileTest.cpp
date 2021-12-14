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
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

namespace {

class ApproxPercentileTest : public AggregationTestBase {
 protected:
  template <typename T>
  void
  testGlobalAgg(const VectorPtr& values, double percentile, T expectedResult) {
    auto op =
        PlanBuilder()
            .values({makeRowVector({values})})
            .singleAggregation(
                {}, {fmt::format("approx_percentile(c0, {})", percentile)})
            .planNode();

    assertQuery(
        op,
        fmt::format(
            "/* global_single_agg_{}_pct */ SELECT {}",
            percentile,
            expectedResult));

    op = PlanBuilder()
             .values({makeRowVector({values})})
             .partialAggregation(
                 {}, {fmt::format("approx_percentile(c0, {})", percentile)})
             .finalAggregation()
             .planNode();

    assertQuery(
        op,
        fmt::format(
            "/* global_{}_pct */ SELECT {}", percentile, expectedResult));
  }

  template <typename T>
  void testGlobalAgg(
      const VectorPtr& values,
      const VectorPtr& weights,
      double percentile,
      T expectedResult) {
    auto op =
        PlanBuilder()
            .values({makeRowVector({values, weights})})
            .singleAggregation(
                {}, {fmt::format("approx_percentile(c0, c1, {})", percentile)})
            .planNode();

    assertQuery(
        op,
        fmt::format(
            "/* global_weighted_single_agg_{}_pct*/ SELECT {}",
            percentile,
            expectedResult));

    op = PlanBuilder()
             .values({makeRowVector({values, weights})})
             .partialAggregation(
                 {}, {fmt::format("approx_percentile(c0, c1, {})", percentile)})
             .finalAggregation()
             .planNode();

    assertQuery(
        op,
        fmt::format(
            "SELECT /* global_weighted_{}_pct*/ {}",
            percentile,
            expectedResult));
  }

  void testGroupByAgg(
      const VectorPtr& keys,
      const VectorPtr& values,
      double percentile,
      const RowVectorPtr& expectedResult) {
    auto rowVector = makeRowVector({keys, values});

    auto op =
        PlanBuilder()
            .values({rowVector})
            .singleAggregation(
                {0}, {fmt::format("approx_percentile(c1, {})", percentile)})
            .planNode();

    assertQuery(op, expectedResult);

    op = PlanBuilder()
             .values({rowVector})
             .partialAggregation(
                 {0}, {fmt::format("approx_percentile(c1, {})", percentile)})
             .finalAggregation()
             .planNode();

    assertQuery(op, expectedResult);
  }

  void testGroupByAgg(
      const VectorPtr& keys,
      const VectorPtr& values,
      const VectorPtr& weights,
      double percentile,
      const RowVectorPtr& expectedResult) {
    auto rowVector = makeRowVector({keys, values, weights});

    auto op =
        PlanBuilder()
            .values({rowVector})
            .singleAggregation(
                {0}, {fmt::format("approx_percentile(c1, c2, {})", percentile)})
            .planNode();

    assertQuery(op, expectedResult);

    op =
        PlanBuilder()
            .values({rowVector})
            .partialAggregation(
                {0}, {fmt::format("approx_percentile(c1, c2, {})", percentile)})
            .finalAggregation()
            .planNode();

    assertQuery(op, expectedResult);
  }
};

TEST_F(ApproxPercentileTest, globalAgg) {
  vector_size_t size = 1'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 23; });
  auto weights =
      makeFlatVector<int64_t>(size, [](auto row) { return row % 23 + 1; });

  testGlobalAgg(values, 0.5, 10);
  testGlobalAgg(values, weights, 0.5, 15);

  auto valuesWithNulls = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 23; }, nullEvery(7));
  auto weightsWithNulls = makeFlatVector<int64_t>(
      size, [](auto row) { return row % 23 + 1; }, nullEvery(11));

  testGlobalAgg(valuesWithNulls, 0.5, 10);
  testGlobalAgg(valuesWithNulls, weights, 0.5, 15);
  testGlobalAgg(valuesWithNulls, weightsWithNulls, 0.5, 15);
}

TEST_F(ApproxPercentileTest, groupByAgg) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 7; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return (row / 7) % 23 + row % 7; });
  auto weights = makeFlatVector<int64_t>(
      size, [](auto row) { return (row / 7) % 23 + 1; });

  auto expectedResult = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6}),
       makeFlatVector(std::vector<int32_t>{10, 11, 12, 13, 14, 15, 16})});
  testGroupByAgg(keys, values, 0.5, expectedResult);

  expectedResult = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6}),
       makeFlatVector(std::vector<int32_t>{15, 16, 17, 18, 19, 20, 21})});
  testGroupByAgg(keys, values, weights, 0.5, expectedResult);

  auto valuesWithNulls = makeFlatVector<int32_t>(
      size, [](auto row) { return (row / 7) % 23 + row % 7; }, nullEvery(11));
  auto weightsWithNulls = makeFlatVector<int64_t>(
      size, [](auto row) { return (row / 7) % 23 + 1; }, nullEvery(13));

  expectedResult = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6}),
       makeFlatVector(std::vector<int32_t>{10, 11, 12, 13, 14, 15, 16})});
  testGroupByAgg(keys, valuesWithNulls, 0.5, expectedResult);

  expectedResult = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6}),
       makeFlatVector(std::vector<int32_t>{15, 16, 17, 18, 19, 20, 22})});
  testGroupByAgg(keys, valuesWithNulls, weightsWithNulls, 0.5, expectedResult);
}

// Test large values of "weight" parameter used in global aggregation.
TEST_F(ApproxPercentileTest, largeWeightsGlobal) {
  vector_size_t size = 1'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 23; });

  // All weights are very large and are about the same.
  auto weights = makeFlatVector<int64_t>(
      size, [](auto row) { return 1'000'000 + row % 23 + 1; });
  testGlobalAgg(values, weights, 0.5, 10);

  // Weights are large, but different.
  weights = makeFlatVector<int64_t>(
      size, [](auto row) { return 1'000 * (row % 23 + 1); });
  testGlobalAgg(values, weights, 0.5, 15);
}

// Test large values of "weight" parameter used in group-by.
TEST_F(ApproxPercentileTest, largeWeightsGroupBy) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 7; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return (row / 7) % 23 + row % 7; });

  // All weights are very large and are about the same.
  auto weights = makeFlatVector<int64_t>(
      size, [](auto row) { return 1'000'000 + (row / 7) % 23 + 1; });

  auto expectedResult = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6}),
       makeFlatVector(std::vector<int32_t>{10, 11, 12, 13, 14, 15, 16})});
  testGroupByAgg(keys, values, weights, 0.5, expectedResult);

  // Weights are large, but different.
  weights = makeFlatVector<int64_t>(
      size, [](auto row) { return 1'000 * ((row / 7) % 23 + 1); });
  expectedResult = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6}),
       makeFlatVector(std::vector<int32_t>{15, 16, 17, 18, 19, 20, 21})});
  testGroupByAgg(keys, values, weights, 0.5, expectedResult);
}

} // namespace
} // namespace facebook::velox::aggregate::test
