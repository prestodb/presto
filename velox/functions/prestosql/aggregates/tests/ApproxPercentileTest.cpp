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
#include "velox/common/base/RandomUtil.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

namespace {

std::string
functionCall(bool keyed, bool weighted, double percentile, double accuracy) {
  std::ostringstream buf;
  int columnIndex = keyed;
  buf << "approx_percentile(c" << columnIndex++;
  if (weighted) {
    buf << ", c" << columnIndex++;
  }
  buf << ", " << percentile;
  if (accuracy > 0) {
    buf << ", " << accuracy;
  }
  buf << ')';
  return buf.str();
}

std::string label(
    const char* prefix,
    bool weighted,
    bool singleAgg,
    double percentile,
    double accuracy) {
  std::ostringstream buf;
  buf << prefix;
  if (weighted) {
    buf << "_weighted";
  }
  if (singleAgg) {
    buf << "_single_agg";
  }
  buf << '_' << percentile << "_pct";
  if (accuracy > 0) {
    buf << "_" << accuracy << "_accuracy";
  }
  return buf.str();
}

class ApproxPercentileTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    random::setSeed(0);
  }

  template <typename T>
  void testGlobalAgg(
      const VectorPtr& values,
      const VectorPtr& weights,
      double percentile,
      double accuracy,
      T expectedResult) {
    auto call = functionCall(false, weights.get(), percentile, accuracy);
    auto rows =
        weights ? makeRowVector({values, weights}) : makeRowVector({values});
    auto op =
        PlanBuilder().values({rows}).singleAggregation({}, {call}).planNode();
    assertQuery(
        op,
        fmt::format(
            "/* {} */ SELECT {}",
            label("global", weights.get(), false, percentile, accuracy),
            expectedResult));
    op = PlanBuilder()
             .values({rows})
             .partialAggregation({}, {call})
             .finalAggregation()
             .planNode();
    assertQuery(
        op,
        fmt::format(
            "/* {} */ SELECT {}",
            label("global", weights.get(), true, percentile, accuracy),
            expectedResult));
  }

  void testGroupByAgg(
      const VectorPtr& keys,
      const VectorPtr& values,
      const VectorPtr& weights,
      double percentile,
      double accuracy,
      const RowVectorPtr& expectedResult) {
    auto call = functionCall(true, weights.get(), percentile, accuracy);
    auto rows = weights ? makeRowVector({keys, values, weights})
                        : makeRowVector({keys, values});
    auto op =
        PlanBuilder().values({rows}).singleAggregation({0}, {call}).planNode();
    assertQuery(op, expectedResult);
    op = PlanBuilder()
             .values({rows})
             .partialAggregation({0}, {call})
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

  testGlobalAgg(values, nullptr, 0.5, -1, 11);
  testGlobalAgg(values, weights, 0.5, -1, 16);
  testGlobalAgg(values, nullptr, 0.5, 0.005, 11);
  testGlobalAgg(values, weights, 0.5, 0.005, 16);

  auto valuesWithNulls = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 23; }, nullEvery(7));
  auto weightsWithNulls = makeFlatVector<int64_t>(
      size, [](auto row) { return row % 23 + 1; }, nullEvery(11));

  testGlobalAgg(valuesWithNulls, nullptr, 0.5, -1, 11);
  testGlobalAgg(valuesWithNulls, weights, 0.5, -1, 16);
  testGlobalAgg(valuesWithNulls, weightsWithNulls, 0.5, -1, 16);
  testGlobalAgg(valuesWithNulls, nullptr, 0.5, 0.005, 11);
  testGlobalAgg(valuesWithNulls, weights, 0.5, 0.005, 16);
  testGlobalAgg(valuesWithNulls, weightsWithNulls, 0.5, 0.005, 16);
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
       makeFlatVector(std::vector<int32_t>{11, 12, 13, 14, 15, 16, 17})});
  testGroupByAgg(keys, values, nullptr, 0.5, -1, expectedResult);
  testGroupByAgg(keys, values, nullptr, 0.5, 0.005, expectedResult);

  expectedResult = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6}),
       makeFlatVector(std::vector<int32_t>{16, 17, 18, 19, 20, 21, 22})});
  testGroupByAgg(keys, values, weights, 0.5, -1, expectedResult);
  testGroupByAgg(keys, values, weights, 0.5, 0.005, expectedResult);

  auto valuesWithNulls = makeFlatVector<int32_t>(
      size, [](auto row) { return (row / 7) % 23 + row % 7; }, nullEvery(11));
  auto weightsWithNulls = makeFlatVector<int64_t>(
      size, [](auto row) { return (row / 7) % 23 + 1; }, nullEvery(13));

  expectedResult = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6}),
       makeFlatVector(std::vector<int32_t>{11, 12, 13, 14, 15, 16, 17})});
  testGroupByAgg(keys, valuesWithNulls, nullptr, 0.5, -1, expectedResult);
  testGroupByAgg(keys, valuesWithNulls, nullptr, 0.5, 0.005, expectedResult);

  expectedResult = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6}),
       makeFlatVector(std::vector<int32_t>{15, 17, 18, 18, 19, 21, 22})});
  testGroupByAgg(
      keys, valuesWithNulls, weightsWithNulls, 0.5, -1, expectedResult);
  testGroupByAgg(
      keys, valuesWithNulls, weightsWithNulls, 0.5, 0.005, expectedResult);
}

// Test large values of "weight" parameter used in global aggregation.
TEST_F(ApproxPercentileTest, largeWeightsGlobal) {
  vector_size_t size = 1'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 23; });

  // All weights are very large and are about the same.
  auto weights = makeFlatVector<int64_t>(
      size, [](auto row) { return 1'000'000 + row % 23 + 1; });
  testGlobalAgg(values, weights, 0.5, -1, 11);

  // Weights are large, but different.
  weights = makeFlatVector<int64_t>(
      size, [](auto row) { return 1'000 * (row % 23 + 1); });
  testGlobalAgg(values, weights, 0.5, -1, 16);
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
       makeFlatVector(std::vector<int32_t>{11, 12, 13, 14, 15, 16, 17})});
  testGroupByAgg(keys, values, weights, 0.5, -1, expectedResult);

  // Weights are large, but different.
  weights = makeFlatVector<int64_t>(
      size, [](auto row) { return 1'000 * ((row / 7) % 23 + 1); });
  expectedResult = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6}),
       makeFlatVector(std::vector<int32_t>{16, 17, 18, 19, 20, 21, 22})});
  testGroupByAgg(keys, values, weights, 0.5, -1, expectedResult);
}

} // namespace
} // namespace facebook::velox::aggregate::test
