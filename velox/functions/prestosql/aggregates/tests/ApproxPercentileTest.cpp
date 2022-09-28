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

std::string functionCall(
    bool keyed,
    bool weighted,
    double percentile,
    double accuracy,
    int percentileCount) {
  std::ostringstream buf;
  int columnIndex = keyed;
  buf << "approx_percentile(c" << columnIndex++;
  if (weighted) {
    buf << ", c" << columnIndex++;
  }
  buf << ", ";
  if (percentileCount == -1) {
    buf << percentile;
  } else {
    buf << "ARRAY[";
    for (int i = 0; i < percentileCount; ++i) {
      buf << (i == 0 ? "" : ",") << percentile;
    }
    buf << ']';
  }
  if (accuracy > 0) {
    buf << ", " << accuracy;
  }
  buf << ')';
  return buf.str();
}

class ApproxPercentileTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    random::setSeed(0);
    allowInputShuffle();
  }

  template <typename T>
  void testGlobalAgg(
      const VectorPtr& values,
      const VectorPtr& weights,
      double percentile,
      double accuracy,
      T expectedResult) {
    SCOPED_TRACE(fmt::format(
        "weighted={} percentile={} accuracy={}",
        weights != nullptr,
        percentile,
        accuracy));
    auto rows =
        weights ? makeRowVector({values, weights}) : makeRowVector({values});
    testAggregations(
        {rows},
        {},
        {functionCall(false, weights.get(), percentile, accuracy, -1)},
        fmt::format("SELECT {}", expectedResult));
    testAggregations(
        {rows},
        {},
        {functionCall(false, weights.get(), percentile, accuracy, 3)},
        fmt::format("SELECT ARRAY[{0},{0},{0}]", expectedResult));
  }

  void testGroupByAgg(
      const VectorPtr& keys,
      const VectorPtr& values,
      const VectorPtr& weights,
      double percentile,
      double accuracy,
      const RowVectorPtr& expectedResult) {
    auto rows = weights ? makeRowVector({keys, values, weights})
                        : makeRowVector({keys, values});
    testAggregations(
        {rows},
        {"c0"},
        {functionCall(true, weights.get(), percentile, accuracy, -1)},
        {expectedResult});
    {
      SCOPED_TRACE("Percentile array");
      auto resultValues = expectedResult->childAt(1);
      auto elements = BaseVector::create(
          resultValues->type(), 3 * resultValues->size(), pool());
      auto offsets = allocateOffsets(resultValues->size(), pool());
      auto rawOffsets = offsets->asMutable<vector_size_t>();
      auto sizes = allocateSizes(resultValues->size(), pool());
      auto rawSizes = sizes->asMutable<vector_size_t>();
      for (int i = 0; i < resultValues->size(); ++i) {
        rawOffsets[i] = 3 * i;
        rawSizes[i] = 3;
        elements->copy(resultValues.get(), 3 * i + 0, i, 1);
        elements->copy(resultValues.get(), 3 * i + 1, i, 1);
        elements->copy(resultValues.get(), 3 * i + 2, i, 1);
      }
      auto expected = makeRowVector(
          {expectedResult->childAt(0),
           std::make_shared<ArrayVector>(
               pool(),
               ARRAY(elements->type()),
               nullptr,
               resultValues->size(),
               offsets,
               sizes,
               elements)});
      testAggregations(
          {rows},
          {"c0"},
          {functionCall(true, weights.get(), percentile, accuracy, 3)},
          {expected});
    }
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

/// Repro of "decodedPercentile_.isConstantMapping() Percentile argument must be
/// constant for all input rows" error caused by (1) HashAggregation keeping a
/// reference to input vectors when partial aggregation ran out of memory; (2)
/// EvalCtx::moveOrCopyResult needlessly flattening constant vector result of a
/// constant expression.
TEST_F(ApproxPercentileTest, partialFull) {
  // Make sure partial aggregation runs out of memory after first batch.
  CursorParameters params;
  params.queryCtx = core::QueryCtx::createForTest();
  params.queryCtx->setConfigOverridesUnsafe({
      {core::QueryConfig::kMaxPartialAggregationMemory, "300000"},
  });

  auto data = {
      makeRowVector({
          makeFlatVector<int32_t>(1'024, [](auto row) { return row % 117; }),
          makeFlatVector<int32_t>(1'024, [](auto /*row*/) { return 10; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(1'024, [](auto row) { return row % 5; }),
          makeFlatVector<int32_t>(1'024, [](auto /*row*/) { return 15; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(1'024, [](auto row) { return row % 7; }),
          makeFlatVector<int32_t>(1'024, [](auto /*row*/) { return 20; }),
      }),
  };

  params.planNode =
      PlanBuilder()
          .values(data)
          .project({"c0", "c1", "1", "0.9995", "0.001"})
          .partialAggregation({"c0"}, {"approx_percentile(c1, p2, p3, p4)"})
          .finalAggregation()
          .planNode();

  auto expected = makeRowVector({
      makeFlatVector<int32_t>(117, [](auto row) { return row; }),
      makeFlatVector<int32_t>(117, [](auto row) { return row < 7 ? 20 : 10; }),
  });
  exec::test::assertQuery(params, {expected});
}

TEST_F(ApproxPercentileTest, finalAggregateAccuracy) {
  auto batch = makeRowVector(
      {makeFlatVector<int32_t>(1000, [](auto row) { return row; })});
  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  std::vector<std::shared_ptr<const core::PlanNode>> sources;
  for (int i = 0; i < 10; ++i) {
    sources.push_back(
        PlanBuilder(planNodeIdGenerator)
            .values({batch})
            .partialAggregation({}, {"approx_percentile(c0, 0.005, 0.0001)"})
            .planNode());
  }
  auto op = PlanBuilder(planNodeIdGenerator)
                .localPartitionRoundRobin(sources)
                .finalAggregation()
                .planNode();
  assertQuery(op, "SELECT 5");
}

} // namespace
} // namespace facebook::velox::aggregate::test
