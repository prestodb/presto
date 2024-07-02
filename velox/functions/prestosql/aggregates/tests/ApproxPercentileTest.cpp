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
#include <algorithm>

#include "velox/common/base/RandomUtil.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/lib/window/tests/WindowTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;
using namespace facebook::velox::window::test;

namespace facebook::velox::aggregate::test {

namespace {

// Return the argument types of an aggregation when the aggregation is
// constructed by `functionCall` with the given dataType, weighted, accuracy,
// and percentileCount.
std::vector<TypePtr> getArgTypes(
    const TypePtr& dataType,
    bool weighted,
    double accuracy,
    int percentileCount) {
  std::vector<TypePtr> argTypes;
  argTypes.push_back(dataType);
  if (weighted) {
    argTypes.push_back(BIGINT());
  }
  if (percentileCount == -1) {
    argTypes.push_back(DOUBLE());
  } else {
    argTypes.push_back(ARRAY(DOUBLE()));
  }
  if (accuracy > 0) {
    argTypes.push_back(DOUBLE());
  }
  return argTypes;
}

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
  }

  template <typename T>
  void testGlobalAgg(
      const VectorPtr& values,
      const VectorPtr& weights,
      double percentile,
      double accuracy,
      std::optional<T> expectedResult) {
    SCOPED_TRACE(fmt::format(
        "weighted={} percentile={} accuracy={}",
        weights != nullptr,
        percentile,
        accuracy));
    auto rows =
        weights ? makeRowVector({values, weights}) : makeRowVector({values});

    auto expected = expectedResult.has_value()
        ? fmt::format("SELECT {}", expectedResult.value())
        : "SELECT NULL";
    auto expectedArray = expectedResult.has_value()
        ? fmt::format("SELECT ARRAY[{0},{0},{0}]", expectedResult.value())
        : "SELECT NULL";
    enableTestStreaming();
    testAggregations(
        {rows},
        {},
        {functionCall(false, weights.get(), percentile, accuracy, -1)},
        expected);
    testAggregations(
        {rows},
        {},
        {functionCall(false, weights.get(), percentile, accuracy, 3)},
        expectedArray);

    // Companion functions of approx_percentile do not support test streaming
    // because intermediate results are KLL that has non-deterministic shape.
    disableTestStreaming();
    testAggregationsWithCompanion(
        {rows},
        [](auto& /*builder*/) {},
        {},
        {functionCall(false, weights.get(), percentile, accuracy, -1)},
        {getArgTypes(values->type(), weights.get(), accuracy, -1)},
        {},
        expected);
    testAggregationsWithCompanion(
        {rows},
        [](auto& /*builder*/) {},
        {},
        {functionCall(false, weights.get(), percentile, accuracy, 3)},
        {getArgTypes(values->type(), weights.get(), accuracy, 3)},
        {},
        expectedArray);
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
    enableTestStreaming();
    testAggregations(
        {rows},
        {"c0"},
        {functionCall(true, weights.get(), percentile, accuracy, -1)},
        {expectedResult});

    // Companion functions of approx_percentile do not support test streaming
    // because intermediate results are KLL that has non-deterministic shape.
    disableTestStreaming();
    testAggregationsWithCompanion(
        {rows},
        [](auto& /*builder*/) {},
        {"c0"},
        {functionCall(true, weights.get(), percentile, accuracy, -1)},
        {getArgTypes(values->type(), weights.get(), accuracy, -1)},
        {},
        {expectedResult});

    {
      SCOPED_TRACE("Percentile array");
      auto resultValues = expectedResult->childAt(1);
      RowVectorPtr expected = nullptr;
      auto size = resultValues->size();
      if (resultValues->nulls() &&
          bits::countNonNulls(resultValues->rawNulls(), 0, size) == 0) {
        expected = makeRowVector(
            {expectedResult->childAt(0),
             BaseVector::createNullConstant(
                 ARRAY(resultValues->type()), size, pool())});
      } else {
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
        expected = makeRowVector(
            {expectedResult->childAt(0),
             std::make_shared<ArrayVector>(
                 pool(),
                 ARRAY(elements->type()),
                 nullptr,
                 resultValues->size(),
                 offsets,
                 sizes,
                 elements)});
      }

      enableTestStreaming();
      testAggregations(
          {rows},
          {"c0"},
          {functionCall(true, weights.get(), percentile, accuracy, 3)},
          {expected});

      // Companion functions of approx_percentile do not support test streaming
      // because intermediate results are KLL that has non-deterministic shape.
      disableTestStreaming();
      testAggregationsWithCompanion(
          {rows},
          [](auto& /*builder*/) {},
          {"c0"},
          {functionCall(true, weights.get(), percentile, accuracy, 3)},
          {getArgTypes(values->type(), weights.get(), accuracy, 3)},
          {},
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

  testGlobalAgg<int32_t>(values, nullptr, 0.5, -1, 11);
  testGlobalAgg<int32_t>(values, weights, 0.5, -1, 16);
  testGlobalAgg<int32_t>(values, nullptr, 0.5, 0.005, 11);
  testGlobalAgg<int32_t>(values, weights, 0.5, 0.005, 16);

  auto valuesWithNulls = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 23; }, nullEvery(7));
  auto weightsWithNulls = makeFlatVector<int64_t>(
      size, [](auto row) { return row % 23 + 1; }, nullEvery(11));

  testGlobalAgg<int32_t>(valuesWithNulls, nullptr, 0.5, -1, 11);
  testGlobalAgg<int32_t>(valuesWithNulls, weights, 0.5, -1, 16);
  testGlobalAgg<int32_t>(valuesWithNulls, weightsWithNulls, 0.5, -1, 16);
  testGlobalAgg<int32_t>(valuesWithNulls, nullptr, 0.5, 0.005, 11);
  testGlobalAgg<int32_t>(valuesWithNulls, weights, 0.5, 0.005, 16);
  testGlobalAgg<int32_t>(valuesWithNulls, weightsWithNulls, 0.5, 0.005, 16);
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
  testGlobalAgg<int32_t>(values, weights, 0.5, -1, 11);

  // Weights are large, but different.
  weights = makeFlatVector<int64_t>(
      size, [](auto row) { return 1'000 * (row % 23 + 1); });
  testGlobalAgg<int32_t>(values, weights, 0.5, -1, 16);
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
  params.queryCtx = velox::core::QueryCtx::create(executor_.get());
  params.queryCtx->testingOverrideConfigUnsafe({
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
  waitForAllTasksToBeDeleted();
}

TEST_F(ApproxPercentileTest, finalAggregateAccuracy) {
  auto batch = makeRowVector(
      {makeFlatVector<int32_t>(1000, [](auto row) { return row; })});
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
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

TEST_F(ApproxPercentileTest, nonFlatPercentileArray) {
  auto indices = AlignedBuffer::allocate<vector_size_t>(3, pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  std::iota(rawIndices, rawIndices + indices->size(), 0);
  auto percentiles = std::make_shared<ArrayVector>(
      pool(),
      ARRAY(DOUBLE()),
      nullptr,
      1,
      AlignedBuffer::allocate<vector_size_t>(1, pool(), 0),
      AlignedBuffer::allocate<vector_size_t>(1, pool(), 3),
      BaseVector::wrapInDictionary(
          nullptr, indices, 3, makeFlatVector<double>({0, 0.5, 1})));
  auto rows = makeRowVector({
      makeFlatVector<int32_t>(10, folly::identity),
      BaseVector::wrapInConstant(1, 0, percentiles),
  });
  auto plan = PlanBuilder()
                  .values({rows})
                  .singleAggregation({}, {"approx_percentile(c0, c1)"})
                  .planNode();
  auto expected = makeRowVector({makeArrayVector<int32_t>({{0, 5, 9}})});
  AssertQueryBuilder(plan).assertResults(expected);
}

TEST_F(ApproxPercentileTest, invalidWeight) {
  constexpr int64_t kMaxWeight = (1ll << 60) - 1;
  auto makePlan = [&](int64_t weight, bool grouped) {
    auto rows = makeRowVector({
        makeConstant<int32_t>(0, 1),
        makeConstant<int64_t>(weight, 1),
        makeConstant<int32_t>(1, 1),
    });
    std::vector<std::string> groupingKeys;
    if (grouped) {
      groupingKeys.push_back("c2");
    }
    return PlanBuilder()
        .values({rows})
        .singleAggregation(groupingKeys, {"approx_percentile(c0, c1, 0.5)"})
        .planNode();
  };
  assertQuery(makePlan(kMaxWeight, false), "SELECT 0");
  assertQuery(makePlan(kMaxWeight, true), "SELECT 1, 0");
  for (bool grouped : {false, true}) {
    AssertQueryBuilder badQuery(makePlan(kMaxWeight + 1, grouped));
    VELOX_ASSERT_THROW(
        badQuery.copyResults(pool()),
        "weight must be in range [1, 1152921504606846975], got 1152921504606846976");
  }
}

TEST_F(ApproxPercentileTest, noInput) {
  const int size = 1000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 7; });
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row % 6; });
  auto weights =
      makeFlatVector<int64_t>(size, [](auto row) { return row % 5 + 1; });
  auto nullValues = makeNullConstant(TypeKind::INTEGER, size);
  auto nullWeights = makeNullConstant(TypeKind::BIGINT, size);

  // Test global.
  {
    testGlobalAgg<int32_t>(nullValues, nullptr, 0.5, -1, std::nullopt);
    testGlobalAgg<int32_t>(nullValues, weights, 0.5, -1, std::nullopt);
    testGlobalAgg<int32_t>(values, nullWeights, 0.5, -1, std::nullopt);
    testGlobalAgg<int32_t>(nullValues, nullWeights, 0.5, -1, std::nullopt);
  }

  // Test group-by.
  {
    auto expected = makeRowVector(
        {makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6}),
         makeNullConstant(TypeKind::INTEGER, 7)});

    testGroupByAgg(keys, nullValues, nullptr, 0.5, -1, expected);
    testGroupByAgg(keys, nullValues, weights, 0.5, -1, expected);
    testGroupByAgg(keys, values, nullWeights, 0.5, -1, expected);
    testGroupByAgg(keys, nullValues, nullWeights, 0.5, -1, expected);
  }

  // Test when all inputs are masked out.
  {
    auto testWithMask = [&](bool groupBy, const RowVectorPtr& expected) {
      std::vector<std::string> groupingKeys;
      if (groupBy) {
        groupingKeys.push_back("c0");
      }
      auto plan =
          PlanBuilder()
              .values({makeRowVector({keys, values, weights})})
              .project(
                  {"c0",
                   "c1",
                   "c2",
                   "array_constructor(0.5) as pct",
                   "c1 > 6 as m1"})
              .singleAggregation(
                  groupingKeys,
                  {"approx_percentile(c1, 0.5)",
                   "approx_percentile(c1, 0.5, 0.2)",
                   "approx_percentile(c1, c2, 0.5)",
                   "approx_percentile(c1, c2, 0.5, 0.2)",
                   "approx_percentile(c1, c2, 0.5)",
                   "approx_percentile(c1, pct)",
                   "approx_percentile(c1, pct, 0.2)",
                   "approx_percentile(c1, c2, pct)",
                   "approx_percentile(c1, c2, pct, 0.2)",
                   "approx_percentile(c1, c2, pct)"},
                  {"m1", "m1", "m1", "m1", "m1", "m1", "m1", "m1", "m1", "m1"})
              .planNode();

      AssertQueryBuilder(plan).assertResults(expected);
    };

    // Global.
    std::vector<VectorPtr> children{10};
    std::fill_n(children.begin(), 5, makeNullConstant(TypeKind::INTEGER, 1));
    std::fill_n(
        children.begin() + 5,
        5,
        BaseVector::createNullConstant(ARRAY(INTEGER()), 1, pool()));
    auto expected1 = makeRowVector(children);
    testWithMask(false, expected1);

    // Group-by.
    children.resize(11);
    children[0] = makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6});
    std::fill_n(
        children.begin() + 1, 5, makeNullConstant(TypeKind::INTEGER, 7));
    std::fill_n(
        children.begin() + 6,
        5,
        BaseVector::createNullConstant(ARRAY(INTEGER()), 7, pool()));
    auto expected2 = makeRowVector(children);
    testWithMask(true, expected2);
  }
}

TEST_F(ApproxPercentileTest, nullPercentile) {
  auto values = makeFlatVector<int32_t>({1, 2, 3, 4});
  auto percentileOfDouble = makeConstant<double>(std::nullopt, 4);
  auto rows = makeRowVector({values, percentileOfDouble});

  // Test null percentile for approx_percentile(value, percentile).
  VELOX_ASSERT_THROW(
      testAggregations(
          {rows}, {}, {"approx_percentile(c0, c1)"}, "SELECT NULL"),
      "Percentile cannot be null");

  auto percentileOfArrayOfDouble = BaseVector::wrapInConstant(
      4,
      0,
      makeNullableArrayVector<double>(
          std::vector<std::vector<std::optional<double>>>{
              {std::nullopt, std::nullopt, std::nullopt, std::nullopt}}));
  rows = makeRowVector({values, percentileOfArrayOfDouble});

  // Test null percentile for approx_percentile(value, percentiles).
  VELOX_ASSERT_THROW(
      testAggregations(
          {rows}, {}, {"approx_percentile(c0, c1)"}, "SELECT NULL"),
      "Percentile cannot be null");
}

class ApproxPercentileWindowTest : public WindowTestBase {
 protected:
  void SetUp() override {
    WindowTestBase::SetUp();
    random::setSeed(0);
  }
};

TEST_F(ApproxPercentileWindowTest, window) {
  auto data = makeRowVector(
      {makeFlatVector<int32_t>({1, 2, 3}),
       makeNullableFlatVector<int32_t>({10, std::nullopt, 30}),
       makeArrayVectorFromJson<double>({"[0.5]", "[0.5]", "[0.5]"})});
  auto expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
      makeNullableFlatVector<int32_t>({10, std::nullopt, 30}),
      makeArrayVectorFromJson<double>({"[0.5]", "[0.5]", "[0.5]"}),
      makeNullableArrayVector<int32_t>({{{10}}, std::nullopt, {{30}}}),
  });
  testWindowFunction(
      {data},
      "approx_percentile(c1, c2)",
      "order by c0",
      "rows between current row and current row",
      expected);
}

} // namespace
} // namespace facebook::velox::aggregate::test
