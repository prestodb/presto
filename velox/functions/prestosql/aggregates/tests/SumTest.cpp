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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/AggregationHook.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

using facebook::velox::exec::test::PlanBuilder;
using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

namespace {

class SumTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }

  template <typename InputType, typename ResultType>
  void testInputTypeLimits(bool expectOverflow = false) {
    std::vector<InputType> underflowTestCase = {
        std::numeric_limits<InputType>::min(),
        std::numeric_limits<InputType>::min() + 2};
    std::vector<InputType> overflowTestCase = {
        std::numeric_limits<InputType>::max(),
        std::numeric_limits<InputType>::max() - 2};
    auto createRowVectorFromSingleValue = [&](InputType value) {
      return makeRowVector(
          {makeFlatVector<InputType>(std::vector<InputType>(1, value))});
    };
    for (auto& testCase : {underflowTestCase, overflowTestCase}) {
      // Test code path for single values with overflow hit in add.
      std::vector<RowVectorPtr> input = {
          makeRowVector({makeFlatVector<InputType>(testCase)})};
      // Test code path for duplicate values with overflow hit in multiply.
      std::vector<RowVectorPtr> inputConstantVector = {
          makeRowVector({makeConstant<InputType>(testCase[0] / 3, 4)})};
      // Test code path for duplicate values with overflow hit in add.
      std::vector<RowVectorPtr> inputHybridVector = {
          createRowVectorFromSingleValue(testCase[0]),
          makeRowVector({makeConstant<InputType>(testCase[1] / 3, 3)})};
      std::vector<core::PlanNodePtr> plansToTest;
      // Single Aggregation (raw input in - final result out)
      plansToTest.push_back(PlanBuilder()
                                .values(input)
                                .singleAggregation({}, {"sum(c0)"})
                                .planNode());
      plansToTest.push_back(PlanBuilder()
                                .values(inputConstantVector)
                                .singleAggregation({}, {"sum(c0)"})
                                .planNode());
      plansToTest.push_back(PlanBuilder()
                                .values(inputHybridVector)
                                .singleAggregation({}, {"sum(c0)"})
                                .planNode());
      // Partial Aggregation (raw input in - partial result out)
      plansToTest.push_back(PlanBuilder()
                                .values(input)
                                .partialAggregation({}, {"sum(c0)"})
                                .planNode());
      plansToTest.push_back(PlanBuilder()
                                .values(inputConstantVector)
                                .partialAggregation({}, {"sum(c0)"})
                                .planNode());
      plansToTest.push_back(PlanBuilder()
                                .values(inputHybridVector)
                                .partialAggregation({}, {"sum(c0)"})
                                .planNode());
      // Final Aggregation (partial result in - final result out):
      // To make sure that the overflow occurs in the final aggregation step, we
      // create 2 plan fragments and plugging their partially aggregated
      // output into a final aggregate plan node. Each of those input fragments
      // only have a single input value under the max limit which when added in
      // the final step causes an overflow.
      auto planNodeIdGenerator =
          std::make_shared<exec::test::PlanNodeIdGenerator>();
      plansToTest.push_back(
          PlanBuilder(planNodeIdGenerator)
              .localPartition(
                  {},
                  {PlanBuilder(planNodeIdGenerator)
                       .values({createRowVectorFromSingleValue(testCase[0])})
                       .partialAggregation({}, {"sum(c0)"})
                       .planNode(),
                   PlanBuilder(planNodeIdGenerator)
                       .values({createRowVectorFromSingleValue(testCase[1])})
                       .partialAggregation({}, {"sum(c0)"})
                       .planNode()})
              .finalAggregation()
              .planNode());
      // Run all plan types
      CursorParameters params;
      for (auto& plan : plansToTest) {
        params.planNode = plan;
        if (expectOverflow) {
          VELOX_ASSERT_THROW(
              readCursor(params, [](auto /*task*/) {}), "overflow");
        } else {
          readCursor(params, [](auto /*task*/) {});
        }
      }
    }
  }
};

TEST_F(SumTest, sumTinyint) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), TINYINT()});
  auto vectors = makeVectors(rowType, 1000, 10);
  createDuckDbTable(vectors);

  // Global aggregation.
  testAggregations(vectors, {}, {"sum(c1)"}, "SELECT sum(c1) FROM tmp");

  // Group by aggregation.
  testAggregations(
      [&](auto& builder) {
        builder.values(vectors).project({"c0 % 10", "c1"});
      },
      {"p0"},
      {"sum(c1)"},
      "SELECT c0 % 10, sum(c1) FROM tmp GROUP BY 1");

  // Encodings: use filter to wrap aggregation inputs in a dictionary.
  testAggregations(
      [&](auto& builder) {
        builder.values(vectors).filter("c0 % 2 = 0").project({"c0 % 11", "c1"});
      },
      {"p0"},
      {"sum(c1)"},
      "SELECT c0 % 11, sum(c1) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");

  testAggregations(
      [&](auto& builder) { builder.values(vectors).filter("c0 % 2 = 0"); },
      {},
      {"sum(c1)"},
      "SELECT sum(c1) FROM tmp WHERE c0 % 2 = 0");
}

TEST_F(SumTest, sumDouble) {
  // Run the test multiple times to ease reproducing the issue:
  // https://github.com/facebookincubator/velox/issues/2198
  for (int iter = 0; iter < 3; ++iter) {
    SCOPED_TRACE(fmt::format("test iterations: {}", iter));
    auto rowType = ROW({"c0", "c1"}, {REAL(), DOUBLE()});
    auto vectors = makeVectors(rowType, 1000, 10);
    createDuckDbTable(vectors);

    float sum = 0;
    for (int i = 0; i < vectors.size(); ++i) {
      for (int j = 0; j < vectors[0]->size(); ++j) {
        sum += vectors[i]->childAt(0)->asFlatVector<float>()->valueAt(j);
      }
    }
    testAggregations(
        vectors,
        {},
        {"sum(c0)", "sum(c1)"},
        "SELECT sum(c0), sum(c1) FROM tmp");
  }
}

TEST_F(SumTest, sumWithMask) {
  auto rowType =
      ROW({"c0", "c1", "c2", "c3", "c4"},
          {INTEGER(), TINYINT(), BIGINT(), BIGINT(), INTEGER()});
  auto vectors = makeVectors(rowType, 100, 10);

  core::PlanNodePtr op;
  createDuckDbTable(vectors);

  // Aggregations 0 and 1 will use the same channel, but different masks.
  // Aggregations 1 and 2 will use different channels, but the same mask.

  // Global partial+final aggregation.
  op = PlanBuilder()
           .values(vectors)
           .project({"c0", "c1", "c2 % 2 = 0 AS m0", "c3 % 3 = 0 AS m1"})
           .partialAggregation(
               {}, {"sum(c0)", "sum(c0)", "sum(c1)"}, {"m0", "m1", "m1"})
           .finalAggregation()
           .planNode();
  assertQuery(
      op,
      "SELECT sum(c0) filter (where c2 % 2 = 0), "
      "sum(c0) filter (where c3 % 3 = 0), sum(c1) filter (where c3 % 3 = 0) "
      "FROM tmp");

  // Use mask that's always false.
  op = PlanBuilder()
           .values(vectors)
           .project({"c0", "c1", "c2 % 2 > 10 AS m0", "c3 % 3 = 0 AS m1"})
           .partialAggregation(
               {}, {"sum(c0)", "sum(c0)", "sum(c1)"}, {"m0", "m1", "m1"})
           .finalAggregation()
           .planNode();
  assertQuery(
      op,
      "SELECT sum(c0) filter (where c2 % 2 > 10), "
      "sum(c0) filter (where c3 % 3 = 0), sum(c1) filter (where c3 % 3 = 0) "
      "FROM tmp");

  // Encodings: use filter to wrap aggregation inputs in a dictionary.
  // Global partial+final aggregation.
  op = PlanBuilder()
           .values(vectors)
           .filter("c3 % 2 = 0")
           .project({"c0", "c1", "c2 % 2 = 0 AS m0", "c3 % 3 = 0 AS m1"})
           .partialAggregation(
               {}, {"sum(c0)", "sum(c0)", "sum(c1)"}, {"m0", "m1", "m1"})
           .finalAggregation()
           .planNode();
  assertQuery(
      op,
      "SELECT sum(c0) filter (where c2 % 2 = 0), "
      "sum(c0) filter (where c3 % 3 = 0), sum(c1) filter (where c3 % 3 = 0) "
      "FROM tmp where c3 % 2 = 0");

  // Group by partial+final aggregation.
  op = PlanBuilder()
           .values(vectors)
           .project({"c4", "c0", "c1", "c2 % 2 = 0 AS m0", "c3 % 3 = 0 AS m1"})
           .partialAggregation(
               {"c4"}, {"sum(c0)", "sum(c0)", "sum(c1)"}, {"m0", "m1", "m1"})
           .finalAggregation()
           .planNode();
  assertQuery(
      op,
      "SELECT c4, sum(c0) filter (where c2 % 2 = 0), "
      "sum(c0) filter (where c3 % 3 = 0), sum(c1) filter (where c3 % 3 = 0) "
      "FROM tmp group by c4");

  // Encodings: use filter to wrap aggregation inputs in a dictionary.
  // Group by partial+final aggregation.
  op = PlanBuilder()
           .values(vectors)
           .filter("c3 % 2 = 0")
           .project({"c4", "c0", "c1", "c2 % 2 = 0 AS m0", "c3 % 3 = 0 AS m1"})
           .partialAggregation(
               {"c4"}, {"sum(c0)", "sum(c0)", "sum(c1)"}, {"m0", "m1", "m1"})
           .finalAggregation()
           .planNode();
  assertQuery(
      op,
      "SELECT c4, sum(c0) filter (where c2 % 2 = 0), "
      "sum(c0) filter (where c3 % 3 = 0), sum(c1) filter (where c3 % 3 = 0) "
      "FROM tmp where c3 % 2 = 0 group by c4");

  // Use mask that's always false.
  op = PlanBuilder()
           .values(vectors)
           .filter("c3 % 2 = 0")
           .project({"c4", "c0", "c1", "c2 % 2 > 10 AS m0", "c3 % 3 = 0 AS m1"})
           .partialAggregation(
               {"c4"}, {"sum(c0)", "sum(c0)", "sum(c1)"}, {"m0", "m1", "m1"})
           .finalAggregation()
           .planNode();
  assertQuery(
      op,
      "SELECT c4, sum(c0) filter (where c2 % 2 > 10), "
      "sum(c0) filter (where c3 % 3 = 0), sum(c1) filter (where c3 % 3 = 0) "
      "FROM tmp where c3 % 2 = 0 group by c4");
}

// Test aggregation over boolean key
TEST_F(SumTest, boolKey) {
  vector_size_t size = 1'000;
  auto rowType = ROW({"c0", "c1"}, {BOOLEAN(), INTEGER()});
  auto vector = makeRowVector(
      {makeFlatVector<bool>(size, [](auto row) { return row % 3 == 0; }),
       makeFlatVector<int32_t>(size, [](auto row) { return row; })});
  createDuckDbTable({vector});

  testAggregations(
      {vector}, {"c0"}, {"sum(c1)"}, "SELECT c0, sum(c1) FROM tmp GROUP BY 1");
}

TEST_F(SumTest, emptyValues) {
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});
  auto vector = makeRowVector(
      {makeFlatVector<int32_t>(std::vector<int32_t>{}),
       makeFlatVector<int64_t>(std::vector<int64_t>{})});

  testAggregations({vector}, {"c0"}, {"sum(c1)"}, "");
}

/// Test aggregating over lots of null values.
TEST_F(SumTest, nulls) {
  vector_size_t size = 10'000;
  auto data = {makeRowVector(
      {"a", "b"},
      {
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
          makeFlatVector<int32_t>(
              size, [](auto row) { return row; }, nullEvery(3)),
      })};
  createDuckDbTable(data);

  testAggregations(
      data,
      {"a"},
      {"sum(b) AS sum_b"},
      "SELECT a, sum(b) as sum_b FROM tmp GROUP BY 1");
}

template <typename Type>
struct SumRow {
  char nulls;
  Type sum;
};

TEST_F(SumTest, hook) {
  SumRow<int64_t> sumRow;
  sumRow.nulls = 1;
  sumRow.sum = 0;

  char* row = reinterpret_cast<char*>(&sumRow);
  uint64_t numNulls = 1;
  aggregate::SumHook<int64_t, int64_t> hook(
      offsetof(SumRow<int64_t>, sum),
      offsetof(SumRow<int64_t>, nulls),
      1,
      &row,
      &numNulls);

  int64_t value = 11;
  hook.addValue(0, &value);
  EXPECT_EQ(0, sumRow.nulls);
  EXPECT_EQ(0, numNulls);
  EXPECT_EQ(value, sumRow.sum);
}

template <typename InputType, typename ResultType>
void testHookLimits(bool expectOverflow = false) {
  // Pair of <limit, value to overflow>.
  std::vector<std::pair<InputType, InputType>> limits = {
      {std::numeric_limits<InputType>::min(), -1},
      {std::numeric_limits<InputType>::max(), 1}};

  for (const auto& [limit, overflow] : limits) {
    SumRow<ResultType> sumRow;
    sumRow.sum = 0;
    ResultType expected = 0;
    char* row = reinterpret_cast<char*>(&sumRow);
    uint64_t numNulls = 0;
    aggregate::SumHook<InputType, ResultType> hook(
        offsetof(SumRow<ResultType>, sum),
        offsetof(SumRow<ResultType>, nulls),
        0,
        &row,
        &numNulls);

    // Adding limit should not overflow.
    ASSERT_NO_THROW(hook.addValue(0, &limit));
    expected += limit;
    EXPECT_EQ(expected, sumRow.sum);
    // Adding overflow based on the ResultType should throw.
    if (expectOverflow) {
      VELOX_ASSERT_THROW(hook.addValue(0, &overflow), "overflow");
    } else {
      ASSERT_NO_THROW(hook.addValue(0, &overflow));
      expected += overflow;
      EXPECT_EQ(expected, sumRow.sum);
    }
  }
}

TEST_F(SumTest, hookLimits) {
  testHookLimits<int32_t, int64_t>();
  testHookLimits<int64_t, int64_t>(true);
  // Float and Double do not throw an overflow error.
  testHookLimits<float, double>();
  testHookLimits<double, double>();
}

TEST_F(SumTest, inputTypeLimits) {
  // Verify sum aggregate function checks and throws an overflow error when
  // appropriate. Since all integer types have output types as int64, overflow
  // only occurs if the sum exceeds the max int64 value. For floating points, an
  // overflow results in an infinite result but does not throw. Results are
  // manually compared instead of comparing with duckDB as it throws an error
  // instead when floating points go over limit.
  testInputTypeLimits<int8_t, int64_t>();
  testInputTypeLimits<int16_t, int64_t>();
  testInputTypeLimits<int32_t, int64_t>();
  testInputTypeLimits<int64_t, int64_t>(true);
  // TODO: enable this test once Issue #2079 is fixed
  // testInputTypeLimits<float, float>();
  testInputTypeLimits<double, double>();
}
} // namespace
} // namespace facebook::velox::aggregate::test
