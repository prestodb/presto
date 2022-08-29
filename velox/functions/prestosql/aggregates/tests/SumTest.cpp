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

  template <
      typename InputType,
      typename ResultType,
      typename IntermediateType = ResultType>
  void testAggregateOverflow(
      bool expectOverflow = false,
      const TypePtr& type = CppToType<InputType>::create());
};

template <typename ResultType>
void verifyAggregates(
    const std::vector<std::pair<core::PlanNodePtr, ResultType>>& aggsToTest,
    bool expectOverflow) {
  for (const auto& [agg, expectedResult] : aggsToTest) {
    if (expectOverflow) {
      VELOX_ASSERT_THROW(readSingleValue(agg), "overflow");
    } else {
      auto result = readSingleValue(agg);
      if constexpr (std::is_same_v<ResultType, float>) {
        ASSERT_FLOAT_EQ(
            result.template value<TypeKind::REAL>(), expectedResult);
      } else if constexpr (std::is_same_v<ResultType, double>) {
        ASSERT_FLOAT_EQ(
            result.template value<TypeKind::DOUBLE>(), expectedResult);
      } else if constexpr (std::is_same_v<ResultType, UnscaledShortDecimal>) {
        auto output = result.template value<TypeKind::SHORT_DECIMAL>();
        ASSERT_EQ(output.value(), expectedResult);
      } else if constexpr (std::is_same_v<ResultType, UnscaledLongDecimal>) {
        auto output = result.template value<TypeKind::LONG_DECIMAL>();
        ASSERT_EQ(output.value(), expectedResult);
      } else {
        ASSERT_EQ(result, expectedResult);
      }
    }
  }
}

template <typename InputType, typename ResultType, typename IntermediateType>
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
__attribute__((no_sanitize("integer")))
#endif
#endif
void SumTest::testAggregateOverflow(
    bool expectOverflow,
    const TypePtr& type) {
  const InputType maxLimit = std::numeric_limits<InputType>::max();
  const InputType overflow = InputType(1);
  const InputType zero = InputType(0);

  // Intermediate type size is always >= result type size. Hence, use
  // intermediate type to calculate the expected output.
  IntermediateType limitResult = IntermediateType(maxLimit);
  IntermediateType overflowResult = IntermediateType(overflow);

  // Single max limit value. 0's to induce dummy calculations.
  auto limitVector =
      makeRowVector({makeFlatVector<InputType>({maxLimit, zero, zero}, type)});

  // Test code path for single values with possible overflow hit in add.
  auto overflowFlatVector =
      makeRowVector({makeFlatVector<InputType>({maxLimit, overflow}, type)});
  IntermediateType expectedFlatSum = limitResult + overflowResult;

  // Test code path for duplicate values with possible overflow hit in
  // multiply.
  auto overflowConstantVector =
      makeRowVector({makeConstant<InputType>(maxLimit / 3, 4, type)});
  IntermediateType expectedConstantSum = (limitResult / 3) * 4;

  // Test code path for duplicate values with possible overflow hit in add.
  auto overflowHybridVector = {limitVector, overflowConstantVector};
  IntermediateType expectedHybridSum = limitResult + expectedConstantSum;

  // Vector with element pairs of a partial aggregate node, expected result.
  std::vector<std::pair<core::PlanNodePtr, IntermediateType>> partialAggsToTest;
  // Partial Aggregation (raw input in - partial result out).
  partialAggsToTest.push_back(
      {PlanBuilder()
           .values({overflowFlatVector})
           .partialAggregation({}, {"sum(c0)"})
           .planNode(),
       expectedFlatSum});
  partialAggsToTest.push_back(
      {PlanBuilder()
           .values({overflowConstantVector})
           .partialAggregation({}, {"sum(c0)"})
           .planNode(),
       expectedConstantSum});
  partialAggsToTest.push_back(
      {PlanBuilder()
           .values(overflowHybridVector)
           .partialAggregation({}, {"sum(c0)"})
           .planNode(),
       expectedHybridSum});

  // Vector with element pairs of a full aggregate node, expected result.
  std::vector<std::pair<core::PlanNodePtr, ResultType>> aggsToTest;
  // Single Aggregation (raw input in - final result out).
  aggsToTest.push_back(
      {PlanBuilder()
           .values({overflowFlatVector})
           .singleAggregation({}, {"sum(c0)"})
           .planNode(),
       expectedFlatSum});
  aggsToTest.push_back(
      {PlanBuilder()
           .values({overflowConstantVector})
           .singleAggregation({}, {"sum(c0)"})
           .planNode(),
       expectedConstantSum});
  aggsToTest.push_back(
      {PlanBuilder()
           .values(overflowHybridVector)
           .singleAggregation({}, {"sum(c0)"})
           .planNode(),
       expectedHybridSum});
  // Final Aggregation (partial result in - final result out):
  // To make sure that the overflow occurs in the final aggregation step, we
  // create 2 plan fragments and plugging their partially aggregated
  // output into a final aggregate plan node. Each of those input fragments
  // only have a single input value under the max limit which when added in
  // the final step causes a potential overflow.
  auto planNodeIdGenerator =
      std::make_shared<exec::test::PlanNodeIdGenerator>();
  aggsToTest.push_back(
      {PlanBuilder(planNodeIdGenerator)
           .localPartition(
               {},
               {PlanBuilder(planNodeIdGenerator)
                    .values({limitVector})
                    .partialAggregation({}, {"sum(c0)"})
                    .planNode(),
                PlanBuilder(planNodeIdGenerator)
                    .values({limitVector})
                    .partialAggregation({}, {"sum(c0)"})
                    .planNode()})
           .finalAggregation()
           .planNode(),
       limitResult + limitResult});

  // Verify all partial aggregates.
  verifyAggregates<IntermediateType>(partialAggsToTest, expectOverflow);
  // Verify all aggregates.
  verifyAggregates<ResultType>(aggsToTest, expectOverflow);
}

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

TEST_F(SumTest, sumFloat) {
  auto data = makeRowVector({makeFlatVector<float>({2.00, 1.00})});
  createDuckDbTable({data});

  testAggregations(
      [&](auto& builder) { builder.values({data}); },
      {},
      {"sum(c0)"},
      "SELECT sum(c0) FROM tmp");
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

TEST_F(SumTest, sumDecimal) {
  auto input = makeRowVector(
      {makeNullableShortDecimalFlatVector(
           {1'000, 2'000, 3'000, 4'000, 5'000, std::nullopt}, DECIMAL(10, 1)),
       makeNullableLongDecimalFlatVector(
           {buildInt128(10, 100),
            buildInt128(10, 200),
            buildInt128(10, 300),
            buildInt128(10, 400),
            buildInt128(10, 500),
            std::nullopt},
           DECIMAL(23, 4))});
  createDuckDbTable({input});
  testAggregations(
      {input}, {}, {"sum(c0)", "sum(c1)"}, "SELECT sum(c0), sum(c1) FROM tmp");
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

TEST_F(SumTest, integerAggregateOverflow) {
  testAggregateOverflow<int8_t, int64_t>();
  testAggregateOverflow<int16_t, int64_t>();
  testAggregateOverflow<int32_t, int64_t>();
  testAggregateOverflow<int64_t, int64_t>(true);
}

TEST_F(SumTest, floatAggregateOverflow) {
  testAggregateOverflow<float, float, double>();
  testAggregateOverflow<double, double>();
}

TEST_F(SumTest, DISABLED_decimalAggregateOverflow) {
  testAggregateOverflow<
      UnscaledShortDecimal,
      UnscaledLongDecimal,
      UnscaledLongDecimal>(false, DECIMAL(10, 5));
  testAggregateOverflow<UnscaledLongDecimal, UnscaledLongDecimal>(
      true, DECIMAL(20, 5));
}
} // namespace
} // namespace facebook::velox::aggregate::test
