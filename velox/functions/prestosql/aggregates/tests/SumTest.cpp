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
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/SumTestBase.h"

using facebook::velox::exec::test::PlanBuilder;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {

namespace {

class SumTest : public SumTestBase {
 public:
  template <
      typename InputType,
      typename ResultType,
      typename IntermediateType = ResultType>
  void testAggregateOverflow(
      bool expectOverflow = false,
      const TypePtr& type = CppToType<InputType>::create()) {
    SumTestBase::testAggregateOverflow<InputType, ResultType, IntermediateType>(
        "sum", expectOverflow, type);
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

TEST_F(SumTest, sumFloat) {
  auto data = makeRowVector({makeFlatVector<float>({2.00, 1.00})});
  createDuckDbTable({data});

  testAggregations(
      [&](auto& builder) { builder.values({data}); },
      {},
      {"sum(c0)"},
      "SELECT sum(c0) FROM tmp");
}

TEST_F(SumTest, sumDoubleAndFloat) {
  for (int iter = 0; iter < 3; ++iter) {
    SCOPED_TRACE(fmt::format("test iterations: {}", iter));
    auto rowType = ROW({"c0", "c1", "c2"}, {REAL(), DOUBLE(), INTEGER()});
    auto vectors = makeVectors(rowType, 1000, 10);

    createDuckDbTable(vectors);

    // With group by.
    testAggregations(
        vectors,
        {"c2"},
        {"sum(c0)", "sum(c1)"},
        "SELECT c2, sum(c0), sum(c1) FROM tmp GROUP BY c2");

    // Without group by.
    testAggregations(
        vectors,
        {},
        {"sum(c0)", "sum(c1)"},
        "SELECT sum(c0), sum(c1) FROM tmp");
  }
}

TEST_F(SumTest, sumDecimal) {
  // Disable incremental aggregation tests because DecimalAggregate doesn't set
  // StringView::prefix when extracting accumulators, leaving the prefix field
  // undefined that fails the test.
  AggregationTestBase::disableTestIncremental();

  // Skip testing with TableScan because decimal is not supported in writers.
  std::vector<std::optional<int64_t>> shortDecimalRawVector;
  std::vector<std::optional<int128_t>> longDecimalRawVector;
  for (int i = 0; i < 1000; ++i) {
    shortDecimalRawVector.push_back(i * 1000);
    longDecimalRawVector.push_back(HugeInt::build(i * 10, i * 100));
  }
  shortDecimalRawVector.push_back(std::nullopt);
  longDecimalRawVector.push_back(std::nullopt);
  auto input = makeRowVector(
      {makeNullableFlatVector<int64_t>(shortDecimalRawVector, DECIMAL(10, 1)),
       makeNullableFlatVector<int128_t>(longDecimalRawVector, DECIMAL(23, 4))});
  createDuckDbTable({input});
  testAggregations(
      {input}, {}, {"sum(c0)", "sum(c1)"}, "SELECT sum(c0), sum(c1) FROM tmp");

  // Decimal sum aggregation with multiple groups.
  auto inputRows = {
      makeRowVector(
          {makeFlatVector<int32_t>({1, 1}),
           makeFlatVector<int64_t>({37220, 53450}, DECIMAL(5, 2))}),
      makeRowVector(
          {makeFlatVector<int32_t>({2, 2}),
           makeFlatVector<int64_t>({10410, 9250}, DECIMAL(5, 2))}),
      makeRowVector(
          {makeFlatVector<int32_t>({3, 3}),
           makeFlatVector<int64_t>({-12783, 0}, DECIMAL(5, 2))}),
      makeRowVector(
          {makeFlatVector<int32_t>({1, 2}),
           makeFlatVector<int64_t>({23178, 41093}, DECIMAL(5, 2))}),
      makeRowVector(
          {makeFlatVector<int32_t>({2, 3}),
           makeFlatVector<int64_t>({-10023, 5290}, DECIMAL(5, 2))}),
  };

  auto expectedResult = {
      makeRowVector(
          {makeFlatVector<int32_t>(std::vector<int32_t>{1}),
           makeFlatVector<int128_t>(
               std::vector<int128_t>{113848}, DECIMAL(38, 2))}),
      makeRowVector(
          {makeFlatVector<int32_t>(std::vector<int32_t>{2}),
           makeFlatVector<int128_t>(
               std::vector<int128_t>{50730}, DECIMAL(38, 2))}),
      makeRowVector(
          {makeFlatVector<int32_t>(std::vector<int32_t>{3}),
           makeFlatVector<int128_t>(
               std::vector<int128_t>{-7493}, DECIMAL(38, 2))})};

  testAggregations(inputRows, {"c0"}, {"sum(c1)"}, expectedResult);

  AggregationTestBase::enableTestIncremental();
}

TEST_F(SumTest, sumDecimalOverflow) {
  AggregationTestBase::disableTestIncremental();

  // Short decimals do not overflow easily.
  std::vector<int64_t> shortDecimalInput;
  for (int i = 0; i < 10'000; ++i) {
    shortDecimalInput.push_back(DecimalUtil::kShortDecimalMax);
  }
  auto input = makeRowVector(
      {makeFlatVector<int64_t>(shortDecimalInput, DECIMAL(17, 5))});
  createDuckDbTable({input});
  testAggregations({input}, {}, {"sum(c0)"}, "SELECT sum(c0) FROM tmp");

  auto decimalSumOverflow = [this](
                                const std::vector<int128_t>& input,
                                const std::vector<int128_t>& output) {
    const TypePtr type = DECIMAL(38, 0);
    auto in = makeRowVector({makeFlatVector<int128_t>({input}, type)});
    auto expected = makeRowVector({makeFlatVector<int128_t>({output}, type)});
    PlanBuilder builder(pool());
    builder.values({in});
    builder.singleAggregation({}, {"sum(c0)"});
    AssertQueryBuilder queryBuilder(
        builder.planNode(), this->duckDbQueryRunner_);
    queryBuilder.assertResults({expected});
  };

  // Test Positive Overflow.
  std::vector<int128_t> longDecimalInput;
  std::vector<int128_t> longDecimalOutput;
  // Create input with 2 UnscaledLongDecimal::max().
  longDecimalInput.push_back(DecimalUtil::kLongDecimalMax);
  longDecimalInput.push_back(DecimalUtil::kLongDecimalMax);
  // The sum must overflow.
  VELOX_ASSERT_THROW(
      decimalSumOverflow(longDecimalInput, longDecimalOutput),
      "Decimal overflow");

  // Now add UnscaledLongDecimal::min().
  // The sum now must not overflow.
  longDecimalInput.push_back(DecimalUtil::kLongDecimalMin);
  longDecimalOutput.push_back(DecimalUtil::kLongDecimalMax);
  decimalSumOverflow(longDecimalInput, longDecimalOutput);

  // Test Negative Overflow.
  longDecimalInput.clear();
  longDecimalOutput.clear();

  // Create input with 2 UnscaledLongDecimal::min().
  longDecimalInput.push_back(DecimalUtil::kLongDecimalMin);
  longDecimalInput.push_back(DecimalUtil::kLongDecimalMin);

  // The sum must overflow.
  VELOX_ASSERT_THROW(
      decimalSumOverflow(longDecimalInput, longDecimalOutput),
      "Decimal overflow");

  // Now add UnscaledLongDecimal::max().
  // The sum now must not overflow.
  longDecimalInput.push_back(DecimalUtil::kLongDecimalMax);
  longDecimalOutput.push_back(DecimalUtil::kLongDecimalMin);
  decimalSumOverflow(longDecimalInput, longDecimalOutput);

  // Check value in range.
  longDecimalInput.clear();
  longDecimalInput.push_back(DecimalUtil::kLongDecimalMax);
  longDecimalInput.push_back(1);
  VELOX_ASSERT_THROW(
      decimalSumOverflow(longDecimalInput, longDecimalOutput),
      "Value '100000000000000000000000000000000000000' is not in the range of Decimal Type");

  longDecimalInput.clear();
  longDecimalInput.push_back(DecimalUtil::kLongDecimalMin);
  longDecimalInput.push_back(-1);
  VELOX_ASSERT_THROW(
      decimalSumOverflow(longDecimalInput, longDecimalOutput),
      "Value '-100000000000000000000000000000000000000' is not in the range of Decimal Type");

  AggregationTestBase::enableTestIncremental();
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
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 5; ++i) {
    vectors.push_back(makeRowVector(
        {makeFlatVector<bool>(size, [](auto row) { return row % 3 == 0; }),
         makeFlatVector<int32_t>(size, [](auto row) { return row; })}));
  }
  createDuckDbTable(vectors);

  testAggregations(
      vectors, {"c0"}, {"sum(c1)"}, "SELECT c0, sum(c1) FROM tmp GROUP BY 1");
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

  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 5; ++i) {
    vectors.push_back(makeRowVector(
        {makeFlatVector<int32_t>(size, [](auto row) { return row; }),
         makeFlatVector<int32_t>(
             size, [](auto row) { return row; }, nullEvery(3))}));
  }

  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {"c0"},
      {"sum(c1) AS sum_c1"},
      "SELECT c0, sum(c1) as sum_c1 FROM tmp GROUP BY 1");
}

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

TEST_F(SumTest, distinct) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 2, 1, 1, 2, 2}),
      makeFlatVector<int32_t>({1, 1, 2, 2, 3, 1, 1, 1}),
  });

  createDuckDbTable({data});

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"sum(distinct c1)"})
                  .planNode();
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults("SELECT sum(distinct c1) FROM tmp");

  plan = PlanBuilder()
             .values({data})
             .singleAggregation({"c0"}, {"sum(distinct c1)"})
             .planNode();
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults("SELECT c0, sum(distinct c1) FROM tmp GROUP BY 1");
}

} // namespace
} // namespace facebook::velox::aggregate::test
