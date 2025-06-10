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

#include <gtest/gtest.h>
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {
class NoisyCountGaussianAggregationTest
    : public functions::aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
  }
  RowTypePtr rowType1_{
      ROW({"c0", "c1", "c2"}, {INTEGER(), BOOLEAN(), VARCHAR()})};
  RowTypePtr rowType2_{
      ROW({"c0", "c1", "c2"}, {DOUBLE(), TIMESTAMP(), BIGINT()})};
};

// Test normal count(*)
TEST_F(NoisyCountGaussianAggregationTest, countStarNoNoise) {
  auto vectors = makeVectors(rowType1_, 10, 3);
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {"noisy_count_gaussian(1, 0.0)"}, // Normal count(*) should be rewriten
                                        // and call noisy_count_gaussian(1, 0.0)
      "SELECT count(*) FROM tmp");
}

// Test cases that take different input types without noise.
TEST_F(NoisyCountGaussianAggregationTest, constNoNoise) {
  auto vectors = {makeRowVector({
      makeFlatVector<int64_t>(10, [](vector_size_t row) { return row / 3; }),
      makeConstant(true, 10),
      makeConstant("foo", 10),
  })};
  createDuckDbTable(vectors);

  // Aggregate on column c2 which contains string values.
  testAggregations(
      vectors,
      {},
      {"noisy_count_gaussian(c2, 0.0)"},
      "SELECT count(c2) FROM tmp");

  // Aggregate on column c1 which contains boolean values.
  testAggregations(
      vectors,
      {},
      {"noisy_count_gaussian(c1, 0.0)"},
      "SELECT count(c1) FROM tmp");

  // Aggregate on column c0 which contains integer values.
  testAggregations(
      vectors,
      {},
      {"noisy_count_gaussian(c0, 0.0)"},
      "SELECT count(c0) FROM tmp");
}

// Test cases where the noise scale is invalid.
TEST_F(NoisyCountGaussianAggregationTest, inValidNoise) {
  auto vectors = makeVectors(rowType1_, 10, 5);
  createDuckDbTable(vectors);

  // Test should fail and output expected error message.
  testFailingAggregations(
      vectors,
      {},
      {"noisy_count_gaussian(c2, -1.0)"},
      "Noise scale must be a non-negative value");
}

// Test cases where the input vector has nulls and non-nulls.
TEST_F(NoisyCountGaussianAggregationTest, aggregateMixedNoNoise) {
  // Make a single row vector with nulls, null every 4th row (0, 4, 8, ...).
  auto vectors = {
      makeRowVector({makeFlatVector<bool>(
          10, [](auto row) { return row % 3 == 0; }, nullEvery(4))}),
  };

  createDuckDbTable(vectors);

  // Test against the DuckDB result.
  testAggregations(
      vectors,
      {},
      {"noisy_count_gaussian(c0, 0.0)"},
      "SELECT count(c0) FROM tmp");

  // Test against the expected result, 3 out of 10 rows are null.
  auto expectedResult =
      makeRowVector({makeConstant(static_cast<int64_t>(7 /*valid rows*/), 1)});

  testAggregations(
      vectors, {}, {"noisy_count_gaussian(c0, 0.0)"}, {expectedResult});
}

// Test cases where the input vector has all null values.
TEST_F(NoisyCountGaussianAggregationTest, aggregateAllNullsNoNoise) {
  auto vectors = {makeRowVector({makeAllNullFlatVector<int>(10)})};

  // DuckDB will output 0 in this case while Presto JAVA noisy functions
  // will output NULL.
  // Test against the expected result, which is NULL.
  auto expectedResult = makeRowVector({makeAllNullFlatVector<int64_t>(1)});

  testAggregations(
      vectors, {}, {"noisy_count_gaussian(c0, 0.0)"}, {expectedResult});
}

// Test cases where the input vector has nulls with multiple groups, no noise.
TEST_F(NoisyCountGaussianAggregationTest, groupByNullNoNoise) {
  // Make a bactch of vectors of which the first contains all nulls, and the
  // second contains integers, and the third contains strings.
  auto vectors = {
      makeRowVector(
          {makeAllNullFlatVector<int>(10),
           makeFlatVector<int>(10, [](auto row) { return row % 3; }),
           makeConstant("foo", 10)}),
  };

  createDuckDbTable(vectors);

  // Test against the DuckDB result.
  testAggregations(
      vectors,
      {"c0", "c1"},
      {"noisy_count_gaussian(c2, 0.0)"},
      "SELECT c0, c1, count(c2) FROM tmp GROUP BY c0, c1");

  // Test against the expected result. Expected result is:
  // c0    c1    noisy_count_gaussian(c2, 0.0)
  // NULL  0     4
  // NULL  1     3
  // NULL  2     3
  auto expectedResult = makeRowVector({
      makeAllNullFlatVector<int>(3), // group by c0 is null
      makeFlatVector<int>(3, [](auto row) { return row % 3; }), // group by c1
      makeFlatVector<int64_t>(
          3, [](auto row) { return row == 0 ? 4 : 3; }), // noisy count.
  });

  testAggregations(
      vectors,
      {"c0", "c1"},
      {"noisy_count_gaussian(c2, 0.0)"},
      {expectedResult});
}

TEST_F(NoisyCountGaussianAggregationTest, oneAggregateSingleGroupNoNoise) {
  // Make two batches of rows: one with nulls; another without.
  auto vectors = makeVectors(rowType1_, 10, 2);

  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {"noisy_count_gaussian(c0, 0.0)"},
      "SELECT count(c0) FROM tmp");
}

TEST_F(NoisyCountGaussianAggregationTest, oneAggregateMultipleGroupsNoNoise) {
  auto vectors = {
      // This test case is designed to test the senario where the aggregated
      // column has null values for one of the groupby keys.
      // c0: 0, 1, 0, 1, 0, 1, 0, 1, 0, 1
      // c1: null, 1, null, 1, null, 1, null, 1, null, 1
      // c2: "foo", "foo", ... "foo", "foo"
      makeRowVector({
          makeFlatVector<int>(10, [](auto row) { return row % 2; }), // c0
          makeFlatVector<int>(
              10, [](auto row) { return row % 2; }, nullEvery(2)), // c1
          makeFlatVector<std::string>(
              10, [](auto row) { return std::to_string(row) + "foo"; }) // c2
      }),
  };

  // EXPECTED RESULT:
  // C0 | noisy_count_gaussian(c1, 0.0)
  // 0  | NULL
  // 1  | 5
  auto expectedResult = makeRowVector(
      {makeFlatVector<int>({0, 1}),
       makeNullableFlatVector<int64_t>({std::nullopt, 5})});

  testAggregations(
      vectors, {"c0"}, {"noisy_count_gaussian(c1, 0.0)"}, {expectedResult});
}

TEST_F(NoisyCountGaussianAggregationTest, twoAggregatesSingleGroupNoNoise) {
  auto vectors = makeVectors(rowType1_, 10, 4);
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {"noisy_count_gaussian(c1, 0.0)", "noisy_count_gaussian(c2, 0.0)"},
      "SELECT count(c1), count(c2) FROM tmp");

  // Test with non-zero noise_scale.
  auto vectors2 = {makeRowVector({makeConstant(1, 100)})}; // 100 rows of 1

  // Theoretically, the outcome of noisy_count_gaussian(1, noise_scale) should
  // be unpredictable, but with about 95% of the valuesfall within +/- 2 *
  // noise_scale, and 99.7% within +/- 3 * noise_scale. We set the noise_scale
  // to 1.0, To avoid any failure by chance, we expect the noisy count to be
  // within +/- 50 of the actual 100. check if the noisy count is within [50,
  // 150].

  auto result =
      AssertQueryBuilder(
          PlanBuilder()
              .values(vectors2)
              .singleAggregation({}, {"noisy_count_gaussian(c0, 1.0)"}, {})
              .planNode(),
          duckDbQueryRunner_)
          .copyResults(pool());
  ASSERT_TRUE(result->size() == 1);
  ASSERT_TRUE(result->childAt(0)->asFlatVector<int64_t>()->valueAt(0) >= 50);
  ASSERT_TRUE(result->childAt(0)->asFlatVector<int64_t>()->valueAt(0) <= 150);
}

} // namespace facebook::velox::aggregate::test
