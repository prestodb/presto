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
  RowTypePtr rowType3_{
      ROW({"c0", "c1", "c2"},
          {REAL(), ARRAY(BIGINT()), MAP(BIGINT(), BIGINT())})};
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

// Test cases where the input vector is empty. Return null.
TEST_F(NoisyCountGaussianAggregationTest, emptyInputNoNoise) {
  auto vectors = makeVectors(rowType1_, 0, 1);

  // Test against the expected result, will return NULL.
  auto expectedResult = makeRowVector({makeAllNullFlatVector<int64_t>(1)});

  testAggregations(
      vectors, {}, {"noisy_count_gaussian(c0, 0.0)"}, {expectedResult});
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

// Test distinct noisy_count.
TEST_F(NoisyCountGaussianAggregationTest, distinctCount) {
  auto vectors = makeVectors(rowType1_, 4, 3);
  createDuckDbTable(vectors);

  // Use singleAggregation directly instead of testAggregations to avoid partial
  // aggregation which doesn't support distinct aggregations
  auto result = AssertQueryBuilder(
                    PlanBuilder()
                        .values(vectors)
                        .markDistinct("distinct", {"c0"})
                        .singleAggregation(
                            {}, {"noisy_count_gaussian(c0, 0.0)"}, {"distinct"})
                        .planNode(),
                    duckDbQueryRunner_)
                    .assertResults("SELECT count(distinct c0) FROM tmp");
}

// Test non-scalar input types.
TEST_F(NoisyCountGaussianAggregationTest, nonScalarInputNoNoise) {
  auto vectors = makeVectors(rowType3_, 10, 3);
  createDuckDbTable(vectors);

  // Aggregate on column c1 which contains array values.
  testAggregations(
      vectors,
      {},
      {"noisy_count_gaussian(c1, 0.0)"},
      "SELECT count(c1) FROM tmp");

  // Aggregate on column c2 which contains map values.
  testAggregations(
      vectors,
      {},
      {"noisy_count_gaussian(c2, 0.0)"},
      "SELECT count(c2) FROM tmp");
}

// Test cases where the noise_scale is BIGINT.
TEST_F(NoisyCountGaussianAggregationTest, noiseScaleBigintNoNoise) {
  auto vectors = makeVectors(rowType1_, 10, 3);
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {"noisy_count_gaussian(c0, 0)"},
      "SELECT count(c0) FROM tmp");
}

TEST_F(NoisyCountGaussianAggregationTest, twoAggregatesMultipleGroupsNoNoise) {
  // Test case that do not contain nulls.
  auto vectors = {
      makeRowVector({
          makeFlatVector<int>(1'000, [](auto row) { return row % 5; }), // c0
          makeFlatVector<int>(1'000, [](auto row) { return row % 2; }), // c1
          makeFlatVector<std::string>(
              1'000, [](auto row) { return std::to_string(row) + "foo"; }) // c2
      }),
  };
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {"c0"},
      {"noisy_count_gaussian(c1, 0.0, 1234)", "noisy_count_gaussian(c2, 0.0)"},
      "SELECT c0, count(c1), count(c2) FROM tmp GROUP BY c0");

  // Test case that contains nulls.
  auto vectors2 = {
      // This test case is designed to test the senario where the aggregated
      // column has null values for one of the groupby keys.
      // c0: null,  1,  0,   1,  0,  null,  0,   1,  0,   1
      // c1: null,  1, null, 1, null,  1,  null, 1, null, 1
      // c2: null, "foo", ... "foo", "foo"
      makeRowVector({
          makeFlatVector<int>(
              10, [](auto row) { return row % 2; }, nullEvery(5)), // c0
          makeFlatVector<int>(
              10, [](auto row) { return row % 2; }, nullEvery(2)), // c1
          makeFlatVector<std::string>(
              10,
              [](auto row) { return std::to_string(row) + "foo"; },
              nullEvery(10)) // c2
      }),
  };

  // EXPECTED RESULT:
  // C0   | noisy_count_gaussian(c1, 0.0) | noisy_count_gaussian(c2, 0.0)
  // NULL | 1                             | 1
  // 0    | NULL                          | 4
  // 1    | 4                             | 4
  auto expectedResult = makeRowVector(
      {makeNullableFlatVector<int>({std::nullopt, 0, 1}),
       makeNullableFlatVector<int64_t>({1, std::nullopt, 4}),
       makeNullableFlatVector<int64_t>({1, 4, 4})});

  testAggregations(
      vectors2,
      {"c0"},
      {"noisy_count_gaussian(c1, 0.0, 1234)",
       "noisy_count_gaussian(c2, 0.0, 5678)"},
      {expectedResult});
}

TEST_F(
    NoisyCountGaussianAggregationTest,
    twoAggregatesMultipleGroupsWrappedNoNoise) {
  auto vectors = makeVectors(rowType1_, 10, 4);
  createDuckDbTable(vectors);

  testAggregations(
      [&](auto& builder) {
        builder.values(vectors)
            .filter("c0 % 2 = 0")
            .project({"c0 % 11 AS c0_mod_11", "c1", "c2"});
      },
      {"c0_mod_11"},
      {"noisy_count_gaussian(c1, 0.0, 000)",
       "noisy_count_gaussian(c2, 0.0, 000)"},
      "SELECT c0 % 11, count(c1), count(c2) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");
}

// Test that the aggregation random seed as input
TEST_F(NoisyCountGaussianAggregationTest, randomSeedNoNoise) {
  auto vectors = makeVectors(rowType2_, 10, 4);
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {"noisy_count_gaussian(c1, 0.0, 456)",
       "noisy_count_gaussian(c2, 0.0, 456)"},
      "SELECT count(c1), count(c2) FROM tmp");
}

// Test that given a random seed, the aggregation is deterministic.
TEST_F(NoisyCountGaussianAggregationTest, randomSeedDeterminism) {
  auto vectors = makeVectors(rowType1_, 10, 4);

  // Now run the aggregation with the given random seed and extract the result
  auto expectedResult =
      AssertQueryBuilder(
          PlanBuilder()
              .values(vectors)
              .singleAggregation({}, {"noisy_count_gaussian(c1, 3.0, 456)"}, {})
              .planNode(),
          duckDbQueryRunner_)
          .copyResults(pool());

  // The result should be the same for the same input, noise scale, and seed
  int numRuns = 10;
  for (int i = 0; i < numRuns; i++) {
    testAggregations(
        vectors, {}, {"noisy_count_gaussian(c1, 3, 456)"}, {expectedResult});
  }
}

} // namespace facebook::velox::aggregate::test
