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

class NoisySumGaussianAggregationTest
    : public functions::aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
  }

  RowTypePtr doubleRowType_{
      ROW({"c0", "c1", "c2"}, {DOUBLE(), DOUBLE(), DOUBLE()})};
  RowTypePtr bigintRowType_{
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), BIGINT()})};
  RowTypePtr decimalRowType_{
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), DECIMAL(20, 5)})};
  RowTypePtr realRowType_{
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), REAL()})};
  RowTypePtr integerRowType_{
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()})};
  RowTypePtr smallintRowType_{
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), SMALLINT()})};
  RowTypePtr tinyintRowType_{
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), TINYINT()})};
};

TEST_F(NoisySumGaussianAggregationTest, simpleTestNoNoise) {
  auto vectors = {makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
       makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
       makeFlatVector<double>({1.0, 2.0, 3.0, 4.0, 5.0})})};

  // Expect the result to be 15.0
  auto expectedResult = makeRowVector({makeConstant(15.0, 1)});
  testAggregations(
      {vectors}, {}, {"noisy_sum_gaussian(c2, 0.0)"}, {expectedResult});

  // test nosie scale of bigint type.
  testAggregations(
      {vectors}, {}, {"noisy_sum_gaussian(c2, 0)"}, {expectedResult});
}

// Test cases where the noise scale is invalid.
TEST_F(NoisySumGaussianAggregationTest, inValidNoise) {
  auto vectors = makeVectors(doubleRowType_, 10, 5);
  createDuckDbTable(vectors);

  // Test should fail and output expected error message.
  testFailingAggregations(
      vectors,
      {},
      {"noisy_sum_gaussian(c2, -1.0)"},
      "Noise scale must be a non-negative value.");
}

TEST_F(NoisySumGaussianAggregationTest, nullTestNoNoise) {
  // Test non-null and null values mixed.
  auto vectors = makeRowVector(
      {makeFlatVector<double>({1, 2, 3, 4, 5}),
       makeFlatVector<double>({1, 2, 3, 4, 5}),
       makeNullableFlatVector<double>({std::nullopt, 2, std::nullopt, 4, 5})});

  // Expect the result to be 11.0
  auto expectedResult = makeRowVector({makeConstant(11.0, 1)});
  testAggregations(
      {vectors}, {}, {"noisy_sum_gaussian(c2, 0.0)"}, {expectedResult});

  // Test all null values.
  auto vectors2 = makeRowVector({makeAllNullFlatVector<double>(10)});

  // Expect the result to be NULL
  auto expectedResult2 = makeRowVector({makeNullConstant(TypeKind::DOUBLE, 1)});
  testAggregations(
      {vectors2}, {}, {"noisy_sum_gaussian(c0, 0.0)"}, {expectedResult2});
}

TEST_F(NoisySumGaussianAggregationTest, emptyTestNoNoise) {
  // Test empty input.
  auto vectors = {makeRowVector(
      {makeFlatVector<int64_t>({}),
       makeFlatVector<int64_t>({}),
       makeFlatVector<double>({})})};

  // Expect the result to be NULL
  auto expectedResult = makeRowVector({makeNullConstant(TypeKind::DOUBLE, 1)});
  testAggregations(
      {vectors}, {}, {"noisy_sum_gaussian(c2, 0.0)"}, {expectedResult});
}

TEST_F(
    NoisySumGaussianAggregationTest,
    singleGroupMultipleAggregationTestNoNoise) {
  auto vectors = makeVectors(doubleRowType_, 5, 3);
  createDuckDbTable(vectors);

  // Single group by key, multiple aggregation functions.
  testAggregations(
      vectors,
      {},
      {"noisy_sum_gaussian(c1, 0.0)", "noisy_sum_gaussian(c2, 0.0)"},
      "SELECT SUM(c1), SUM(c2) FROM tmp");
}

TEST_F(
    NoisySumGaussianAggregationTest,
    multipleGroupSingleAggregationTestNoNoise) {
  auto vectors = makeVectors(doubleRowType_, 5, 3);
  createDuckDbTable(vectors);

  // Multiple group by keys, single aggregation functions.
  testAggregations(
      vectors,
      {"c0", "c1"},
      {"noisy_sum_gaussian(c2, 0.0)"},
      "SELECT c0, c1, SUM(c2) FROM tmp GROUP BY c0, c1");
}

TEST_F(
    NoisySumGaussianAggregationTest,
    multipleGroupMultipleAggregationTestNoNoise) {
  auto vectors = makeVectors(doubleRowType_, 5, 3);
  createDuckDbTable(vectors);

  // Multiple group by keys, multiple aggregation functions.
  testAggregations(
      vectors,
      {"c0"},
      {"noisy_sum_gaussian(c1, 0.0)", "noisy_sum_gaussian(c2, 0.0)"},
      "SELECT c0, SUM(c1), SUM(c2) FROM tmp GROUP BY c0");
}

TEST_F(NoisySumGaussianAggregationTest, groupByNullTestNoNoise) {
  // Test group
  auto vectors = makeRowVector(
      {makeNullableFlatVector<double>({std::nullopt, 2, std::nullopt, 2, 2}),
       makeFlatVector<double>({1, 1, 3, 3, 3}),
       makeNullableFlatVector<double>({std::nullopt, 2, std::nullopt, 2, 2})});

  // Group by c0, aggregate c1, expect the result:
  // c0   | noisy_sum_gaussian(c1, 0.0)
  // NULL | 4
  // 2    | 7
  auto expectedResult = makeRowVector(
      {makeNullableFlatVector<double>({std::nullopt, 2}),
       makeNullableFlatVector<double>({4, 7})});
  testAggregations(
      {vectors}, {"c0"}, {"noisy_sum_gaussian(c1, 0.0)"}, {expectedResult});

  // Group by c0, aggregate c2, expect the result:
  // c0   | noisy_sum_gaussian(c2, 0.0)
  // NULL | NULL
  // 2    | 6
  auto expectedResult2 = makeRowVector(
      {makeNullableFlatVector<double>({std::nullopt, 2}),
       makeNullableFlatVector<double>({std::nullopt, 6})});

  testAggregations(
      {vectors}, {"c0"}, {"noisy_sum_gaussian(c2, 0.0)"}, {expectedResult2});
}

TEST_F(NoisySumGaussianAggregationTest, inputTypeTestNoNoise) {
  // Test that the function supports various input types.
  auto doubleVector = makeVectors(doubleRowType_, 5, 3);
  auto bigintVector = makeVectors(bigintRowType_, 5, 3);
  auto decimalVector = makeVectors(decimalRowType_, 5, 3);
  auto realVector = makeVectors(realRowType_, 5, 3);
  auto integerVector = makeVectors(integerRowType_, 5, 3);
  auto smallintVector = makeVectors(smallintRowType_, 5, 3);
  auto tinyintVector = makeVectors(tinyintRowType_, 5, 3);

  createDuckDbTable(doubleVector);
  testAggregations(
      doubleVector,
      {},
      {"noisy_sum_gaussian(c2, 0.0)"}, // double noise_scale
      "SELECT sum(c2) FROM tmp");

  createDuckDbTable(bigintVector);
  testAggregations(
      bigintVector,
      {},
      {"noisy_sum_gaussian(c2, 0.0)"},
      "SELECT sum(c2) FROM tmp");

  createDuckDbTable(decimalVector);
  testAggregations(
      decimalVector,
      {},
      {"noisy_sum_gaussian(c2, 0.0)"},
      "SELECT sum(c2) FROM tmp");

  createDuckDbTable(realVector);
  testAggregations(
      realVector,
      {},
      {"noisy_sum_gaussian(c2, 0)"}, // bigint noise_scale
      "SELECT sum(c2) FROM tmp");

  createDuckDbTable(integerVector);
  testAggregations(
      integerVector,
      {},
      {"noisy_sum_gaussian(c2, 0)"},
      "SELECT sum(c2) FROM tmp");

  createDuckDbTable(smallintVector);
  testAggregations(
      smallintVector,
      {},
      {"noisy_sum_gaussian(c2, 0)"},
      "SELECT sum(c2) FROM tmp");

  createDuckDbTable(tinyintVector);
  testAggregations(
      tinyintVector,
      {},
      {"noisy_sum_gaussian(c2, 0)"},
      "SELECT sum(c2) FROM tmp");
}

TEST_F(NoisySumGaussianAggregationTest, randomSeedTestNoNoise) {
  // Test that the function support random seed, no noise.
  auto vectors = makeVectors(integerRowType_, 5, 3);
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      // noisy_sum_gaussian(col, noise_scale, randon_seed)
      {"noisy_sum_gaussian(c2, 0.0, 12345)"},
      "SELECT SUM(CASE WHEN c2 IS NOT NULL THEN c2 END) FROM tmp");
}

TEST_F(NoisySumGaussianAggregationTest, randomSeedTestWithNoise) {
  auto vectors = makeVectors(integerRowType_, 5, 3);
  createDuckDbTable(vectors);
  // Test that the noise is deterministic given the same noise_scale,
  // random_seed. noisy_sum_gaussian(col, noise_scale, randon_seed)
  auto expectedResult =
      AssertQueryBuilder(
          PlanBuilder()
              .values(vectors)
              .singleAggregation({}, {"noisy_sum_gaussian(c2, 20, 12345)"}, {})
              .planNode(),
          duckDbQueryRunner_)
          .copyResults(pool());

  int numRuns = 10;
  for (int i = 0; i < numRuns; i++) {
    testAggregations(
        vectors, {}, {"noisy_sum_gaussian(c2, 20, 12345)"}, {expectedResult});
  }
}

TEST_F(NoisySumGaussianAggregationTest, boundsTestNoNoise) {
  // Test easy case with lower and upper bounds.
  auto vectors1 = {makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
       makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
       makeFlatVector<double>({1.0, 2.0, 3.0, 4.0, 5.0})})};

  // With lower bound = 3.0 and upper bound = 5.0
  // Expect the result to be 18.0
  auto expectedResult = makeRowVector({makeConstant(18.0, 1)});
  testAggregations(
      {vectors1},
      {},
      {"noisy_sum_gaussian(c2, 0.0, 3.0, 5.0)"},
      {expectedResult});

  // Test against DuckDB.
  auto vectors2 = makeVectors(doubleRowType_, 5, 3);
  createDuckDbTable(vectors2);

  // Test double lower bound and double upper bound.
  testAggregations(
      vectors2,
      {},
      {"noisy_sum_gaussian(c2, 0.0, 0.1, 0.8)"},
      "SELECT SUM(CASE WHEN c2 IS NOT NULL THEN GREATEST(LEAST(c2, 0.8), 0.1) END) FROM tmp");

  // Test double lower bound and bigint upper bound.
  testAggregations(
      vectors2,
      {},
      {"noisy_sum_gaussian(c2, 0.0, 0.1, 800)"},
      "SELECT SUM(CASE WHEN c2 IS NOT NULL THEN GREATEST(LEAST(c2, 800), 0.1) END) FROM tmp");

  // Test bigint lower bound and double upper bound.
  testAggregations(
      vectors2,
      {},
      {"noisy_sum_gaussian(c2, 0.0, 10, 50.8)"},
      "SELECT SUM(CASE WHEN c2 IS NOT NULL THEN GREATEST(LEAST(c2, 50.8), 10) END) FROM tmp");

  // Test bigint lower bound and bigint upper bound.
  testAggregations(
      vectors2,
      {},
      {"noisy_sum_gaussian(c2, 0.0, 100, 800)"},
      "SELECT SUM(CASE WHEN c2 IS NOT NULL THEN GREATEST(LEAST(c2, 800), 100) END) FROM tmp");

  // Test lower bound > upper bound.
  testFailingAggregations(
      vectors2,
      {},
      {"noisy_sum_gaussian(c2, 0.0, 100, 80)"},
      "Lower bound must be less than or equal to upper bound.");
}

TEST_F(NoisySumGaussianAggregationTest, boundsTestWithNoise) {
  auto vectors = makeVectors(integerRowType_, 5, 3);
  createDuckDbTable(vectors);
  // Test that the noise is deterministic given the same noise_scale,
  // random_seed. noisy_sum_gaussian(col, noise_scale, lower_bound, upper_bound,
  // randon_seed)
  auto expectedResult =
      AssertQueryBuilder(
          PlanBuilder()
              .values(vectors)
              .singleAggregation(
                  {}, {"noisy_sum_gaussian(c2, 20, 10, 100, 12345)"}, {})
              .planNode(),
          duckDbQueryRunner_)
          .copyResults(pool());

  for (int i = 0; i < 10; i++) {
    testAggregations(
        vectors,
        {},
        {"noisy_sum_gaussian(c2, 20, 10, 100, 12345)"},
        {expectedResult});
  }
}

TEST_F(NoisySumGaussianAggregationTest, noisySumAndBoundConsistencyTest) {
  // True sum of c0 is always 0.
  auto vectors = {
      makeRowVector({makeFlatVector<double>({0.0, 0.0, 0.0, 0.0, 0.0})})};

  // Test that if upper bound <= 0, then the noisy sum is always <= 0.
  for (int i = 0; i < 10; i++) {
    auto result =
        AssertQueryBuilder(
            PlanBuilder()
                .values(vectors)
                .singleAggregation(
                    {}, {"noisy_sum_gaussian(c0, 2, -2, -0.1, 12345)"}, {})
                .planNode(),
            duckDbQueryRunner_)
            .copyResults(pool());

    ASSERT_TRUE(result->childAt(0)->asFlatVector<double>()->valueAt(0) <= 0);
  }

  // Test that if lower bound >= 0, then the noisy sum is always >= 0.
  for (int i = 0; i < 10; i++) {
    auto result =
        AssertQueryBuilder(
            PlanBuilder()
                .values(vectors)
                .singleAggregation(
                    {}, {"noisy_sum_gaussian(c0, 2, 0.1, 2, 12345)"}, {})
                .planNode(),
            duckDbQueryRunner_)
            .copyResults(pool());

    ASSERT_TRUE(result->childAt(0)->asFlatVector<double>()->valueAt(0) >= 0);
  }
}
} // namespace facebook::velox::aggregate::test
