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
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

class NoisyCountIfGaussianAggregationTest
    : public facebook::velox::functions::aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2"}, {INTEGER(), BOOLEAN(), BOOLEAN()})};
};

TEST_F(NoisyCountIfGaussianAggregationTest, constNoNoise) {
  auto vectors = {
      makeRowVector({
          makeFlatVector<int64_t>(
              10, [](vector_size_t row) { return row / 3; }),
          makeConstant(true, 10),
          makeConstant(false, 10),
      }),
  };

  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {"noisy_count_if_gaussian(c1, 0.0)", "noisy_count_if_gaussian(c2, 0.0)"},
      "SELECT sum(if(c1, 1, 0)), sum(if(c2, 1, 0)) FROM tmp");

  testAggregations(
      vectors,
      {"c0"},
      {"noisy_count_if_gaussian(c1, 0.0)", "noisy_count_if_gaussian(c2, 0.0)"},
      "SELECT c0, sum(if(c1, 1, 0)), sum(if(c2, 1, 0)) FROM tmp group by c0");
}

TEST_F(NoisyCountIfGaussianAggregationTest, invalidNoise) {
  // Make two batches of rows: one with nulls; another without.
  auto vectors = {
      makeRowVector(
          {makeFlatVector<bool>(10, [](auto row) { return row % 5 == 0; })}),
      makeRowVector({makeFlatVector<bool>(
          10, [](auto row) { return row % 3 == 0; }, nullEvery(4))}),
  };

  createDuckDbTable(vectors);

  testFailingAggregations(
      vectors,
      {},
      {"noisy_count_if_gaussian(c0, -1.0)"},
      "Noise scale must be a non-negative value");
}

TEST_F(NoisyCountIfGaussianAggregationTest, oneAggregateSingleGroupNoNoise) {
  // Make two batches of rows: one with nulls; another without.
  auto vectors = {
      makeRowVector(
          {makeFlatVector<bool>(10, [](auto row) { return row % 5 == 0; })}),
      makeRowVector({makeFlatVector<bool>(
          10, [](auto row) { return row % 3 == 0; }, nullEvery(4))}),
  };

  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {"noisy_count_if_gaussian(c0, 0.0)"},
      "SELECT sum(if(c0, 1, 0)) FROM tmp");
}

TEST_F(NoisyCountIfGaussianAggregationTest, oneAggregateSingleGroupEmpty) {
  auto vectors = {
      makeRowVector({makeAllNullFlatVector<bool>(10)}),
  };

  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {"noisy_count_if_gaussian(c0, 0.0)"},
      "SELECT sum(if(c0, 1, NULL)) FROM tmp");
}

TEST_F(NoisyCountIfGaussianAggregationTest, oneAggregateMultipleGroupsNoNoise) {
  auto vectors = {
      makeRowVector(
          {makeFlatVector<int64_t>(
               1'000, [](auto row) { return row % 5; }), // c0
           makeFlatVector<bool>(
               1'000,
               [](auto row) { return row % 3 == 0; },
               nullEvery(7))}), // c1
  };
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {"c0"},
      {"noisy_count_if_gaussian(c1, 0.0)"},
      "SELECT c0, sum(if(c1, 1, 0)) FROM tmp GROUP BY c0");
}

TEST_F(NoisyCountIfGaussianAggregationTest, twoAggregatesSingleGroupNoNoise) {
  auto vectors = makeVectors(rowType_, 10, 4);
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {"noisy_count_if_gaussian(c1, 0.0)", "noisy_count_if_gaussian(c2, 0.0)"},
      "SELECT sum(if(c1, 1, 0)), sum(if(c2, 1, 0)) FROM tmp");
}

TEST_F(
    NoisyCountIfGaussianAggregationTest,
    twoAggregatesMultipleGroupsNoNoise) {
  auto vectors = {
      makeRowVector({
          makeFlatVector<int64_t>(
              1'000, [](auto row) { return row % 5; }), // c0
          makeFlatVector<bool>(
              1'000, [](auto row) { return row % 3 == 0; }, nullEvery(7)), // c1
          makeFlatVector<bool>(
              1'000, [](auto row) { return row % 4 == 0; }, nullEvery(11)) // c2
      }),

  };
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {"c0"},
      {"noisy_count_if_gaussian(c1, 0.0)", "noisy_count_if_gaussian(c2, 0.0)"},
      "SELECT c0, SUM(if(c1, 1, 0)), SUM(if(c2, 1, 0)) FROM tmp GROUP BY c0");
}

TEST_F(
    NoisyCountIfGaussianAggregationTest,
    twoAggregatesMultipleGroupsWrappedNoNoise) {
  auto vectors = makeVectors(rowType_, 10, 4);
  createDuckDbTable(vectors);

  testAggregations(
      [&](auto& builder) {
        builder.values(vectors)
            .filter("c0 % 2 = 0")
            .project({"c0 % 11 AS c0_mod_11", "c1", "c2"});
      },
      {"c0_mod_11"},
      {"noisy_count_if_gaussian(c1, 0.0)", "noisy_count_if_gaussian(c2, 0.0)"},
      "SELECT c0 % 11, SUM(if(c1, 1, 0)), SUM(if(c2, 1, 0)) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");
}

// Test that the aggregation works with zero rows, multiple groups and a
// constant noise scale.
TEST_F(NoisyCountIfGaussianAggregationTest, zeroRowsMultipleGroupsNoNoise) {
  auto vectors = makeVectors(rowType_, 0, 5);
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {"c0"},
      {"noisy_count_if_gaussian(c1, 0.0)", "noisy_count_if_gaussian(c2, 0.0)"},
      "SELECT c0, sum(if(c1, 1, 0)), sum(if(c2, 1, 0)) FROM tmp group by c0, c1");
}

// Test that the aggregation supports BIGINT as a valid
// noise scale.
TEST_F(NoisyCountIfGaussianAggregationTest, bigIntNoiseScaleTypeNoNoise) {
  auto vectors = makeVectors(rowType_, 10, 4);
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {"noisy_count_if_gaussian(c1, 0)", "noisy_count_if_gaussian(c2, 0)"},
      "SELECT sum(if(c1, 1, 0)), sum(if(c2, 1, 0)) FROM tmp");
}

// Test that the aggregation random seed as input
TEST_F(NoisyCountIfGaussianAggregationTest, randomSeedNoNoise) {
  auto vectors = makeVectors(rowType_, 10, 4);
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {"noisy_count_if_gaussian(c1, 0.0, 456)",
       "noisy_count_if_gaussian(c2, 0.0, 456)"},
      "SELECT sum(if(c1, 1, 0)), sum(if(c2, 1, 0)) FROM tmp");
}

// Test that given a random seed, the aggregation is deterministic.
TEST_F(NoisyCountIfGaussianAggregationTest, randomSeedDeterminism) {
  auto vectors = {
      makeRowVector({
          makeFlatVector<int64_t>(
              20, [](vector_size_t row) { return row / 3; }),
          makeConstant(true, 20),
          makeConstant(false, 20),
      }),
  };

  // Now run the aggregation with the given random seed and extract the result
  auto expectedResult =
      AssertQueryBuilder(
          PlanBuilder()
              .values(vectors)
              .singleAggregation(
                  {}, {"noisy_count_if_gaussian(c1, 3.0, 456)"}, {})
              .planNode(),
          duckDbQueryRunner_)
          .copyResults(pool());

  // The result should be the same for the same input, noise scale, and seed
  int numRuns = 10;
  for (int i = 0; i < numRuns; i++) {
    testAggregations(
        vectors, {}, {"noisy_count_if_gaussian(c1, 3, 456)"}, {expectedResult});
  }
}

} // namespace facebook::velox::aggregate::test
