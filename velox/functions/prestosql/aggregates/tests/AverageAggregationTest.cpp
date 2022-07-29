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
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

namespace {

class AverageAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), SMALLINT(), INTEGER(), BIGINT(), REAL(), DOUBLE()})};
};

TEST_F(AverageAggregationTest, avgConst) {
  // Have two row vectors a lest as it triggers different code paths.
  auto vectors = {
      makeRowVector({
          makeFlatVector<int64_t>(
              10, [](vector_size_t row) { return row / 3; }),
          makeConstant(5, 10),
          makeConstant(6.0, 10),
      }),
      makeRowVector({
          makeFlatVector<int64_t>(
              10, [](vector_size_t row) { return row / 3; }),
          makeConstant(5, 10),
          makeConstant(6.0, 10),
      }),
  };

  createDuckDbTable(vectors);

  testAggregations(
      vectors, {}, {"avg(c1)", "avg(c2)"}, "SELECT avg(c1), avg(c2) FROM tmp");

  testAggregations(
      vectors,
      {"c0"},
      {"avg(c1)", "avg(c2)"},
      "SELECT c0, avg(c1), avg(c2) FROM tmp group by c0");

  testAggregations(vectors, {}, {"avg(c0)"}, "SELECT avg(c0) FROM tmp");

  testAggregations(
      [&](auto& builder) {
        builder.values(vectors).project({"c0 % 2 AS c0_mod_2", "c0"});
      },
      {"c0_mod_2"},
      {"avg(c0)"},
      "SELECT c0 % 2, avg(c0) FROM tmp group by 1");
}

TEST_F(AverageAggregationTest, avgConstNull) {
  // Have at least two row vectors as it triggers different code paths.
  auto vectors = {
      makeRowVector({
          makeNullableFlatVector<int64_t>({0, 1, 2, 0, 1, 2, 0, 1, 2, 0}),
          makeNullConstant(TypeKind::BIGINT, 10),
          makeNullConstant(TypeKind::DOUBLE, 10),
      }),
      makeRowVector({
          makeNullableFlatVector<int64_t>({0, 1, 2, 0, 1, 2, 0, 1, 2, 0}),
          makeNullConstant(TypeKind::BIGINT, 10),
          makeNullConstant(TypeKind::DOUBLE, 10),
      }),
  };

  createDuckDbTable(vectors);

  testAggregations(
      vectors, {}, {"avg(c1)", "avg(c2)"}, "SELECT avg(c1), avg(c2) FROM tmp");

  testAggregations(
      vectors,
      {"c0"},
      {"avg(c1)", "avg(c2)"},
      "SELECT c0, avg(c1), avg(c2) FROM tmp group by c0");

  testAggregations(vectors, {}, {"avg(c0)"}, "SELECT avg(c0) FROM tmp");
}

TEST_F(AverageAggregationTest, avgNulls) {
  // Have two row vectors a lest as it triggers different code paths.
  auto vectors = {
      makeRowVector({
          makeNullableFlatVector<int64_t>({0, std::nullopt, 2, 0, 1}),
          makeNullableFlatVector<int64_t>({0, 1, std::nullopt, 3, 4}),
          makeNullableFlatVector<double>({0.1, 1.2, 2.3, std::nullopt, 4.4}),
      }),
      makeRowVector({
          makeNullableFlatVector<int64_t>({0, std::nullopt, 2, 0, 1}),
          makeNullableFlatVector<int64_t>({0, 1, std::nullopt, 3, 4}),
          makeNullableFlatVector<double>({0.1, 1.2, 2.3, std::nullopt, 4.4}),
      }),
  };

  createDuckDbTable(vectors);

  testAggregations(
      vectors, {}, {"avg(c1)", "avg(c2)"}, "SELECT avg(c1), avg(c2) FROM tmp");

  testAggregations(
      vectors,
      {"c0"},
      {"avg(c1)", "avg(c2)"},
      "SELECT c0, avg(c1), avg(c2) FROM tmp group by c0");
}

TEST_F(AverageAggregationTest, avg) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);

  // global aggregation
  testAggregations(
      vectors,
      {},
      {"avg(c1)", "avg(c2)", "avg(c4)", "avg(c5)"},
      "SELECT avg(c1), avg(c2), avg(c4), avg(c5) FROM tmp");

  // global aggregation; no input
  testAggregations(
      [&](auto& builder) { builder.values(vectors).filter("c0 % 2 = 5"); },
      {},
      {"avg(c0)"},
      "SELECT null");

  // global aggregation over filter
  testAggregations(
      [&](auto& builder) { builder.values(vectors).filter("c0 % 5 = 3"); },
      {},
      {"avg(c1)"},
      "SELECT avg(c1) FROM tmp WHERE c0 % 5 = 3");

  // group by
  testAggregations(
      [&](auto& builder) {
        builder.values(vectors).project(
            {"c0 % 10 AS c0_mod_10", "c1", "c2", "c3", "c4", "c5"});
      },
      {"c0_mod_10"},
      {"avg(c1)", "avg(c2)", "avg(c3)", "avg(c4)", "avg(c5)"},
      "SELECT c0 % 10, avg(c1), avg(c2), avg(c3::DOUBLE), "
      "avg(c4), avg(c5) FROM tmp GROUP BY 1");

  // group by; no input
  testAggregations(
      [&](auto& builder) {
        builder.values(vectors)
            .project({"c0 % 10 AS c0_mod_10", "c1"})
            .filter("c0_mod_10 > 10");
      },
      {"c0_mod_10"},
      {"avg(c1)"},
      "");

  // group by over filter
  testAggregations(
      [&](auto& builder) {
        builder.values(vectors)
            .filter("c2 % 5 = 3")
            .project({"c0 % 10 AS c0_mod_10", "c1"});
      },
      {"c0_mod_10"},
      {"avg(c1)"},
      "SELECT c0 % 10, avg(c1) FROM tmp WHERE c2 % 5 = 3 GROUP BY 1");
}

TEST_F(AverageAggregationTest, partialResults) {
  auto data = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; })});

  auto plan = PlanBuilder()
                  .values({data})
                  .partialAggregation({}, {"avg(c0)"})
                  .planNode();

  assertQuery(plan, "SELECT row(4950, 100)");
}
} // namespace
} // namespace facebook::velox::aggregate::test
