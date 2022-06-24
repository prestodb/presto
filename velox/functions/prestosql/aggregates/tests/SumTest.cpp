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
#include "velox/exec/AggregationHook.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

using facebook::velox::exec::test::PlanBuilder;

namespace facebook::velox::aggregate::test {

namespace {

class SumTest : public AggregationTestBase {};

TEST_F(SumTest, sumTinyint) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), TINYINT()});
  auto vectors = makeVectors(rowType, 1000, 10);
  createDuckDbTable(vectors);

  // Global partial aggregation.
  auto agg = PlanBuilder()
                 .values(vectors)
                 .partialAggregation({}, {"sum(c1)"})
                 .planNode();
  assertQuery(agg, "SELECT sum(c1) FROM tmp");

  // Global final aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .partialAggregation({}, {"sum(c1)"})
            .finalAggregation()
            .planNode();
  assertQuery(agg, "SELECT sum(c1) FROM tmp");

  // Group by partial aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10", "c1"})
            .partialAggregation({"p0"}, {"sum(c1)"})
            .planNode();
  assertQuery(agg, "SELECT c0 % 10, sum(c1) FROM tmp GROUP BY 1");

  // Group by final aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10", "c1"})
            .partialAggregation({"p0"}, {"sum(c1)"})
            .finalAggregation()
            .planNode();
  assertQuery(agg, "SELECT c0 % 10, sum(c1) FROM tmp GROUP BY 1");

  // encodings: use filter to wrap aggregation inputs in a dictionary.
  agg = PlanBuilder()
            .values(vectors)
            .filter("c0 % 2 = 0")
            .project({"c0 % 11", "c1"})
            .partialAggregation({"p0"}, {"sum(c1)"})
            .planNode();
  assertQuery(
      agg, "SELECT c0 % 11, sum(c1) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");

  agg = PlanBuilder()
            .values(vectors)
            .filter("c0 % 2 = 0")
            .partialAggregation({}, {"sum(c1)"})
            .planNode();
  assertQuery(agg, "SELECT sum(c1) FROM tmp WHERE c0 % 2 = 0");
}

TEST_F(SumTest, sumDouble) {
  auto rowType = ROW({"c0", "c1"}, {REAL(), DOUBLE()});
  auto vectors = makeVectors(rowType, 1000, 10);
  createDuckDbTable(vectors);

  // Global final aggregation.
  auto agg = PlanBuilder()
                 .values(vectors)
                 .partialAggregation({}, {"sum(c0)", "sum(c1)"})
                 .intermediateAggregation()
                 .finalAggregation()
                 .planNode();
  assertQuery(agg, "SELECT sum(c0), sum(c1) FROM tmp");
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

  auto agg = PlanBuilder()
                 .values({vector})
                 .partialAggregation({"c0"}, {"sum(c1)"})
                 .planNode();
  assertQuery(agg, "SELECT c0, sum(c1) FROM tmp GROUP BY 1");
}

TEST_F(SumTest, emptyValues) {
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});
  auto vector = makeRowVector(
      {makeFlatVector<int32_t>(std::vector<int32_t>{}),
       makeFlatVector<int64_t>(std::vector<int64_t>{})});

  auto agg = PlanBuilder()
                 .values({vector})
                 .partialAggregation({"c0"}, {"sum(c1)"})
                 .planNode();
  assertQuery(agg, "SELECT 1 LIMIT 0");
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

  auto plan = PlanBuilder()
                  .values(data)
                  .partialAggregation({"a"}, {"sum(b) AS sum_b"})
                  .finalAggregation()
                  .planNode();

  assertQuery(plan, "SELECT a, sum(b) as sum_b FROM tmp GROUP BY 1");
}

struct SumRow {
  char nulls;
  int64_t sum;
};

TEST_F(SumTest, hook) {
  SumRow sumRow;
  sumRow.nulls = 1;
  sumRow.sum = 0;

  char* row = reinterpret_cast<char*>(&sumRow);
  uint64_t numNulls = 1;
  aggregate::SumHook<int64_t, int64_t> hook(
      offsetof(SumRow, sum), offsetof(SumRow, nulls), 1, &row, &numNulls);

  int64_t value = 11;
  hook.addValue(0, &value);
  EXPECT_EQ(0, sumRow.nulls);
  EXPECT_EQ(0, numNulls);
  EXPECT_EQ(value, sumRow.sum);
}
} // namespace
} // namespace facebook::velox::aggregate::test
