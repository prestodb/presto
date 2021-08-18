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
#include "velox/aggregates/AggregationHook.h"
#include "velox/aggregates/tests/AggregationTestBase.h"
#include "velox/exec/tests/PlanBuilder.h"

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
            .finalAggregation({}, {"sum(a0)"})
            .planNode();
  assertQuery(agg, "SELECT sum(c1) FROM tmp");

  // Group by partial aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10", "c1"}, {"c0 % 10", "c1"})
            .partialAggregation({0}, {"sum(c1)"})
            .planNode();
  assertQuery(agg, "SELECT c0 % 10, sum(c1) FROM tmp GROUP BY 1");

  // Group by final aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10", "c1"}, {"c0 % 10", "c1"})
            .partialAggregation({0}, {"sum(c1)"})
            .finalAggregation({0}, {"sum(a0)"})
            .planNode();
  assertQuery(agg, "SELECT c0 % 10, sum(c1) FROM tmp GROUP BY 1");

  // encodings: use filter to wrap aggregation inputs in a dictionary.
  agg = PlanBuilder()
            .values(vectors)
            .filter("c0 % 2 = 0")
            .project({"c0 % 11", "c1"}, {"c0_mod_11", "c1"})
            .partialAggregation({0}, {"sum(c1)"})
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
                 .partialAggregation({0}, {"sum(c1)"})
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
                 .partialAggregation({0}, {"sum(c1)"})
                 .planNode();
  assertQuery(agg, "SELECT 1 LIMIT 0");
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
