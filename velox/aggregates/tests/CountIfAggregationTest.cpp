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
#include "velox/aggregates/AggregateNames.h"
#include "velox/aggregates/tests/AggregationTestBase.h"
#include "velox/exec/tests/PlanBuilder.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

class CountIfAggregationTest : public AggregationTestBase {
 protected:
  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2"}, {INTEGER(), BOOLEAN(), BOOLEAN()})};
};

TEST_F(CountIfAggregationTest, oneAggregateSingleGroup) {
  // Make two batches of rows: one with nulls; another without.
  auto vectors = {
      makeRowVector(
          {makeFlatVector<bool>(1'000, [](auto row) { return row % 5 == 0; })}),
      makeRowVector({makeFlatVector<bool>(
          1'100, [](auto row) { return row % 3 == 0; }, nullEvery(7))}),
  };

  createDuckDbTable(vectors);
  auto agg = PlanBuilder()
                 .values(vectors)
                 .partialAggregation({}, {"count_if(c0)"})
                 .finalAggregation({}, {"count(a0)"})
                 .planNode();
  assertQuery(agg, "SELECT sum(if(c0, 1, 0)) FROM tmp");
}

TEST_F(CountIfAggregationTest, oneAggregateMultipleGroups) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  auto agg = PlanBuilder()
                 .values(vectors)
                 .partialAggregation({0}, {"count_if(c1)"})
                 .finalAggregation({0}, {"count_if(a0)"})
                 .planNode();
  assertQuery(
      agg,
      "SELECT c0, sum(CASE WHEN c1 THEN 1 ELSE 0 END) FROM tmp GROUP BY c0");
}

TEST_F(CountIfAggregationTest, twoAggregatesSingleGroup) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  auto agg = PlanBuilder()
                 .values(vectors)
                 .partialAggregation({}, {"count_if(c1)", "count_if(c2)"})
                 .finalAggregation({}, {"count_if(a0)", "count_if(a1)"})
                 .planNode();
  assertQuery(
      agg,
      "SELECT sum(CASE WHEN c1 THEN 1 ELSE 0 END), "
      "sum(CASE WHEN c2 THEN 1 ELSE 0 END) FROM tmp");
}

TEST_F(CountIfAggregationTest, twoAggregatesMultipleGroups) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  auto agg = PlanBuilder()
                 .values(vectors)
                 .partialAggregation({0}, {"count_if(c1)", "count_if(c2)"})
                 .finalAggregation({0}, {"count_if(a0)", "count_if(a1)"})
                 .planNode();
  assertQuery(
      agg,
      "SELECT c0, SUM(CASE WHEN c1 THEN 1 ELSE 0 END), "
      "SUM(CASE WHEN c2 THEN 1 ELSE 0 END) FROM tmp GROUP BY c0");
}

TEST_F(CountIfAggregationTest, twoAggregatesMultipleGroupsWrapped) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  auto agg = PlanBuilder()
                 .values(vectors)
                 .filter("c0 % 2 = 0")
                 .project({"c0 % 11", "c1", "c2"}, {"c0_mod_11", "c1", "c2"})
                 .partialAggregation({0}, {"count_if(c1)", "count_if(c2)"})
                 .finalAggregation({0}, {"count_if(a0)", "count_if(a1)"})
                 .planNode();
  assertQuery(
      agg,
      "SELECT c0 % 11, SUM(CASE WHEN c1 THEN 1 ELSE 0 END), "
      "SUM(CASE WHEN c2 THEN 1 ELSE 0 END) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");
}

} // namespace facebook::velox::aggregate::test
