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

class CountAggregation : public AggregationTestBase {
 protected:
  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"},
          {BIGINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           VARCHAR(),
           TINYINT()})};
};

TEST_F(CountAggregation, count) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);

  auto agg = PlanBuilder()
                 .values(vectors)
                 .partialAggregation({}, {"count(1)"})
                 .planNode();
  assertQuery(agg, "SELECT count(1) FROM tmp");

  // count over column with nulls; only non-null rows should be counted
  agg = PlanBuilder()
            .values(vectors)
            .partialAggregation({}, {"count(c1)"})
            .finalAggregation()
            .planNode();
  assertQuery(agg, "SELECT count(c1) FROM tmp");

  // count over zero rows; the result should be 0, not null
  agg = PlanBuilder()
            .values(vectors)
            .filter("c0 % 3 > 5")
            .partialAggregation({}, {"count(c1)"})
            .finalAggregation()
            .planNode();
  assertQuery(agg, "SELECT count(c1) FROM tmp WHERE c0 % 3 > 5");

  // final count aggregation is a sum of partial counts
  agg = PlanBuilder()
            .values(vectors)
            .project({"cast(c1 as bigint) AS c1_bigint"})
            .finalAggregation({}, {"count(c1_bigint)"}, {BIGINT()})
            .planNode();
  assertQuery(agg, "SELECT sum(c1) FROM tmp");

  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10 AS c0_mod_10", "c1"})
            .partialAggregation({"c0_mod_10"}, {"count(1)"})
            .finalAggregation()
            .planNode();
  assertQuery(agg, "SELECT c0 % 10, count(1) FROM tmp GROUP BY 1");

  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10 AS c0_mod_10", "c7"})
            .partialAggregation({"c0_mod_10"}, {"count(c7)"})
            .planNode();
  assertQuery(agg, "SELECT c0 % 10, count(c7) FROM tmp GROUP BY 1");

  // Add local exchange before intermediate aggregation.
  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();

  CursorParameters params;
  params.planNode = PlanBuilder(planNodeIdGenerator)
                        .localPartition(
                            {"p0"},
                            {PlanBuilder(planNodeIdGenerator)
                                 .values(vectors)
                                 .project({"c0 % 10", "c1"})
                                 .partialAggregation({"p0"}, {"count(1)"})
                                 .planNode()})
                        .intermediateAggregation()
                        .planNode();
  params.maxDrivers = 2;
  assertQuery(params, "SELECT c0 % 10, count(1) FROM tmp GROUP BY 1");

  // Add local exchange before intermediate aggregation. Single group.
  params.planNode = PlanBuilder(planNodeIdGenerator)
                        .localPartition(
                            {},
                            {PlanBuilder(planNodeIdGenerator)
                                 .localPartitionRoundRobin(
                                     {PlanBuilder(planNodeIdGenerator)
                                          .values(vectors)
                                          .project({"c0"})
                                          .partialAggregation({}, {"count(1)"})
                                          .planNode()})
                                 .intermediateAggregation()
                                 .planNode()})
                        .finalAggregation()
                        .planNode();
  assertQuery(params, "SELECT count(1) FROM tmp");
}

} // namespace
} // namespace facebook::velox::aggregate::test
