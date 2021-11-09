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
#include "velox/exec/tests/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

using facebook::velox::exec::test::PlanBuilder;

namespace facebook::velox::aggregate::test {

namespace {

class BoolAndOrTest : public AggregationTestBase {};

TEST_F(BoolAndOrTest, boolOr) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), BOOLEAN()});
  auto vectors = makeVectors(rowType, 1000, 10);
  createDuckDbTable(vectors);

  // Global partial aggregation.
  auto agg = PlanBuilder()
                 .values(vectors)
                 .partialAggregation({}, {"bool_or(c1)"})
                 .planNode();
  assertQuery(agg, "SELECT bit_or(c1::TINYINT) FROM tmp");

  // Global final aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .partialAggregation({}, {"bool_or(c1)"})
            .finalAggregation({}, {"bool_or(a0)"})
            .planNode();
  assertQuery(agg, "SELECT bit_or(c1::TINYINT) FROM tmp");

  // Group by partial aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10", "c1"}, {"c0 % 10", "c1"})
            .partialAggregation({0}, {"bool_or(c1)"})
            .planNode();
  assertQuery(agg, "SELECT c0 % 10, bit_or(c1::TINYINT) FROM tmp GROUP BY 1");

  // Group by final aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10", "c1"}, {"c0 % 10", "c1"})
            .partialAggregation({0}, {"bool_or(c1)"})
            .finalAggregation({0}, {"bool_or(a0)"})
            .planNode();
  assertQuery(agg, "SELECT c0 % 10, bit_or(c1::TINYINT) FROM tmp GROUP BY 1");

  // encodings: use filter to wrap aggregation inputs in a dictionary.
  agg = PlanBuilder()
            .values(vectors)
            .filter("c0 % 2 = 0")
            .project({"c0 % 11", "c1"}, {"c0_mod_11", "c1"})
            .partialAggregation({0}, {"bool_and(c1)"})
            .planNode();

  assertQuery(
      agg,
      "SELECT c0 % 11, bit_and(c1::TINYINT) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");

  agg = PlanBuilder()
            .values(vectors)
            .filter("c0 % 2 = 0")
            .partialAggregation({}, {"bool_and(c1)"})
            .planNode();
  assertQuery(agg, "SELECT bit_and(c1::TINYINT) FROM tmp WHERE c0 % 2 = 0");
}

TEST_F(BoolAndOrTest, boolAnd) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), BOOLEAN()});
  auto vectors = makeVectors(rowType, 1000, 10);
  createDuckDbTable(vectors);

  // Global partial aggregation.
  auto agg = PlanBuilder()
                 .values(vectors)
                 .partialAggregation({}, {"bool_and(c1)"})
                 .planNode();
  assertQuery(agg, "SELECT bit_and(c1::TINYINT) FROM tmp");

  // Global final aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .partialAggregation({}, {"bool_and(c1)"})
            .finalAggregation({}, {"bool_and(a0)"})
            .planNode();
  assertQuery(agg, "SELECT bit_and(c1::TINYINT) FROM tmp");

  // Group by partial aggregation.

  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10", "c1"}, {"c0 % 10", "c1"})
            .partialAggregation({0}, {"bool_and(c1)"})
            .planNode();
  assertQuery(agg, "SELECT c0 % 10, bit_and(c1::TINYINT) FROM tmp GROUP BY 1");

  // Group by final aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10", "c1"}, {"c0 % 10", "c1"})
            .partialAggregation({0}, {"bool_and(c1)"})
            .finalAggregation({0}, {"bool_and(a0)"})
            .planNode();
  assertQuery(agg, "SELECT c0 % 10, bit_and(c1::TINYINT) FROM tmp GROUP BY 1");

  // encodings: use filter to wrap aggregation inputs in a dictionary.
  agg = PlanBuilder()
            .values(vectors)
            .filter("c0 % 2 = 0")
            .project({"c0 % 11", "c1"}, {"c0_mod_11", "c1"})
            .partialAggregation({0}, {"bool_and(c1)"})
            .planNode();

  assertQuery(
      agg,
      "SELECT c0 % 11, bit_and(c1::TINYINT) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");

  agg = PlanBuilder()
            .values(vectors)
            .filter("c0 % 2 = 0")
            .partialAggregation({}, {"bool_and(c1)"})
            .planNode();
  assertQuery(agg, "SELECT bit_and(c1::TINYINT) FROM tmp WHERE c0 % 2 = 0");
}

} // namespace
} // namespace facebook::velox::aggregate::test
