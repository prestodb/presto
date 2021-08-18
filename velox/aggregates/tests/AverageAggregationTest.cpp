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
#include "velox/aggregates/tests/AggregationTestBase.h"
#include "velox/exec/tests/PlanBuilder.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

namespace {

class AverageAggregation : public AggregationTestBase {
 protected:
  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), SMALLINT(), INTEGER(), BIGINT(), REAL(), DOUBLE()})};
};

TEST_F(AverageAggregation, avg) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);

  // global aggregation
  {
    auto agg =
        PlanBuilder()
            .values(vectors)
            .partialAggregation(
                {}, {"avg(c1)", "avg(c2)", "avg(c4)", "avg(c5)"})
            .finalAggregation({}, {"avg(a0)", "avg(a1)", "avg(a2)", "avg(a3)"})
            .planNode();
    assertQuery(agg, "SELECT avg(c1), avg(c2), avg(c4), avg(c5) FROM tmp");

    agg =
        PlanBuilder()
            .values(vectors)
            .singleAggregation({}, {"avg(c1)", "avg(c2)", "avg(c4)", "avg(c5)"})
            .planNode();
    assertQuery(agg, "SELECT avg(c1), avg(c2), avg(c4), avg(c5) FROM tmp");

    agg =
        PlanBuilder()
            .values(vectors)
            .partialAggregation(
                {}, {"avg(c1)", "avg(c2)", "avg(c4)", "avg(c5)"})
            .intermediateAggregation(
                {}, {"avg(a0)", "avg(a1)", "avg(a2)", "avg(a3)"})
            .finalAggregation({}, {"avg(a0)", "avg(a1)", "avg(a2)", "avg(a3)"})
            .planNode();
    assertQuery(agg, "SELECT avg(c1), avg(c2), avg(c4), avg(c5) FROM tmp");
  }

  // global aggregation; no input
  {
    auto agg = PlanBuilder()
                   .values(vectors)
                   .filter("c0 % 2 = 5")
                   .partialAggregation({}, {"avg(c0)"})
                   .finalAggregation({}, {"avg(a0)"})
                   .planNode();
    assertQuery(agg, "SELECT null");
  }

  // global aggregation over filter
  {
    auto agg = PlanBuilder()
                   .values(vectors)
                   .filter("c0 % 5 = 3")
                   .partialAggregation({}, {"avg(c1)"})
                   .finalAggregation({}, {"avg(a0)"})
                   .planNode();
    assertQuery(agg, "SELECT avg(c1) FROM tmp WHERE c0 % 5 = 3");
  }

  // group by
  {
    auto agg =
        PlanBuilder()
            .values(vectors)
            .project(
                std::vector<std::string>{
                    "c0 % 10", "c1", "c2", "c3", "c4", "c5"},
                std::vector<std::string>{
                    "c0_mod_10", "c1", "c2", "c3", "c4", "c5"})
            .partialAggregation(
                {0}, {"avg(c1)", "avg(c2)", "avg(c3)", "avg(c4)", "avg(c5)"})
            .finalAggregation(
                {0}, {"avg(a0)", "avg(a1)", "avg(a2)", "avg(a3)", "avg(a4)"})
            .planNode();
    assertQuery(
        agg,
        "SELECT c0 % 10, avg(c1), avg(c2), avg(c3::DOUBLE), avg(c4), avg(c5) FROM tmp GROUP BY 1");

    agg = PlanBuilder()
              .values(vectors)
              .project(
                  std::vector<std::string>{
                      "c0 % 10", "c1", "c2", "c3", "c4", "c5"},
                  std::vector<std::string>{
                      "c0_mod_10", "c1", "c2", "c3", "c4", "c5"})
              .singleAggregation(
                  {0}, {"avg(c1)", "avg(c2)", "avg(c3)", "avg(c4)", "avg(c5)"})
              .planNode();
    assertQuery(
        agg,
        "SELECT c0 % 10, avg(c1), avg(c2), avg(c3::DOUBLE), avg(c4), avg(c5) FROM tmp GROUP BY 1");

    agg = PlanBuilder()
              .values(vectors)
              .project(
                  std::vector<std::string>{
                      "c0 % 10", "c1", "c2", "c3", "c4", "c5"},
                  std::vector<std::string>{
                      "c0_mod_10", "c1", "c2", "c3", "c4", "c5"})
              .partialAggregation(
                  {0}, {"avg(c1)", "avg(c2)", "avg(c3)", "avg(c4)", "avg(c5)"})
              .intermediateAggregation(
                  {0}, {"avg(a0)", "avg(a1)", "avg(a2)", "avg(a3)", "avg(a4)"})
              .finalAggregation(
                  {0}, {"avg(a0)", "avg(a1)", "avg(a2)", "avg(a3)", "avg(a4)"})
              .planNode();
    assertQuery(
        agg,
        "SELECT c0 % 10, avg(c1), avg(c2), avg(c3::DOUBLE), avg(c4), avg(c5) FROM tmp GROUP BY 1");
  }

  // group by; no input
  {
    auto agg = PlanBuilder()
                   .values(vectors)
                   .project(
                       std::vector<std::string>{"c0 % 10", "c1"},
                       std::vector<std::string>{"c0_mod_10", "c1"})
                   .filter("c0_mod_10 > 10")
                   .partialAggregation({0}, {"avg(c1)"})
                   .finalAggregation({0}, {"avg(a0)"})
                   .planNode();
    assertQuery(agg, "");
  }

  // group by over filter
  {
    auto agg = PlanBuilder()
                   .values(vectors)
                   .filter("c2 % 5 = 3")
                   .project(
                       std::vector<std::string>{"c0 % 10", "c1"},
                       std::vector<std::string>{"c0_mod_10", "c1"})
                   .partialAggregation({0}, {"avg(c1)"})
                   .finalAggregation({0}, {"avg(a0)"})
                   .planNode();
    assertQuery(
        agg, "SELECT c0 % 10, avg(c1) FROM tmp WHERE c2 % 5 = 3 GROUP BY 1");
  }
}

} // namespace
} // namespace facebook::velox::aggregate::test
