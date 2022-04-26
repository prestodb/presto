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

// Replace the given query's placeholders '{0}' with the given aggregation name.
std::string genAggrQuery(const char* query, const char* aggrName) {
  return fmt::format(query, aggrName);
}

// Helper generates aggregation over column string.
std::string genAggr(const char* aggrName, const char* colName) {
  return fmt::format("{}({})", aggrName, colName);
}

// Macro to make it even shorter (assumes we have 'aggrName' var on the stack).
#define GEN_AGG(_colName_) genAggr(aggrName, _colName_)

// The test class.
class VarianceAggregationTest : public AggregationTestBase {
 protected:
  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), SMALLINT(), INTEGER(), BIGINT(), REAL(), DOUBLE()})};

  // We test these aggregation in this class.
  // It is possible to temporarily alter this array to only focus on one, when
  // debugging tests.
  const std::vector<const char*> aggrNames_{
      "stddev",
      "stddev_pop",
      "stddev_samp",
      "variance",
      "var_pop",
      "var_samp"};
};

TEST_F(VarianceAggregationTest, varianceConst) {
  // Have two row vectors at least as it triggers different code paths.
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

  for (const auto& aggrName : aggrNames_) {
    auto agg = PlanBuilder()
                   .values(vectors)
                   .partialAggregation({}, {GEN_AGG("c1"), GEN_AGG("c2")})
                   .finalAggregation()
                   .planNode();
    auto sql = genAggrQuery("SELECT {0}(c1), {0}(c2) FROM tmp", aggrName);
    assertQuery(agg, sql);

    agg = PlanBuilder()
              .values(vectors)
              .partialAggregation({"c0"}, {GEN_AGG("c1"), GEN_AGG("c2")})
              .finalAggregation()
              .planNode();
    sql = genAggrQuery(
        "SELECT c0, {0}(c1), {0}(c2) FROM tmp GROUP BY 1", aggrName);
    assertQuery(agg, sql);

    agg = PlanBuilder()
              .values(vectors)
              .partialAggregation({}, {GEN_AGG("c0")})
              .finalAggregation()
              .planNode();
    sql = genAggrQuery("SELECT {0}(c0) FROM tmp", aggrName);
    assertQuery(agg, sql);

    agg = PlanBuilder()
              .values(vectors)
              .project({"c0 % 2 AS c0_mod_2", "c0"})
              .partialAggregation({"c0_mod_2"}, {GEN_AGG("c0")})
              .finalAggregation()
              .planNode();
    sql = genAggrQuery("SELECT c0 % 2, {0}(c0) FROM tmp GROUP BY 1", aggrName);
    assertQuery(agg, sql);
  }
}

TEST_F(VarianceAggregationTest, varianceConstNull) {
  // Have two row vectors at least as it triggers different code paths.
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

  std::string sql;
  std::shared_ptr<core::PlanNode> agg;
  for (const auto& aggrName : aggrNames_) {
    agg = PlanBuilder()
              .values(vectors)
              .partialAggregation({}, {GEN_AGG("c1"), GEN_AGG("c2")})
              .finalAggregation()
              .planNode();
    sql = genAggrQuery("SELECT {0}(c1), {0}(c2) FROM tmp", aggrName);
    assertQuery(agg, sql);

    agg = PlanBuilder()
              .values(vectors)
              .partialAggregation({"c0"}, {GEN_AGG("c1"), GEN_AGG("c2")})
              .finalAggregation()
              .planNode();
    sql = genAggrQuery(
        "SELECT c0, {0}(c1), {0}(c2) FROM tmp group by c0", aggrName);
    assertQuery(agg, sql);

    agg = PlanBuilder()
              .values(vectors)
              .partialAggregation({}, {GEN_AGG("c0")})
              .finalAggregation()
              .project({"round(a0, cast (10 as int))"})
              .planNode();
    sql = genAggrQuery("SELECT round({0}(c0), 10) FROM tmp", aggrName);
    assertQuery(agg, sql);
  }
}

TEST_F(VarianceAggregationTest, varianceNulls) {
  // Have two row vectors at least as it triggers different code paths.
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

  for (const auto& aggrName : aggrNames_) {
    auto agg = PlanBuilder()
                   .values(vectors)
                   .partialAggregation({}, {GEN_AGG("c1"), GEN_AGG("c2")})
                   .finalAggregation()
                   .planNode();
    auto sql = genAggrQuery("SELECT {0}(c1), {0}(c2) FROM tmp", aggrName);
    assertQuery(agg, sql);

    agg = PlanBuilder()
              .values(vectors)
              .partialAggregation({"c0"}, {GEN_AGG("c1"), GEN_AGG("c2")})
              .finalAggregation()
              .planNode();
    sql = genAggrQuery(
        "SELECT c0, {0}(c1), {0}(c2) FROM tmp group by c0", aggrName);
    assertQuery(agg, sql);
  }
}

TEST_F(VarianceAggregationTest, variance) {
  auto vectors = makeVectors(rowType_, 10, 20);
  createDuckDbTable(vectors);

  for (const auto& aggrName : aggrNames_) {
    // Global aggregation
    auto agg =
        PlanBuilder()
            .values(vectors)
            .partialAggregation(
                {},
                {GEN_AGG("c1"), GEN_AGG("c2"), GEN_AGG("c4"), GEN_AGG("c5")})
            .finalAggregation()
            .planNode();
    auto sql = genAggrQuery(
        "SELECT {0}(c1), {0}(c2), {0}(c4), {0}(c5) FROM tmp", aggrName);
    assertQuery(agg, sql);

    agg = PlanBuilder()
              .values(vectors)
              .singleAggregation(
                  {},
                  {GEN_AGG("c1"), GEN_AGG("c2"), GEN_AGG("c4"), GEN_AGG("c5")})
              .planNode();
    assertQuery(agg, sql);

    agg = PlanBuilder()
              .values(vectors)
              .partialAggregation(
                  {},
                  {GEN_AGG("c1"), GEN_AGG("c2"), GEN_AGG("c4"), GEN_AGG("c5")})
              .planNode();

    auto partialResults = getResults(agg);

    agg = PlanBuilder()
              .values({partialResults})
              .finalAggregation(
                  {},
                  {GEN_AGG("a0"), GEN_AGG("a1"), GEN_AGG("a2"), GEN_AGG("a3")},
                  {DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()})
              .planNode();
    assertQuery(agg, sql);

    agg = PlanBuilder()
              .values(vectors)
              .partialAggregation(
                  {},
                  {GEN_AGG("c1"), GEN_AGG("c2"), GEN_AGG("c4"), GEN_AGG("c5")})
              .intermediateAggregation()
              .finalAggregation()
              .planNode();
    sql = genAggrQuery(
        "SELECT {0}(c1), {0}(c2), {0}(c4), {0}(c5) FROM tmp", aggrName);
    assertQuery(agg, sql);

    // Global aggregation; no input
    agg = PlanBuilder()
              .values(vectors)
              .filter("c0 % 2 = 5")
              .partialAggregation({}, {GEN_AGG("c0")})
              .finalAggregation()
              .planNode();
    sql = genAggrQuery("SELECT {0}(c0) FROM tmp WHERE c0 % 2 = 5", aggrName);
    assertQuery(agg, sql);

    // Global aggregation over filter
    agg = PlanBuilder()
              .values(vectors)
              .filter("c0 % 5 = 3")
              .partialAggregation({}, {GEN_AGG("c1")})
              .finalAggregation()
              .planNode();
    sql = genAggrQuery("SELECT {0}(c1) FROM tmp WHERE c0 % 5 = 3", aggrName);
    assertQuery(agg, sql);

    // Group by
    agg = PlanBuilder()
              .values(vectors)
              .project({"c0 % 10", "c1", "c2", "c3", "c4", "c5"})
              .partialAggregation(
                  {"p0"},
                  {GEN_AGG("c1"),
                   GEN_AGG("c2"),
                   GEN_AGG("c3"),
                   GEN_AGG("c4"),
                   GEN_AGG("c5")})
              .finalAggregation()
              .planNode();
    sql = genAggrQuery(
        "SELECT c0 % 10, {0}(c1), {0}(c2), {0}(c3::DOUBLE), {0}(c4), {0}(c5) "
        "FROM tmp GROUP BY 1",
        aggrName);
    assertQuery(agg, sql);

    agg = PlanBuilder()
              .values(vectors)
              .project({"c0 % 10", "c1", "c2", "c3", "c4", "c5"})
              .singleAggregation(
                  {"p0"},
                  {GEN_AGG("c1"),
                   GEN_AGG("c2"),
                   GEN_AGG("c3"),
                   GEN_AGG("c4"),
                   GEN_AGG("c5")})
              .planNode();
    sql = genAggrQuery(
        "SELECT c0 % 10, {0}(c1), {0}(c2), {0}(c3::DOUBLE), {0}(c4), {0}(c5) "
        "FROM tmp GROUP BY 1",
        aggrName);
    assertQuery(agg, sql);

    agg = PlanBuilder()
              .values(vectors)
              .project({"c0 % 10", "c1", "c2", "c3", "c4", "c5"})
              .partialAggregation(
                  {"p0"},
                  {GEN_AGG("c1"),
                   GEN_AGG("c2"),
                   GEN_AGG("c3"),
                   GEN_AGG("c4"),
                   GEN_AGG("c5")})
              .intermediateAggregation()
              .finalAggregation()
              .planNode();
    sql = genAggrQuery(
        "SELECT c0 % 10, {0}(c1), {0}(c2), {0}(c3::DOUBLE), {0}(c4), {0}(c5) "
        "FROM tmp GROUP BY 1",
        aggrName);
    assertQuery(agg, sql);

    // Group by; no input
    agg = PlanBuilder()
              .values(vectors)
              .project({"c0 % 10 AS c0_mod_10", "c1"})
              .filter("c0_mod_10 > 10")
              .partialAggregation({"c0_mod_10"}, {GEN_AGG("c1")})
              .finalAggregation()
              .planNode();
    assertQueryReturnsEmptyResult(agg);

    // Group by over filter
    agg = PlanBuilder()
              .values(vectors)
              .filter("c2 % 5 = 3")
              .project({"c0 % 10", "c1"})
              .partialAggregation({"p0"}, {GEN_AGG("c1")})
              .finalAggregation()
              .planNode();
    sql = genAggrQuery(
        "SELECT c0 % 10, {0}(c1) FROM tmp WHERE c2 % 5 = 3 GROUP BY 1",
        aggrName);
    assertQuery(agg, sql);
  }
}

} // namespace
} // namespace facebook::velox::aggregate::test
