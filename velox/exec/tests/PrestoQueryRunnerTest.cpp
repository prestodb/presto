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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/fuzzer/PrestoQueryRunner.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::test;

namespace facebook::velox::exec::test {

class PrestoQueryRunnerTest : public ::testing::Test,
                              public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    velox::functions::prestosql::registerAllScalarFunctions();
    facebook::velox::window::prestosql::registerAllWindowFunctions();
    velox::aggregate::prestosql::registerAllAggregateFunctions();
    velox::parse::registerTypeResolver();
  }
};

// This test requires a Presto Coordinator running at localhost, so disable it
// by default.
TEST_F(PrestoQueryRunnerTest, DISABLED_basic) {
  auto aggregatePool = rootPool_->addAggregateChild("basic");
  auto queryRunner = std::make_unique<PrestoQueryRunner>(
      aggregatePool.get(),
      "http://127.0.0.1:8080",
      "hive",
      static_cast<std::chrono::milliseconds>(1000));

  auto results = queryRunner->execute("SELECT count(*) FROM nation");
  auto expected = makeRowVector({
      makeConstant<int64_t>(25, 1),
  });
  velox::exec::test::assertEqualResults({expected}, {results});

  results =
      queryRunner->execute("SELECT regionkey, count(*) FROM nation GROUP BY 1");

  expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
      makeFlatVector<int64_t>({5, 5, 5, 5, 5}),
  });
  velox::exec::test::assertEqualResults({expected}, {results});
}

// This test requires a Presto Coordinator running at localhost, so disable it
// by default.
TEST_F(PrestoQueryRunnerTest, DISABLED_fuzzer) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      makeArrayVectorFromJson<int64_t>({
          "[]",
          "[1, 2, 3]",
          "null",
          "[1, 2, 3, 4]",
      }),
  });

  auto plan = velox::exec::test::PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"min(c0)", "max(c0)", "set_agg(c1)"})
                  .project({"a0", "a1", "array_sort(a2)"})
                  .planNode();

  auto aggregatePool = rootPool_->addAggregateChild("fuzzer");
  auto queryRunner = std::make_unique<PrestoQueryRunner>(
      aggregatePool.get(),
      "http://127.0.0.1:8080",
      "hive",
      static_cast<std::chrono::milliseconds>(1000));

  std::optional<std::multiset<std::vector<velox::variant>>> prestoResults =
      queryRunner->execute(plan).first;
  ASSERT_TRUE(prestoResults.has_value());

  auto veloxResults =
      velox::exec::test::AssertQueryBuilder(plan).copyResults(pool());
  velox::exec::test::assertEqualResults(
      *prestoResults, plan->outputType(), {veloxResults});
}

TEST_F(PrestoQueryRunnerTest, sortedAggregation) {
  auto aggregatePool = rootPool_->addAggregateChild("sortedAggregation");
  auto queryRunner = std::make_unique<PrestoQueryRunner>(
      aggregatePool.get(),
      "http://unused",
      "hive",
      static_cast<std::chrono::milliseconds>(1000));

  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 1, 2, 1}),
      makeFlatVector<int64_t>({2, 3, 4, 1, 4}),
      makeFlatVector<int64_t>({5, 6, 7, 8, 3}),
  });

  auto plan =
      velox::exec::test::PlanBuilder()
          .values({data})
          .singleAggregation({}, {"multimap_agg(c0, c1 order by c0 asc)"})
          .planNode();

  auto sql = queryRunner->toSql(plan);
  ASSERT_TRUE(sql.has_value());

  ASSERT_THAT(
      *sql,
      ::testing::HasSubstr(
          "SELECT multimap_agg(c0, c1 ORDER BY c0 ASC NULLS LAST) as a0 FROM "));

  // Plans with multiple order by's in the aggregate.

  plan =
      velox::exec::test::PlanBuilder()
          .values({data})
          .singleAggregation(
              {},
              {"multimap_agg(c0, c1 order by c1 asc nulls first, c0 desc nulls last, c2 asc nulls last)"})
          .planNode();

  sql = queryRunner->toSql(plan);
  ASSERT_TRUE(sql.has_value());
  ASSERT_THAT(
      *sql,
      ::testing::HasSubstr(
          "SELECT multimap_agg(c0, c1 ORDER BY c1 ASC NULLS FIRST, c0 DESC NULLS LAST, c2 ASC NULLS LAST) as a0 FROM "));
}

TEST_F(PrestoQueryRunnerTest, distinctAggregation) {
  auto aggregatePool = rootPool_->addAggregateChild("distinctAggregation");
  auto queryRunner = std::make_unique<PrestoQueryRunner>(
      aggregatePool.get(),
      "http://unused",
      "hive",
      static_cast<std::chrono::milliseconds>(1000));

  auto data =
      makeRowVector({makeFlatVector<int64_t>({}), makeFlatVector<int64_t>({})});

  auto plan = velox::exec::test::PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"array_agg(distinct c0)"})
                  .planNode();

  auto sql = queryRunner->toSql(plan);
  ASSERT_TRUE(sql.has_value());
  ASSERT_THAT(
      *sql, ::testing::HasSubstr("SELECT array_agg(distinct c0) as a0 FROM "));
}

TEST_F(PrestoQueryRunnerTest, toSql) {
  auto aggregatePool = rootPool_->addAggregateChild("toSql");
  auto queryRunner = std::make_unique<PrestoQueryRunner>(
      aggregatePool.get(),
      "http://unused",
      "hive",
      static_cast<std::chrono::milliseconds>(1000));
  auto dataType = ROW({"c0", "c1", "c2"}, {BIGINT(), BIGINT(), BOOLEAN()});

  // Test window queries.
  {
    auto queryRunnerContext = queryRunner->queryRunnerContext();
    core::PlanNodeId id;
    const auto frameClause =
        "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
    auto plan =
        PlanBuilder()
            .tableScan("tmp", dataType)
            .window({"first_value(c0) over (partition by c1 order by c2)"})
            .capturePlanNodeId(id)
            .planNode();
    queryRunnerContext->windowFrames_[id] = {frameClause};
    EXPECT_EQ(
        queryRunner->toSql(plan),
        fmt::format(
            "SELECT c0, c1, c2, first_value(c0) OVER (PARTITION BY c1 ORDER BY c2 ASC NULLS LAST {}) FROM tmp",
            frameClause));

    const auto firstValueFrame =
        "ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING";
    const auto lastValueFrame =
        "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
    plan =
        PlanBuilder()
            .tableScan("tmp", dataType)
            .window(
                {"first_value(c0) over (partition by c1 order by c2 desc nulls first rows between 1 following and unbounded following)",
                 "last_value(c0) over (partition by c1 order by c2 desc nulls first)"})
            .capturePlanNodeId(id)
            .planNode();
    queryRunnerContext->windowFrames_[id] = {firstValueFrame, lastValueFrame};
    EXPECT_EQ(
        queryRunner->toSql(plan),
        fmt::format(
            "SELECT c0, c1, c2, first_value(c0) OVER (PARTITION BY c1 ORDER BY c2 DESC NULLS FIRST {}), last_value(c0) OVER (PARTITION BY c1 ORDER BY c2 DESC NULLS FIRST {}) FROM tmp",
            firstValueFrame,
            lastValueFrame));
  }

  // Test aggregation queries.
  {
    auto plan = PlanBuilder()
                    .tableScan("tmp", dataType)
                    .singleAggregation({"c1"}, {"avg(c0)"})
                    .planNode();
    EXPECT_EQ(
        queryRunner->toSql(plan),
        "SELECT c1, avg(c0) as a0 FROM tmp GROUP BY c1");

    plan = PlanBuilder()
               .tableScan("tmp", dataType)
               .singleAggregation({"c1"}, {"sum(c0)"})
               .project({"a0 + c1"})
               .planNode();
    EXPECT_EQ(
        queryRunner->toSql(plan),
        "SELECT (a0 + c1) as p0 FROM (SELECT c1, sum(c0) as a0 FROM tmp GROUP BY c1)");

    plan = PlanBuilder()
               .tableScan("tmp", dataType)
               .singleAggregation({}, {"avg(c0)", "avg(c1)"}, {"c2"})
               .planNode();
    EXPECT_EQ(
        queryRunner->toSql(plan),
        "SELECT avg(c0) filter (where c2) as a0, avg(c1) as a1 FROM tmp");
  }
}

TEST_F(PrestoQueryRunnerTest, toSqlJoins) {
  auto aggregatePool = rootPool_->addAggregateChild("toSqlJoins");
  auto queryRunner = std::make_unique<PrestoQueryRunner>(
      aggregatePool.get(),
      "http://unused",
      "hive",
      static_cast<std::chrono::milliseconds>(1000));

  auto t = makeRowVector(
      {"t0", "t1", "t2"},
      {
          makeFlatVector<int64_t>({}),
          makeFlatVector<int32_t>({}),
          makeFlatVector<bool>({}),
      });
  auto u = makeRowVector(
      {"u0", "u1", "u2"},
      {
          makeFlatVector<int64_t>({}),
          makeFlatVector<int32_t>({}),
          makeFlatVector<bool>({}),
      });
  auto v = makeRowVector(
      {"v0", "v1", "v2"},
      {
          makeFlatVector<int64_t>({}),
          makeFlatVector<int32_t>({}),
          makeFlatVector<bool>({}),
      });
  auto w = makeRowVector(
      {"w0", "w1", "w2"},
      {
          makeFlatVector<int64_t>({}),
          makeFlatVector<int32_t>({}),
          makeFlatVector<bool>({}),
      });

  // Single join.
  {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values({t})
                    .hashJoin(
                        {"t0"},
                        {"u0"},
                        PlanBuilder(planNodeIdGenerator).values({u}).planNode(),
                        /*filter=*/"",
                        {"t0", "t1"},
                        core::JoinType::kInner)
                    .planNode();
    EXPECT_EQ(
        *queryRunner->toSql(plan),
        "SELECT t0, t1 FROM t_0 INNER JOIN t_1 ON t0 = u0");
  }

  // Two joins with a filter.
  {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values({t})
                    .hashJoin(
                        {"t0"},
                        {"u0"},
                        PlanBuilder(planNodeIdGenerator).values({u}).planNode(),
                        /*filter=*/"",
                        {"t0"},
                        core::JoinType::kLeftSemiFilter)
                    .hashJoin(
                        {"t0"},
                        {"v0"},
                        PlanBuilder(planNodeIdGenerator).values({v}).planNode(),
                        "v1 > 0",
                        {"t0", "v1"},
                        core::JoinType::kInner)
                    .planNode();
    EXPECT_EQ(
        *queryRunner->toSql(plan),
        "SELECT t0, v1"
        " FROM (SELECT t0 FROM t_0 WHERE t0 IN (SELECT u0 FROM t_1))"
        " INNER JOIN t_3 ON t0 = v0 AND (cast(v1 as BIGINT) > 0)");
  }

  // Three joins.
  {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values({t})
                    .hashJoin(
                        {"t0"},
                        {"u0"},
                        PlanBuilder(planNodeIdGenerator).values({u}).planNode(),
                        /*filter=*/"",
                        {"t0", "t1"},
                        core::JoinType::kLeft)
                    .hashJoin(
                        {"t0"},
                        {"v0"},
                        PlanBuilder(planNodeIdGenerator).values({v}).planNode(),
                        /*filter=*/"",
                        {"t0", "v1"},
                        core::JoinType::kInner)
                    .hashJoin(
                        {"t0", "v1"},
                        {"w0", "w1"},
                        PlanBuilder(planNodeIdGenerator).values({w}).planNode(),
                        /*filter=*/"",
                        {"t0", "w1"},
                        core::JoinType::kFull)
                    .planNode();
    EXPECT_EQ(
        *queryRunner->toSql(plan),
        "SELECT t0, w1"
        " FROM (SELECT t0, v1 FROM (SELECT t0, t1 FROM t_0 LEFT JOIN t_1 ON t0 = u0)"
        " INNER JOIN t_3 ON t0 = v0)"
        " FULL OUTER JOIN t_5 ON t0 = w0 AND v1 = w1");
  }
}

} // namespace facebook::velox::exec::test
