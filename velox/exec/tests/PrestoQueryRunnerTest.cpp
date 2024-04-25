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
  auto queryRunner = std::make_unique<PrestoQueryRunner>(
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

  auto queryRunner = std::make_unique<PrestoQueryRunner>(
      "http://127.0.0.1:8080",
      "hive",
      static_cast<std::chrono::milliseconds>(1000));
  auto sql = queryRunner->toSql(plan);
  ASSERT_TRUE(sql.has_value());

  auto prestoResults = queryRunner->execute(
      sql.value(),
      {data},
      ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), ARRAY(BIGINT())}));

  auto veloxResults =
      velox::exec::test::AssertQueryBuilder(plan).copyResults(pool());
  velox::exec::test::assertEqualResults(
      prestoResults, plan->outputType(), {veloxResults});
}

TEST_F(PrestoQueryRunnerTest, sortedAggregation) {
  auto queryRunner = std::make_unique<PrestoQueryRunner>(
      "http://unused", "hive", static_cast<std::chrono::milliseconds>(1000));

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

  ASSERT_EQ(
      "SELECT multimap_agg(c0, c1 ORDER BY c0 ASC NULLS LAST) as a0 FROM tmp",
      sql.value());

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
  ASSERT_EQ(
      "SELECT multimap_agg(c0, c1 ORDER BY c1 ASC NULLS FIRST, c0 DESC NULLS LAST, c2 ASC NULLS LAST) as a0 FROM tmp",
      sql.value());
}

TEST_F(PrestoQueryRunnerTest, distinctAggregation) {
  auto queryRunner = std::make_unique<PrestoQueryRunner>(
      "http://unused", "hive", static_cast<std::chrono::milliseconds>(1000));

  auto data =
      makeRowVector({makeFlatVector<int64_t>({}), makeFlatVector<int64_t>({})});

  auto plan = velox::exec::test::PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"array_agg(distinct c0)"})
                  .planNode();

  auto sql = queryRunner->toSql(plan);
  ASSERT_TRUE(sql.has_value());
  ASSERT_EQ("SELECT array_agg(distinct c0) as a0 FROM tmp", sql.value());
}

TEST_F(PrestoQueryRunnerTest, toSql) {
  auto queryRunner = std::make_unique<PrestoQueryRunner>(
      "http://unused", "hive", static_cast<std::chrono::milliseconds>(1000));
  auto dataType = ROW({"c0", "c1", "c2"}, {BIGINT(), BIGINT(), BOOLEAN()});

  // Test window queries.
  {
    auto plan =
        PlanBuilder()
            .tableScan("tmp", dataType)
            .window({"first_value(c0) over (partition by c1 order by c2)"})
            .planNode();
    EXPECT_EQ(
        queryRunner->toSql(plan),
        "SELECT c0, c1, c2, first_value(c0) OVER (PARTITION BY c1 ORDER BY c2 ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM tmp");

    plan =
        PlanBuilder()
            .tableScan("tmp", dataType)
            .window(
                {"first_value(c0) over (partition by c1 order by c2 desc nulls first rows between 1 following and unbounded following)",
                 "last_value(c0) over (partition by c1 order by c2 desc nulls first)"})
            .planNode();
    EXPECT_EQ(
        queryRunner->toSql(plan),
        "SELECT c0, c1, c2, first_value(c0) OVER (PARTITION BY c1 ORDER BY c2 DESC NULLS FIRST ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING), last_value(c0) OVER (PARTITION BY c1 ORDER BY c2 DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM tmp");
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
        "SELECT plus(a0, c1) as p0 FROM (SELECT c1, sum(c0) as a0 FROM tmp GROUP BY c1)");

    plan = PlanBuilder()
               .tableScan("tmp", dataType)
               .singleAggregation({}, {"avg(c0)", "avg(c1)"}, {"c2"})
               .planNode();
    EXPECT_EQ(
        queryRunner->toSql(plan),
        "SELECT avg(c0) filter (where c2) as a0, avg(c1) as a1 FROM tmp");
  }
}

} // namespace facebook::velox::exec::test
