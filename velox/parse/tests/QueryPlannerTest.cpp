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
#include "velox/parse/QueryPlanner.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::core::test {

class QueryPlannerTest : public testing::Test {
 protected:
  void assertPlan(
      const std::string& sql,
      const std::unordered_map<std::string, std::vector<RowVectorPtr>>&
          inMemoryTables,
      const std::string& expectedPlan) {
    SCOPED_TRACE(sql);
    auto plan = parseQuery(sql, pool_.get(), inMemoryTables);
    ASSERT_EQ(expectedPlan, plan->toString(false, true));
  }

  void assertPlanError(const std::string& sql, const char* expectedMessage) {
    SCOPED_TRACE(sql);
    SCOPED_TRACE(expectedMessage);
    try {
      parseQuery(sql, pool_.get());
      FAIL() << "Expected an exception: " << expectedMessage;
    } catch (std::exception& e) {
      ASSERT_TRUE(strstr(e.what(), expectedMessage) != nullptr) << e.what();
    }
  }

  void assertPlan(const std::string& sql, const std::string& expectedPlan) {
    assertPlan(sql, {}, expectedPlan);
  }

  RowVectorPtr makeEmptyRowVector(const RowTypePtr& rowType) {
    return std::make_shared<RowVector>(
        pool_.get(),
        rowType,
        nullptr, // nulls
        0,
        std::vector<VectorPtr>{});
  }

  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
};

TEST_F(QueryPlannerTest, values) {
  assertPlan(
      "SELECT x, x + 5 FROM UNNEST([1, 2, 3]) as t(x)",
      "-- Project\n"
      "  -- Unnest\n"
      "    -- Project\n"
      "      -- Values\n");

  assertPlan(
      "SELECT sum(x) FROM UNNEST([1, 2, 3]) as t(x)",
      "-- Project\n"
      "  -- Aggregation\n"
      "    -- Unnest\n"
      "      -- Project\n"
      "        -- Values\n");

  assertPlan(
      "SELECT x % 5, sum(x) FROM UNNEST([1, 2, 3]) as t(x) GROUP BY 1",
      "-- Project\n"
      "  -- Aggregation\n"
      "    -- Project\n"
      "      -- Unnest\n"
      "        -- Project\n"
      "          -- Values\n");

  assertPlan(
      "SELECT sum(x * 4) FROM UNNEST([1, 2, 3]) as t(x)",
      "-- Project\n"
      "  -- Aggregation\n"
      "    -- Project\n"
      "      -- Unnest\n"
      "        -- Project\n"
      "          -- Values\n");
}

TEST_F(QueryPlannerTest, tableScan) {
  std::unordered_map<std::string, std::vector<RowVectorPtr>> inMemoryTables = {
      {"t",
       {makeEmptyRowVector(
           ROW({"a", "b", "c"}, {BIGINT(), INTEGER(), SMALLINT()}))}},
      {"u", {makeEmptyRowVector(ROW({"a", "b"}, {BIGINT(), DOUBLE()}))}},
  };

  assertPlan(
      "SELECT a, sum(b) FROM t WHERE c > 5 GROUP BY 1",
      inMemoryTables,
      "-- Project\n"
      "  -- Aggregation\n"
      "    -- Filter\n"
      "      -- Values\n");

  assertPlan(
      "SELECT t.a, t.b, t.c, u.b FROM t, u WHERE t.a = u.a",
      inMemoryTables,
      "-- Project\n"
      "  -- Filter\n"
      "    -- CrossJoin\n"
      "      -- Values\n"
      "      -- Values\n");
}

TEST_F(QueryPlannerTest, customScalarFunctions) {
  DuckDbQueryPlanner planner(pool_.get());
  planner.registerScalarFunction("foo", {BIGINT()}, BOOLEAN());
  planner.registerScalarFunction("bar", {ARRAY(BIGINT())}, BIGINT());

  auto plan =
      planner.plan("SELECT foo(x), bar([x]) FROM UNNEST([1, 2, 3]) as t(x)");
  ASSERT_EQ(
      plan->toString(false, true),
      "-- Project\n"
      "  -- Unnest\n"
      "    -- Project\n"
      "      -- Values\n");
}

TEST_F(QueryPlannerTest, customAggregateFunctions) {
  DuckDbQueryPlanner planner(pool_.get());

  planner.registerAggregateFunction("foo_agg", {BIGINT(), BIGINT()}, DOUBLE());
  planner.registerAggregateFunction(
      "bar_agg", {ARRAY(BIGINT()), BIGINT()}, ARRAY(BIGINT()));

  auto plan = planner.plan(
      "SELECT foo_agg(x, x + 5), bar_agg([x], 1) FROM UNNEST([1, 2, 3]) as t(x)");
  ASSERT_EQ(
      plan->toString(false, true),
      "-- Project\n"
      "  -- Aggregation\n"
      "    -- Project\n"
      "      -- Unnest\n"
      "        -- Project\n"
      "          -- Values\n");
}

TEST_F(QueryPlannerTest, error) {
  assertPlanError(
      "SELECT * FROM my_table", "Table with name my_table does not exist");
  assertPlanError(
      "SELECT my_function(1)",
      "Scalar Function with name my_function does not exist");
  // DuckDB gives the same error regardless of whether scalar or aggregate
  // function is missing.
  assertPlanError(
      "SELECT x % 5, my_agg(x) FROM UNNEST([1, 2, 3]) as t(x) GROUP BY 1",
      "Scalar Function with name my_agg does not exist");
  assertPlanError("SELECT 'test ", "unterminated quoted string");
}

} // namespace facebook::velox::core::test
