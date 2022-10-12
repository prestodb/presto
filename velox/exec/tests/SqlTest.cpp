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
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/parse/QueryPlanner.h"

namespace facebook::velox::exec::test {

class SqlTest : public OperatorTestBase {
 protected:
  void assertSql(const std::string& sql, const std::string& duckSql = "") {
    auto plan = planner_.plan(sql);
    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .assertResults(duckSql.empty() ? sql : duckSql);
  }

  core::DuckDbQueryPlanner planner_{pool()};
};

TEST_F(SqlTest, values) {
  assertSql("SELECT x, x + 5 FROM UNNEST([1, 2, 3]) as t(x)");
  assertSql("SELECT avg(x), count(*) FROM UNNEST([1, 2, 3]) as t(x)");
  assertSql("SELECT x % 5, avg(x) FROM UNNEST([1, 2, 3]) as t(x) GROUP BY 1");
  assertSql("SELECT avg(x * 4) FROM UNNEST([1, 2, 3]) as t(x)");
  assertSql(
      "SELECT x / 5, avg(x * 4) FROM UNNEST([1, 2, 3]) as t(x) GROUP BY 1");
}

TEST_F(SqlTest, customScalarFunctions) {
  planner_.registerScalarFunction(
      "array_join", {ARRAY(BIGINT()), VARCHAR()}, VARCHAR());

  assertSql("SELECT array_join([1, 2, 3], '-')", "SELECT '1-2-3'");
}

TEST_F(SqlTest, customAggregateFunctions) {
  planner_.registerAggregateFunction("count_if", {BOOLEAN()}, BIGINT());

  assertSql(
      "SELECT count_if(x > 2) FROM UNNEST([1, 2, 3]) as t(x)", "SELECT 1");
  assertSql(
      "SELECT x % 2, count_if(x > 0) FROM UNNEST([1, 2, 3]) as t(x) GROUP BY 1",
      "VALUES (0, 1), (1, 2)");
}

TEST_F(SqlTest, tableScan) {
  std::unordered_map<std::string, std::vector<RowVectorPtr>> data = {
      {"t",
       {makeRowVector(
           {"a", "b", "c"},
           {
               makeFlatVector<int64_t>({1, 2, 3, 1, 2, 3}),
               makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
               makeFlatVector<int16_t>({3, 6, 8, 2, 9, 10}),
           })}},
      {"u",
       {makeRowVector(
           {"a", "b"},
           {
               makeFlatVector<int64_t>({1, 2, 3, 4, 5, 1}),
               makeFlatVector<double>({1.1, 2.2, 3.3, 4.4, 5.5, 1.2}),
           })}},
  };

  createDuckDbTable("t", data.at("t"));
  createDuckDbTable("u", data.at("u"));

  planner_.registerTable("t", data.at("t"));
  planner_.registerTable("u", data.at("u"));

  assertSql("SELECT a, avg(b) FROM t WHERE c > 5 GROUP BY 1");
  assertSql("SELECT * FROM t, u WHERE t.a = u.a");
  assertSql("SELECT t.a, t.b, t.c, u.b FROM t, u WHERE t.a = u.a");
}
} // namespace facebook::velox::exec::test
