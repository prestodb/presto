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

#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/exec/WindowFunction.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::common::test;

using facebook::velox::exec::test::PlanBuilder;

class PlanNodeToStringTest : public testing::Test, public test::VectorTestBase {
 public:
  PlanNodeToStringTest() {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();
    data_ = makeRowVector(
        {makeFlatVector<int16_t>({0, 1, 2, 3, 4}),
         makeFlatVector<int32_t>({0, 1, 2, 3, 4}),
         makeFlatVector<int64_t>({0, 1, 2, 3, 4})});

    plan_ = PlanBuilder()
                .values({data_})
                .filter("c0 % 10 < 9")
                .project({"c0 AS out1", "c0 % 100 + c1 % 50 AS out2"})
                .filter("out1 % 10 < 8")
                .project({"out1 + 10 AS out3"})
                .planNode();
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  RowVectorPtr data_;
  core::PlanNodePtr plan_;
};

TEST_F(PlanNodeToStringTest, basic) {
  ASSERT_EQ("-- Project[4]\n", plan_->toString());
}

TEST_F(PlanNodeToStringTest, recursive) {
  ASSERT_EQ(
      "-- Project[4]\n"
      "  -- Filter[3]\n"
      "    -- Project[2]\n"
      "      -- Filter[1]\n"
      "        -- Values[0]\n",
      plan_->toString(false, true));
}

TEST_F(PlanNodeToStringTest, detailed) {
  ASSERT_EQ(
      "-- Project[4][expressions: (out3:BIGINT, plus(cast ROW[\"out1\"] as BIGINT,10))] -> out3:BIGINT\n",
      plan_->toString(true, false));
}

TEST_F(PlanNodeToStringTest, recursiveAndDetailed) {
  ASSERT_EQ(
      "-- Project[4][expressions: (out3:BIGINT, plus(cast ROW[\"out1\"] as BIGINT,10))] -> out3:BIGINT\n"
      "  -- Filter[3][expression: lt(mod(cast ROW[\"out1\"] as BIGINT,10),8)] -> out1:SMALLINT, out2:BIGINT\n"
      "    -- Project[2][expressions: (out1:SMALLINT, ROW[\"c0\"]), (out2:BIGINT, plus(mod(cast ROW[\"c0\"] as BIGINT,100),mod(cast ROW[\"c1\"] as BIGINT,50)))] -> out1:SMALLINT, out2:BIGINT\n"
      "      -- Filter[1][expression: lt(mod(cast ROW[\"c0\"] as BIGINT,10),9)] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n"
      "        -- Values[0][5 rows in 1 vectors] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan_->toString(true, true));
}

TEST_F(PlanNodeToStringTest, withContext) {
  auto addContext = [](const core::PlanNodeId& planNodeId,
                       const std::string& /* indentation */,
                       std::stringstream& stream) {
    stream << "Context for " << planNodeId;
  };

  ASSERT_EQ(
      "-- Project[4]\n"
      "   Context for 4\n",
      plan_->toString(false, false, addContext));

  ASSERT_EQ(
      "-- Project[4][expressions: (out3:BIGINT, plus(cast ROW[\"out1\"] as BIGINT,10))] -> out3:BIGINT\n"
      "   Context for 4\n",
      plan_->toString(true, false, addContext));

  ASSERT_EQ(
      "-- Project[4]\n"
      "   Context for 4\n"
      "  -- Filter[3]\n"
      "     Context for 3\n"
      "    -- Project[2]\n"
      "       Context for 2\n"
      "      -- Filter[1]\n"
      "         Context for 1\n"
      "        -- Values[0]\n"
      "           Context for 0\n",
      plan_->toString(false, true, addContext));

  ASSERT_EQ(
      "-- Project[4][expressions: (out3:BIGINT, plus(cast ROW[\"out1\"] as BIGINT,10))] -> out3:BIGINT\n"
      "   Context for 4\n"
      "  -- Filter[3][expression: lt(mod(cast ROW[\"out1\"] as BIGINT,10),8)] -> out1:SMALLINT, out2:BIGINT\n"
      "     Context for 3\n"
      "    -- Project[2][expressions: (out1:SMALLINT, ROW[\"c0\"]), (out2:BIGINT, plus(mod(cast ROW[\"c0\"] as BIGINT,100),mod(cast ROW[\"c1\"] as BIGINT,50)))] -> out1:SMALLINT, out2:BIGINT\n"
      "       Context for 2\n"
      "      -- Filter[1][expression: lt(mod(cast ROW[\"c0\"] as BIGINT,10),9)] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n"
      "         Context for 1\n"
      "        -- Values[0][5 rows in 1 vectors] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n"
      "           Context for 0\n",
      plan_->toString(true, true, addContext));
}

TEST_F(PlanNodeToStringTest, withMultiLineContext) {
  auto addContext = [](const core::PlanNodeId& planNodeId,
                       const std::string& indentation,
                       std::stringstream& stream) {
    stream << "Context for " << planNodeId << ": line 1" << std::endl;
    stream << indentation << "Context for " << planNodeId << ": line 2";
  };

  ASSERT_EQ(
      "-- Project[4]\n"
      "   Context for 4: line 1\n"
      "   Context for 4: line 2\n",
      plan_->toString(false, false, addContext));

  ASSERT_EQ(
      "-- Project[4][expressions: (out3:BIGINT, plus(cast ROW[\"out1\"] as BIGINT,10))] -> out3:BIGINT\n"
      "   Context for 4: line 1\n"
      "   Context for 4: line 2\n",
      plan_->toString(true, false, addContext));
}

TEST_F(PlanNodeToStringTest, values) {
  auto plan = PlanBuilder().values({data_}).planNode();

  ASSERT_EQ("-- Values[0]\n", plan->toString());
  ASSERT_EQ(
      "-- Values[0][5 rows in 1 vectors] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder().values({data_}, true).planNode();

  ASSERT_EQ("-- Values[0]\n", plan->toString());
  ASSERT_EQ(
      "-- Values[0][5 rows in 1 vectors] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder().values({data_}, false, 10).planNode();

  ASSERT_EQ("-- Values[0]\n", plan->toString());
  ASSERT_EQ(
      "-- Values[0][5 rows in 1 vectors, repeat 10 times] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder().values({data_}, true, 10).planNode();

  ASSERT_EQ("-- Values[0]\n", plan->toString());
  ASSERT_EQ(
      "-- Values[0][5 rows in 1 vectors, repeat 10 times] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, aggregation) {
  // Global aggregation.
  auto plan = PlanBuilder()
                  .values({data_})
                  .partialAggregation(
                      {},
                      {"sum(c0) AS a",
                       "avg(c1) AS b",
                       "min(c2) AS c",
                       "count(distinct c1)"})
                  .planNode();

  ASSERT_EQ("-- Aggregation[1]\n", plan->toString());
  ASSERT_EQ(
      "-- Aggregation[1][PARTIAL a := sum(ROW[\"c0\"]), b := avg(ROW[\"c1\"]), c := min(ROW[\"c2\"]), a3 := count(ROW[\"c1\"]) distinct] "
      "-> a:BIGINT, b:ROW<\"\":DOUBLE,\"\":BIGINT>, c:BIGINT, a3:BIGINT\n",
      plan->toString(true, false));

  // Global aggregation with masks.
  auto data = makeRowVector(
      ROW({"c0", "c1", "c2", "m0", "m1", "m2"},
          {BIGINT(), INTEGER(), BIGINT(), BOOLEAN(), BOOLEAN(), BOOLEAN()}),
      100);
  plan = PlanBuilder()
             .values({data})
             .partialAggregation(
                 {},
                 {"sum(c0) AS a", "avg(c1) AS b", "min(c2) AS c"},
                 {"m0", "", "m2"})
             .planNode();

  ASSERT_EQ("-- Aggregation[1]\n", plan->toString());
  ASSERT_EQ(
      "-- Aggregation[1][PARTIAL a := sum(ROW[\"c0\"]) mask: m0, b := avg(ROW[\"c1\"]), c := min(ROW[\"c2\"]) mask: m2] -> a:BIGINT, b:ROW<\"\":DOUBLE,\"\":BIGINT>, c:BIGINT\n",
      plan->toString(true, false));

  // Group-by aggregation.
  plan = PlanBuilder()
             .values({data_})
             .singleAggregation(
                 {"c0"}, {"sum(c1) AS a", "avg(c2) AS b", "count(distinct c2)"})
             .planNode();

  ASSERT_EQ("-- Aggregation[1]\n", plan->toString());
  ASSERT_EQ(
      "-- Aggregation[1][SINGLE [c0] a := sum(ROW[\"c1\"]), b := avg(ROW[\"c2\"]), a2 := count(ROW[\"c2\"]) distinct] "
      "-> c0:SMALLINT, a:BIGINT, b:DOUBLE, a2:BIGINT\n",
      plan->toString(true, false));

  // Group-by aggregation with masks.
  plan = PlanBuilder()
             .values({data})
             .singleAggregation(
                 {"c0"}, {"sum(c1) AS a", "avg(c2) AS b"}, {"m1", "m2"})
             .planNode();

  ASSERT_EQ("-- Aggregation[1]\n", plan->toString());
  ASSERT_EQ(
      "-- Aggregation[1][SINGLE [c0] a := sum(ROW[\"c1\"]) mask: m1, b := avg(ROW[\"c2\"]) mask: m2] -> c0:BIGINT, a:BIGINT, b:DOUBLE\n",
      plan->toString(true, false));

  // Aggregation over sorted inputs.
  plan = PlanBuilder()
             .values({data})
             .singleAggregation({"c0"}, {"array_agg(c1 ORDER BY c2 DESC)"})
             .planNode();

  ASSERT_EQ("-- Aggregation[1]\n", plan->toString());
  ASSERT_EQ(
      "-- Aggregation[1][SINGLE [c0] a0 := array_agg(ROW[\"c1\"]) ORDER BY c2 DESC NULLS LAST] -> c0:BIGINT, a0:ARRAY<INTEGER>\n",
      plan->toString(true, false));

  // Aggregation over GroupId with global grouping sets.
  plan = PlanBuilder()
             .values({data_})
             .groupId({"c0"}, {{"c0"}, {}}, {"c1"})
             .singleAggregation({"c0", "group_id"}, {"sum(c1) as sum_c1"}, {})
             .planNode();
  ASSERT_EQ("-- Aggregation[2]\n", plan->toString());
  ASSERT_EQ(
      "-- Aggregation[2][SINGLE [c0, group_id] sum_c1 := sum(ROW[\"c1\"]) global group IDs: [ 1 ] Group Id key: group_id] -> c0:SMALLINT, group_id:BIGINT, sum_c1:BIGINT\n",
      plan->toString(true, false));

  // Aggregation over GroupId with > 1 global grouping sets.
  plan = PlanBuilder()
             .values({data_})
             .groupId({"c0"}, {{"c0"}, {}, {}}, {"c1"})
             .singleAggregation({"c0", "group_id"}, {"sum(c1) as sum_c1"}, {})
             .planNode();
  ASSERT_EQ("-- Aggregation[2]\n", plan->toString());
  ASSERT_EQ(
      "-- Aggregation[2][SINGLE [c0, group_id] sum_c1 := sum(ROW[\"c1\"]) global group IDs: [ 1, 2 ] Group Id key: group_id] -> c0:SMALLINT, group_id:BIGINT, sum_c1:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .values({data_})
             .partialStreamingAggregation({"c0"}, {"sum(c1) AS a"})
             .planNode();
  ASSERT_EQ(
      "-- Aggregation[1][PARTIAL STREAMING [c0] a := sum(ROW[\"c1\"])] -> c0:SMALLINT, a:BIGINT\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, expand) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .expand(
                      {{"c0", "null::integer as c1", "c2", "0 as gid"},
                       {"null as c0", "c1", "c2", "1 as gid"}})
                  .planNode();
  ASSERT_EQ("-- Expand[1]\n", plan->toString());
  ASSERT_EQ(
      "-- Expand[1][[c0, null, c2, 0], [null, c1, c2, 1]] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT, gid:BIGINT\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, groupId) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .groupId({"c0", "c1"}, {{"c0"}, {"c1"}}, {"c2"})
                  .planNode();
  ASSERT_EQ("-- GroupId[1]\n", plan->toString());
  ASSERT_EQ(
      "-- GroupId[1][[c0], [c1]] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT, group_id:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .values({data_})
             .groupId({"c0", "c1"}, {{"c0", "c1"}, {"c1"}}, {"c2"}, "gid")
             .planNode();
  ASSERT_EQ("-- GroupId[1]\n", plan->toString());
  ASSERT_EQ(
      "-- GroupId[1][[c0, c1], [c1]] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT, gid:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .values({data_})
             .groupId({"c0", "c0 as c1"}, {{"c0", "c1"}, {"c1"}}, {"c2"}, "gid")
             .planNode();
  ASSERT_EQ("-- GroupId[1]\n", plan->toString());
  ASSERT_EQ(
      "-- GroupId[1][[c0, c1], [c1]] -> c0:SMALLINT, c1:SMALLINT, c2:BIGINT, gid:BIGINT\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, hashJoin) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .project({"c0 as t_c0", "c1 as t_c1"})
                  .hashJoin(
                      {"t_c0"},
                      {"u_c0"},
                      PlanBuilder()
                          .values({data_})
                          .project({"c0 as u_c0", "c1 as u_c1"})
                          .planNode(),
                      "",
                      {"t_c0", "t_c1", "u_c1"})
                  .planNode();

  ASSERT_EQ("-- HashJoin[2]\n", plan->toString());
  ASSERT_EQ(
      "-- HashJoin[2][INNER t_c0=u_c0] -> t_c0:SMALLINT, t_c1:INTEGER, u_c1:INTEGER\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .values({data_})
             .project({"c0 as t_c0", "c1 as t_c1"})
             .hashJoin(
                 {"t_c0"},
                 {"u_c0"},
                 PlanBuilder()
                     .values({data_})
                     .project({"c0 as u_c0", "c1 as u_c1"})
                     .planNode(),
                 "t_c1 > u_c1",
                 {"t_c0", "t_c1", "u_c1"},
                 core::JoinType::kLeft)
             .planNode();

  ASSERT_EQ("-- HashJoin[2]\n", plan->toString());
  ASSERT_EQ(
      "-- HashJoin[2][LEFT t_c0=u_c0, filter: gt(ROW[\"t_c1\"],ROW[\"u_c1\"])] -> t_c0:SMALLINT, t_c1:INTEGER, u_c1:INTEGER\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .values({data_})
             .project({"c0 as t_c0", "c1 as t_c1"})
             .hashJoin(
                 {"t_c0"},
                 {"u_c0"},
                 PlanBuilder()
                     .values({data_})
                     .project({"c0 as u_c0", "c1 as u_c1"})
                     .planNode(),
                 "",
                 {"t_c0", "t_c1"},
                 core::JoinType::kAnti,
                 true /*nullAware*/)
             .planNode();

  ASSERT_EQ("-- HashJoin[2]\n", plan->toString());
  ASSERT_EQ(
      "-- HashJoin[2][ANTI t_c0=u_c0, null aware] -> t_c0:SMALLINT, t_c1:INTEGER\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .values({data_})
             .project({"c0 as t_c0", "c1 as t_c1"})
             .hashJoin(
                 {"t_c0"},
                 {"u_c0"},
                 PlanBuilder()
                     .values({data_})
                     .project({"c0 as u_c0", "c1 as u_c1"})
                     .planNode(),
                 "",
                 {"t_c0", "t_c1"},
                 core::JoinType::kAnti,
                 false /*nullAware*/)
             .planNode();

  ASSERT_EQ("-- HashJoin[2]\n", plan->toString());
  ASSERT_EQ(
      "-- HashJoin[2][ANTI t_c0=u_c0] -> t_c0:SMALLINT, t_c1:INTEGER\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, mergeJoin) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .project({"c0 as t_c0", "c1 as t_c1"})
                  .mergeJoin(
                      {"t_c0"},
                      {"u_c0"},
                      PlanBuilder()
                          .values({data_})
                          .project({"c0 as u_c0", "c1 as u_c1"})
                          .planNode(),
                      "",
                      {"t_c0", "t_c1", "u_c1"})
                  .planNode();

  ASSERT_EQ("-- MergeJoin[2]\n", plan->toString());
  ASSERT_EQ(
      "-- MergeJoin[2][INNER t_c0=u_c0] -> t_c0:SMALLINT, t_c1:INTEGER, u_c1:INTEGER\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .values({data_})
             .project({"c0 as t_c0", "c1 as t_c1"})
             .mergeJoin(
                 {"t_c0"},
                 {"u_c0"},
                 PlanBuilder()
                     .values({data_})
                     .project({"c0 as u_c0", "c1 as u_c1"})
                     .planNode(),
                 "t_c1 > u_c1",
                 {"t_c0", "t_c1", "u_c1"},
                 core::JoinType::kLeft)
             .planNode();

  ASSERT_EQ("-- MergeJoin[2]\n", plan->toString());
  ASSERT_EQ(
      "-- MergeJoin[2][LEFT t_c0=u_c0, filter: gt(ROW[\"t_c1\"],ROW[\"u_c1\"])] -> t_c0:SMALLINT, t_c1:INTEGER, u_c1:INTEGER\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, nestedLoopJoin) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .project({"c0 as t_c0", "c1 as t_c1"})
                  .nestedLoopJoin(
                      PlanBuilder()
                          .values({data_})
                          .project({"c0 as u_c0", "c1 as u_c1"})
                          .planNode(),
                      {"t_c0", "t_c1", "u_c1"})
                  .planNode();

  ASSERT_EQ("-- NestedLoopJoin[2]\n", plan->toString());
  ASSERT_EQ(
      "-- NestedLoopJoin[2][INNER] -> t_c0:SMALLINT, t_c1:INTEGER, u_c1:INTEGER\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, orderBy) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .orderBy({"c1 ASC NULLS FIRST"}, true)
                  .planNode();

  ASSERT_EQ("-- OrderBy[1]\n", plan->toString());
  ASSERT_EQ(
      "-- OrderBy[1][PARTIAL c1 ASC NULLS FIRST] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .values({data_})
             .orderBy({"c1 ASC NULLS FIRST", "c0 DESC NULLS LAST"}, false)
             .planNode();

  ASSERT_EQ("-- OrderBy[1]\n", plan->toString());
  ASSERT_EQ(
      "-- OrderBy[1][c1 ASC NULLS FIRST, c0 DESC NULLS LAST] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, limit) {
  auto plan = PlanBuilder().values({data_}).limit(0, 10, true).planNode();

  ASSERT_EQ("-- Limit[1]\n", plan->toString());
  ASSERT_EQ(
      "-- Limit[1][PARTIAL 10] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder().values({data_}).limit(7, 12, false).planNode();

  ASSERT_EQ("-- Limit[1]\n", plan->toString());
  ASSERT_EQ(
      "-- Limit[1][12 offset 7] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, topN) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .topN({"c0 NULLS FIRST"}, 10, true)
                  .planNode();

  ASSERT_EQ("-- TopN[1]\n", plan->toString());
  ASSERT_EQ(
      "-- TopN[1][PARTIAL 10 c0 ASC NULLS FIRST] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .values({data_})
             .topN({"c1 NULLS FIRST", "c0 DESC"}, 10, false)
             .planNode();

  ASSERT_EQ("-- TopN[1]\n", plan->toString());
  ASSERT_EQ(
      "-- TopN[1][10 c1 ASC NULLS FIRST, c0 DESC NULLS LAST] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, enforceSingleRow) {
  auto plan = PlanBuilder().values({data_}).enforceSingleRow().planNode();

  ASSERT_EQ("-- EnforceSingleRow[1]\n", plan->toString());
  ASSERT_EQ(
      "-- EnforceSingleRow[1][] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, assignUniqueId) {
  auto plan =
      PlanBuilder().values({data_}).assignUniqueId("unique_id", 123).planNode();

  ASSERT_EQ("-- AssignUniqueId[1]\n", plan->toString());
  ASSERT_EQ(
      "-- AssignUniqueId[1][] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT, unique_id:BIGINT\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, unnest) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .project({"array_constructor(c0) AS a0", "c1"})
                  .unnest({"c1"}, {"a0"})
                  .planNode();

  ASSERT_EQ("-- Unnest[2]\n", plan->toString());
  ASSERT_EQ(
      "-- Unnest[2][a0] -> c1:INTEGER, a0_e:SMALLINT\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, localPartition) {
  auto plan = PlanBuilder().values({data_}).localPartition({"c0"}).planNode();

  ASSERT_EQ("-- LocalPartition[1]\n", plan->toString());
  ASSERT_EQ(
      "-- LocalPartition[1][REPARTITION HASH(c0)] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .values({data_})
             .localPartition(std::vector<std::string>{})
             .planNode();

  ASSERT_EQ("-- LocalPartition[1]\n", plan->toString());
  ASSERT_EQ(
      "-- LocalPartition[1][GATHER] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder().values({data_}).localPartitionRoundRobin().planNode();

  ASSERT_EQ("-- LocalPartition[1]\n", plan->toString());
  ASSERT_EQ(
      "-- LocalPartition[1][REPARTITION ROUND ROBIN] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, partitionedOutput) {
  auto plan =
      PlanBuilder().values({data_}).partitionedOutput({"c0"}, 4).planNode();

  ASSERT_EQ("-- PartitionedOutput[1]\n", plan->toString());
  ASSERT_EQ(
      "-- PartitionedOutput[1][partitionFunction: HASH(c0) with 4 partitions] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder().values({data_}).partitionedOutputBroadcast().planNode();

  ASSERT_EQ("-- PartitionedOutput[1]\n", plan->toString());
  ASSERT_EQ(
      "-- PartitionedOutput[1][BROADCAST] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder().values({data_}).partitionedOutput({}, 1).planNode();

  ASSERT_EQ("-- PartitionedOutput[1]\n", plan->toString());
  ASSERT_EQ(
      "-- PartitionedOutput[1][SINGLE] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .values({data_})
             .partitionedOutput({"c1", "c2"}, 5, true)
             .planNode();

  ASSERT_EQ("-- PartitionedOutput[1]\n", plan->toString());
  ASSERT_EQ(
      "-- PartitionedOutput[1][partitionFunction: HASH(c1, c2) with 5 partitions replicate nulls and any] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));

  auto hiveSpec = std::make_shared<connector::hive::HivePartitionFunctionSpec>(
      4,
      std::vector<int>{0, 1, 0, 1},
      std::vector<column_index_t>{1, 2},
      std::vector<VectorPtr>{});

  plan = PlanBuilder()
             .values({data_})
             .partitionedOutput({"c1", "c2"}, 2, false, hiveSpec)
             .planNode();
  ASSERT_EQ("-- PartitionedOutput[1]\n", plan->toString());
  ASSERT_EQ(
      "-- PartitionedOutput[1][partitionFunction: HIVE((1, 2) buckets: 4) with 2 partitions] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, localMerge) {
  auto plan =
      PlanBuilder()
          .localMerge(
              {"c1 NULLS FIRST"}, {PlanBuilder().values({data_}).planNode()})
          .planNode();

  ASSERT_EQ("-- LocalMerge[0]\n", plan->toString());
  ASSERT_EQ(
      "-- LocalMerge[0][c1 ASC NULLS FIRST] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .localMerge(
                 {"c1 NULLS FIRST", "c0 DESC"},
                 {PlanBuilder().values({data_}).planNode()})
             .planNode();

  ASSERT_EQ("-- LocalMerge[0]\n", plan->toString());
  ASSERT_EQ(
      "-- LocalMerge[0][c1 ASC NULLS FIRST, c0 DESC NULLS LAST] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, exchange) {
  auto plan =
      PlanBuilder().exchange(ROW({"a", "b"}, {BIGINT(), VARCHAR()})).planNode();

  ASSERT_EQ("-- Exchange[0]\n", plan->toString());
  ASSERT_EQ(
      "-- Exchange[0][] -> a:BIGINT, b:VARCHAR\n", plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, tableScan) {
  RowTypePtr rowType{
      ROW({"discount", "quantity", "shipdate", "comment"},
          {DOUBLE(), DOUBLE(), VARCHAR(), VARCHAR()})};
  {
    auto plan = PlanBuilder(pool_.get())
                    .tableScan(
                        rowType,
                        {"shipdate between '1994-01-01' and '1994-12-31'",
                         "discount between 0.05 and 0.07",
                         "quantity < 24.0::DOUBLE"},
                        "comment NOT LIKE '%special%request%'")
                    .planNode();

    ASSERT_EQ("-- TableScan[0]\n", plan->toString());
    const char* output =
        "-- TableScan[0][table: hive_table, "
        "range filters: [(discount, DoubleRange: [0.050000, 0.070000] no nulls), "
        "(quantity, DoubleRange: (-inf, 24.000000) no nulls), "
        "(shipdate, BytesRange: [1994-01-01, 1994-12-31] no nulls)], "
        "remaining filter: (not(like(ROW[\"comment\"],\"%special%request%\")))] "
        "-> discount:DOUBLE, quantity:DOUBLE, shipdate:VARCHAR, comment:VARCHAR\n";
    ASSERT_EQ(output, plan->toString(true, false));
  }
  {
    auto plan =
        PlanBuilder(pool_.get())
            .tableScan(rowType, {}, "comment NOT LIKE '%special%request%'")
            .planNode();

    ASSERT_EQ(
        "-- TableScan[0][table: hive_table, remaining filter: (not(like(ROW[\"comment\"],\"%special%request%\")))] "
        "-> discount:DOUBLE, quantity:DOUBLE, shipdate:VARCHAR, comment:VARCHAR\n",
        plan->toString(true, false));
  }
}

TEST_F(PlanNodeToStringTest, decimalConstant) {
  parse::ParseOptions options;
  options.parseDecimalAsDouble = false;

  auto plan = PlanBuilder()
                  .setParseOptions(options)
                  .tableScan(ROW({"a"}, {VARCHAR()}))
                  .project({"a", "1.234"})
                  .planNode();

  ASSERT_EQ(
      "-- Project[1][expressions: (a:VARCHAR, ROW[\"a\"]), (p1:DECIMAL(4, 3), 1.234)] -> a:VARCHAR, p1:DECIMAL(4, 3)\n",
      plan->toString(true));
}

TEST_F(PlanNodeToStringTest, window) {
  std::vector<exec::FunctionSignaturePtr> signatures{
      exec::FunctionSignatureBuilder()
          .argumentType("BIGINT")
          .returnType("BIGINT")
          .build(),
  };
  exec::registerWindowFunction("window1", std::move(signatures), nullptr);

  auto plan =
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window({"window1(c) over (partition by a order by b "
                   "range between 10 preceding and unbounded following) AS d"})
          .planNode();
  ASSERT_EQ("-- Window[1]\n", plan->toString());
  ASSERT_EQ(
      "-- Window[1][partition by [a] order by [b ASC NULLS LAST] "
      "d := window1(ROW[\"c\"]) RANGE between 10 PRECEDING and UNBOUNDED FOLLOWING] "
      "-> a:VARCHAR, b:BIGINT, c:BIGINT, d:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
             .window({"window1(c) over (partition by a "
                      "rows between current row and b following)"})
             .planNode();
  ASSERT_EQ("-- Window[1]\n", plan->toString());
  ASSERT_EQ(
      "-- Window[1][partition by [a] "
      "w0 := window1(ROW[\"c\"]) ROWS between CURRENT ROW and b FOLLOWING] "
      "-> a:VARCHAR, b:BIGINT, c:BIGINT, w0:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
             .window({"window1(c) over (order by a "
                      "range between current row and b following)"})
             .planNode();
  ASSERT_EQ("-- Window[1]\n", plan->toString());
  ASSERT_EQ(
      "-- Window[1][order by [a ASC NULLS LAST] "
      "w0 := window1(ROW[\"c\"]) RANGE between CURRENT ROW and b FOLLOWING] "
      "-> a:VARCHAR, b:BIGINT, c:BIGINT, w0:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
             .streamingWindow(
                 {"window1(c) over (partition by a order by b "
                  "range between 10 preceding and unbounded following) AS d"})
             .planNode();
  ASSERT_EQ("-- Window[1]\n", plan->toString());
  ASSERT_EQ(
      "-- Window[1][STREAMING partition by [a] order by [b ASC NULLS LAST] "
      "d := window1(ROW[\"c\"]) RANGE between 10 PRECEDING and UNBOUNDED FOLLOWING] "
      "-> a:VARCHAR, b:BIGINT, c:BIGINT, d:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
             .streamingWindow({"window1(c) over (partition by a "
                               "rows between current row and b following)"})
             .planNode();
  ASSERT_EQ("-- Window[1]\n", plan->toString());
  ASSERT_EQ(
      "-- Window[1][STREAMING partition by [a] "
      "w0 := window1(ROW[\"c\"]) ROWS between CURRENT ROW and b FOLLOWING] "
      "-> a:VARCHAR, b:BIGINT, c:BIGINT, w0:BIGINT\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, rowNumber) {
  // Emit row number.
  auto plan =
      PlanBuilder().tableScan(ROW({"a"}, {VARCHAR()})).rowNumber({}).planNode();

  ASSERT_EQ("-- RowNumber[1]\n", plan->toString());
  ASSERT_EQ(
      "-- RowNumber[1][] -> a:VARCHAR, row_number:BIGINT\n",
      plan->toString(true, false));

  // Don't emit row number.
  plan = PlanBuilder()
             .tableScan(ROW({"a"}, {VARCHAR()}))
             .rowNumber({}, std::nullopt, false)
             .planNode();

  ASSERT_EQ("-- RowNumber[1]\n", plan->toString());
  ASSERT_EQ("-- RowNumber[1][] -> a:VARCHAR\n", plan->toString(true, false));

  // Emit row number.
  plan = PlanBuilder()
             .tableScan(ROW({"a", "b"}, {BIGINT(), VARCHAR()}))
             .rowNumber({"a", "b"})
             .planNode();

  ASSERT_EQ("-- RowNumber[1]\n", plan->toString());
  ASSERT_EQ(
      "-- RowNumber[1][partition by (a, b)] -> a:BIGINT, b:VARCHAR, row_number:BIGINT\n",
      plan->toString(true, false));

  // Don't emit row number.
  plan = PlanBuilder()
             .tableScan(ROW({"a", "b"}, {BIGINT(), VARCHAR()}))
             .rowNumber({"a", "b"}, std::nullopt, false)
             .planNode();

  ASSERT_EQ("-- RowNumber[1]\n", plan->toString());
  ASSERT_EQ(
      "-- RowNumber[1][partition by (a, b)] -> a:BIGINT, b:VARCHAR\n",
      plan->toString(true, false));

  // Emit row number.
  plan = PlanBuilder()
             .tableScan(ROW({"a", "b"}, {BIGINT(), VARCHAR()}))
             .rowNumber({"b"}, 10)
             .planNode();

  ASSERT_EQ("-- RowNumber[1]\n", plan->toString());
  ASSERT_EQ(
      "-- RowNumber[1][partition by (b) limit 10] -> a:BIGINT, b:VARCHAR, row_number:BIGINT\n",
      plan->toString(true, false));

  // Don't emit row number.
  plan = PlanBuilder()
             .tableScan(ROW({"a", "b"}, {BIGINT(), VARCHAR()}))
             .rowNumber({"b"}, 10, false)
             .planNode();

  ASSERT_EQ("-- RowNumber[1]\n", plan->toString());
  ASSERT_EQ(
      "-- RowNumber[1][partition by (b) limit 10] -> a:BIGINT, b:VARCHAR\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, topNRowNumber) {
  auto rowType = ROW({"a", "b"}, {BIGINT(), VARCHAR()});
  auto plan = PlanBuilder()
                  .tableScan(rowType)
                  .topNRowNumber({}, {"a DESC"}, 10, false)
                  .planNode();

  ASSERT_EQ("-- TopNRowNumber[1]\n", plan->toString());
  ASSERT_EQ(
      "-- TopNRowNumber[1][order by (a DESC NULLS LAST) limit 10] -> a:BIGINT, b:VARCHAR\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .tableScan(rowType)
             .topNRowNumber({}, {"a DESC"}, 10, true)
             .planNode();

  ASSERT_EQ("-- TopNRowNumber[1]\n", plan->toString());
  ASSERT_EQ(
      "-- TopNRowNumber[1][order by (a DESC NULLS LAST) limit 10] -> a:BIGINT, b:VARCHAR, row_number:BIGINT\n",
      plan->toString(true, false));

  plan = PlanBuilder()
             .tableScan(rowType)
             .topNRowNumber({"a"}, {"b"}, 10, false)
             .planNode();

  ASSERT_EQ("-- TopNRowNumber[1]\n", plan->toString());
  ASSERT_EQ(
      "-- TopNRowNumber[1][partition by (a) order by (b ASC NULLS LAST) limit 10] -> a:BIGINT, b:VARCHAR\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, markDistinct) {
  auto op =
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .markDistinct("marker", {"a", "b"})
          .planNode();
  ASSERT_EQ("-- MarkDistinct[1]\n", op->toString());
  ASSERT_EQ(
      "-- MarkDistinct[1][a, b] -> a:VARCHAR, b:BIGINT, c:BIGINT, marker:BOOLEAN\n",
      op->toString(true, false));
}
