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
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/tests/FilterBuilder.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"
#include "velox/vector/tests/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::common::test;

using facebook::velox::exec::test::PlanBuilder;

class PlanNodeToStringTest : public testing::Test, public test::VectorTestBase {
 public:
  PlanNodeToStringTest() {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
    data_ = makeRowVector({
        makeFlatVector<int16_t>({0, 1, 2, 3, 4}),
        makeFlatVector<int32_t>({0, 1, 2, 3, 4}),
        makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
    });

    plan_ = PlanBuilder()
                .values({data_})
                .filter("c0 % 10 < 9")
                .project({"c0 AS out1", "c0 % 100 + c1 % 50 AS out2"})
                .filter("out1 % 10 < 8")
                .project({"out1 + 10 AS out3"})
                .planNode();
  }

  RowVectorPtr data_;
  std::shared_ptr<core::PlanNode> plan_;
};

TEST_F(PlanNodeToStringTest, basic) {
  ASSERT_EQ("-> Project\n", plan_->toString());
}

TEST_F(PlanNodeToStringTest, recursive) {
  ASSERT_EQ(
      "-> Project\n"
      "  -> Filter\n"
      "    -> Project\n"
      "      -> Filter\n"
      "        -> Values\n",
      plan_->toString(false, true));
}

TEST_F(PlanNodeToStringTest, detailed) {
  ASSERT_EQ(
      "-> Project[expressions: (out3:BIGINT, plus(cast ROW[\"out1\"] as BIGINT,10)), ]\n",
      plan_->toString(true, false));
}

TEST_F(PlanNodeToStringTest, recursiveAndDetailed) {
  ASSERT_EQ(
      "-> Project[expressions: (out3:BIGINT, plus(cast ROW[\"out1\"] as BIGINT,10)), ]\n"
      "  -> Filter[expression: lt(mod(cast ROW[\"out1\"] as BIGINT,10),8)]\n"
      "    -> Project[expressions: (out1:SMALLINT, ROW[\"c0\"]), (out2:BIGINT, plus(mod(cast ROW[\"c0\"] as BIGINT,100),mod(cast ROW[\"c1\"] as BIGINT,50))), ]\n"
      "      -> Filter[expression: lt(mod(cast ROW[\"c0\"] as BIGINT,10),9)]\n"
      "        -> Values[5 rows in 1 vectors]\n",
      plan_->toString(true, true));
}

TEST_F(PlanNodeToStringTest, withContext) {
  auto addContext = [](const core::PlanNodeId& planNodeId,
                       const std::string& /* indentation */,
                       std::stringstream& stream) {
    stream << "Context for " << planNodeId;
  };

  ASSERT_EQ(
      "-> Project\n"
      "   Context for 4\n",
      plan_->toString(false, false, addContext));

  ASSERT_EQ(
      "-> Project[expressions: (out3:BIGINT, plus(cast ROW[\"out1\"] as BIGINT,10)), ]\n"
      "   Context for 4\n",
      plan_->toString(true, false, addContext));

  ASSERT_EQ(
      "-> Project\n"
      "   Context for 4\n"
      "  -> Filter\n"
      "     Context for 3\n"
      "    -> Project\n"
      "       Context for 2\n"
      "      -> Filter\n"
      "         Context for 1\n"
      "        -> Values\n"
      "           Context for 0\n",
      plan_->toString(false, true, addContext));

  ASSERT_EQ(
      "-> Project[expressions: (out3:BIGINT, plus(cast ROW[\"out1\"] as BIGINT,10)), ]\n"
      "   Context for 4\n"
      "  -> Filter[expression: lt(mod(cast ROW[\"out1\"] as BIGINT,10),8)]\n"
      "     Context for 3\n"
      "    -> Project[expressions: (out1:SMALLINT, ROW[\"c0\"]), (out2:BIGINT, plus(mod(cast ROW[\"c0\"] as BIGINT,100),mod(cast ROW[\"c1\"] as BIGINT,50))), ]\n"
      "       Context for 2\n"
      "      -> Filter[expression: lt(mod(cast ROW[\"c0\"] as BIGINT,10),9)]\n"
      "         Context for 1\n"
      "        -> Values[5 rows in 1 vectors]\n"
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
      "-> Project\n"
      "   Context for 4: line 1\n"
      "   Context for 4: line 2\n",
      plan_->toString(false, false, addContext));

  ASSERT_EQ(
      "-> Project[expressions: (out3:BIGINT, plus(cast ROW[\"out1\"] as BIGINT,10)), ]\n"
      "   Context for 4: line 1\n"
      "   Context for 4: line 2\n",
      plan_->toString(true, false, addContext));
}

TEST_F(PlanNodeToStringTest, aggregation) {
  // Global aggregation.
  auto plan = PlanBuilder()
                  .values({data_})
                  .partialAggregation(
                      {}, {"sum(c0) AS a", "avg(c1) AS b", "min(c2) AS c"})
                  .planNode();

  ASSERT_EQ("-> Aggregation\n", plan->toString());
  ASSERT_EQ(
      "-> Aggregation[PARTIAL a := sum(ROW[\"c0\"]), b := avg(ROW[\"c1\"]), c := min(ROW[\"c2\"])]\n",
      plan->toString(true, false));

  // Group-by aggregation.
  plan = PlanBuilder()
             .values({data_})
             .singleAggregation({0}, {"sum(c1) AS a", "avg(c2) AS b"})
             .planNode();

  ASSERT_EQ("-> Aggregation\n", plan->toString());
  ASSERT_EQ(
      "-> Aggregation[SINGLE [c0] a := sum(ROW[\"c1\"]), b := avg(ROW[\"c2\"])]\n",
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

  ASSERT_EQ("-> HashJoin\n", plan->toString());
  ASSERT_EQ("-> HashJoin[INNER t_c0=u_c0]\n", plan->toString(true, false));

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

  ASSERT_EQ("-> HashJoin\n", plan->toString());
  ASSERT_EQ(
      "-> HashJoin[LEFT t_c0=u_c0, filter: gt(ROW[\"t_c1\"],ROW[\"u_c1\"])]\n",
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

  ASSERT_EQ("-> MergeJoin\n", plan->toString());
  ASSERT_EQ("-> MergeJoin[INNER t_c0=u_c0]\n", plan->toString(true, false));

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

  ASSERT_EQ("-> MergeJoin\n", plan->toString());
  ASSERT_EQ(
      "-> MergeJoin[LEFT t_c0=u_c0, filter: gt(ROW[\"t_c1\"],ROW[\"u_c1\"])]\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, crossJoin) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .project({"c0 as t_c0", "c1 as t_c1"})
                  .crossJoin(
                      PlanBuilder()
                          .values({data_})
                          .project({"c0 as u_c0", "c1 as u_c1"})
                          .planNode(),
                      {"t_c0", "t_c1", "u_c1"})
                  .planNode();

  ASSERT_EQ("-> CrossJoin\n", plan->toString());
  ASSERT_EQ("-> CrossJoin[]\n", plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, orderBy) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .orderBy({"c1 ASC NULLS FIRST"}, true)
                  .planNode();

  ASSERT_EQ("-> OrderBy\n", plan->toString());
  ASSERT_EQ(
      "-> OrderBy[PARTIAL c1 ASC NULLS FIRST]\n", plan->toString(true, false));

  plan = PlanBuilder()
             .values({data_})
             .orderBy({"c1 ASC NULLS FIRST", "c0 DESC NULLS LAST"}, false)
             .planNode();

  ASSERT_EQ("-> OrderBy\n", plan->toString());
  ASSERT_EQ(
      "-> OrderBy[c1 ASC NULLS FIRST, c0 DESC NULLS LAST]\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, limit) {
  auto plan = PlanBuilder().values({data_}).limit(0, 10, true).planNode();

  ASSERT_EQ("-> Limit\n", plan->toString());
  ASSERT_EQ("-> Limit[PARTIAL 10]\n", plan->toString(true, false));

  plan = PlanBuilder().values({data_}).limit(7, 12, false).planNode();

  ASSERT_EQ("-> Limit\n", plan->toString());
  ASSERT_EQ("-> Limit[12 offset 7]\n", plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, topN) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .topN({"c0 NULLS FIRST"}, 10, true)
                  .planNode();

  ASSERT_EQ("-> TopN\n", plan->toString());
  ASSERT_EQ(
      "-> TopN[PARTIAL 10 c0 ASC NULLS FIRST]\n", plan->toString(true, false));

  plan = PlanBuilder()
             .values({data_})
             .topN({"c1 NULLS FIRST", "c0 DESC"}, 10, false)
             .planNode();

  ASSERT_EQ("-> TopN\n", plan->toString());
  ASSERT_EQ(
      "-> TopN[10 c1 ASC NULLS FIRST, c0 DESC NULLS LAST]\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, enforceSingleRow) {
  auto plan = PlanBuilder().values({data_}).enforceSingleRow().planNode();

  ASSERT_EQ("-> EnforceSingleRow\n", plan->toString());
  ASSERT_EQ("-> EnforceSingleRow[]\n", plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, assignUniqueId) {
  auto plan =
      PlanBuilder().values({data_}).assignUniqueId("unique_id", 123).planNode();

  ASSERT_EQ("-> AssignUniqueId\n", plan->toString());
  ASSERT_EQ("-> AssignUniqueId[]\n", plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, unnest) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .project({"array_constructor(c0) AS a0", "c1"})
                  .unnest({"c1"}, {"a0"})
                  .planNode();

  ASSERT_EQ("-> Unnest\n", plan->toString());
  ASSERT_EQ("-> Unnest[a0]\n", plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, localPartition) {
  auto plan =
      PlanBuilder()
          .localPartition({0}, {PlanBuilder().values({data_}).planNode()})
          .planNode();

  ASSERT_EQ("-> LocalPartition\n", plan->toString());
  ASSERT_EQ("-> LocalPartition[REPARTITION]\n", plan->toString(true, false));

  plan = PlanBuilder()
             .localPartition({}, {PlanBuilder().values({data_}).planNode()})
             .planNode();

  ASSERT_EQ("-> LocalPartition\n", plan->toString());
  ASSERT_EQ("-> LocalPartition[GATHER]\n", plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, partitionedOutput) {
  auto plan =
      PlanBuilder().values({data_}).partitionedOutput({0}, 4).planNode();

  ASSERT_EQ("-> PartitionedOutput\n", plan->toString());
  ASSERT_EQ("-> PartitionedOutput[HASH(c0) 4]\n", plan->toString(true, false));

  plan = PlanBuilder().values({data_}).partitionedOutputBroadcast().planNode();

  ASSERT_EQ("-> PartitionedOutput\n", plan->toString());
  ASSERT_EQ("-> PartitionedOutput[BROADCAST]\n", plan->toString(true, false));

  plan = PlanBuilder().values({data_}).partitionedOutput({}, 1).planNode();

  ASSERT_EQ("-> PartitionedOutput\n", plan->toString());
  ASSERT_EQ("-> PartitionedOutput[SINGLE]\n", plan->toString(true, false));

  plan = PlanBuilder()
             .values({data_})
             .partitionedOutput({1, 2}, 5, true)
             .planNode();

  ASSERT_EQ("-> PartitionedOutput\n", plan->toString());
  ASSERT_EQ(
      "-> PartitionedOutput[HASH(c1, c2) 5 replicate nulls and any]\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, localMerge) {
  auto plan =
      PlanBuilder()
          .localMerge(
              {"c1 NULLS FIRST"}, {PlanBuilder().values({data_}).planNode()})
          .planNode();

  ASSERT_EQ("-> LocalMerge\n", plan->toString());
  ASSERT_EQ("-> LocalMerge[c1 ASC NULLS FIRST]\n", plan->toString(true, false));

  plan = PlanBuilder()
             .localMerge(
                 {"c1 NULLS FIRST", "c0 DESC"},
                 {PlanBuilder().values({data_}).planNode()})
             .planNode();

  ASSERT_EQ("-> LocalMerge\n", plan->toString());
  ASSERT_EQ(
      "-> LocalMerge[c1 ASC NULLS FIRST, c0 DESC NULLS LAST]\n",
      plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, exchange) {
  auto plan =
      PlanBuilder().exchange(ROW({"a", "b"}, {BIGINT(), VARCHAR()})).planNode();

  ASSERT_EQ("-> Exchange\n", plan->toString());
  ASSERT_EQ("-> Exchange[]\n", plan->toString(true, false));
}

TEST_F(PlanNodeToStringTest, tableScan) {
  RowTypePtr rowType{ROW(
      {"discount", "quantity", "shipdate"}, {DOUBLE(), DOUBLE(), VARCHAR()})};
  auto filters = SubfieldFiltersBuilder()
                     .add("shipdate", between("1994-01-01", "1994-12-31"))
                     .add("discount", betweenDouble(0.05, 0.07))
                     .add("quantity", lessThanDouble(24.0))
                     .build();
  auto tableHandle = exec::test::HiveConnectorTestBase::makeTableHandle(
      std::move(filters), nullptr, "lineitem");
  auto plan =
      PlanBuilder()
          .tableScan(
              rowType,
              tableHandle,
              exec::test::HiveConnectorTestBase::allRegularColumns(rowType))
          .planNode();

  ASSERT_EQ("-> TableScan\n", plan->toString());
  ASSERT_EQ(
      "-> TableScan[Table: lineitem, Filters: [(discount, DoubleRange: [0.050000, 0.070000] no nulls), (quantity, DoubleRange: (-inf, 24.000000) no nulls), (shipdate, BytesRange: [1994-01-01, 1994-12-31] no nulls)]]\n",
      plan->toString(true, false));
}
