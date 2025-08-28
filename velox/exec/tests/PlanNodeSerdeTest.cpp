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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace facebook::velox::exec::test {

class PlanNodeSerdeTest : public testing::Test,
                          public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  PlanNodeSerdeTest() {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();

    Type::registerSerDe();
    common::Filter::registerSerDe();
    connector::hive::HiveTableHandle::registerSerDe();
    connector::hive::LocationHandle::registerSerDe();
    connector::hive::HiveColumnHandle::registerSerDe();
    connector::hive::HiveInsertTableHandle::registerSerDe();
    connector::hive::HiveInsertFileNameGenerator::registerSerDe();
    connector::hive::registerHivePartitionFunctionSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();
    registerPartitionFunctionSerDe();

    data_ = {makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3}),
        makeFlatVector<int32_t>({10, 20, 30}),
        makeConstant(true, 3),
        makeArrayVector<int32_t>({
            {1, 2},
            {3, 4, 5},
            {},
        }),
    })};
  }

  void testSerde(const core::PlanNodePtr& plan) {
    {
      const auto serialized = plan->serialize();
      const auto copy =
          velox::ISerializable::deserialize<core::PlanNode>(serialized, pool());
      ASSERT_EQ(plan->toString(true, true), copy->toString(true, true));
    }
    {
      // Test serde with type cache enabled.
      auto& cache = serializedTypeCache();
      cache.enable({.minRowTypeSize = 1});
      SCOPE_EXIT {
        cache.disable();
        cache.clear();
        deserializedTypeCache().clear();
      };

      const auto serialized = plan->serialize();

      const auto serializedCache = cache.serialize();
      deserializedTypeCache().deserialize(serializedCache);

      const auto copy =
          velox::ISerializable::deserialize<core::PlanNode>(serialized, pool());
      ASSERT_EQ(plan->toString(true, true), copy->toString(true, true));
    }
  }

  void topNRankSerdeTest(std::string_view function) {
    auto plan = PlanBuilder()
                    .values({data_})
                    .topNRank(function, {}, {"c0", "c2"}, 10, false)
                    .planNode();
    testSerde(plan);

    plan = PlanBuilder()
               .values({data_})
               .topNRank(function, {}, {"c0", "c2"}, 10, true)
               .planNode();
    testSerde(plan);

    plan = PlanBuilder()
               .values({data_})
               .topNRank(function, {"c0"}, {"c1", "c2"}, 10, false)
               .planNode();
    testSerde(plan);
  }

  std::vector<RowVectorPtr> data_;
};

TEST_F(PlanNodeSerdeTest, aggregation) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .partialAggregation({"c0"}, {"count(1)", "sum(c1)"})
                  .finalAggregation()
                  .planNode();

  testSerde(plan);

  plan = PlanBuilder(pool_.get())
             .values({data_})
             .partialAggregation({"c0"}, {"count(ARRAY[1, 2])"})
             .finalAggregation()
             .planNode();

  testSerde(plan);

  // Aggregation over sorted inputs.
  plan = PlanBuilder()
             .values({data_})
             .singleAggregation(
                 {"c0"}, {"array_agg(c1 ORDER BY c2 DESC)", "sum(c1)"})
             .planNode();

  testSerde(plan);

  // Aggregation over distinct inputs.
  plan = PlanBuilder()
             .values({data_})
             .singleAggregation({"c0"}, {"sum(distinct c1)", "avg(c1)"})
             .planNode();

  testSerde(plan);

  // Aggregation over GroupId with global grouping sets.
  plan = PlanBuilder()
             .values({data_})
             .groupId({"c0"}, {{"c0"}, {}}, {"c1"})
             .singleAggregation({"c0", "group_id"}, {"sum(c1) as sum_c1"}, {})
             .project({"sum_c1"})
             .planNode();

  testSerde(plan);

  // Aggregation over GroupId with multiple global grouping sets.
  plan = PlanBuilder()
             .values({data_})
             .groupId({"c0"}, {{"c0"}, {}, {}}, {"c1"})
             .singleAggregation({"c0", "group_id"}, {"sum(c1) as sum_c1"}, {})
             .project({"sum_c1"})
             .planNode();

  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, assignUniqueId) {
  auto plan = PlanBuilder().values({data_}).assignUniqueId().planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, markDistinct) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .markDistinct("marker", {"c0", "c1", "c2"})
                  .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, nestedLoopJoin) {
  auto left = makeRowVector(
      {"t0", "t1", "t2"},
      {
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<int64_t>({10, 20, 30}),
          makeFlatVector<bool>({true, true, false}),
      });

  auto right = makeRowVector(
      {"u0", "u1", "u2"},
      {
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<int64_t>({10, 20, 30}),
          makeFlatVector<bool>({true, true, false}),
      });

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  {
    auto plan =
        PlanBuilder(planNodeIdGenerator)
            .values({left})
            .nestedLoopJoin(
                PlanBuilder(planNodeIdGenerator).values({right}).planNode(),
                {"t0", "u1", "t2", "t1"})
            .planNode();
    testSerde(plan);
  }
  {
    auto plan =
        PlanBuilder(planNodeIdGenerator)
            .values({left})
            .nestedLoopJoin(
                PlanBuilder(planNodeIdGenerator).values({right}).planNode(),
                "t0 < u0",
                {"t0", "u1", "t2", "t1"})
            .planNode();
    testSerde(plan);
  }
}

TEST_F(PlanNodeSerdeTest, enforceSingleRow) {
  auto plan = PlanBuilder().values({data_}).enforceSingleRow().planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, exchange) {
  for (auto serdeKind : std::vector<VectorSerde::Kind>{
           VectorSerde::Kind::kPresto,
           VectorSerde::Kind::kCompactRow,
           VectorSerde::Kind::kUnsafeRow}) {
    SCOPED_TRACE(fmt::format("serdeKind: {}", serdeKind));
    auto plan = PlanBuilder()
                    .exchange(
                        ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), VARCHAR()}),
                        serdeKind)
                    .planNode();
    testSerde(plan);
  }
}

TEST_F(PlanNodeSerdeTest, filter) {
  auto plan = PlanBuilder().values({data_}).filter("c0 > 100").planNode();
  testSerde(plan);

  plan = PlanBuilder(pool_.get())
             .values({data_})
             .filter("c3 = ARRAY[1,2,3]")
             .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, groupId) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .groupId({"c0", "c1"}, {{"c0"}, {"c0", "c1"}}, {"c2"})
                  .planNode();
  testSerde(plan);

  plan = PlanBuilder()
             .values({data_})
             .groupId({"c0", "c0 as c1"}, {{"c0"}, {"c0", "c1"}}, {"c2"})
             .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, expand) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .expand(
                      {{"c0", "c1", "c2", "0 as gid"},
                       {"c0", "c1", "null as c2", "1  as gid"},
                       {"c0", "null as c1", "null as c2", "2  as gid"}})
                  .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, localPartition) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .localPartition(std::vector<std::string>{})
                  .planNode();
  testSerde(plan);

  plan = PlanBuilder().values({data_}).localPartition({"c0", "c1"}).planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, scaleWriterlocalPartition) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .scaleWriterlocalPartition(std::vector<std::string>{"c0"})
                  .planNode();
  testSerde(plan);

  plan = PlanBuilder()
             .values({data_})
             .scaleWriterlocalPartitionRoundRobin()
             .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, limit) {
  auto plan = PlanBuilder().values({data_}).limit(0, 10, true).planNode();
  testSerde(plan);

  plan = PlanBuilder().values({data_}).limit(0, 10, false).planNode();
  testSerde(plan);

  plan = PlanBuilder().values({data_}).limit(12, 10, false).planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, mergeExchange) {
  for (auto serdeKind : std::vector<VectorSerde::Kind>{
           VectorSerde::Kind::kPresto,
           VectorSerde::Kind::kCompactRow,
           VectorSerde::Kind::kUnsafeRow}) {
    auto plan = PlanBuilder()
                    .mergeExchange(
                        ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), VARCHAR()}),
                        {"a DESC", "b NULLS FIRST"},
                        serdeKind)
                    .planNode();
    testSerde(plan);
  }
}

TEST_F(PlanNodeSerdeTest, localMerge) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .localMerge(
              {"c0"},
              {
                  PlanBuilder(planNodeIdGenerator).values({data_}).planNode(),
                  PlanBuilder(planNodeIdGenerator).values({data_}).planNode(),
              })
          .planNode();

  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, mergeJoin) {
  auto probe = makeRowVector(
      {"t0", "t1", "t2", "t3"},
      {
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<int64_t>({10, 20, 30}),
          makeFlatVector<bool>({true, true, false}),
          makeArrayVector<int32_t>({
              {1, 2},
              {3, 4, 5},
              {},
          }),
      });

  auto build = makeRowVector(
      {"u0", "u1", "u2"},
      {
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<int64_t>({10, 20, 30}),
          makeFlatVector<bool>({true, true, false}),
      });

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values({probe})
          .mergeJoin(
              {"t0"},
              {"u0"},
              PlanBuilder(planNodeIdGenerator).values({build}).planNode(),
              "t1 > u1",
              {"t0", "t1", "u2", "t2"},
              core::JoinType::kInner)
          .planNode();

  testSerde(plan);

  plan = PlanBuilder(planNodeIdGenerator, pool_.get())
             .values({probe})
             .mergeJoin(
                 {"t0"},
                 {"u0"},
                 PlanBuilder(planNodeIdGenerator).values({build}).planNode(),
                 "t3 = ARRAY[1,2,3]",
                 {"t0", "t1", "u2", "t2"},
                 core::JoinType::kInner)
             .planNode();

  testSerde(plan);

  plan = PlanBuilder(planNodeIdGenerator)
             .values({probe})
             .mergeJoin(
                 {"t0"},
                 {"u0"},
                 PlanBuilder(planNodeIdGenerator).values({build}).planNode(),
                 "", // no filter
                 {"t0", "t1", "u2", "t2"},
                 core::JoinType::kLeft)
             .planNode();

  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, orderBy) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .orderBy({"c0", "c1 DESC NULLS FIRST"}, true)
                  .planNode();

  testSerde(plan);

  plan = PlanBuilder()
             .values({data_})
             .orderBy({"c0", "c1 DESC NULLS FIRST"}, false)
             .planNode();

  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, partitionedOutput) {
  for (auto serdeKind : std::vector<VectorSerde::Kind>{
           VectorSerde::Kind::kPresto,
           VectorSerde::Kind::kCompactRow,
           VectorSerde::Kind::kUnsafeRow}) {
    SCOPED_TRACE(fmt::format("serdeKind: {}", serdeKind));

    auto plan = PlanBuilder()
                    .values({data_})
                    .partitionedOutputBroadcast(/*outputLayout=*/{}, serdeKind)
                    .planNode();
    testSerde(plan);

    plan = PlanBuilder()
               .values({data_})
               .partitionedOutput({"c0"}, 50, /*outputLayout=*/{}, serdeKind)
               .planNode();
    testSerde(plan);

    plan = PlanBuilder()
               .values({data_})
               .partitionedOutput({"c0"}, 50, {"c1", {"c2"}, "c0"}, serdeKind)
               .planNode();
    testSerde(plan);
  }
}

TEST_F(PlanNodeSerdeTest, project) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .project({"c0 * 10", "c0 + c1", "c1 > 0"})
                  .planNode();

  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, parallelProjet) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .parallelProject({{"c0 + 1", "c0 * 2"}, {"c1 + 1", "c1 * 2"}})
                  .planNode();

  testSerde(plan);

  plan = PlanBuilder()
             .values({data_})
             .parallelProject(
                 {{"c0 + 1", "c0 * 2"}, {"c1 + 1", "c1 * 2"}}, {"c2", "c3"})
             .planNode();

  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, hashJoin) {
  auto probe = makeRowVector(
      {"t0", "t1", "t2", "t3"},
      {
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<int64_t>({10, 20, 30}),
          makeFlatVector<bool>({true, true, false}),
          makeArrayVector<int32_t>({
              {1, 2},
              {3, 4, 5},
              {},
          }),
      });

  auto build = makeRowVector(
      {"u0", "u1", "u2"},
      {
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<int64_t>({10, 20, 30}),
          makeFlatVector<bool>({true, true, false}),
      });

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values({probe})
          .hashJoin(
              {"t0"},
              {"u0"},
              PlanBuilder(planNodeIdGenerator).values({build}).planNode(),
              "t1 > u1",
              {"t0", "t1", "u2", "t2"},
              core::JoinType::kInner)
          .planNode();

  testSerde(plan);

  plan = PlanBuilder(planNodeIdGenerator, pool_.get())
             .values({probe})
             .hashJoin(
                 {"t0"},
                 {"u0"},
                 PlanBuilder(planNodeIdGenerator).values({build}).planNode(),
                 "t3 = ARRAY[1,2,3]",
                 {"t0", "t1", "u2", "t2"},
                 core::JoinType::kInner)
             .planNode();

  testSerde(plan);

  plan = PlanBuilder(planNodeIdGenerator)
             .values({probe})
             .hashJoin(
                 {"t0"},
                 {"u0"},
                 PlanBuilder(planNodeIdGenerator).values({build}).planNode(),
                 "", // no filter
                 {"t0", "t1", "u2", "t2"},
                 core::JoinType::kLeft)
             .planNode();

  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, topN) {
  auto plan = PlanBuilder().values({data_}).topN({"c0"}, 10, true).planNode();
  testSerde(plan);

  plan = PlanBuilder().values({data_}).topN({"c0 DESC"}, 10, false).planNode();
  testSerde(plan);

  plan = PlanBuilder()
             .values({data_})
             .topN({"c0", "c1 DESC NULLS FIRST"}, 10, true)
             .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, unnest) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeArrayVector<int32_t>({
          {1, 2},
          {3, 4, 5},
          {},
      }),
      makeMapVector<int64_t, int32_t>(
          {{{1, 10}, {2, 20}}, {{3, 30}, {4, 40}, {5, 50}}, {}}),
  });

  auto plan = PlanBuilder()
                  .values({data})
                  .unnest({"c0"}, {"c1", "c2"}, std::nullopt)
                  .planNode();
  testSerde(plan);

  plan =
      PlanBuilder().values({data}).unnest({"c0"}, {"c1"}, "ordinal").planNode();
  testSerde(plan);

  plan = PlanBuilder()
             .values({data})
             .unnest({"c0"}, {"c1"}, "ordinal", "emptyUnnestValue")
             .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, values) {
  auto plan = PlanBuilder().values({data_}).planNode();
  testSerde(plan);

  plan = PlanBuilder()
             .values({data_}, true /*parallelizable*/, 5 /*repeatTimes*/)
             .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, window) {
  auto plan =
      PlanBuilder()
          .values({data_})
          .window(
              {"sum(c0) over (partition by c1 order by c2 rows between 10 preceding and 5 following)"})
          .planNode();

  testSerde(plan);

  plan = PlanBuilder()
             .values({data_})
             .window({"sum(c0) over (partition by c1 order by c2)"})
             .planNode();

  testSerde(plan);

  // Test StreamingWindow serde.
  plan =
      PlanBuilder()
          .values({data_})
          .streamingWindow(
              {"sum(c0) over (partition by c1 order by c2 rows between 10 preceding and 5 following)"})
          .planNode();

  testSerde(plan);

  plan = PlanBuilder()
             .values({data_})
             .streamingWindow({"sum(c0) over (partition by c1 order by c2)"})
             .planNode();

  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, rowNumber) {
  // Test with emitting the row number.
  auto plan = PlanBuilder().values({data_}).rowNumber({}).planNode();
  testSerde(plan);

  plan = PlanBuilder().values({data_}).rowNumber({"c2", "c0"}).planNode();
  testSerde(plan);

  plan = PlanBuilder().values({data_}).rowNumber({"c1", "c2"}, 10).planNode();
  testSerde(plan);

  // Test without emitting the row number.
  plan = PlanBuilder()
             .values({data_})
             .rowNumber({}, std::nullopt, false)
             .planNode();
  testSerde(plan);

  plan = PlanBuilder()
             .values({data_})
             .rowNumber({"c2", "c0"}, std::nullopt, false)
             .planNode();
  testSerde(plan);

  plan = PlanBuilder()
             .values({data_})
             .rowNumber({"c1", "c2"}, 10, false)
             .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, scan) {
  auto plan = PlanBuilder(pool_.get())
                  .tableScan(
                      ROW({"a", "b", "c", "d"},
                          {BIGINT(), BIGINT(), BOOLEAN(), DOUBLE()}),
                      {"a < 5", "b = 7", "c = true", "d > 0.01"},
                      "a + b < 100")
                  .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, topNRowNumber) {
  topNRankSerdeTest("row_number");
}

TEST_F(PlanNodeSerdeTest, topNRank) {
  topNRankSerdeTest("rank");
}

TEST_F(PlanNodeSerdeTest, topNDenseRank) {
  topNRankSerdeTest("dense_rank");
}

TEST_F(PlanNodeSerdeTest, write) {
  auto rowTypePtr = ROW({"c0", "c1", "c2"}, {BIGINT(), BOOLEAN(), VARBINARY()});
  auto planBuilder =
      PlanBuilder(pool_.get()).tableScan(rowTypePtr, {"c1 = true"}, "c0 < 100");
  auto plan = planBuilder.tableWrite("targetDirectory").planNode();
  testSerde(plan);

  plan = PlanBuilder(pool_.get())
             .values(data_)
             .tableWrite("targetDirectory")
             .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, tableWriteMerge) {
  auto rowTypePtr = ROW({"c0", "c1", "c2"}, {BIGINT(), BOOLEAN(), VARBINARY()});
  auto planBuilder =
      PlanBuilder(pool_.get()).tableScan(rowTypePtr, {"c1 = true"}, "c0 < 100");
  auto plan = planBuilder.tableWrite("targetDirectory")
                  .localPartition(std::vector<std::string>{})
                  .tableWriteMerge()
                  .planNode();
  testSerde(plan);

  plan = PlanBuilder(pool_.get())
             .values(data_)
             .tableWrite(
                 "targetDirectory", dwio::common::FileFormat::DWRF, {"min(c0)"})
             .localPartition(std::vector<std::string>{})
             .tableWriteMerge()
             .planNode();
  testSerde(plan);

  plan = PlanBuilder(pool_.get())
             .values(data_)
             .tableWrite(
                 "targetDirectory", dwio::common::FileFormat::DWRF, {"min(c0)"})
             .localPartition(std::vector<std::string>{})
             .tableWriteMerge()
             .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, tableWriteWithStats) {
  auto rowTypePtr = ROW({"c0", "c1", "c2"}, {BIGINT(), BOOLEAN(), VARCHAR()});
  auto planBuilder =
      PlanBuilder(pool_.get()).tableScan(rowTypePtr, {"c1 = true"}, "c0 < 100");
  auto plan = planBuilder
                  .tableWrite(
                      "targetDirectory",
                      dwio::common::FileFormat::DWRF,
                      {"min(c0)",
                       "max(c0)",
                       "count(c2)",
                       "approx_distinct(c2)",
                       "sum_data_size_for_stats(c2)",
                       "max_data_size_for_stats(c2)"})
                  .localPartition(std::vector<std::string>{})
                  .tableWriteMerge()
                  .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, columnStatsSpec) {
  // Helper function to create typed expressions
  const auto createFieldAccess = [&](const std::string& name,
                                     const TypePtr& type) {
    return std::make_shared<core::FieldAccessTypedExpr>(type, name);
  };

  const auto createCallExpr = [&](const std::string& name,
                                  const TypePtr& returnType,
                                  const std::vector<core::TypedExprPtr>& args) {
    return std::make_shared<core::CallTypedExpr>(returnType, args, name);
  };

  // Test 1: Empty ColumnStatsSpec
  {
    VELOX_ASSERT_THROW(
        std::make_unique<core::ColumnStatsSpec>(
            std::vector<core::FieldAccessTypedExprPtr>{},
            core::AggregationNode::Step::kSingle,
            std::vector<std::string>{},
            std::vector<core::AggregationNode::Aggregate>{}),
        "");
  }

  // Test 2: ColumnStatsSpec with only aggregates (no grouping keys)
  {
    std::vector<core::FieldAccessTypedExprPtr> groupingKeys;
    std::vector<std::string> aggregateNames = {"min", "max", "count"};
    std::vector<core::AggregationNode::Aggregate> aggregates;

    // Create min(c0) aggregate
    auto minInput = createFieldAccess("c0", BIGINT());
    auto minCall = createCallExpr("min", BIGINT(), {minInput});
    aggregates.push_back({minCall, {BIGINT()}, nullptr, {}, {}, false});

    // Create max(c0) aggregate
    auto maxInput = createFieldAccess("c0", BIGINT());
    auto maxCall = createCallExpr("max", BIGINT(), {maxInput});
    aggregates.push_back({maxCall, {BIGINT()}, nullptr, {}, {}, false});

    // Create count(c1) aggregate
    auto countInput = createFieldAccess("c1", INTEGER());
    auto countCall = createCallExpr("count", BIGINT(), {countInput});
    aggregates.push_back({countCall, {INTEGER()}, nullptr, {}, {}, false});

    core::ColumnStatsSpec spec(
        std::move(groupingKeys),
        core::AggregationNode::Step::kPartial,
        std::move(aggregateNames),
        std::move(aggregates));

    auto serialized = spec.serialize();
    auto deserialized = core::ColumnStatsSpec::create(serialized, pool());

    EXPECT_TRUE(deserialized.groupingKeys.empty());
    EXPECT_EQ(deserialized.aggregateNames.size(), 3);
    EXPECT_EQ(deserialized.aggregates.size(), 3);
    EXPECT_EQ(
        deserialized.aggregationStep, core::AggregationNode::Step::kPartial);
    EXPECT_EQ(deserialized.aggregateNames.size(), 3);

    EXPECT_EQ(deserialized.aggregateNames[0], "min");
    EXPECT_EQ(deserialized.aggregateNames[1], "max");
    EXPECT_EQ(deserialized.aggregateNames[2], "count");
    EXPECT_EQ(deserialized.aggregates[0].call->name(), "min");
    EXPECT_EQ(deserialized.aggregates[1].call->name(), "max");
    EXPECT_EQ(deserialized.aggregates[2].call->name(), "count");
  }

  // Test 3: ColumnStatsSpec with grouping keys and aggregates
  {
    std::vector<core::FieldAccessTypedExprPtr> groupingKeys;
    groupingKeys.push_back(createFieldAccess("partition_key", VARCHAR()));
    groupingKeys.push_back(createFieldAccess("bucket_id", INTEGER()));

    std::vector<std::string> aggregateNames = {"min", "sum"};
    std::vector<core::AggregationNode::Aggregate> aggregates;

    // Create min(c0) aggregate
    auto minInput = createFieldAccess("c0", BIGINT());
    auto minCall = createCallExpr("min", BIGINT(), {minInput});
    aggregates.push_back({minCall, {BIGINT()}, nullptr, {}, {}, false});

    // Create sum(c1) aggregate
    auto sumInput = createFieldAccess("c1", DOUBLE());
    auto sumCall = createCallExpr("sum", DOUBLE(), {sumInput});
    aggregates.push_back({sumCall, {DOUBLE()}, nullptr, {}, {}, false});

    core::ColumnStatsSpec spec(
        std::move(groupingKeys),
        core::AggregationNode::Step::kIntermediate,
        std::move(aggregateNames),
        std::move(aggregates));

    auto serialized = spec.serialize();
    auto deserialized = core::ColumnStatsSpec::create(serialized, pool());

    EXPECT_EQ(deserialized.groupingKeys.size(), 2);
    EXPECT_EQ(deserialized.aggregateNames.size(), 2);
    EXPECT_EQ(deserialized.aggregates.size(), 2);
    EXPECT_EQ(
        deserialized.aggregationStep,
        core::AggregationNode::Step::kIntermediate);
    EXPECT_EQ(deserialized.aggregateNames.size(), 2);

    EXPECT_EQ(deserialized.groupingKeys[0]->name(), "partition_key");
    EXPECT_EQ(deserialized.groupingKeys[0]->type(), VARCHAR());
    EXPECT_EQ(deserialized.groupingKeys[1]->name(), "bucket_id");
    EXPECT_EQ(deserialized.groupingKeys[1]->type(), INTEGER());

    EXPECT_EQ(deserialized.aggregateNames[0], "min");
    EXPECT_EQ(deserialized.aggregateNames[1], "sum");
    EXPECT_EQ(deserialized.aggregates[0].call->name(), "min");
    EXPECT_EQ(deserialized.aggregates[1].call->name(), "sum");
  }

  // Test 4: ColumnStatsSpec with different aggregation steps
  for (auto step :
       {core::AggregationNode::Step::kSingle,
        core::AggregationNode::Step::kPartial,
        core::AggregationNode::Step::kIntermediate,
        core::AggregationNode::Step::kFinal}) {
    std::vector<core::FieldAccessTypedExprPtr> groupingKeys;
    std::vector<std::string> aggregateNames = {"count"};
    std::vector<core::AggregationNode::Aggregate> aggregates;

    auto countInput = createFieldAccess("test_col", BIGINT());
    auto countCall = createCallExpr("count", BIGINT(), {countInput});
    aggregates.push_back({countCall, {BIGINT()}, nullptr, {}, {}, false});

    core::ColumnStatsSpec spec(
        std::move(groupingKeys),
        step,
        std::move(aggregateNames),
        std::move(aggregates));

    auto serialized = spec.serialize();
    auto deserialized = core::ColumnStatsSpec::create(serialized, pool());

    EXPECT_EQ(deserialized.aggregationStep, step);
    EXPECT_EQ(deserialized.aggregateNames.size(), 1);
    EXPECT_EQ(deserialized.aggregateNames[0], "count");
  }

  // Test 5: ColumnStatsSpec with complex aggregates (distinct, with mask, with
  // sorting)
  {
    std::vector<core::FieldAccessTypedExprPtr> groupingKeys;
    std::vector<std::string> aggregateNames = {
        "count_distinct", "array_agg_sorted"};
    std::vector<core::AggregationNode::Aggregate> aggregates;

    // Create count(distinct c0) aggregate
    auto distinctInput = createFieldAccess("c0", VARCHAR());
    auto countDistinctCall = createCallExpr("count", BIGINT(), {distinctInput});
    auto maskField = createFieldAccess("mask_col", BOOLEAN());
    aggregates.push_back(
        {countDistinctCall, {VARCHAR()}, maskField, {}, {}, true});

    // Create array_agg(c1 ORDER BY c2) aggregate
    auto arrayInput = createFieldAccess("c1", INTEGER());
    auto arrayAggCall =
        createCallExpr("array_agg", ARRAY(INTEGER()), {arrayInput});
    auto sortingKey = createFieldAccess("c2", BIGINT());
    std::vector<core::FieldAccessTypedExprPtr> sortingKeys = {sortingKey};
    std::vector<core::SortOrder> sortingOrders = {core::SortOrder(true, true)};
    aggregates.push_back(
        {arrayAggCall,
         {INTEGER()},
         nullptr,
         sortingKeys,
         sortingOrders,
         false});

    core::ColumnStatsSpec spec(
        std::move(groupingKeys),
        core::AggregationNode::Step::kSingle,
        std::move(aggregateNames),
        std::move(aggregates));

    auto serialized = spec.serialize();
    auto deserialized = core::ColumnStatsSpec::create(serialized, pool());

    EXPECT_EQ(deserialized.aggregates.size(), 2);

    // Check distinct aggregate
    EXPECT_TRUE(deserialized.aggregates[0].distinct);
    EXPECT_NE(deserialized.aggregates[0].mask, nullptr);
    EXPECT_EQ(deserialized.aggregates[0].mask->name(), "mask_col");

    // Check sorted aggregate
    EXPECT_FALSE(deserialized.aggregates[1].distinct);
    EXPECT_EQ(deserialized.aggregates[1].mask, nullptr);
    EXPECT_EQ(deserialized.aggregates[1].sortingKeys.size(), 1);
    EXPECT_EQ(deserialized.aggregates[1].sortingKeys[0]->name(), "c2");
    EXPECT_EQ(deserialized.aggregates[1].sortingOrders.size(), 1);
    EXPECT_TRUE(deserialized.aggregates[1].sortingOrders[0].isAscending());
    EXPECT_TRUE(deserialized.aggregates[1].sortingOrders[0].isNullsFirst());
  }

  // Error cases.
  {
    std::vector<core::FieldAccessTypedExprPtr> groupingKeys;
    groupingKeys.push_back(createFieldAccess("partition_key", VARCHAR()));
    groupingKeys.push_back(createFieldAccess("bucket_id", INTEGER()));

    std::vector<std::string> aggregateNames = {"min", "sum"};
    std::vector<core::AggregationNode::Aggregate> aggregates;

    // Create min(c0) aggregate
    auto minInput = createFieldAccess("c0", BIGINT());
    auto minCall = createCallExpr("min", BIGINT(), {minInput});
    aggregates.push_back({minCall, {BIGINT()}, nullptr, {}, {}, false});
    VELOX_ASSERT_THROW(
        core::ColumnStatsSpec(
            std::move(groupingKeys),
            core::AggregationNode::Step::kSingle,
            std::move(aggregateNames),
            std::move(aggregates)),
        "");
  }
}

} // namespace facebook::velox::exec::test
