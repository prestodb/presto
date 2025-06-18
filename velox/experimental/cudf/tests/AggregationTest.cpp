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

#include "velox/experimental/cudf/exec/ToCudf.h"

#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::exec::test {

using core::QueryConfig;
using facebook::velox::test::BatchMaker;
using namespace common::testutil;

class AggregationTest : public OperatorTestBase {
 protected:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
    TestValue::enable();
  }

  void SetUp() override {
    OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();
    cudf_velox::registerCudf();
  }

  void TearDown() override {
    cudf_velox::unregisterCudf();
    OperatorTestBase::TearDown();
  }

  std::vector<RowVectorPtr>
  makeVectors(const RowTypePtr& rowType, size_t size, int numVectors) {
    std::vector<RowVectorPtr> vectors;
    VectorFuzzer fuzzer({.vectorSize = size}, pool());
    for (int32_t i = 0; i < numVectors; ++i) {
      vectors.push_back(fuzzer.fuzzInputRow(rowType));
    }
    return vectors;
  }

  template <typename T>
  void testSingleKey(
      const std::vector<RowVectorPtr>& vectors,
      const std::string& keyName,
      bool ignoreNullKeys,
      bool distinct) {
    std::vector<std::string> aggregates;
    if (!distinct) {
      // TODO (dm): "sum(15)", "sum(0.1)",  "min(15)",  "min(0.1)", "max(15)",
      // "max(0.1)",
      aggregates = {
          "sum(c1)",
          "sum(c2)",
          "sum(c4)",
          "sum(c5)",
          "min(c1)",
          "min(c2)",
          "min(c3)",
          "min(c4)",
          "min(c5)",
          "max(c1)",
          "max(c2)",
          "max(c3)",
          "max(c4)",
          "max(c5)"};
    }

    auto op = PlanBuilder()
                  .values(vectors)
                  .aggregation(
                      {keyName},
                      aggregates,
                      {},
                      core::AggregationNode::Step::kPartial,
                      ignoreNullKeys)
                  .planNode();

    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause += " WHERE " + keyName + " IS NOT NULL";
    }
    if (distinct) {
      assertQuery(op, "SELECT distinct " + keyName + " " + fromClause);
    } else {
      // TODO (dm): sum(15), sum(cast(0.1 as double)), min(15), min(0.1),
      // max(15), max(0.1),
      assertQuery(
          op,
          "SELECT " + keyName +
              ", sum(c1), sum(c2), sum(c4), sum(c5) , min(c1), min(c2), min(c3), min(c4), min(c5), max(c1), max(c2), max(c3), max(c4), max(c5) " +
              fromClause + " GROUP BY " + keyName);
    }
  }

  void testMultiKey(
      const std::vector<RowVectorPtr>& vectors,
      bool ignoreNullKeys,
      bool distinct) {
    std::vector<std::string> aggregates;
    // TODO (dm): "sum(15)", "sum(0.1)",  "min(15)",  "min(0.1)", "max(15)",
    // "max(0.1)"
    if (!distinct) {
      aggregates = {
          "sum(c4)",
          "sum(c5)",
          "min(c3)",
          "min(c4)",
          "min(c5)",
          "max(c3)",
          "max(c4)",
          "max(c5)"};
    }
    auto op = PlanBuilder()
                  .values(vectors)
                  .aggregation(
                      {"c0", "c1", "c6"},
                      aggregates,
                      {},
                      core::AggregationNode::Step::kPartial,
                      ignoreNullKeys)
                  .planNode();

    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause +=
          " WHERE c0 IS NOT NULL AND c1 IS NOT NULL AND c6 IS NOT NULL";
    }
    if (distinct) {
      assertQuery(op, "SELECT distinct c0, c1, c6 " + fromClause);
    } else {
      // TODO (dm): sum(15), sum(cast(0.1 as double)), min(15), min(0.1),
      // max(15), max(0.1),, sum(1)
      assertQuery(
          op,
          "SELECT c0, c1, c6, sum(c4), sum(c5), min(c3), min(c4), min(c5),  max(c3), max(c4), max(c5) " +
              fromClause + " GROUP BY c0, c1, c6");
    }
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {BIGINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           DOUBLE(), // DM: This used to be REAL() but we don't support that
           DOUBLE(),
           VARCHAR()})};
};

TEST_F(AggregationTest, global) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);

  // DM: removed "sum(15)","min(15)","max(15)",
  auto op = PlanBuilder()
                .values(vectors)
                .aggregation(
                    {},
                    {"sum(c1)",
                     "sum(c2)",
                     "sum(c4)",
                     "sum(c5)",

                     "min(c1)",
                     "min(c2)",
                     "min(c3)",
                     "min(c4)",
                     "min(c5)",

                     "max(c1)",
                     "max(c2)",
                     "max(c3)",
                     "max(c4)",
                     "max(c5)"},
                    {},
                    core::AggregationNode::Step::kPartial,
                    false)
                .planNode();

  // DM: removed sum(15), min(15), max(15),
  assertQuery(
      op,
      "SELECT sum(c1), sum(c2), sum(c4), sum(c5), "
      "min(c1), min(c2), min(c3), min(c4), min(c5), "
      "max(c1), max(c2), max(c3), max(c4), max(c5) FROM tmp");
}

TEST_F(AggregationTest, singleBigintKey) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testSingleKey<int64_t>(vectors, "c0", false, false);
  testSingleKey<int64_t>(vectors, "c0", true, false);
}

TEST_F(AggregationTest, singleBigintKeyDistinct) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testSingleKey<int64_t>(vectors, "c0", false, true);
  testSingleKey<int64_t>(vectors, "c0", true, true);
}

TEST_F(AggregationTest, singleStringKey) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testSingleKey<StringView>(vectors, "c6", false, false);
  testSingleKey<StringView>(vectors, "c6", true, false);
}

TEST_F(AggregationTest, singleStringKeyDistinct) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testSingleKey<StringView>(vectors, "c6", false, true);
  testSingleKey<StringView>(vectors, "c6", true, true);
}

TEST_F(AggregationTest, multiKey) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testMultiKey(vectors, false, false);
  testMultiKey(vectors, true, false);
}

TEST_F(AggregationTest, multiKeyDistinct) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testMultiKey(vectors, false, true);
  testMultiKey(vectors, true, true);
}

TEST_F(AggregationTest, aggregateOfNulls) {
  auto rowVector = makeRowVector({
      BatchMaker::createVector<TypeKind::BIGINT>(
          rowType_->childAt(0), 100, *pool_),
      makeNullConstant(TypeKind::SMALLINT, 100),
  });

  auto vectors = {rowVector};
  createDuckDbTable(vectors);

  auto op = PlanBuilder()
                .values(vectors)
                .aggregation(
                    {"c0"},
                    {"sum(c1)", "min(c1)", "max(c1)"},
                    {},
                    core::AggregationNode::Step::kPartial,
                    false)
                .planNode();

  assertQuery(op, "SELECT c0, sum(c1), min(c1), max(c1) FROM tmp GROUP BY c0");

  // global aggregation
  op = PlanBuilder()
           .values(vectors)
           .aggregation(
               {},
               {"sum(c1)", "min(c1)", "max(c1)"},
               {},
               core::AggregationNode::Step::kPartial,
               false)
           .planNode();

  assertQuery(op, "SELECT sum(c1), min(c1), max(c1) FROM tmp");
}

TEST_F(AggregationTest, allKeyTypes) {
  // Covers different key types. Unlike the integer/string tests, the
  // hash table begins life in the generic mode, not array or
  // normalized key. Add types here as they become supported.
  auto rowType = ROW(
      {"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
      {DOUBLE(), REAL(), BIGINT(), INTEGER(), BOOLEAN(), VARCHAR(), DOUBLE()});

  std::vector<RowVectorPtr> batches;
  for (auto i = 0; i < 10; ++i) {
    batches.push_back(std::static_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType, 100, *pool_)));
  }
  createDuckDbTable(batches);
  auto op =
      PlanBuilder()
          .values(batches)
          .singleAggregation({"c0", "c1", "c2", "c3", "c4", "c5"}, {"sum(c6)"})
          .planNode();

  // DM: Instead of sum(c6), this was sum(1) but we don't yet support constants
  assertQuery(
      op,
      "SELECT c0, c1, c2, c3, c4, c5, sum(c6) FROM tmp "
      " GROUP BY c0, c1, c2, c3, c4, c5");
}

TEST_F(AggregationTest, ignoreNullKeys) {
  // Some keys are null.
  auto data = makeRowVector({
      makeNullableFlatVector<int32_t>(
          {std::nullopt, 1, std::nullopt, 2, std::nullopt, 1, 2}),
      makeFlatVector<int32_t>({-1, 1, -2, 2, -3, 3, 4}),
  });

  auto makePlan = [&](bool ignoreNullKeys) {
    return PlanBuilder()
        .values({data})
        .aggregation(
            {"c0"},
            {"sum(c1)"},
            {},
            core::AggregationNode::Step::kPartial,
            ignoreNullKeys)
        .planNode();
  };

  auto expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeFlatVector<int64_t>({4, 6}),
  });
  AssertQueryBuilder(makePlan(true)).assertResults(expected);

  expected = makeRowVector({
      makeNullableFlatVector<int32_t>({std::nullopt, 1, 2}),
      makeFlatVector<int64_t>({-6, 4, 6}),
  });
  AssertQueryBuilder(makePlan(false)).assertResults(expected);

  // All keys are null.
  data = makeRowVector({
      makeAllNullFlatVector<int32_t>(3),
      makeFlatVector<int32_t>({1, 2, 3}),
  });

  AssertQueryBuilder(makePlan(true)).assertEmptyResults();
}

TEST_F(AggregationTest, avgSingleGrouped) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);

  // DM: removed avg(c3). We're having overflow issues with int64_t.
  std::vector<std::string> aggregates = {
      "avg(c1)", "avg(c2)", "avg(c4)", "avg(c5)"};

  std::string keyName = "c0";
  auto op = PlanBuilder()
                .values(vectors)
                .singleAggregation({keyName}, aggregates)
                .planNode();

  assertQuery(
      op,
      "SELECT " + keyName + ", avg(c1), avg(c2), avg(c4), avg(c5) " +
          "FROM tmp GROUP BY " + keyName);
}

TEST_F(AggregationTest, avgPartialFinalGrouped) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);

  // DM: removed avg(c3). We're having overflow issues with int64_t.
  std::vector<std::string> aggregates = {
      "avg(c1)", "avg(c2)", "avg(c4)", "avg(c5)"};

  std::string keyName = "c0";
  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({keyName}, aggregates)
                .finalAggregation()
                .planNode();

  assertQuery(
      op,
      "SELECT " + keyName + ", avg(c1), avg(c2), avg(c4), avg(c5) " +
          "FROM tmp GROUP BY " + keyName);
}

TEST_F(AggregationTest, avgSingleGlobal) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);

  std::vector<std::string> aggregates = {
      "avg(c1)", "avg(c2)", "avg(c4)", "avg(c5)"};
  auto op = PlanBuilder()
                .values(vectors)
                .singleAggregation({}, aggregates)
                .planNode();

  assertQuery(op, "SELECT avg(c1), avg(c2), avg(c4), avg(c5) FROM tmp");
}

TEST_F(AggregationTest, avgPartialFinalGlobal) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);

  std::vector<std::string> aggregates = {
      "avg(c1)", "avg(c2)", "avg(c4)", "avg(c5)"};

  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({}, aggregates)
                .finalAggregation()
                .planNode();

  assertQuery(op, "SELECT avg(c1), avg(c2), avg(c4), avg(c5) FROM tmp");
}

TEST_F(AggregationTest, countSingleGroupBy) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);

  std::string keyName = "c0";
  std::vector<std::string> aggregates = {"count(0)"};
  auto op = PlanBuilder()
                .values(vectors)
                .singleAggregation({keyName}, aggregates)
                .planNode();

  assertQuery(
      op, "SELECT " + keyName + ", count(*) FROM tmp GROUP BY " + keyName);
}

TEST_F(AggregationTest, countPartialFinalGroupBy) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);

  std::string keyName = "c0";
  std::vector<std::string> aggregates = {"count(0)"};
  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({keyName}, aggregates)
                .finalAggregation()
                .planNode();

  assertQuery(
      op, "SELECT " + keyName + ", count(*) FROM tmp GROUP BY " + keyName);
}

TEST_F(AggregationTest, countSingleGlobal) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);

  std::vector<std::string> aggregates = {"count(0)"};
  auto op = PlanBuilder()
                .values(vectors)
                .singleAggregation({}, aggregates)
                .planNode();

  assertQuery(op, "SELECT count(*) FROM tmp");
}

TEST_F(AggregationTest, countPartialFinalGlobal) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);

  std::vector<std::string> aggregates = {"count(0)"};
  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({}, aggregates)
                .finalAggregation()
                .planNode();

  assertQuery(op, "SELECT count(*) FROM tmp");
}

/// Tests the spark scenario of having different types of aggs in the same
/// planNode Specific example being tested is
/// https://github.com/facebookincubator/velox/issues/12830#issuecomment-2783340233
TEST_F(AggregationTest, CompanionAggs) {
  std::vector<int64_t> keys0{1, 1, 1, 2, 1, 1, 2, 2};
  std::vector<int64_t> keys1{1, 2, 1, 2, 1, 2, 1, 2};
  std::vector<int64_t> values{1, 2, 3, 4, 5, 6, 7, 8};
  auto rowVector = makeRowVector(
      {makeFlatVector<int64_t>(keys0),
       makeFlatVector<int64_t>(keys1),
       makeFlatVector<int64_t>(values)});

  createDuckDbTable({rowVector});

  auto op =
      PlanBuilder()
          .values({rowVector})
          .singleAggregation({"c2", "c0"}, {"count_partial(c1)"})
          .localPartition({"c2", "c0"})
          .singleAggregation({"c0"}, {"count_merge(a0)", "count_partial(c2)"})
          .localPartition({"c0"})
          .singleAggregation({"c0"}, {"count_merge(a0)", "count_merge(a1)"})
          .planNode();
  assertQuery(
      op, "SELECT c0, count(c1), count(distinct c2) FROM tmp GROUP BY c0");
}

TEST_F(AggregationTest, partialAggregationMemoryLimit) {
  auto vectors = {
      makeRowVector({makeFlatVector<int32_t>(
          100, [](auto row) { return row; }, nullEvery(5))}),
      makeRowVector({makeFlatVector<int32_t>(
          110, [](auto row) { return row + 29; }, nullEvery(7))}),
      makeRowVector({makeFlatVector<int32_t>(
          90, [](auto row) { return row - 71; }, nullEvery(7))}),
  };

  createDuckDbTable(vectors);

  // Set an artificially low limit on the amount of data to accumulate in
  // the partial aggregation.

  // Distinct aggregation.
  core::PlanNodeId aggNodeId;
  auto task = AssertQueryBuilder(duckDbQueryRunner_)
                  .config(QueryConfig::kMaxPartialAggregationMemory, 100)
                  .plan(PlanBuilder()
                            .values(vectors)
                            .partialAggregation({"c0"}, {})
                            .capturePlanNodeId(aggNodeId)
                            .finalAggregation()
                            .planNode())
                  .assertResults("SELECT distinct c0 FROM tmp");

  auto rowFlushStats = toPlanStats(task->taskStats())
                           .at(aggNodeId)
                           .customStats.at("flushRowCount");
  EXPECT_GT(rowFlushStats.sum, 0);
  EXPECT_GT(rowFlushStats.max, 0);

  // Count aggregation.
  task = AssertQueryBuilder(duckDbQueryRunner_)
             .config(QueryConfig::kMaxPartialAggregationMemory, 1)
             .plan(PlanBuilder()
                       .values(vectors)
                       .partialAggregation({"c0"}, {"count(1)"})
                       .capturePlanNodeId(aggNodeId)
                       .finalAggregation()
                       .planNode())
             .assertResults("SELECT c0, count(1) FROM tmp GROUP BY 1");

  rowFlushStats = toPlanStats(task->taskStats())
                      .at(aggNodeId)
                      .customStats.at("flushRowCount");
  EXPECT_GT(rowFlushStats.sum, 0);
  EXPECT_GT(rowFlushStats.max, 0);

  // Global aggregation.
  task = AssertQueryBuilder(duckDbQueryRunner_)
             .config(QueryConfig::kMaxPartialAggregationMemory, 1)
             .plan(PlanBuilder()
                       .values(vectors)
                       .partialAggregation({}, {"sum(c0)"})
                       .capturePlanNodeId(aggNodeId)
                       .finalAggregation()
                       .planNode())
             .assertResults("SELECT sum(c0) FROM tmp");
  EXPECT_EQ(
      0,
      toPlanStats(task->taskStats())
          .at(aggNodeId)
          .customStats.count("flushRowCount"));
}

} // namespace facebook::velox::exec::test
