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
#include "velox/core/Expressions.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/SumNonPODAggregate.h"
#include "velox/exec/tests/utils/TempFilePath.h"

namespace facebook::velox::exec {
namespace {

using namespace facebook::velox::exec::test;

class StreamingAggregationTest : public HiveConnectorTestBase,
                                 public testing::WithParamInterface<int32_t> {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    registerSumNonPODAggregate("sumnonpod", 64);
  }

  int32_t flushRows() {
    return GetParam();
  }

  AssertQueryBuilder& config(
      AssertQueryBuilder builder,
      uint32_t outputBatchSize) {
    return builder
        .config(
            core::QueryConfig::kPreferredOutputBatchRows,
            std::to_string(outputBatchSize))
        .config(
            core::QueryConfig::kStreamingAggregationMinOutputBatchRows,
            std::to_string(flushRows()));
  }

  void testAggregation(
      const std::vector<VectorPtr>& keys,
      uint32_t outputBatchSize) {
    auto data = addPayload(keys, 1);
    createDuckDbTable(data);

    auto plan = PlanBuilder()
                    .values(data)
                    .partialStreamingAggregation(
                        {"c0"},
                        {"count(1)",
                         "min(c1)",
                         "max(c1)",
                         "sum(c1)",
                         "sumnonpod(1)",
                         "sum(cast(NULL as INT))",
                         "approx_percentile(c1, 0.95)"})
                    .finalAggregation()
                    .planNode();

    config(AssertQueryBuilder(plan, duckDbQueryRunner_), outputBatchSize)
        .assertResults(
            "SELECT c0, count(1), min(c1), max(c1), sum(c1), sum(1), sum(cast(NULL as INT))"
            "     , approx_quantile(c1, 0.95) "
            "FROM tmp GROUP BY 1");

    EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);

    plan =
        PlanBuilder()
            .values(data)
            .project({"c1", "c0"})
            .partialStreamingAggregation(
                {"c0"},
                {"count(1)", "min(c1)", "max(c1)", "sum(c1)", "sumnonpod(1)"})
            .finalAggregation()
            .planNode();

    config(AssertQueryBuilder(plan, duckDbQueryRunner_), outputBatchSize)
        .assertResults(
            "SELECT c0, count(1), min(c1), max(c1), sum(c1), sum(1) FROM tmp GROUP BY 1");

    EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);

    // Test aggregation masks: one aggregate without a mask, two with the same
    // mask, one with a different mask.
    plan = PlanBuilder()
               .values(data)
               .project({"c0", "c1", "c1 % 7 = 0 AS m1", "c1 % 11 = 0 AS m2"})
               .partialStreamingAggregation(
                   {"c0"},
                   {"count(1)", "min(c1)", "max(c1)", "sum(c1)"},
                   {"", "m1", "m2", "m1"})
               .finalAggregation()
               .planNode();

    config(AssertQueryBuilder(plan, duckDbQueryRunner_), outputBatchSize)
        .assertResults(
            "SELECT c0, count(1), min(c1) filter (where c1 % 7 = 0), "
            "max(c1) filter (where c1 % 11 = 0), sum(c1) filter (where c1 % 7 = 0) "
            "FROM tmp GROUP BY 1");
  }

  void testSortedAggregation(
      const std::vector<VectorPtr>& keys,
      uint32_t outputBatchSize) {
    auto data = addPayload(keys, 2);
    createDuckDbTable(data);

    auto plan = PlanBuilder()
                    .values(data)
                    .streamingAggregation(
                        {"c0"},
                        {"max(c1 order by c2)",
                         "max(c1 order by c2 desc)",
                         "array_agg(c1 order by c2)"},
                        {},
                        core::AggregationNode::Step::kSingle,
                        false)
                    .planNode();

    config(AssertQueryBuilder(plan, duckDbQueryRunner_), outputBatchSize)
        .assertResults(
            "SELECT c0, max(c1 order by c2), max(c1 order by c2 desc), array_agg(c1 order by c2) FROM tmp GROUP BY c0");
  }

  void testSortedAggregationWithBarrier(
      const std::vector<VectorPtr>& keys,
      uint32_t outputBatchSize) {
    const auto inputVectors = addPayload(keys, 2);
    int expectedNumOuputBatchesWithBarrier{0};
    int numInputRows{0};
    for (const auto& inputVector : inputVectors) {
      numInputRows += inputVector->size();
      if (outputBatchSize > inputVector->size()) {
        ++expectedNumOuputBatchesWithBarrier;
      } else {
        expectedNumOuputBatchesWithBarrier +=
            bits::divRoundUp(inputVector->size(), outputBatchSize);
      }
    }
    const int expectedNumOuputBatches =
        bits::divRoundUp(numInputRows, outputBatchSize);

    std::vector<std::shared_ptr<TempFilePath>> tempFiles;
    const int numSplits = keys.size();
    for (int32_t i = 0; i < numSplits; ++i) {
      tempFiles.push_back(TempFilePath::create());
    }
    writeToFiles(toFilePaths(tempFiles), inputVectors);

    createDuckDbTable(inputVectors);

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId aggregationNodeId;
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .startTableScan()
                    .outputType(std::dynamic_pointer_cast<const RowType>(
                        inputVectors[0]->type()))
                    .endTableScan()
                    .streamingAggregation(
                        {"c0"},
                        {"max(c1 order by c2)",
                         "max(c1 order by c2 desc)",
                         "array_agg(c1 order by c2)"},
                        {},
                        core::AggregationNode::Step::kSingle,
                        false)
                    .capturePlanNodeId(aggregationNodeId)
                    .planNode();

    for (const auto barrierExecution : {false, true}) {
      SCOPED_TRACE(fmt::format("barrierExecution {}", barrierExecution));
      auto task =
          AssertQueryBuilder(plan, duckDbQueryRunner_)
              .splits(makeHiveConnectorSplits(tempFiles))
              .serialExecution(true)
              .barrierExecution(barrierExecution)
              .config(
                  core::QueryConfig::kPreferredOutputBatchRows,
                  std::to_string(outputBatchSize))
              .assertResults(
                  "SELECT c0, max(c1 order by c2), max(c1 order by c2 desc), array_agg(c1 order by c2) FROM tmp GROUP BY c0");
      const auto taskStats = task->taskStats();
      ASSERT_EQ(taskStats.numBarriers, barrierExecution ? numSplits : 0);
      ASSERT_EQ(taskStats.numFinishedSplits, numSplits);
      ASSERT_EQ(
          velox::exec::toPlanStats(taskStats)
              .at(aggregationNodeId)
              .outputVectors,
          barrierExecution ? expectedNumOuputBatchesWithBarrier
                           : expectedNumOuputBatches);
    }
  }

  void testDistinctAggregation(
      const std::vector<VectorPtr>& keys,
      uint32_t outputBatchSize) {
    auto data = addPayload(keys, 2);
    createDuckDbTable(data);

    {
      auto plan = PlanBuilder()
                      .values(data)
                      .streamingAggregation(
                          {"c0"},
                          {"array_agg(distinct c1)",
                           "array_agg(c1 order by c2)",
                           "count(distinct c1)",
                           "array_agg(c2)"},
                          {},
                          core::AggregationNode::Step::kSingle,
                          false)
                      .planNode();

      config(AssertQueryBuilder(plan, duckDbQueryRunner_), outputBatchSize)
          .assertResults(
              "SELECT c0, array_agg(distinct c1), array_agg(c1 order by c2), "
              "count(distinct c1), array_agg(c2) FROM tmp GROUP BY c0");
    }

    {
      auto plan =
          PlanBuilder()
              .values(data)
              .streamingAggregation(
                  {"c0"}, {}, {}, core::AggregationNode::Step::kSingle, false)
              .planNode();

      config(AssertQueryBuilder(plan, duckDbQueryRunner_), outputBatchSize)
          .assertResults("SELECT distinct c0 FROM tmp");
    }
  }

  void testDistinctAggregationWithBarrier(
      const std::vector<VectorPtr>& keys,
      uint32_t outputBatchSize) {
    const auto inputVectors = addPayload(keys, 2);
    int expectedNumOuputBatchesWithBarrier{0};
    int numInputRows{0};
    for (const auto& inputVector : inputVectors) {
      numInputRows += inputVector->size();
      if (outputBatchSize > inputVector->size()) {
        ++expectedNumOuputBatchesWithBarrier;
      } else {
        expectedNumOuputBatchesWithBarrier +=
            bits::divRoundUp(inputVector->size(), outputBatchSize);
      }
    }
    const int expectedNumOuputBatches =
        bits::divRoundUp(numInputRows, outputBatchSize);

    std::vector<std::shared_ptr<TempFilePath>> tempFiles;
    const int numSplits = keys.size();
    for (int32_t i = 0; i < numSplits; ++i) {
      tempFiles.push_back(TempFilePath::create());
    }
    writeToFiles(toFilePaths(tempFiles), inputVectors);

    createDuckDbTable(inputVectors);

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    {
      core::PlanNodeId aggregationNodeId;
      auto plan = PlanBuilder(planNodeIdGenerator)
                      .startTableScan()
                      .outputType(std::dynamic_pointer_cast<const RowType>(
                          inputVectors[0]->type()))
                      .endTableScan()
                      .streamingAggregation(
                          {"c0"},
                          {"array_agg(distinct c1)",
                           "array_agg(c1 order by c2)",
                           "count(distinct c1)",
                           "array_agg(c2)"},
                          {},
                          core::AggregationNode::Step::kSingle,
                          false)
                      .capturePlanNodeId(aggregationNodeId)
                      .planNode();
      for (const auto barrierExecution : {false, true}) {
        SCOPED_TRACE(fmt::format("barrierExecution {}", barrierExecution));
        auto task =
            AssertQueryBuilder(plan, duckDbQueryRunner_)
                .splits(makeHiveConnectorSplits(tempFiles))
                .serialExecution(true)
                .barrierExecution(barrierExecution)
                .config(
                    core::QueryConfig::kPreferredOutputBatchRows,
                    std::to_string(outputBatchSize))
                .assertResults(
                    "SELECT c0, array_agg(distinct c1), array_agg(c1 order by c2), "
                    "count(distinct c1), array_agg(c2) FROM tmp GROUP BY c0");
        const auto taskStats = task->taskStats();
        ASSERT_EQ(taskStats.numBarriers, barrierExecution ? numSplits : 0);
        ASSERT_EQ(taskStats.numFinishedSplits, numSplits);
        ASSERT_EQ(
            velox::exec::toPlanStats(taskStats)
                .at(aggregationNodeId)
                .outputVectors,
            barrierExecution ? expectedNumOuputBatchesWithBarrier
                             : expectedNumOuputBatches);
      }
    }

    {
      core::PlanNodeId aggregationNodeId;
      auto plan =
          PlanBuilder(planNodeIdGenerator)
              .startTableScan()
              .outputType(std::dynamic_pointer_cast<const RowType>(
                  inputVectors[0]->type()))
              .endTableScan()
              .streamingAggregation(
                  {"c0"}, {}, {}, core::AggregationNode::Step::kSingle, false)
              .capturePlanNodeId(aggregationNodeId)
              .planNode();

      for (const auto barrierExecution : {false, true}) {
        SCOPED_TRACE(fmt::format("barrierExecution {}", barrierExecution));
        auto task = AssertQueryBuilder(plan, duckDbQueryRunner_)
                        .splits(makeHiveConnectorSplits(tempFiles))
                        .serialExecution(true)
                        .barrierExecution(barrierExecution)
                        .config(
                            core::QueryConfig::kPreferredOutputBatchRows,
                            std::to_string(outputBatchSize))
                        .assertResults("SELECT distinct c0 FROM tmp");
        const auto taskStats = task->taskStats();
        ASSERT_EQ(taskStats.numBarriers, barrierExecution ? numSplits : 0);
        ASSERT_EQ(taskStats.numFinishedSplits, numSplits);
        ASSERT_EQ(
            velox::exec::toPlanStats(taskStats)
                .at(aggregationNodeId)
                .outputVectors,
            barrierExecution ? expectedNumOuputBatchesWithBarrier
                             : expectedNumOuputBatches);
      }
    }
  }

  std::vector<RowVectorPtr> addPayload(
      const std::vector<VectorPtr>& keys,
      int numPayloadColumns) {
    std::vector<RowVectorPtr> data;
    vector_size_t totalSize = 0;
    for (const auto& keyVector : keys) {
      auto size = keyVector->size();
      auto payload = makeFlatVector<int32_t>(
          size, [totalSize](auto row) { return totalSize + row; });
      std::vector<VectorPtr> columns;
      columns.push_back(keyVector);
      for (int i = 0; i < numPayloadColumns; ++i) {
        columns.push_back(payload);
      }
      data.push_back(makeRowVector(columns));
      totalSize += size;
    }
    return data;
  }

  std::vector<RowVectorPtr> addPayload(const std::vector<RowVectorPtr>& keys) {
    auto numKeys = keys[0]->type()->size();

    std::vector<RowVectorPtr> data;

    vector_size_t totalSize = 0;
    for (const auto& keyVector : keys) {
      auto size = keyVector->size();
      auto payload = makeFlatVector<int32_t>(
          size, [totalSize](auto row) { return totalSize + row; });

      auto children = keyVector->as<RowVector>()->children();
      VELOX_CHECK_EQ(numKeys, children.size());
      children.push_back(payload);
      data.push_back(makeRowVector(children));
      totalSize += size;
    }
    return data;
  }

  size_t numKeys(const std::vector<RowVectorPtr>& keys) {
    return keys[0]->type()->size();
  }

  void testMultiKeyAggregation(
      const std::vector<RowVectorPtr>& keys,
      uint32_t outputBatchSize) {
    testMultiKeyAggregation(
        keys, keys[0]->type()->asRow().names(), outputBatchSize);
  }

  void testMultiKeyAggregation(
      const std::vector<RowVectorPtr>& keys,
      const std::vector<std::string>& preGroupedKeys,
      uint32_t outputBatchSize) {
    auto data = addPayload(keys);
    createDuckDbTable(data);

    auto plan =
        PlanBuilder()
            .values(data)
            .aggregation(
                keys[0]->type()->asRow().names(),
                preGroupedKeys,
                {"count(1)", "min(c1)", "max(c1)", "sum(c1)", "sumnonpod(1)"},
                {},
                core::AggregationNode::Step::kPartial,
                false)
            .finalAggregation()
            .planNode();

    // Generate a list of grouping keys to use in the query: c0, c1, c2,..
    std::ostringstream keySql;
    keySql << "c0";
    for (auto i = 1; i < numKeys(keys); i++) {
      keySql << ", c" << i;
    }

    const auto sql = fmt::format(
        "SELECT {}, count(1), min(c1), max(c1), sum(c1), sum(1) FROM tmp GROUP BY {}",
        keySql.str(),
        keySql.str());

    config(AssertQueryBuilder(plan, duckDbQueryRunner_), outputBatchSize)
        .assertResults(sql);

    EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);

    // Force partial aggregation flush after every batch of input.
    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(core::QueryConfig::kMaxPartialAggregationMemory, "0")
        .assertResults(sql);

    EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);
  }

  void testMultiKeyDistinctAggregation(
      const std::vector<RowVectorPtr>& keys,
      uint32_t outputBatchSize) {
    auto data = addPayload(keys);
    createDuckDbTable(data);

    {
      auto plan =
          PlanBuilder()
              .values(data)
              .streamingAggregation(
                  keys[0]->type()->asRow().names(),
                  {"count(distinct c1)", "array_agg(c1)", "sumnonpod(1)"},
                  {},
                  core::AggregationNode::Step::kSingle,
                  false)
              .planNode();

      // Generate a list of grouping keys to use in the query: c0, c1, c2,..
      std::ostringstream keySql;
      keySql << "c0";
      for (auto i = 1; i < numKeys(keys); i++) {
        keySql << ", c" << i;
      }

      const auto sql = fmt::format(
          "SELECT {}, count(distinct c1), array_agg(c1), sum(1) FROM tmp GROUP BY {}",
          keySql.str(),
          keySql.str());

      config(AssertQueryBuilder(plan, duckDbQueryRunner_), outputBatchSize)
          .assertResults(sql);

      EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);
    }

    {
      auto plan = PlanBuilder()
                      .values(data)
                      .streamingAggregation(
                          keys[0]->type()->asRow().names(),
                          {},
                          {},
                          core::AggregationNode::Step::kSingle,
                          false)
                      .planNode();

      // Generate a list of grouping keys to use in the query: c0, c1, c2,..
      std::ostringstream keySql;
      keySql << "c0";
      for (auto i = 1; i < numKeys(keys); i++) {
        keySql << ", c" << i;
      }

      const auto sql = fmt::format("SELECT distinct {} FROM tmp", keySql.str());

      config(AssertQueryBuilder(plan, duckDbQueryRunner_), outputBatchSize)
          .assertResults(sql);
    }
  }

  void testMultiKeyDistinctAggregationWithBarrier(
      const std::vector<RowVectorPtr>& keys,
      uint32_t outputBatchSize) {
    const auto inputVectors = addPayload(keys);
    int expectedNumOuputBatchesWithBarrier{0};
    int numInputRows{0};
    for (const auto& inputVector : inputVectors) {
      numInputRows += inputVector->size();
      if (outputBatchSize > inputVector->size()) {
        ++expectedNumOuputBatchesWithBarrier;
      } else {
        expectedNumOuputBatchesWithBarrier +=
            bits::divRoundUp(inputVector->size(), outputBatchSize);
      }
    }
    const int expectedNumOuputBatches =
        bits::divRoundUp(numInputRows, outputBatchSize);
    std::vector<std::shared_ptr<TempFilePath>> tempFiles;
    const int numSplits = keys.size();
    for (int32_t i = 0; i < numSplits; ++i) {
      tempFiles.push_back(TempFilePath::create());
    }
    writeToFiles(toFilePaths(tempFiles), inputVectors);

    createDuckDbTable(inputVectors);

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    {
      core::PlanNodeId aggregationNodeId;
      auto plan =
          PlanBuilder(planNodeIdGenerator)
              .startTableScan()
              .outputType(std::dynamic_pointer_cast<const RowType>(
                  inputVectors[0]->type()))
              .endTableScan()
              .streamingAggregation(
                  keys[0]->type()->asRow().names(),
                  {"count(distinct c1)", "array_agg(c1)", "sumnonpod(1)"},
                  {},
                  core::AggregationNode::Step::kSingle,
                  false)
              .capturePlanNodeId(aggregationNodeId)
              .planNode();

      // Generate a list of grouping keys to use in the query: c0, c1, c2,..
      std::ostringstream keySql;
      keySql << "c0";
      for (auto i = 1; i < numKeys(keys); i++) {
        keySql << ", c" << i;
      }

      const auto sql = fmt::format(
          "SELECT {}, count(distinct c1), array_agg(c1), sum(1) FROM tmp GROUP BY {}",
          keySql.str(),
          keySql.str());

      for (const auto barrierExecution : {false, true}) {
        SCOPED_TRACE(fmt::format("barrierExecution {}", barrierExecution));
        auto task = AssertQueryBuilder(plan, duckDbQueryRunner_)
                        .splits(makeHiveConnectorSplits(tempFiles))
                        .serialExecution(true)
                        .barrierExecution(barrierExecution)
                        .config(
                            core::QueryConfig::kPreferredOutputBatchRows,
                            std::to_string(outputBatchSize))
                        .assertResults(sql);
        const auto taskStats = task->taskStats();
        ASSERT_EQ(taskStats.numBarriers, barrierExecution ? numSplits : 0);
        ASSERT_EQ(taskStats.numFinishedSplits, numSplits);
        ASSERT_EQ(
            velox::exec::toPlanStats(taskStats)
                .at(aggregationNodeId)
                .outputVectors,
            barrierExecution ? expectedNumOuputBatchesWithBarrier
                             : expectedNumOuputBatches);
        EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);
      }
    }

    {
      core::PlanNodeId aggregationNodeId;
      auto plan = PlanBuilder(planNodeIdGenerator)
                      .startTableScan()
                      .outputType(std::dynamic_pointer_cast<const RowType>(
                          inputVectors[0]->type()))
                      .endTableScan()
                      .streamingAggregation(
                          keys[0]->type()->asRow().names(),
                          {},
                          {},
                          core::AggregationNode::Step::kSingle,
                          false)
                      .capturePlanNodeId(aggregationNodeId)
                      .planNode();

      // Generate a list of grouping keys to use in the query: c0, c1, c2,..
      std::ostringstream keySql;
      keySql << "c0";
      for (auto i = 1; i < numKeys(keys); i++) {
        keySql << ", c" << i;
      }

      const auto sql = fmt::format("SELECT distinct {} FROM tmp", keySql.str());

      for (const auto barrierExecution : {false, true}) {
        SCOPED_TRACE(fmt::format("barrierExecution {}", barrierExecution));
        auto task = AssertQueryBuilder(plan, duckDbQueryRunner_)
                        .splits(makeHiveConnectorSplits(tempFiles))
                        .serialExecution(true)
                        .barrierExecution(barrierExecution)
                        .config(
                            core::QueryConfig::kPreferredOutputBatchRows,
                            std::to_string(outputBatchSize))
                        .assertResults(sql);
        const auto taskStats = task->taskStats();
        ASSERT_EQ(taskStats.numBarriers, barrierExecution ? numSplits : 0);
        ASSERT_EQ(taskStats.numFinishedSplits, numSplits);
        ASSERT_EQ(
            velox::exec::toPlanStats(taskStats)
                .at(aggregationNodeId)
                .outputVectors,
            barrierExecution ? expectedNumOuputBatchesWithBarrier
                             : expectedNumOuputBatches);
        EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);
      }
    }
  }
};

VELOX_INSTANTIATE_TEST_SUITE_P(
    StreamingAggregationTest,
    StreamingAggregationTest,
    testing::ValuesIn({0, 1, 64, std::numeric_limits<int32_t>::max()}),
    [](const testing::TestParamInfo<int32_t>& info) {
      return fmt::format(
          "streamingMinOutputBatchSize_{}",
          info.param == std::numeric_limits<int32_t>::max()
              ? "inf"
              : std::to_string(info.param));
    });

TEST_P(StreamingAggregationTest, smallInputBatches) {
  // Use grouping keys that span one or more batches.
  std::vector<VectorPtr> keys = {
      makeNullableFlatVector<int32_t>({1, 1, std::nullopt, 2, 2}),
      makeFlatVector<int32_t>({2, 3, 3, 4}),
      makeFlatVector<int32_t>({5, 6, 6, 6}),
      makeFlatVector<int32_t>({6, 6, 6, 6}),
      makeFlatVector<int32_t>({6, 7, 8}),
  };

  testAggregation(keys, 1024);

  // Cut output into tiny batches of size 3.
  testAggregation(keys, 3);
}

TEST_P(StreamingAggregationTest, multipleKeys) {
  std::vector<RowVectorPtr> keys = {
      makeRowVector({
          makeFlatVector<int32_t>({1, 1, 2, 2, 2}),
          makeFlatVector<int64_t>({10, 20, 20, 30, 30}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({2, 3, 3, 3, 4}),
          makeFlatVector<int64_t>({30, 30, 40, 40, 40}),
      }),
      makeRowVector({
          makeNullableFlatVector<int32_t>({5, std::nullopt, 6, 6, 6}),
          makeNullableFlatVector<int64_t>({40, 50, 50, 50, std::nullopt}),
      }),
  };

  testMultiKeyAggregation(keys, 1024);

  // Cut output into tiny batches of size 3.
  testMultiKeyAggregation(keys, 3);
}

TEST_P(StreamingAggregationTest, regularSizeInputBatches) {
  auto size = 1'024;

  std::vector<VectorPtr> keys = {
      makeFlatVector<int32_t>(size, [](auto row) { return row / 5; }),
      makeFlatVector<int32_t>(
          size, [size](auto row) { return (size + row) / 5; }),
      makeFlatVector<int32_t>(
          size, [size](auto row) { return (2 * size + row) / 5; }),
      makeFlatVector<int32_t>(
          78, [size](auto row) { return (3 * size + row) / 5; }),
  };

  testAggregation(keys, 1024);

  // Cut output into small batches of size 100.
  testAggregation(keys, 100);
}

TEST_P(StreamingAggregationTest, uniqueKeys) {
  auto size = 1'024;

  std::vector<VectorPtr> keys = {
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [size](auto row) { return (size + row); }),
      makeFlatVector<int32_t>(
          size, [size](auto row) { return 2 * size + row; }),
      makeFlatVector<int32_t>(78, [size](auto row) { return 3 * size + row; }),
  };

  testAggregation(keys, 1024);

  // Cut output into small batches of size 100.
  testAggregation(keys, 100);
}

TEST_P(StreamingAggregationTest, partialStreaming) {
  auto size = 1'024;

  // Generate 2 key columns. First key is clustered / pre-grouped. Second key
  // is not. Make one value of the clustered key last for exactly one batch,
  // another value span two bathes.
  auto keys = {
      makeRowVector({
          makeFlatVector<int32_t>({-10, -10, -5, -5, -5}),
          makeFlatVector<int32_t>({0, 1, 2, 1, 4}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({-5, -5, -4, -3, -2}),
          makeFlatVector<int32_t>({0, 1, 2, 1, 4}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({-1, -1, -1, -1, -1}),
          makeFlatVector<int32_t>({0, 1, 2, 1, 4}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({0, 0, 0, 0, 0}),
          makeFlatVector<int32_t>({0, 4, 2, 3, 4}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(size, [](auto row) { return row / 7; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(
              size, [&](auto row) { return (size + row) / 7; }),
          makeFlatVector<int32_t>(
              size, [&](auto row) { return (size + row) % 5; }),
      }),
  };

  testMultiKeyAggregation(keys, {"c0"}, 1024);
}

// Test StreamingAggregation being closed without being initialized. Create a
// pipeline with Project followed by StreamingAggregation. Make
// Project::initialize fail by using non-existent function.
TEST_P(StreamingAggregationTest, closeUninitialized) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
  });
  auto plan = PlanBuilder()
                  .values({data})
                  .addNode([](auto nodeId, auto source) -> core::PlanNodePtr {
                    return std::make_shared<core::ProjectNode>(
                        nodeId,
                        std::vector<std::string>{"c0", "x"},
                        std::vector<core::TypedExprPtr>{
                            std::make_shared<core::FieldAccessTypedExpr>(
                                BIGINT(), "c0"),
                            std::make_shared<core::CallTypedExpr>(
                                BIGINT(),
                                std::vector<core::TypedExprPtr>{},
                                "do-not-exist")},
                        source);
                  })
                  .partialStreamingAggregation({"c0"}, {"sum(x)"})
                  .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      "Scalar function name not registered: do-not-exist");
}

TEST_P(StreamingAggregationTest, sortedAggregations) {
  auto size = 1024;

  std::vector<VectorPtr> keys = {
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [size](auto row) { return (size + row); }),
      makeFlatVector<int32_t>(
          size, [size](auto row) { return (2 * size + row); }),
      makeFlatVector<int32_t>(
          78, [size](auto row) { return (3 * size + row); }),
  };

  testSortedAggregation(keys, 1024);
  testSortedAggregation(keys, 32);
}

TEST_P(StreamingAggregationTest, distinctAggregations) {
  auto size = 1024;

  std::vector<VectorPtr> keys = {
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [size](auto row) { return (size + row); }),
      makeFlatVector<int32_t>(
          size, [size](auto row) { return (2 * size + row); }),
      makeFlatVector<int32_t>(
          78, [size](auto row) { return (3 * size + row); }),
  };

  testDistinctAggregation(keys, 1024);
  testDistinctAggregation(keys, 32);

  std::vector<RowVectorPtr> multiKeys = {
      makeRowVector({
          makeFlatVector<int32_t>({1, 1, 2, 2, 2}),
          makeFlatVector<int64_t>({10, 20, 20, 30, 30}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({2, 3, 3, 3, 4}),
          makeFlatVector<int64_t>({30, 30, 40, 40, 40}),
      }),
      makeRowVector({
          makeNullableFlatVector<int32_t>({5, 5, 6, 6, 6}),
          makeNullableFlatVector<int64_t>({40, 50, 50, 50, 50}),
      }),
  };

  testMultiKeyDistinctAggregation(multiKeys, 1024);
  testMultiKeyDistinctAggregation(multiKeys, 3);
}

TEST_P(StreamingAggregationTest, clusteredInput) {
  std::vector<VectorPtr> keys = {
      makeNullableFlatVector<int32_t>({1, 1, std::nullopt, 2, 2}),
      makeFlatVector<int32_t>({2, 3, 3, 4}),
      makeFlatVector<int32_t>({5, 6, 6, 6}),
      makeFlatVector<int32_t>({6, 6, 6, 6}),
      makeFlatVector<int32_t>({6, 7, 8}),
  };
  auto data = addPayload(keys, 1);
  auto plan = PlanBuilder()
                  .values(data)
                  .partialStreamingAggregation(
                      {"c0"}, {"count(c1)", "arbitrary(c1)", "array_agg(c1)"})
                  .finalAggregation()
                  .planNode();
  auto expected = makeRowVector({
      makeNullableFlatVector<int32_t>({1, std::nullopt, 2, 3, 4, 5, 6, 7, 8}),
      makeFlatVector<int64_t>({2, 1, 3, 2, 1, 1, 8, 1, 1}),
      makeFlatVector<int32_t>({0, 2, 3, 6, 8, 9, 10, 18, 19}),
      makeArrayVector<int32_t>(
          {{0, 1},
           {2},
           {3, 4, 5},
           {6, 7},
           {8},
           {9},
           {10, 11, 12, 13, 14, 15, 16, 17},
           {18},
           {19}}),
  });
  for (auto batchSize : {3, 20}) {
    SCOPED_TRACE(fmt::format("batchSize={}", batchSize));
    config(AssertQueryBuilder(plan), batchSize).assertResults(expected);
  }
}

TEST_P(StreamingAggregationTest, clusteredInputWithOutputSplit) {
  std::vector<VectorPtr> keysWithOverlap = {
      makeNullableFlatVector<int32_t>({1, 1, std::nullopt, 2, 2}),
      makeFlatVector<int32_t>({2, 3, 3, 4}),
      makeFlatVector<int32_t>({5, 6, 6, 6}),
      makeFlatVector<int32_t>({6, 6, 6, 6}),
      makeFlatVector<int32_t>({6, 7, 8}),
  };
  auto dataWithOverlap = addPayload(keysWithOverlap, 1);
  auto planWithOverlap = PlanBuilder()
                             .values(dataWithOverlap)
                             .streamingAggregation(
                                 {"c0"},
                                 {"arbitrary(c1)", "array_agg(c1)"},
                                 {},
                                 core::AggregationNode::Step::kSingle,
                                 false)
                             .planNode();
  const auto expectedWithOverlap = makeRowVector({
      makeNullableFlatVector<int32_t>({1, std::nullopt, 2, 3, 4, 5, 6, 7, 8}),
      makeFlatVector<int32_t>({0, 2, 3, 6, 8, 9, 10, 18, 19}),
      makeArrayVector<int32_t>(
          {{0, 1},
           {2},
           {3, 4, 5},
           {6, 7},
           {8},
           {9},
           {10, 11, 12, 13, 14, 15, 16, 17},
           {18},
           {19}}),
  });
  for (auto batchSize : {1, 3, 20}) {
    SCOPED_TRACE(fmt::format("batchSize={}", batchSize));
    config(AssertQueryBuilder(planWithOverlap), batchSize)
        .assertResults(expectedWithOverlap);
  }

  std::vector<VectorPtr> keysWithoutOverlap = {
      makeNullableFlatVector<int32_t>({1, 1, std::nullopt, 2, 2}),
      makeFlatVector<int32_t>({3, 3, 4, 4}),
      makeFlatVector<int32_t>({5, 6, 6, 7}),
      makeFlatVector<int32_t>({8, 8, 9, 9}),
      makeFlatVector<int32_t>({10, 11, 12}),
  };
  auto dataWithoutOverlap = addPayload(keysWithoutOverlap, 1);
  auto planWithoutOverlap = PlanBuilder()
                                .values(dataWithoutOverlap)
                                .streamingAggregation(
                                    {"c0"},
                                    {"arbitrary(c1)", "array_agg(c1)"},
                                    {},
                                    core::AggregationNode::Step::kSingle,
                                    false)
                                .planNode();
  const auto expectedWithoutOverlap = makeRowVector(
      {makeNullableFlatVector<int32_t>(
           {1, std::nullopt, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
       makeFlatVector<int32_t>({0, 2, 3, 5, 7, 9, 10, 12, 13, 15, 17, 18, 19}),
       makeArrayVector<int32_t>(
           {{0, 1},
            {2},
            {3, 4},
            {5, 6},
            {7, 8},
            {9},
            {10, 11},
            {12},
            {13, 14},
            {15, 16},
            {17},
            {18},
            {19}})});
  for (auto batchSize : {1, 3, 20}) {
    SCOPED_TRACE(fmt::format("batchSize={}", batchSize));
    config(AssertQueryBuilder(planWithoutOverlap), batchSize)
        .assertResults(expectedWithoutOverlap);
  }

  std::vector<VectorPtr> mixedKeys = {
      makeNullableFlatVector<int32_t>({1, 1, std::nullopt, std::nullopt, 2}),
      makeFlatVector<int32_t>({3, 3, 4, 4}),
      makeFlatVector<int32_t>({6, 6, 6, 7}),
      makeFlatVector<int32_t>({7, 8, 9, 9}),
      makeFlatVector<int32_t>({10, 11, 12}),
  };
  auto mixedData = addPayload(mixedKeys, 1);
  auto mixedPlan = PlanBuilder()
                       .values(mixedData)
                       .streamingAggregation(
                           {"c0"},
                           {"arbitrary(c1)", "array_agg(c1)"},
                           {},
                           core::AggregationNode::Step::kSingle,
                           false)
                       .planNode();
  const auto expectedMixedResult = makeRowVector(
      {makeNullableFlatVector<int32_t>(
           {1, std::nullopt, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12}),
       makeFlatVector<int32_t>({0, 2, 4, 5, 7, 9, 12, 14, 15, 17, 18, 19}),
       makeArrayVector<int32_t>(
           {{0, 1},
            {2, 3},
            {4},
            {5, 6},
            {7, 8},
            {9, 10, 11},
            {12, 13},
            {14},
            {15, 16},
            {17},
            {18},
            {19}})});
  for (auto batchSize : {1, 3, 20}) {
    SCOPED_TRACE(fmt::format("batchSize={}", batchSize));
    config(AssertQueryBuilder(mixedPlan), batchSize)
        .assertResults(expectedMixedResult);
  }
}

TEST_P(StreamingAggregationTest, clusteredInputWithNulls) {
  std::vector<VectorPtr> keyVectors = {
      makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2, 3, 3, 3, 3}),
      makeFlatVector<int32_t>({4, 4, 4, 4, 5, 5, 5, 5, 6, 6}),
      makeFlatVector<int32_t>({7, 7, 7, 8}),
      makeFlatVector<int32_t>({8, 8, 8, 9, 9, 9, 10, 10, 10}),
      makeFlatVector<int32_t>({11, 11, 11}),
  };
  std::vector<VectorPtr> dataVectors = {
      makeRowVector(
          {makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2, 3, 3, 3, 3}),
           makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2, 3, 3, 3, 3})},
          [](auto row) { return row < 3; }),
      makeRowVector(
          {makeFlatVector<int32_t>({4, 4, 4, 4, 5, 5, 5, 5, 6, 6}),
           makeFlatVector<int32_t>({4, 4, 4, 4, 5, 5, 5, 5, 6, 6})},
          [](auto row) { return row < 4 || row > 7; }),

      makeRowVector(
          {makeFlatVector<int32_t>({7, 7, 7, 8}),
           makeFlatVector<int32_t>({7, 7, 7, 8})},
          [](auto row) { return row > 2; }),

      makeRowVector(
          {makeFlatVector<int32_t>({8, 8, 8, 9, 9, 9, 10, 10, 10}),
           makeFlatVector<int32_t>({8, 8, 8, 9, 9, 9, 10, 10, 10})},
          [](auto row) { return row < 3; }),

      makeRowVector(
          {makeFlatVector<int32_t>({11, 11, 11}),
           makeFlatVector<int32_t>({11, 11, 11})},
          [](auto /*unused*/) { return true; })};
  ASSERT_EQ(keyVectors.size(), dataVectors.size());
  std::vector<RowVectorPtr> rowVectors;
  for (int i = 0; i < keyVectors.size(); ++i) {
    rowVectors.emplace_back(makeRowVector({keyVectors[i], dataVectors[i]}));
  }

  const auto plan =
      PlanBuilder()
          .values(rowVectors)
          .partialStreamingAggregation({"c0"}, {"count(c1)", "arbitrary(c1)"})
          .finalAggregation()
          .planNode();

  const auto expected = makeRowVector(
      {makeNullableFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}),
       makeFlatVector<int64_t>({0, 3, 4, 0, 4, 0, 3, 0, 3, 3, 0}),
       makeRowVector(
           {makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}),
            makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})},
           [](auto row) {
             if (row == 0 || row == 3 || row == 5 || row == 7 || row == 10) {
               return true;
             }
             return false;
           })});
  for (auto batchSize : {20}) {
    SCOPED_TRACE(fmt::format("batchSize={}", batchSize));
    config(AssertQueryBuilder(plan), batchSize).assertResults(expected);
  }
}

TEST_P(StreamingAggregationTest, sortedAggregationsWithBarrier) {
  const auto size = 1024;
  const std::vector<VectorPtr> keys = {
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [size](auto row) { return (size + row); }),
      makeFlatVector<int32_t>(
          size, [size](auto row) { return (2 * size + row); }),
      makeFlatVector<int32_t>(
          78, [size](auto row) { return (3 * size + row); }),
  };

  testSortedAggregationWithBarrier(keys, 1024);
  testSortedAggregationWithBarrier(keys, 32);
}

TEST_P(StreamingAggregationTest, clusteredInputWithBarrier) {
  const std::vector<VectorPtr> keys = {
      makeNullableFlatVector<int32_t>({1, 2, std::nullopt, 3, 4}),
      makeFlatVector<int32_t>({9, 10, 11, 12}),
      makeFlatVector<int32_t>({17, 18, 19}),
  };
  auto inputVectors = addPayload(keys, 1);
  std::vector<std::shared_ptr<TempFilePath>> tempFiles;
  const int numSplits = keys.size();
  for (int32_t i = 0; i < numSplits; ++i) {
    tempFiles.push_back(TempFilePath::create());
  }
  writeToFiles(toFilePaths(tempFiles), inputVectors);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId streamingAggregationNodeId;
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .startTableScan()
                  .outputType(std::dynamic_pointer_cast<const RowType>(
                      inputVectors[0]->type()))
                  .endTableScan()
                  .partialStreamingAggregation(
                      {"c0"}, {"count(c1)", "arbitrary(c1)", "array_agg(c1)"})
                  .capturePlanNodeId(streamingAggregationNodeId)
                  .finalAggregation()
                  .planNode();
  const auto expected = makeRowVector(
      {makeNullableFlatVector<int32_t>(
           {1, 2, std::nullopt, 3, 4, 9, 10, 11, 12, 17, 18, 19}),
       makeFlatVector<int64_t>({1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
       makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}),
       makeArrayVector<int32_t>(
           {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}})});
  struct {
    int batchSize;
    bool barrierExecution;

    std::string debugString() const {
      return fmt::format(
          "batchSize={}, barrierExecution={}", batchSize, barrierExecution);
    }
  } testSettings[] = {{3, true}, {3, false}, {20, true}, {20, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    int expectedNumOuputBatchesWithBarrier{0};
    int numInputRows{0};
    for (const auto& inputVector : inputVectors) {
      numInputRows += inputVector->size();
      if (testData.batchSize > inputVector->size()) {
        ++expectedNumOuputBatchesWithBarrier;
      } else {
        expectedNumOuputBatchesWithBarrier +=
            bits::divRoundUp(inputVector->size(), testData.batchSize);
      }
    }
    const int expectedNumOuputBatches =
        bits::divRoundUp(numInputRows, testData.batchSize);

    auto task = AssertQueryBuilder(plan, duckDbQueryRunner_)
                    .splits(makeHiveConnectorSplits(tempFiles))
                    .serialExecution(true)
                    .barrierExecution(testData.barrierExecution)
                    .config(
                        core::QueryConfig::kPreferredOutputBatchRows,
                        std::to_string(testData.batchSize))
                    .assertResults(expected);
    const auto taskStats = task->taskStats();
    ASSERT_EQ(taskStats.numBarriers, testData.barrierExecution ? numSplits : 0);
    ASSERT_EQ(taskStats.numFinishedSplits, numSplits);
    ASSERT_EQ(
        velox::exec::toPlanStats(taskStats)
            .at(streamingAggregationNodeId)
            .outputVectors,
        testData.barrierExecution ? expectedNumOuputBatchesWithBarrier
                                  : expectedNumOuputBatches);
  }
}

TEST_P(StreamingAggregationTest, distinctAggregationsWithBarrier) {
  const auto size = 1024;
  const std::vector<VectorPtr> keys = {
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [size](auto row) { return (size + row); }),
      makeFlatVector<int32_t>(
          size, [size](auto row) { return (2 * size + row); }),
      makeFlatVector<int32_t>(
          78, [size](auto row) { return (3 * size + row); }),
  };

  testDistinctAggregationWithBarrier(keys, 1024);
  testDistinctAggregationWithBarrier(keys, 32);

  std::vector<RowVectorPtr> multiKeys = {
      makeRowVector({
          makeFlatVector<int32_t>({1, 1, 2, 2, 2}),
          makeFlatVector<int64_t>({10, 20, 20, 30, 40}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({3, 3, 3, 3, 4}),
          makeFlatVector<int64_t>({30, 40, 50, 60, 40}),
      }),
      makeRowVector({
          makeNullableFlatVector<int32_t>({5, 5, 6, 6, 6}),
          makeNullableFlatVector<int64_t>({40, 50, 60, 70, 80}),
      }),
  };
  testMultiKeyDistinctAggregationWithBarrier(multiKeys, 1024);
  testMultiKeyDistinctAggregationWithBarrier(multiKeys, 3);
}
} // namespace
} // namespace facebook::velox::exec
