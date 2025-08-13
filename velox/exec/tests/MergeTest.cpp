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

#include "velox/exec/Merge.h"
#include "folly/experimental/EventCount.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::exec::test;

class MergeTest : public OperatorTestBase {
 public:
  MergeTest() {
    filesystems::registerLocalFileSystem();
  }

 protected:
  void testSingleKey(
      const std::vector<RowVectorPtr>& inputVectors,
      const std::string& key) {
    auto keyIndex = inputVectors[0]->type()->asRow().getChildIdx(key);

    std::vector<std::string> sortOrderSqls = {
        "NULLS LAST", "NULLS FIRST", "DESC NULLS FIRST", "DESC NULLS LAST"};
    for (const auto& sortOrderSql : sortOrderSqls) {
      const auto orderByClause = fmt::format("{} {}", key, sortOrderSql);
      auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
      auto plan = PlanBuilder(planNodeIdGenerator)
                      .localMerge(
                          {orderByClause},
                          {PlanBuilder(planNodeIdGenerator)
                               .values(inputVectors)
                               .orderBy({orderByClause}, true)
                               .planNode()})
                      .planNode();
      // Use single source for local merge.
      CursorParameters params;
      params.planNode = plan;
      params.maxDrivers = 1;
      assertQueryOrdered(
          params,
          "SELECT * FROM (SELECT * FROM tmp) ORDER BY " + orderByClause,
          {keyIndex});

      // Use multiple sources for local merge.
      std::vector<std::shared_ptr<const core::PlanNode>> sources;
      for (const auto& input : inputVectors) {
        sources.push_back(PlanBuilder(planNodeIdGenerator)
                              .values({input})
                              .orderBy({orderByClause}, true)
                              .planNode());
      }
      plan = PlanBuilder(planNodeIdGenerator)
                 .localMerge({orderByClause}, std::move(sources))
                 .planNode();

      assertQueryOrdered(
          plan, "SELECT * FROM tmp ORDER BY " + orderByClause, {keyIndex});
    }
  }

  void testTwoKeys(
      const std::vector<RowVectorPtr>& inputVectors,
      const std::string& key1,
      const std::string& key2) {
    auto& rowType = inputVectors[0]->type()->asRow();
    auto sortingKeys = {rowType.getChildIdx(key1), rowType.getChildIdx(key2)};

    std::vector<core::SortOrder> sortOrders = {
        core::kAscNullsLast,
        core::kAscNullsFirst,
        core::kDescNullsFirst,
        core::kDescNullsLast};
    std::vector<std::string> sortOrderSqls = {
        "NULLS LAST", "NULLS FIRST", "DESC NULLS FIRST", "DESC NULLS LAST"};

    for (auto i = 0; i < sortOrders.size(); ++i) {
      for (auto j = 0; j < sortOrders.size(); ++j) {
        const std::vector<std::string> orderByClauses = {
            fmt::format("{} {}", key1, sortOrderSqls[i]),
            fmt::format("{} {}", key2, sortOrderSqls[j])};
        const auto orderBySql = fmt::format(
            "ORDER BY {}, {}", orderByClauses[0], orderByClauses[1]);
        auto planNodeIdGenerator =
            std::make_shared<core::PlanNodeIdGenerator>();
        auto plan = PlanBuilder(planNodeIdGenerator)
                        .localMerge(
                            orderByClauses,
                            {PlanBuilder(planNodeIdGenerator)
                                 .values(inputVectors)
                                 .orderBy(orderByClauses, true)
                                 .planNode()})
                        .planNode();
        // Use single source for local merge.
        CursorParameters params;
        params.planNode = plan;
        params.maxDrivers = 1;
        assertQueryOrdered(
            params,
            "SELECT * FROM (SELECT * FROM tmp) " + orderBySql,
            sortingKeys);

        // Use multiple sources for local merge.
        std::vector<std::shared_ptr<const core::PlanNode>> sources;
        for (const auto& input : inputVectors) {
          sources.push_back(PlanBuilder(planNodeIdGenerator)
                                .values({input})
                                .orderBy(orderByClauses, true)
                                .planNode());
        }
        plan = PlanBuilder(planNodeIdGenerator)
                   .localMerge(orderByClauses, std::move(sources))
                   .planNode();

        assertQueryOrdered(
            plan, "SELECT * FROM tmp " + orderBySql, sortingKeys);
      }
    }
  }

  void testSingleKeyWithSpill(
      const std::vector<RowVectorPtr>& inputVectors,
      const std::string& key) {
    auto keyIndex = inputVectors[0]->type()->asRow().getChildIdx(key);

    std::vector<std::string> sortOrderSqls = {
        "NULLS LAST", "NULLS FIRST", "DESC NULLS FIRST", "DESC NULLS LAST"};

    for (const auto& sortOrderSql : sortOrderSqls) {
      const auto orderByClause = fmt::format("{} {}", key, sortOrderSql);
      const auto planNodeIdGenerator =
          std::make_shared<core::PlanNodeIdGenerator>();
      const std::shared_ptr<TempDirectoryPath> spillDirectory =
          TempDirectoryPath::create();
      std::vector<std::shared_ptr<const core::PlanNode>> sources;
      for (const auto& input : inputVectors) {
        sources.push_back(PlanBuilder(planNodeIdGenerator)
                              .values({input})
                              .orderBy({orderByClause}, true)
                              .planNode());
      }
      core::PlanNodeId nodeId;
      const auto plan = PlanBuilder(planNodeIdGenerator)
                            .localMerge({orderByClause}, std::move(sources))
                            .capturePlanNodeId(nodeId)
                            .planNode();
      CursorParameters params;
      params.planNode = plan;
      params.maxDrivers = 2;
      params.queryCtx = createQueryCtx(
          {{"spill_enabled", "true"},
           {"local_merge_spill_enabled", "true"},
           {"local_merge_max_num_merge_sources", "2"}});
      params.spillDirectory = spillDirectory->getPath();
      auto task = assertQueryOrdered(
          params, "SELECT * FROM tmp ORDER BY " + orderByClause, {keyIndex});
      auto taskStats = toPlanStats(task->taskStats());
      auto& planStats = taskStats.at(nodeId);
      auto expectedNumSpillFiles = (inputVectors.size() + 2 - 1) / 2;
      ASSERT_GT(planStats.spilledBytes, 0);
      ASSERT_EQ(planStats.spilledPartitions, expectedNumSpillFiles);
      ASSERT_EQ(planStats.spilledFiles, expectedNumSpillFiles);
      ASSERT_EQ(
          planStats.spilledRows, inputVectors.size() * inputVectors[0]->size());
    }
  }

  void testTwoKeysWithSpill(
      const std::vector<RowVectorPtr>& inputVectors,
      const std::string& key1,
      const std::string& key2) {
    auto& rowType = inputVectors[0]->type()->asRow();
    auto sortingKeys = {rowType.getChildIdx(key1), rowType.getChildIdx(key2)};

    std::vector<core::SortOrder> sortOrders = {
        core::kAscNullsLast,
        core::kAscNullsFirst,
        core::kDescNullsFirst,
        core::kDescNullsLast};
    std::vector<std::string> sortOrderSqls = {
        "NULLS LAST", "NULLS FIRST", "DESC NULLS FIRST", "DESC NULLS LAST"};

    for (auto i = 0; i < sortOrders.size(); ++i) {
      for (auto j = 0; j < sortOrders.size(); ++j) {
        const std::vector<std::string> orderByClauses = {
            fmt::format("{} {}", key1, sortOrderSqls[i]),
            fmt::format("{} {}", key2, sortOrderSqls[j])};
        const auto orderBySql = fmt::format(
            "ORDER BY {}, {}", orderByClauses[0], orderByClauses[1]);
        const auto planNodeIdGenerator =
            std::make_shared<core::PlanNodeIdGenerator>();
        const std::shared_ptr<TempDirectoryPath> spillDirectory =
            TempDirectoryPath::create();
        std::vector<std::shared_ptr<const core::PlanNode>> sources;
        for (const auto& input : inputVectors) {
          sources.push_back(PlanBuilder(planNodeIdGenerator)
                                .values({input})
                                .orderBy(orderByClauses, true)
                                .planNode());
        }
        core::PlanNodeId nodeId;
        const auto plan = PlanBuilder(planNodeIdGenerator)
                              .localMerge({orderByClauses}, std::move(sources))
                              .capturePlanNodeId(nodeId)
                              .planNode();
        CursorParameters params;
        params.planNode = plan;
        params.maxDrivers = 2;
        params.queryCtx = createQueryCtx(
            {{"spill_enabled", "true"},
             {"local_merge_spill_enabled", "true"},
             {"local_merge_max_num_merge_sources", "2"}});
        params.spillDirectory = spillDirectory->getPath();
        auto task = assertQueryOrdered(
            params, "SELECT * FROM tmp " + orderBySql, sortingKeys);
        auto taskStats = toPlanStats(task->taskStats());
        auto& planStats = taskStats.at(nodeId);
        auto expectedNumSpillFiles = (inputVectors.size() + 2 - 1) / 2;
        ASSERT_GT(planStats.spilledBytes, 0);
        ASSERT_EQ(planStats.spilledPartitions, expectedNumSpillFiles);
        ASSERT_EQ(planStats.spilledFiles, expectedNumSpillFiles);
        ASSERT_EQ(
            planStats.spilledRows,
            inputVectors.size() * inputVectors[0]->size());
      }
    }
  }

  void testLocalMerge(
      int numInputSources,
      int numInputBatches,
      int inputBatchSize,
      int maxBatchRows,
      int maxBatchBytes,
      int expectedOuputBatches) {
    std::vector<std::vector<RowVectorPtr>> inputVectors;
    for (int32_t i = 0; i < numInputSources; ++i) {
      std::vector<RowVectorPtr> vectors;
      for (int32_t j = 0; j < numInputBatches; ++j) {
        auto c0 = makeFlatVector<int64_t>(
            inputBatchSize,
            [&](auto row) { return inputBatchSize * j + row; },
            nullEvery(5));
        auto c1 = makeFlatVector<int64_t>(
            inputBatchSize, [&](auto row) { return row; }, nullEvery(5));
        auto c2 = makeFlatVector<double>(
            inputBatchSize, [](auto row) { return row * 0.1; }, nullEvery(11));
        auto c3 = makeFlatVector<StringView>(inputBatchSize, [](auto row) {
          return StringView::makeInline(std::to_string(row));
        });
        vectors.push_back(makeRowVector({c0, c1, c2, c3}));
      }
      inputVectors.push_back(std::move(vectors));
    }
    std::vector<RowVectorPtr> duckInputs;
    for (const auto& input : inputVectors) {
      for (const auto& vector : input) {
        duckInputs.push_back(vector);
      }
    }
    createDuckDbTable(duckInputs);
    auto keyIndex = inputVectors[0][0]->type()->asRow().getChildIdx("c0");

    const auto orderByClause = fmt::format("{}", "c0");
    const auto planNodeIdGenerator =
        std::make_shared<core::PlanNodeIdGenerator>();
    const std::shared_ptr<TempDirectoryPath> spillDirectory =
        TempDirectoryPath::create();
    std::vector<std::shared_ptr<const core::PlanNode>> sources;
    sources.reserve(inputVectors.size());
    for (const auto& vectors : inputVectors) {
      sources.push_back(PlanBuilder(planNodeIdGenerator)
                            .values(vectors)
                            .orderBy({orderByClause}, true)
                            .planNode());
    }
    core::PlanNodeId localMergeNodeId;
    const auto plan = PlanBuilder(planNodeIdGenerator)
                          .localMerge({orderByClause}, std::move(sources))
                          .capturePlanNodeId(localMergeNodeId)
                          .planNode();
    CursorParameters params;
    params.planNode = plan;
    params.maxDrivers = numInputSources;
    params.queryCtx = createQueryCtx(
        {{"spill_enabled", "false"},
         {"local_merge_spill_enabled", "false"},
         {"preferred_output_batch_bytes", std::to_string(maxBatchBytes)},
         {"preferred_output_batch_rows", std::to_string(maxBatchRows)}},
        false);
    auto task = assertQueryOrdered(
        params, "SELECT * FROM tmp ORDER BY " + orderByClause, {keyIndex});

    auto taskStats = toPlanStats(task->taskStats());
    const auto& mergeStats = taskStats.at(localMergeNodeId);
    ASSERT_EQ(mergeStats.spilledBytes, 0);
    ASSERT_EQ(mergeStats.spilledPartitions, 0);
    ASSERT_EQ(mergeStats.spilledFiles, 0);
    ASSERT_EQ(mergeStats.spilledRows, 0);
    ASSERT_EQ(
        mergeStats.outputRows,
        numInputSources * numInputBatches * inputBatchSize);
    ASSERT_EQ(mergeStats.outputVectors, expectedOuputBatches);
  }

  void testLocalMergeSpill(
      int numInputSources,
      int numInputBatches,
      int inputBatchSize,
      int maxBatchRows,
      int maxBatchBytes,
      int numMaxMergeSources,
      bool hasSpillExecutor,
      int expectedOuputBatches) {
    std::vector<std::vector<RowVectorPtr>> inputVectors;
    for (int32_t i = 0; i < numInputSources; ++i) {
      std::vector<RowVectorPtr> vectors;
      for (int32_t j = 0; j < numInputBatches; ++j) {
        auto c0 = makeFlatVector<int64_t>(
            inputBatchSize,
            [&](auto row) { return inputBatchSize * j + row; },
            nullEvery(5));
        auto c1 = makeFlatVector<int64_t>(
            inputBatchSize, [&](auto row) { return row; }, nullEvery(5));
        auto c2 = makeFlatVector<double>(
            inputBatchSize, [](auto row) { return row * 0.1; }, nullEvery(11));
        auto c3 = makeFlatVector<StringView>(inputBatchSize, [](auto row) {
          return StringView::makeInline(std::to_string(row));
        });
        vectors.push_back(makeRowVector({c0, c1, c2, c3}));
      }
      inputVectors.push_back(std::move(vectors));
    }
    std::vector<RowVectorPtr> duckInputs;
    for (const auto& input : inputVectors) {
      for (const auto& vector : input) {
        duckInputs.push_back(vector);
      }
    }
    createDuckDbTable(duckInputs);
    auto keyIndex = inputVectors[0][0]->type()->asRow().getChildIdx("c0");

    const auto orderByClause = fmt::format("{}", "c0");
    const auto planNodeIdGenerator =
        std::make_shared<core::PlanNodeIdGenerator>();
    const std::shared_ptr<TempDirectoryPath> spillDirectory =
        TempDirectoryPath::create();
    std::vector<std::shared_ptr<const core::PlanNode>> sources;
    sources.reserve(inputVectors.size());
    for (const auto& vectors : inputVectors) {
      sources.push_back(PlanBuilder(planNodeIdGenerator)
                            .values(vectors)
                            .orderBy({orderByClause}, true)
                            .planNode());
    }
    core::PlanNodeId nodeId;
    const auto plan = PlanBuilder(planNodeIdGenerator)
                          .localMerge({orderByClause}, std::move(sources))
                          .capturePlanNodeId(nodeId)
                          .planNode();
    CursorParameters params;
    params.planNode = plan;
    params.maxDrivers = 2;
    params.queryCtx = createQueryCtx(
        {{"spill_enabled", "true"},
         {"local_merge_spill_enabled", "true"},
         {"local_merge_max_num_merge_sources",
          std::to_string(numMaxMergeSources)},
         {"preferred_output_batch_bytes", std::to_string(maxBatchBytes)},
         {"preferred_output_batch_rows", std::to_string(maxBatchRows)}},
        hasSpillExecutor);
    params.spillDirectory = spillDirectory->getPath();
    auto task = assertQueryOrdered(
        params, "SELECT * FROM tmp ORDER BY " + orderByClause, {keyIndex});

    auto taskStats = toPlanStats(task->taskStats());
    auto& planStats = taskStats.at(nodeId);
    if (inputBatchSize == 0 || numMaxMergeSources >= numInputSources ||
        !hasSpillExecutor) {
      ASSERT_EQ(planStats.spilledBytes, 0);
      ASSERT_EQ(planStats.spilledPartitions, 0);
      ASSERT_EQ(planStats.spilledFiles, 0);
      ASSERT_EQ(planStats.spilledRows, 0);
      ASSERT_EQ(
          planStats.customStats.count(Merge::kSpilledSourceReadWallNanos), 0);
      ASSERT_GE(
          planStats.customStats.count(Merge::kStreamingSourceReadWallNanos), 0);
    } else {
      const auto expectedFiles =
          (inputVectors.size() + numMaxMergeSources - 1) / numMaxMergeSources;
      const auto expectedSpillRows = inputBatchSize * duckInputs.size();
      ASSERT_GT(planStats.spilledBytes, 0);
      ASSERT_EQ(planStats.spilledPartitions, expectedFiles);
      ASSERT_EQ(planStats.spilledFiles, expectedFiles);
      ASSERT_EQ(planStats.spilledRows, expectedSpillRows);
      ASSERT_GE(
          planStats.customStats.count(Merge::kSpilledSourceReadWallNanos), 0);
      ASSERT_GE(
          planStats.customStats.count(Merge::kStreamingSourceReadWallNanos), 0);
    }
    ASSERT_EQ(
        planStats.outputRows,
        numInputSources * numInputBatches * inputBatchSize);
    ASSERT_EQ(planStats.outputVectors, expectedOuputBatches);
  }

  std::shared_ptr<core::QueryCtx> createQueryCtx(
      std::unordered_map<std::string, std::string> queryConfigs = {},
      bool hasSpillExecutor = true) const {
    return core::QueryCtx::create(
        executor_.get(),
        core::QueryConfig{std::move(queryConfigs)},
        {},
        nullptr,
        nullptr,
        hasSpillExecutor ? spillExecutor_.get() : nullptr);
  }
};

TEST_F(MergeTest, localMergeSpillBasic) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 9; ++i) {
    constexpr vector_size_t batchSize = 137;
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [&](auto row) { return batchSize * i + row; }, nullEvery(5));
    auto c1 = makeFlatVector<int64_t>(
        batchSize, [&](auto row) { return row; }, nullEvery(5));
    auto c2 = makeFlatVector<double>(
        batchSize, [](auto row) { return row * 0.1; }, nullEvery(11));
    auto c3 = makeFlatVector<StringView>(batchSize, [](auto row) {
      return StringView::makeInline(std::to_string(row));
    });
    vectors.push_back(makeRowVector({c0, c1, c2, c3}));
  }
  createDuckDbTable(vectors);

  testSingleKeyWithSpill(vectors, "c0");
  testSingleKeyWithSpill(vectors, "c3");

  testTwoKeysWithSpill(vectors, "c0", "c3");
  testTwoKeysWithSpill(vectors, "c3", "c0");
}

TEST_F(MergeTest, localMergeSpill) {
  struct TestParam {
    int numInputSources;
    int numInputBatches;
    int inputBatchSize;
    int maxNumMergeSources;
    int maxOutputBatchRows;
    int maxOutputBatchBytes;
    int numExpectedOutputBatches;
    bool hasSpillExecutor;

    std::string debugString() const {
      return fmt::format(
          "numInputSources {}, numInputBatches {}, inputBatchSize {}, maxNumMergeSources {}, maxOutputBatchRows {}, maxOutputBatchBytes {}, numExpectedOutputBatches {}, hasSpillExecutor {}",
          numInputSources,
          numInputBatches,
          inputBatchSize,
          maxNumMergeSources,
          maxOutputBatchRows,
          maxOutputBatchBytes,
          numExpectedOutputBatches,
          hasSpillExecutor);
    }
  } testSettings[]{
      {1, 1, 1, 1, 1, std::numeric_limits<int>::max(), 1, true},
      {1, 1, 1, 1, 1, std::numeric_limits<int>::max(), 1, false},
      {1, 4, 1, 1, 1, std::numeric_limits<int>::max(), 4, true},
      {1, 4, 1, 1, 1, std::numeric_limits<int>::max(), 4, false},
      {1, 4, 32, 1, 1, std::numeric_limits<int>::max(), 4 * 32, true},
      {1, 4, 32, 1, 1, std::numeric_limits<int>::max(), 4 * 32, false},
      {3, 4, 32, 1, 1, std::numeric_limits<int>::max(), 3 * 4 * 32, true},
      {3, 4, 32, 1, 1, std::numeric_limits<int>::max(), 3 * 4 * 32, false},
      {3, 4, 32, 2, 1, std::numeric_limits<int>::max(), 3 * 4 * 32, true},
      {3, 4, 32, 2, 1, std::numeric_limits<int>::max(), 3 * 4 * 32, false},
      {3, 4, 32, 3, 1, std::numeric_limits<int>::max(), 3 * 4 * 32, true},
      {3, 4, 32, 3, 1, std::numeric_limits<int>::max(), 3 * 4 * 32, false},
      {3, 4, 32, 4, 1, std::numeric_limits<int>::max(), 3 * 4 * 32, true},
      {3, 4, 32, 4, 1, std::numeric_limits<int>::max(), 3 * 4 * 32, false},
      {1, 1, 1, 1, 1024, std::numeric_limits<int>::max(), 1, true},
      {1, 1, 1, 1, 1024, std::numeric_limits<int>::max(), 1, false},
      {1, 4, 1, 1, 1024, std::numeric_limits<int>::max(), 1, true},
      {1, 4, 1, 1, 1024, std::numeric_limits<int>::max(), 1, false},
      {1, 4, 32, 1, 1024, std::numeric_limits<int>::max(), 1, true},
      {1, 4, 32, 1, 1024, std::numeric_limits<int>::max(), 1, false},
      {3, 4, 32, 1, 1024, std::numeric_limits<int>::max(), 1, true},
      {3, 4, 32, 1, 1024, std::numeric_limits<int>::max(), 1, false},
      {3, 4, 32, 2, 1024, std::numeric_limits<int>::max(), 1, true},
      {3, 4, 32, 2, 1024, std::numeric_limits<int>::max(), 1, false},
      {3, 4, 32, 3, 1024, std::numeric_limits<int>::max(), 1, true},
      {3, 4, 32, 3, 1024, std::numeric_limits<int>::max(), 1, false},
      {3, 4, 32, 4, 1024, std::numeric_limits<int>::max(), 1, true},
      {3, 4, 32, 4, 1024, std::numeric_limits<int>::max(), 1, false},
      {1, 1, 1, 1, 1024, 1, 1, true},
      {1, 1, 1, 1, 1024, 1, 1, false},
      {1, 4, 1, 1, 1024, 1, 4, true},
      {1, 4, 1, 1, 1024, 1, 4, false},
      {1, 4, 32, 1, 1024, 1, 4 * 32, true},
      {1, 4, 32, 1, 1024, 1, 4 * 32, false},
      {3, 4, 32, 1, 1024, 1, 3 * 4 * 32, true},
      {3, 4, 32, 1, 1024, 1, 3 * 4 * 32, false},
      {3, 4, 32, 2, 1024, 1, 3 * 4 * 32, true},
      {3, 4, 32, 2, 1024, 1, 3 * 4 * 32, false},
      {3, 4, 32, 3, 1024, 1, 3 * 4 * 32, true},
      {3, 4, 32, 3, 1024, 1, 3 * 4 * 32, false},
      {3, 4, 32, 4, 1024, 1, 3 * 4 * 32, true},
      {3, 4, 32, 4, 1024, 1, 3 * 4 * 32, false}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    testLocalMergeSpill(
        testData.numInputSources,
        testData.numInputBatches,
        testData.inputBatchSize,
        testData.maxOutputBatchRows,
        testData.maxOutputBatchBytes,
        testData.maxNumMergeSources,
        testData.hasSpillExecutor,
        testData.numExpectedOutputBatches);
  }
}

TEST_F(MergeTest, localMergeSpillPartialEmpty) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 9; ++i) {
    auto batchSize = 30;
    if (i % 2 == 0) {
      batchSize = 0;
    }

    auto c0 = makeFlatVector<int64_t>(
        batchSize, [&](auto row) { return batchSize * i + row; }, nullEvery(5));
    auto c1 = makeFlatVector<int64_t>(
        batchSize, [&](auto row) { return row; }, nullEvery(5));
    auto c2 = makeFlatVector<double>(
        batchSize, [](auto row) { return row * 0.1; }, nullEvery(11));
    auto c3 = makeFlatVector<StringView>(batchSize, [](auto row) {
      return StringView::makeInline(std::to_string(row));
    });
    vectors.push_back(makeRowVector({c0, c1, c2, c3}));
  }
  createDuckDbTable(vectors);
  auto keyIndex = vectors[0]->type()->asRow().getChildIdx("c0");

  const auto orderByClause = fmt::format("{}", "c0");
  const auto planNodeIdGenerator =
      std::make_shared<core::PlanNodeIdGenerator>();
  const std::shared_ptr<TempDirectoryPath> spillDirectory =
      TempDirectoryPath::create();
  std::vector<std::shared_ptr<const core::PlanNode>> sources;
  sources.reserve(vectors.size());
  for (const auto& vector : vectors) {
    sources.push_back(PlanBuilder(planNodeIdGenerator)
                          .values({vector})
                          .orderBy({orderByClause}, true)
                          .planNode());
  }
  core::PlanNodeId nodeId;
  const auto plan = PlanBuilder(planNodeIdGenerator)
                        .localMerge({orderByClause}, std::move(sources))
                        .capturePlanNodeId(nodeId)
                        .planNode();
  CursorParameters params;
  params.planNode = plan;
  params.maxDrivers = 2;
  params.queryCtx = createQueryCtx(
      {{"spill_enabled", "true"},
       {"local_merge_spill_enabled", "true"},
       {"local_merge_max_num_merge_sources", "2"}});
  params.spillDirectory = spillDirectory->getPath();
  auto task = assertQueryOrdered(
      params, "SELECT * FROM tmp ORDER BY " + orderByClause, {keyIndex});

  auto taskStats = toPlanStats(task->taskStats());
  auto& planStats = taskStats.at(nodeId);
  ASSERT_GT(planStats.spilledBytes, 0);
  ASSERT_EQ(planStats.spilledPartitions, 4);
  ASSERT_EQ(planStats.spilledFiles, 4);
  ASSERT_EQ(planStats.spilledRows, 120);
}

DEBUG_ONLY_TEST_F(MergeTest, localMergeSmallBatch) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 9; ++i) {
    auto batchSize = 30;
    if (i != 0) {
      batchSize = 0;
    }

    auto c0 = makeFlatVector<int64_t>(
        batchSize, [&](auto row) { return batchSize * i + row; }, nullEvery(5));
    auto c1 = makeFlatVector<int64_t>(
        batchSize, [&](auto row) { return row; }, nullEvery(5));
    auto c2 = makeFlatVector<double>(
        batchSize, [](auto row) { return row * 0.1; }, nullEvery(11));
    auto c3 = makeFlatVector<StringView>(batchSize, [](auto row) {
      return StringView::makeInline(std::to_string(row));
    });
    vectors.push_back(makeRowVector({c0, c1, c2, c3}));
  }
  createDuckDbTable(vectors);
  auto keyIndex = vectors[0]->type()->asRow().getChildIdx("c0");

  const auto orderByClause = fmt::format("{}", "c0");
  const auto planNodeIdGenerator =
      std::make_shared<core::PlanNodeIdGenerator>();
  const std::shared_ptr<TempDirectoryPath> spillDirectory =
      TempDirectoryPath::create();
  std::vector<std::shared_ptr<const core::PlanNode>> sources;
  sources.reserve(vectors.size());
  for (const auto& vector : vectors) {
    sources.push_back(PlanBuilder(planNodeIdGenerator)
                          .values({vector})
                          .orderBy({orderByClause}, true)
                          .planNode());
  }
  core::PlanNodeId nodeId;
  const auto plan = PlanBuilder(planNodeIdGenerator)
                        .localMerge({orderByClause}, std::move(sources))
                        .capturePlanNodeId(nodeId)
                        .planNode();

  std::atomic_bool blockFlag{true};
  folly::Promise<folly::Unit> promise;
  folly::EventCount callWait;
  std::atomic_bool callWaitFlag{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::SourceMerger::getOutput",
      std::function<void(std::vector<ContinueFuture>*)>(
          [&](std::vector<ContinueFuture>* sourceBlockingFutures) {
            if (blockFlag) {
              blockFlag = false;
              auto [p, f] = folly::makePromiseContract<folly::Unit>();
              sourceBlockingFutures->push_back(std::move(f));
              promise = std::move(p);
              callWaitFlag = false;
              callWait.notifyAll();
            }
          }));

  std::thread promiseThread([&]() {
    callWait.await([&]() { return !callWaitFlag.load(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(1'000)); // NOLINT
    promise.setValue();
  });

  CursorParameters params;
  params.planNode = plan;
  auto task = assertQueryOrdered(
      params, "SELECT * FROM tmp ORDER BY " + orderByClause, {keyIndex});
  auto taskStats = toPlanStats(task->taskStats());
  ASSERT_EQ(taskStats[nodeId].outputRows, 30);
  ASSERT_EQ(taskStats[nodeId].outputVectors, 1);
  promiseThread.join();
}

DEBUG_ONLY_TEST_F(MergeTest, localMergeAbort) {
  std::vector<std::vector<RowVectorPtr>> inputVectors;
  for (int32_t i = 0; i < 4; ++i) {
    std::vector<RowVectorPtr> vectors;
    for (int32_t j = 0; j < 13; ++j) {
      constexpr auto batchSize = 5000;
      auto c0 = makeFlatVector<int64_t>(
          batchSize,
          [&](auto row) { return batchSize * j + row; },
          nullEvery(5));
      auto c1 = makeFlatVector<int64_t>(
          batchSize, [&](auto row) { return row; }, nullEvery(5));
      auto c2 = makeFlatVector<double>(
          batchSize, [](auto row) { return row * 0.1; }, nullEvery(11));
      auto c3 = makeFlatVector<StringView>(batchSize, [](auto row) {
        return StringView::makeInline(std::to_string(row));
      });
      vectors.push_back(makeRowVector({c0, c1, c2, c3}));
    }
    inputVectors.push_back(std::move(vectors));
  }

  const auto orderByClause = fmt::format("{}", "c0");
  const auto planNodeIdGenerator =
      std::make_shared<core::PlanNodeIdGenerator>();
  const std::shared_ptr<TempDirectoryPath> spillDirectory =
      TempDirectoryPath::create();
  std::vector<std::shared_ptr<const core::PlanNode>> sources;
  sources.reserve(inputVectors.size());
  for (const auto& vectors : inputVectors) {
    sources.push_back(PlanBuilder(planNodeIdGenerator)
                          .values(vectors)
                          .orderBy({orderByClause}, true)
                          .planNode());
  }
  core::PlanNodeId nodeId;
  const auto plan = PlanBuilder(planNodeIdGenerator)
                        .localMerge({orderByClause}, std::move(sources))
                        .capturePlanNodeId(nodeId)
                        .planNode();
  std::atomic_int cnt{0};
  std::atomic_bool blocked{false};
  folly::EventCount callWait;
  std::atomic_bool callWaitFlag{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::SpillMerger::getOutput",
      std::function<void(std::vector<ContinueFuture>*)>(
          [&](std::vector<ContinueFuture>* /*unused*/) {
            if (blocked) {
              std::this_thread::sleep_for(
                  std::chrono::milliseconds(1'000)); // NOLINT
              blocked = false;
              callWaitFlag = false;
              callWait.notifyAll();
              VELOX_USER_FAIL("Abort merge");
            }
          }));

  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::SpillMerger::asyncReadFromSpillFileStream",
      std::function<void(void*)>([&](void* /*unused*/) {
        if (cnt++ == 2) {
          blocked = true;
          callWait.await([&]() { return callWaitFlag.load(); });
          std::this_thread::sleep_for(
              std::chrono::milliseconds(1'000)); // NOLINT
        }
      }));

  auto queryCtx = createQueryCtx();
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .spillDirectory(spillDirectory->getPath())
          .queryCtx(queryCtx)
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kLocalMergeSpillEnabled, true)
          .config(core::QueryConfig::kLocalMergeMaxNumMergeSources, 2)
          .config(core::QueryConfig::kMaxOutputBatchRows, 10)
          .config(core::QueryConfig::kPreferredOutputBatchRows, 10)
          .copyResults(pool()),
      "Abort merge");
  std::dynamic_pointer_cast<folly::CPUThreadPoolExecutor>(spillExecutor_)
      ->join();
}

TEST_F(MergeTest, localMerge) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    static constexpr vector_size_t kBatchSize = 100;
    auto c0 = makeFlatVector<int64_t>(
        kBatchSize,
        [&](auto row) { return kBatchSize * i + row; },
        nullEvery(5));
    auto c1 = makeFlatVector<int64_t>(
        kBatchSize, [&](auto row) { return row; }, nullEvery(5));
    auto c2 = makeFlatVector<double>(
        kBatchSize, [](auto row) { return row * 0.1; }, nullEvery(11));
    auto c3 = makeFlatVector<StringView>(kBatchSize, [](auto row) {
      return StringView::makeInline(std::to_string(row));
    });
    vectors.push_back(makeRowVector({c0, c1, c2, c3}));
  }
  createDuckDbTable(vectors);

  testSingleKey(vectors, "c0");
  testSingleKey(vectors, "c3");

  testTwoKeys(vectors, "c0", "c3");
  testTwoKeys(vectors, "c3", "c0");
}

DEBUG_ONLY_TEST_F(MergeTest, localMergeStart) {
  const auto data1 = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 10}),
  });
  const auto data2 = makeRowVector({
      makeFlatVector<int64_t>({2, 3, 4, 5}),
  });

  folly::EventCount sourceStartCallWait;
  std::atomic_bool sourceStartCallWaitFlag{true};
  folly::EventCount sourceStartWait;
  std::atomic_bool sourceStartWaitFlag{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::LocalMergeSource::start",
      std::function<void(MergeSource*)>(([&](MergeSource* /*unused*/) {
        sourceStartCallWaitFlag = false;
        sourceStartCallWait.notifyAll();
        sourceStartWait.await([&]() { return !sourceStartWaitFlag.load(); });
      })));
  std::atomic_bool getOutput{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(MergeSource*)>(
          ([&](MergeSource* /*unused*/) { getOutput = true; })));
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .localMerge(
              {"c0"},
              {
                  PlanBuilder(planNodeIdGenerator).values({data1}).planNode(),
                  PlanBuilder(planNodeIdGenerator).values({data2}).planNode(),
              })
          .planNode();

  std::thread queryThread([&]() {
    CursorParameters params;
    params.planNode = plan;
    params.queryCtx = core::QueryCtx::create(executor_.get());
    assertQueryOrdered(
        params, "VALUES (0), (1), (2), (3), (4), (5), (10)", {0});
  });
  sourceStartCallWait.await([&]() { return !sourceStartCallWaitFlag.load(); });
  std::this_thread::sleep_for(std::chrono::milliseconds(1'000)); // NOLINT
  ASSERT_FALSE(getOutput);
  sourceStartWaitFlag = false;
  sourceStartWait.notifyAll();
  queryThread.join();
  ASSERT_TRUE(getOutput);
}

/// Verifies an edge case where output batch fills up when one of the sources
/// has only one row left.
TEST_F(MergeTest, offByOne) {
  auto data1 = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 10}),
  });

  auto data2 = makeRowVector({
      makeFlatVector<int64_t>({2, 3, 4, 5}),
  });

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .localMerge(
              {"c0"},
              {
                  PlanBuilder(planNodeIdGenerator).values({data1}).planNode(),
                  PlanBuilder(planNodeIdGenerator).values({data2}).planNode(),
              })
          .planNode();

  CursorParameters params;
  params.planNode = plan;
  params.queryCtx = core::QueryCtx::create(executor_.get());
  params.queryCtx->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kPreferredOutputBatchRows, "6"}});
  assertQueryOrdered(params, "VALUES (0), (1), (2), (3), (4), (5), (10)", {0});
}

TEST_F(MergeTest, localMergeOutputSizeWithoutSpill) {
  struct TestParam {
    int numSources;
    int numInputBatches;
    int inputBatchSize;
    int maxOutputBatchRows;
    int maxOutputBatchBytes;
    int numExpectedOutputBatches;

    std::string debugString() const {
      return fmt::format(
          "numSources {}, numInputBatches {}, inputBatchSize {}, maxOutputBatchRows {}, maxOutputBatchBytes {}, numExpectedOutputBatches {}",
          numSources,
          numInputBatches,
          inputBatchSize,
          maxOutputBatchRows,
          maxOutputBatchBytes,
          numExpectedOutputBatches);
    }
  } testSettings[]{
      {1, 1, 1, 1, 1'000'000'000, 1},
      {3, 1, 1, 1, 1'000'000'000, 3},
      {3, 4, 1, 1, 1'000'000'000, 3 * 4},
      {3, 4, 32, 1, 1'000'000'000, 3 * 4 * 32},
      {1, 1, 1, 1024, 1'000'000'000, 1},
      {3, 1, 1, 1024, 1'000'000'000, 1},
      {3, 4, 32, 1024, 1'000'000'000, 1},
      {1, 1, 1, 1, 1, 1},
      {3, 1, 1, 1, 1, 3},
      {3, 4, 1, 1, 1, 3 * 4},
      {3, 4, 32, 1, 1, 3 * 4 * 32},
      {1, 1, 1, 1024, 1, 1},
      {3, 1, 1, 1024, 1, 3},
      {3, 4, 1, 1024, 1, 3 * 4},
      {3, 4, 32, 1024, 1, 3 * 4 * 32}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    testLocalMerge(
        testData.numSources,
        testData.numInputBatches,
        testData.inputBatchSize,
        testData.maxOutputBatchRows,
        testData.maxOutputBatchBytes,
        testData.numExpectedOutputBatches);
  }
}
