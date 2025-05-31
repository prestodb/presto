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

#include "folly/experimental/EventCount.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/PlanNodeStats.h"
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
      sources.reserve(inputVectors.size());
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
      params.queryConfigs = {
          {"spill_enabled", "true"},
          {"local_merge_enabled", "true"},
          {"local_merge_max_num_merge_sources", "2"}};
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
        sources.reserve(inputVectors.size());
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
        params.queryConfigs = {
            {"spill_enabled", "true"},
            {"local_merge_enabled", "true"},
            {"local_merge_max_num_merge_sources", "2"}};
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

  void testLocalMergeSpill(
      const uint32_t batchSize,
      const uint32_t numMaxMergeSources) {
    std::vector<std::vector<RowVectorPtr>> inputVectors;
    for (int32_t i = 0; i < 9; ++i) {
      std::vector<RowVectorPtr> vectors;
      for (int32_t i = 0; i < 3; ++i) {
        auto c0 = makeFlatVector<int64_t>(
            batchSize,
            [&](auto row) { return batchSize * i + row; },
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
    params.queryConfigs = {
        {"spill_enabled", "true"},
        {"local_merge_enabled", "true"},
        {"local_merge_max_num_merge_sources",
         std::to_string(numMaxMergeSources)}};
    params.spillDirectory = spillDirectory->getPath();
    auto task = assertQueryOrdered(
        params, "SELECT * FROM tmp ORDER BY " + orderByClause, {keyIndex});

    auto taskStats = toPlanStats(task->taskStats());
    auto& planStats = taskStats.at(nodeId);
    if (batchSize == 0 || numMaxMergeSources >= inputVectors.size()) {
      ASSERT_EQ(planStats.spilledBytes, 0);
      ASSERT_EQ(planStats.spilledPartitions, 0);
      ASSERT_EQ(planStats.spilledFiles, 0);
      ASSERT_EQ(planStats.spilledRows, 0);
    } else {
      const auto expectedFiles =
          (inputVectors.size() + numMaxMergeSources - 1) / numMaxMergeSources;
      const auto expectedSpillRows = batchSize * duckInputs.size();
      ASSERT_GT(planStats.spilledBytes, 0);
      ASSERT_EQ(planStats.spilledPartitions, expectedFiles);
      ASSERT_EQ(planStats.spilledFiles, expectedFiles);
      ASSERT_EQ(planStats.spilledRows, expectedSpillRows);
    }
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
    uint32_t maxNumMergeSources;
    uint32_t batchSize;

    std::string debugString() const {
      return fmt::format(
          "maxNumMergeSources {}, batchSize {}", maxNumMergeSources, batchSize);
    }
  } testSettings[]{
      {1, 0},
      {10, 0},
      {1, 30},
      {3, 30},
      {8, 30},
      {9, 30},
      {std::numeric_limits<uint32_t>::max(), 30},
  };
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    testLocalMergeSpill(testData.batchSize, testData.maxNumMergeSources);
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
  params.queryConfigs = {
      {"spill_enabled", "true"},
      {"local_merge_enabled", "true"},
      {"local_merge_max_num_merge_sources", "2"}};
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

TEST_F(MergeTest, localMerge) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    constexpr vector_size_t batchSize = 100;
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
