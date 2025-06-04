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

#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

using namespace facebook::velox::exec;

namespace facebook::velox::exec::test {
namespace {

std::unordered_map<std::string, std::unordered_map<core::PlanNodeId, int64_t>>
    numOfSplitsByUuid;

class CountSplitListener : public SplitListener {
 public:
  CountSplitListener(const std::string& taskId, const std::string& taskUuid)
      : SplitListener(taskId, taskUuid) {}

  void onTaskCompletion() override {
    auto& map = numOfSplitsByUuid[taskUuid_];
    for (auto& [planNodeId, count] : counts_) {
      map[planNodeId] += count;
    }
  }

  void onAddSplit(
      const core::PlanNodeId& planNodeId,
      const exec::Split& /*split*/) override {
    counts_[planNodeId]++;
  }

 private:
  std::unordered_map<core::PlanNodeId, int64_t> counts_;
};

class CountAgainSplitListener : public SplitListener {
 public:
  CountAgainSplitListener(
      const std::string& taskId,
      const std::string& taskUuid)
      : SplitListener(taskId, taskUuid) {}

  void onTaskCompletion() override {
    auto& map = numOfSplitsByUuid[taskUuid_];
    for (auto& [planNodeId, count] : counts_) {
      map[planNodeId] += count;
    }
  }

  void onAddSplit(
      const core::PlanNodeId& planNodeId,
      const exec::Split& /*split*/) override {
    counts_[planNodeId]++;
  }

 private:
  std::unordered_map<core::PlanNodeId, int64_t> counts_;
};

template <typename T>
class TestSplitListenerFactory : public SplitListenerFactory {
 public:
  ~TestSplitListenerFactory() override = default;

  std::unique_ptr<SplitListener> create(
      const std::string& taskId,
      const std::string& taskUuid) override {
    return std::make_unique<T>(taskId, taskUuid);
  }
};

class SplitListenerTest : public HiveConnectorTestBase {
 public:
  SplitListenerTest() {
    countSplitListenerFactory_ =
        std::make_shared<TestSplitListenerFactory<CountSplitListener>>();
    countAgainSplitListenerFactory_ =
        std::make_shared<TestSplitListenerFactory<CountAgainSplitListener>>();
  }

 protected:
  void makeTable() {
    rowType_ = ROW({"c0"}, {BIGINT()});
    RowVectorPtr table =
        makeRowVector({"c0"}, {makeFlatVector<int64_t>({1, 2, 3, 4, 5})});
    directory_ = TempDirectoryPath::create();
    auto directoryPath = directory_->getPath();
    auto tablePath = fmt::format("{}/t", directoryPath);
    auto fs = filesystems::getFileSystem(tablePath, {});
    fs->mkdir(tablePath);
    // Write the table three times to make multiple splits.
    for (auto i = 0; i < 3; ++i) {
      auto filePath = fmt::format("{}/f{}", tablePath, i);
      writeToFile(filePath, table);
      filePaths_.emplace_back(filePath);
    }
  }

  std::vector<std::shared_ptr<connector::ConnectorSplit>> getSplits() {
    std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
    splits.reserve(filePaths_.size());
    for (const auto& filePath : filePaths_) {
      splits.emplace_back(connector::hive::HiveConnectorSplitBuilder(filePath)
                              .connectorId(kHiveConnectorId)
                              .fileFormat(dwio::common::FileFormat::DWRF)
                              .build());
    }
    return splits;
  }

  RowTypePtr rowType_;
  std::shared_ptr<TempDirectoryPath> directory_;
  std::vector<std::string> filePaths_;

  std::shared_ptr<TestSplitListenerFactory<CountSplitListener>>
      countSplitListenerFactory_;
  std::shared_ptr<TestSplitListenerFactory<CountAgainSplitListener>>
      countAgainSplitListenerFactory_;
};

} // namespace
} // namespace facebook::velox::exec::test

namespace facebook::velox::exec::test {

TEST_F(SplitListenerTest, basic) {
  ASSERT_TRUE(exec::registerSplitListenerFactory(countSplitListenerFactory_));
  // Not allowing register the same split listener factory twice.
  ASSERT_FALSE(exec::registerSplitListenerFactory(countSplitListenerFactory_));

  makeTable();

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId nodeId;
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(rowType_)
                  .capturePlanNodeId(nodeId)
                  .project({"c0 + 1 as c0"})
                  .planNode();

  auto checkCount = [&](std::shared_ptr<Task>& task) {
    const auto uuid = task->uuid();
    task = nullptr;
    EXPECT_GT(numOfSplitsByUuid.count(uuid), 0);
    EXPECT_EQ(numOfSplitsByUuid[uuid].size(), 1);
    EXPECT_GT(numOfSplitsByUuid[uuid].count(nodeId), 0);
    EXPECT_EQ(numOfSplitsByUuid[uuid][nodeId], 3);
  };

  for (const auto addWithSequence : {false, true}) {
    const auto splits = getSplits();
    std::shared_ptr<Task> task;
    AssertQueryBuilder(plan)
        .splits(nodeId, splits)
        .addSplitWithSequence(addWithSequence)
        .copyResults(pool_.get(), task);
    checkCount(task);
  }

  ASSERT_TRUE(exec::unregisterSplitListenerFactory(countSplitListenerFactory_));
}

TEST_F(SplitListenerTest, multipleListeners) {
  ASSERT_TRUE(exec::registerSplitListenerFactory(countSplitListenerFactory_));
  ASSERT_TRUE(
      exec::registerSplitListenerFactory(countAgainSplitListenerFactory_));

  makeTable();

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId nodeId;
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(rowType_)
                  .capturePlanNodeId(nodeId)
                  .project({"c0 + 1 as c0"})
                  .planNode();

  const auto splits = getSplits();
  std::shared_ptr<Task> task;
  AssertQueryBuilder(plan)
      .splits(nodeId, splits)
      .copyResults(pool_.get(), task);

  const auto uuid = task->uuid();
  task = nullptr;
  EXPECT_GT(numOfSplitsByUuid.count(uuid), 0);
  EXPECT_EQ(numOfSplitsByUuid[uuid].size(), 1);
  EXPECT_GT(numOfSplitsByUuid[uuid].count(nodeId), 0);
  EXPECT_EQ(numOfSplitsByUuid[uuid][nodeId], 6);

  ASSERT_TRUE(exec::unregisterSplitListenerFactory(countSplitListenerFactory_));
  ASSERT_TRUE(
      exec::unregisterSplitListenerFactory(countAgainSplitListenerFactory_));
}

} // namespace facebook::velox::exec::test
