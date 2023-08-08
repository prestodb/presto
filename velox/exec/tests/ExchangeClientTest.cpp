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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/PartitionedOutputBufferManager.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec {

namespace {

class ExchangeClientTest : public testing::Test,
                           public velox::test::VectorTestBase {
 protected:
  void SetUp() override {
    exec::ExchangeSource::factories().clear();
    exec::ExchangeSource::registerFactory(test::createLocalExchangeSource);
    if (!isRegisteredVectorSerde()) {
      velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }

    bufferManager_ = PartitionedOutputBufferManager::getInstance().lock();
  }

  std::unique_ptr<SerializedPage> toSerializedPage(const RowVectorPtr& vector) {
    auto data = std::make_unique<VectorStreamGroup>(pool());
    auto size = vector->size();
    auto range = IndexRange{0, size};
    data->createStreamTree(asRowType(vector->type()), size);
    data->append(vector, folly::Range(&range, 1));
    auto listener = bufferManager_->newListener();
    IOBufOutputStream stream(*pool(), listener.get(), data->size());
    data->flush(&stream);
    return std::make_unique<SerializedPage>(stream.getIOBuf());
  }

  std::shared_ptr<Task> makeTask(
      const std::string& taskId,
      const core::PlanNodePtr& planNode,
      int destination) {
    auto queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
    queryCtx->testingOverrideMemoryPool(
        memory::defaultMemoryManager().addRootPool(queryCtx->queryId()));
    core::PlanFragment planFragment{planNode};
    return Task::create(
        taskId, std::move(planFragment), destination, std::move(queryCtx));
  }

  int32_t enqueue(
      const std::string& taskId,
      int32_t destination,
      const RowVectorPtr& data) {
    auto page = toSerializedPage(data);
    const auto pageSize = page->size();
    ContinueFuture unused;
    auto blocked =
        bufferManager_->enqueue(taskId, destination, std::move(page), &unused);
    VELOX_CHECK(!blocked);
    return pageSize;
  }

  void fetchPages(ExchangeClient& client, int32_t numPages) {
    for (auto i = 0; i < numPages; ++i) {
      bool atEnd;
      ContinueFuture future;
      auto page = client.next(&atEnd, &future);
      ASSERT_TRUE(page != nullptr);
    }
  }

  std::shared_ptr<PartitionedOutputBufferManager> bufferManager_;
};

TEST_F(ExchangeClientTest, nonVeloxCreateExchangeSourceException) {
  ExchangeSource::registerFactory(
      [](const auto& taskId, auto destination, auto queue, auto pool)
          -> std::shared_ptr<ExchangeSource> {
        throw std::runtime_error("Testing error");
      });

  ExchangeClient client(1, pool(), ExchangeClient::kDefaultMaxQueuedBytes);
  VELOX_ASSERT_THROW(
      client.addRemoteTaskId("task.1.2.3"),
      "Failed to create ExchangeSource: Testing error. Task ID: task.1.2.3.");

  // Test with a very long task ID. Make sure it is truncated.
  VELOX_ASSERT_THROW(
      client.addRemoteTaskId(std::string(1024, 'x')),
      "Failed to create ExchangeSource: Testing error. "
      "Task ID: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.");
}

TEST_F(ExchangeClientTest, stats) {
  auto data = {
      makeRowVector({makeFlatVector<int32_t>({1, 2, 3})}),
      makeRowVector({makeFlatVector<int32_t>({1, 2, 3, 4, 5})}),
      makeRowVector({makeFlatVector<int32_t>({1, 2})}),
  };

  auto plan = test::PlanBuilder()
                  .values(data)
                  .partitionedOutput({"c0"}, 100)
                  .planNode();
  auto taskId = "local://t1";
  auto task = makeTask(taskId, plan, 17);

  bufferManager_->initializeTask(
      task, core::PartitionedOutputNode::Kind::kPartitioned, 100, 16);

  ExchangeClient client(17, pool(), ExchangeClient::kDefaultMaxQueuedBytes);
  client.addRemoteTaskId(taskId);

  // Enqueue 3 pages.
  std::vector<uint64_t> pageBytes;
  uint64_t totalBytes = 0;
  for (auto vector : data) {
    const auto pageSize = enqueue(taskId, 17, vector);
    totalBytes += pageSize;
    pageBytes.push_back(pageSize);
  }

  // ExchangeSource should have fetched first page and placed it into the queue.
  // Once we fetch this page from the queue, the ExchangeSource will go fetch
  // the remaining 2 pages.

  fetchPages(client, 3);

  auto stats = client.stats();
  EXPECT_EQ(pageBytes[1] + pageBytes[2], stats.at("peakBytes").sum);
  EXPECT_EQ(data.size(), stats.at("numReceivedPages").sum);
  EXPECT_EQ(totalBytes / data.size(), stats.at("averageReceivedPageBytes").sum);

  task->requestCancel();
  bufferManager_->removeTask(taskId);
}

} // namespace
} // namespace facebook::velox::exec
