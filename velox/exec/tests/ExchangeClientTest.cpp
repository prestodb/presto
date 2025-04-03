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
#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/SerializedPageUtil.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/serializers/UnsafeRowSerializer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec {

namespace {

static constexpr int32_t kDefaultMinExchangeOutputBatchBytes{2 << 20}; // 2 MB.

class ExchangeClientTest
    : public testing::Test,
      public velox::test::VectorTestBase,
      public testing::WithParamInterface<VectorSerde::Kind> {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
    if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
      serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
    }
    if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kCompactRow)) {
      serializer::CompactRowVectorSerde::registerNamedVectorSerde();
    }
    if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kUnsafeRow)) {
      serializer::spark::UnsafeRowVectorSerde::registerNamedVectorSerde();
    }
  }

  void SetUp() override {
    serdeKind_ = GetParam();
    test::testingStartLocalExchangeSource();
    executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(16);
    exec::ExchangeSource::factories().clear();
    exec::ExchangeSource::registerFactory(test::createLocalExchangeSource);
    if (!isRegisteredVectorSerde()) {
      velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }
    bufferManager_ = OutputBufferManager::getInstanceRef();

    common::testutil::TestValue::enable();
  }

  void TearDown() override {
    exec::test::waitForAllTasksToBeDeleted();
    test::testingShutdownLocalExchangeSource();
  }

  std::shared_ptr<Task> makeTask(
      const std::string& taskId,
      const std::optional<uint64_t> maxOutputBufferSizeInBytes = {}) {
    std::unordered_map<std::string, std::string> config;
    if (maxOutputBufferSizeInBytes.has_value()) {
      config[core::QueryConfig::kMaxOutputBufferSize] =
          std::to_string(maxOutputBufferSizeInBytes.value());
    }
    auto queryCtx = core::QueryCtx::create(
        executor_.get(), core::QueryConfig{std::move(config)});
    queryCtx->testingOverrideMemoryPool(
        memory::memoryManager()->addRootPool(queryCtx->queryId()));
    auto plan = test::PlanBuilder().values({}).planNode();
    return Task::create(
        taskId,
        core::PlanFragment{plan},
        0,
        std::move(queryCtx),
        Task::ExecutionMode::kParallel);
  }

  int32_t enqueue(
      const std::string& taskId,
      int32_t destination,
      const RowVectorPtr& data) {
    auto page =
        test::toSerializedPage(data, serdeKind_, bufferManager_, pool());
    const auto pageSize = page->size();
    ContinueFuture unused;
    auto blocked =
        bufferManager_->enqueue(taskId, destination, std::move(page), &unused);
    VELOX_CHECK(!blocked);
    return pageSize;
  }

  std::vector<std::unique_ptr<SerializedPage>>
  fetchPages(int consumerId, ExchangeClient& client, int32_t numPages) {
    std::vector<std::unique_ptr<SerializedPage>> allPages;
    for (auto i = 0; i < numPages; ++i) {
      bool atEnd{false};
      ContinueFuture future;
      auto pages = client.next(consumerId, 1, &atEnd, &future);
      while (!atEnd && pages.empty()) {
        auto& exec = folly::QueuedImmediateExecutor::instance();
        std::move(future).via(&exec).wait();
        pages = client.next(consumerId, 1, &atEnd, &future);
      }
      EXPECT_EQ(1, pages.size());
      allPages.push_back(std::move(pages.at(0)));
    }
    return allPages;
  }

  static void addSources(ExchangeQueue& queue, int32_t numSources) {
    {
      std::lock_guard<std::mutex> l(queue.mutex());
      for (auto i = 0; i < numSources; ++i) {
        queue.addSourceLocked();
      }
    }
    queue.noMoreSources();
  }

  static void enqueue(
      ExchangeQueue& queue,
      std::unique_ptr<SerializedPage> page) {
    std::vector<ContinuePromise> promises;
    {
      std::lock_guard<std::mutex> l(queue.mutex());
      queue.enqueueLocked(std::move(page), promises);
    }
    for (auto& promise : promises) {
      promise.setValue();
    }
  }

  static std::unique_ptr<SerializedPage> makePage(uint64_t size) {
    auto ioBuf = folly::IOBuf::create(size);
    ioBuf->append(size);
    return std::make_unique<SerializedPage>(std::move(ioBuf), nullptr, 1);
  }

  folly::Executor* executor() const {
    return executor_.get();
  }

  VectorSerde::Kind serdeKind_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<OutputBufferManager> bufferManager_;
};

TEST_P(ExchangeClientTest, nonVeloxCreateExchangeSourceException) {
  ExchangeSource::registerFactory(
      [](const auto& taskId, auto destination, auto queue, auto pool)
          -> std::shared_ptr<ExchangeSource> {
        throw std::runtime_error("Testing error");
      });

  auto client = std::make_shared<ExchangeClient>(
      "t", 1, ExchangeClient::kDefaultMaxQueuedBytes, 1, 0, pool(), executor());

  VELOX_ASSERT_THROW(
      client->addRemoteTaskId("task.1.2.3"),
      "Failed to create ExchangeSource: Testing error. Task ID: task.1.2.3.");

  // Test with a very long task ID. Make sure it is truncated.
  VELOX_ASSERT_THROW(
      client->addRemoteTaskId(std::string(1024, 'x')),
      "Failed to create ExchangeSource: Testing error. "
      "Task ID: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.");

  client->close();
}

TEST_P(ExchangeClientTest, stats) {
  auto data = {
      makeRowVector({makeFlatVector<int32_t>({1, 2, 3})}),
      makeRowVector({makeFlatVector<int32_t>({1, 2, 3, 4, 5})}),
      makeRowVector({makeFlatVector<int32_t>({1, 2})}),
  };

  auto taskId = "local://t1";
  auto task = makeTask(taskId);

  bufferManager_->initializeTask(
      task, core::PartitionedOutputNode::Kind::kPartitioned, 100, 16);

  auto client = std::make_shared<ExchangeClient>(
      "t",
      17,
      ExchangeClient::kDefaultMaxQueuedBytes,
      1,
      kDefaultMinExchangeOutputBatchBytes,
      pool(),
      executor());
  client->addRemoteTaskId(taskId);

  // Enqueue 3 pages.
  std::vector<uint64_t> pageBytes;
  uint64_t totalBytes = 0;
  for (auto vector : data) {
    const auto pageSize = enqueue(taskId, 17, vector);
    totalBytes += pageSize;
    pageBytes.push_back(pageSize);
  }

  fetchPages(1, *client, 3);

  auto stats = client->stats();
  // Since we run exchange source response callback in an executor, then we
  // might start to fetch from the client before all the source buffers are
  // enqueued.
  ASSERT_GE(totalBytes, stats.at("peakBytes").sum);
  ASSERT_EQ(data.size(), stats.at("numReceivedPages").sum);
  ASSERT_EQ(totalBytes / data.size(), stats.at("averageReceivedPageBytes").sum);

  task->requestCancel();
  bufferManager_->removeTask(taskId);

  client->close();
}

// Test scenario where fetching data from all sources at once would exceed queue
// size. Verify that ExchangeClient is fetching data only from a few sources at
// a time to avoid exceeding the limit.
TEST_P(ExchangeClientTest, flowControl) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>(10'000, [](auto row) { return row; }),
  });

  auto page = test::toSerializedPage(data, serdeKind_, bufferManager_, pool());

  // Set limit at 3.5 pages
  // Set the minOutputBatchBytes to be 1024 since now the client
  // will request if bytes in queue + pendingBytes < minOutputBatchBytes
  auto client = std::make_shared<ExchangeClient>(
      "flow.control", 17, page->size() * 3.5, 1, 1024, pool(), executor());

  // Make 10 tasks.
  std::vector<std::shared_ptr<Task>> tasks;
  for (auto i = 0; i < 10; ++i) {
    auto taskId = fmt::format("local://t{}", i);
    auto task = makeTask(taskId);

    bufferManager_->initializeTask(
        task, core::PartitionedOutputNode::Kind::kPartitioned, 100, 16);

    // Enqueue 3 pages.
    for (auto j = 0; j < 3; ++j) {
      enqueue(taskId, 17, data);
    }

    tasks.push_back(task);
    client->addRemoteTaskId(taskId);
  }

  fetchPages(1, *client, 3 * tasks.size());

  const auto stats = client->stats();
  EXPECT_LE(stats.at("peakBytes").sum, page->size() * 4);
  EXPECT_EQ(30, stats.at("numReceivedPages").sum);
  EXPECT_EQ(page->size(), stats.at("averageReceivedPageBytes").sum);

  for (auto& task : tasks) {
    task->requestCancel();
    bufferManager_->removeTask(task->taskId());
  }

  client->close();
}

// Test that small pages will block and we will keep
// requesting from the queue if we do not have enough buffer
// to fillout minOutputBatchBytes
TEST_P(ExchangeClientTest, smallPage) {
  const int64_t clientBufferSize = 1024;
  const int64_t minOutputBatchBytes = clientBufferSize;
  const int64_t maxOutputBatchBytes = clientBufferSize;
  auto client = std::make_shared<ExchangeClient>(
      "local://test-acknowledge-client-task",
      maxOutputBatchBytes,
      clientBufferSize,
      1,
      minOutputBatchBytes,
      pool(),
      executor());

  const auto& queue = client->queue();
  addSources(*queue, 1);

  // Enqueue a tiny page
  enqueue(*queue, makePage(100));

  bool atEnd;
  ContinueFuture future = ContinueFuture::makeEmpty();

  // Should unblock because because first page
  auto pages = client->next(1, 1, &atEnd, &future);
  EXPECT_FALSE(pages.empty());

  // Enqueue another tiny page, and still will not block
  // because we have less than minOutputBatchBytes
  enqueue(*queue, makePage(1));
  pages = client->next(1, 1, &atEnd, &future);
  ASSERT_FALSE(pages.empty());

  // Signal no-more-data.
  enqueue(*queue, nullptr);

  // Drain the rest and close it
  pages = client->next(1, minOutputBatchBytes, &atEnd, &future);
  ASSERT_TRUE(atEnd);

  client->close();
}

TEST_P(ExchangeClientTest, largeSinglePage) {
  auto data = {
      makeRowVector({makeFlatVector<int64_t>(10000, folly::identity)}),
      // second page is >1% of total payload size
      makeRowVector({makeFlatVector<int64_t>(150, folly::identity)}),
  };
  auto client = std::make_shared<ExchangeClient>(
      "test",
      1,
      1000,
      1,
      kDefaultMinExchangeOutputBatchBytes,
      pool(),
      executor());
  auto task = makeTask("local://producer");
  bufferManager_->initializeTask(
      task, core::PartitionedOutputNode::Kind::kArbitrary, 1, 1);
  for (auto& batch : data) {
    enqueue(task->taskId(), 0, batch);
  }
  client->addRemoteTaskId(task->taskId());
  auto pages = fetchPages(1, *client, 1);
  ASSERT_EQ(pages.size(), 1);
  ASSERT_GT(pages[0]->size(), 80000);
  pages = fetchPages(1, *client, 1);
  ASSERT_EQ(pages.size(), 1);
  ASSERT_LT(pages[0]->size(), 4000);
  task->requestCancel();
  bufferManager_->removeTask(task->taskId());
  client->close();
}

TEST_P(ExchangeClientTest, multiPageFetch) {
  auto client = std::make_shared<ExchangeClient>(
      "test",
      17,
      1 << 20,
      1,
      kDefaultMinExchangeOutputBatchBytes,
      pool(),
      executor());

  {
    bool atEnd;
    ContinueFuture future = ContinueFuture::makeEmpty();
    auto pages = client->next(1, 1, &atEnd, &future);
    ASSERT_EQ(0, pages.size());
    ASSERT_FALSE(atEnd);
    ASSERT_TRUE(future.valid());
  }

  const auto& queue = client->queue();
  addSources(*queue, 1);

  for (auto i = 0; i < 10; ++i) {
    enqueue(*queue, makePage(1'000 + i));
  }

  // Fetch one page.
  bool atEnd;
  ContinueFuture future = ContinueFuture::makeEmpty();
  auto pages = client->next(1, 1, &atEnd, &future);
  ASSERT_EQ(1, pages.size());
  ASSERT_FALSE(atEnd);
  ASSERT_FALSE(future.valid());

  // Fetch multiple pages. Each page is slightly larger than 1K bytes, hence,
  // only 4 pages fit.
  pages = client->next(1, 5'000, &atEnd, &future);
  ASSERT_EQ(4, pages.size());
  ASSERT_FALSE(atEnd);
  ASSERT_FALSE(future.valid());

  // Fetch the rest of the pages.
  pages = client->next(1, 10'000, &atEnd, &future);
  ASSERT_EQ(5, pages.size());
  ASSERT_FALSE(atEnd);
  ASSERT_FALSE(future.valid());

  // Signal no-more-data.
  enqueue(*queue, nullptr);

  pages = client->next(1, 10'000, &atEnd, &future);
  ASSERT_EQ(0, pages.size());
  ASSERT_TRUE(atEnd);
  ASSERT_FALSE(future.valid());

  client->close();
}

TEST_P(ExchangeClientTest, sourceTimeout) {
  constexpr int32_t kNumSources = 3;
  auto client = std::make_shared<ExchangeClient>(
      "test",
      17,
      1 << 20,
      1,
      kDefaultMinExchangeOutputBatchBytes,
      pool(),
      executor());

  bool atEnd;
  ContinueFuture future;
  auto pages = client->next(1, 1, &atEnd, &future);
  ASSERT_EQ(0, pages.size());
  ASSERT_FALSE(atEnd);

  for (auto i = 0; i < kNumSources; ++i) {
    client->addRemoteTaskId(fmt::format("local://{}", i));
  }
  client->noMoreRemoteTasks();

  // Fetch a page. No page is found. All sources are fetching.
  pages = client->next(1, 1, &atEnd, &future);
  EXPECT_TRUE(pages.empty());

  std::mutex mutex;
  std::unordered_set<void*> sourcesWithTimeout;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::test::LocalExchangeSource::timeout",
      std::function<void(void*)>(([&](void* source) {
        std::lock_guard<std::mutex> l(mutex);
        sourcesWithTimeout.insert(source);
        LOG(INFO) << "inside lambda" << source
                  << " n=" << sourcesWithTimeout.size();
      })));

#ifndef NDEBUG
  // Wait until all sources have timed out at least once.
  auto deadline = std::chrono::system_clock::now() +
      3 * kNumSources *
          std::chrono::seconds(client->requestDataSizesMaxWaitSec());
  while (std::chrono::system_clock::now() < deadline) {
    {
      std::lock_guard<std::mutex> l(mutex);
      if (sourcesWithTimeout.size() >= kNumSources) {
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  EXPECT_LT(std::chrono::system_clock::now(), deadline);
#endif

  const auto& queue = client->queue();
  for (auto i = 0; i < 10; ++i) {
    enqueue(*queue, makePage(1'000 + i));
  }

  // Fetch one page.
  pages = client->next(1, 1, &atEnd, &future);
  EXPECT_EQ(1, pages.size());
  EXPECT_FALSE(atEnd);

  // Fetch multiple pages. Each page is slightly larger than 1K bytes, hence,
  // only 4 pages fit.
  pages = client->next(1, 5'000, &atEnd, &future);
  EXPECT_EQ(4, pages.size());
  EXPECT_FALSE(atEnd);

  // Fetch the rest of the pages.
  pages = client->next(1, 10'000, &atEnd, &future);
  EXPECT_EQ(5, pages.size());
  EXPECT_FALSE(atEnd);

  // Signal no-more-data for all sources.
  for (auto i = 0; i < kNumSources; ++i) {
    enqueue(*queue, nullptr);
  }
  pages = client->next(1, 10'000, &atEnd, &future);
  EXPECT_EQ(0, pages.size());
  EXPECT_TRUE(atEnd);

  client->close();
}

TEST_P(ExchangeClientTest, callNextAfterClose) {
  constexpr int32_t kNumSources = 3;
  common::testutil::TestValue::enable();
  auto client = std::make_shared<ExchangeClient>(
      "test",
      17,
      1 << 20,
      1,
      kDefaultMinExchangeOutputBatchBytes,
      pool(),
      executor());

  bool atEnd;
  ContinueFuture future;
  auto pages = client->next(1, 1, &atEnd, &future);
  ASSERT_EQ(0, pages.size());
  ASSERT_FALSE(atEnd);

  for (auto i = 0; i < kNumSources; ++i) {
    client->addRemoteTaskId(fmt::format("local://{}", i));
  }
  client->noMoreRemoteTasks();

  // Fetch a page. No page is found. All sources are fetching.
  pages = client->next(1, 1, &atEnd, &future);
  EXPECT_TRUE(pages.empty());

  const auto& queue = client->queue();
  for (auto i = 0; i < 10; ++i) {
    enqueue(*queue, makePage(1'000 + i));
  }

  // Fetch multiple pages. Each page is slightly larger than 1K bytes, hence,
  // only 4 pages fit.
  pages = client->next(1, 5'000, &atEnd, &future);
  EXPECT_EQ(4, pages.size());
  EXPECT_FALSE(atEnd);

  // Close the client and try calling next again.
  client->close();

  // Here we should have no pages returned, be at end (we are closed) and the
  // future should be invalid (not based on a valid promise).
  ContinueFuture futureFinal{ContinueFuture::makeEmpty()};
  pages = client->next(1, 10'000, &atEnd, &futureFinal);
  EXPECT_EQ(0, pages.size());
  EXPECT_TRUE(atEnd);
  EXPECT_FALSE(futureFinal.valid());

  client->close();
}

TEST_P(ExchangeClientTest, acknowledge) {
  const int64_t pageSize = 1024;
  const int64_t clientBufferSize = pageSize;
  const int64_t serverBufferSize = 2 * pageSize;

  const auto sourceTaskId = "local://test-acknowledge-source-task";
  const auto task = makeTask(sourceTaskId, serverBufferSize);
  auto taskRemoveGuard =
      folly::makeGuard([bufferManager = bufferManager_, task]() {
        task->requestCancel();
        bufferManager->removeTask(task->taskId());
      });

  bufferManager_->initializeTask(
      task, core::PartitionedOutputNode::Kind::kPartitioned, 2, 1);
  // Set the minOutputBatchBytes to be 1024 since now the client
  // will request if bytes in queue + pendingBytes < minOutputBatchBytes
  auto client = std::make_shared<ExchangeClient>(
      "local://test-acknowledge-client-task",
      1,
      clientBufferSize,
      1,
      1024,
      pool(),
      executor());
  auto clientCloseGuard = folly::makeGuard([client]() { client->close(); });

  std::atomic_int numberOfAcknowledgeRequests{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::test::LocalExchangeSource::pause",
      std::function<void(void*)>(([&numberOfAcknowledgeRequests](void*) {
        numberOfAcknowledgeRequests++;
      })));

  {
    // adding the first page should not block as there is enough space in
    // the output buffer for two pages
    ContinueFuture future;
    bufferManager_->enqueue(sourceTaskId, 1, makePage(pageSize), &future);
    ASSERT_TRUE(future.isReady());
  }

  {
    // adding the second page may block but will get unblocked once the
    // client fetches a single page
    ContinueFuture future;
    bufferManager_->enqueue(sourceTaskId, 1, makePage(pageSize), &future);

    // start fetching
    client->addRemoteTaskId(sourceTaskId);
    client->noMoreRemoteTasks();

    ASSERT_TRUE(std::move(future)
                    .via(executor())
                    .wait(std::chrono::seconds{10})
                    .isReady());

#ifndef NDEBUG
    // The client knew there is more data available but could not fetch any more
    // Explicit acknowledge was required
    EXPECT_EQ(numberOfAcknowledgeRequests, 1);
#endif
  }

  {
    // adding the third page should block (one page is in the exchange queue,
    // another two pages are in the output buffer)
    ContinueFuture enqueueDetachedFuture;
    bufferManager_->enqueue(
        sourceTaskId, 1, makePage(pageSize), &enqueueDetachedFuture);
    ASSERT_FALSE(enqueueDetachedFuture.isReady());

    auto enqueueFuture = std::move(enqueueDetachedFuture)
                             .via(executor())
                             .wait(std::chrono::milliseconds{100});
    ASSERT_FALSE(enqueueFuture.isReady());

    // removing one page from the exchange queue should trigger a fetch and
    // a subsequent acknowledge to release the output buffer memory
    bool atEnd;
    ContinueFuture dequeueDetachedFuture;
    auto pages = client->next(1, 1, &atEnd, &dequeueDetachedFuture);
    ASSERT_EQ(1, pages.size());
    ASSERT_FALSE(atEnd);
    ASSERT_TRUE(dequeueDetachedFuture.isReady());

    ASSERT_TRUE(
        std::move(enqueueFuture).wait(std::chrono::seconds{10}).isReady());
#ifndef NDEBUG
    // The client knew there is more data available but could not fetch any more
    // Explicit acknowledge was required
    EXPECT_EQ(numberOfAcknowledgeRequests, 2);
#endif
  }

  // one page is still in the buffer at this point
  ASSERT_EQ(bufferManager_->getUtilization(sourceTaskId), 0.5);

  auto pages = fetchPages(1, *client, 1);
  ASSERT_EQ(1, pages.size());

  {
    // at this point the output buffer is expected to be empty
    int attempts = 100;
    bool outputBuffersEmpty;
    while (attempts > 0) {
      attempts--;
      outputBuffersEmpty = bufferManager_->getUtilization(sourceTaskId) == 0;
      if (outputBuffersEmpty) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }
    ASSERT_TRUE(outputBuffersEmpty);
#ifndef NDEBUG
    // The output buffer is empty now
    // Explicit acknowledge is not necessary as a blocking getDataSize is sent
    // right away
    EXPECT_EQ(numberOfAcknowledgeRequests, 2);
#endif
  }

  pages = fetchPages(1, *client, 1);
  ASSERT_EQ(1, pages.size());

  bufferManager_->noMoreData(sourceTaskId);

  bool atEnd;
  ContinueFuture dequeueEndOfDataFuture;
  pages = client->next(1, 1, &atEnd, &dequeueEndOfDataFuture);
  ASSERT_EQ(0, pages.size());

  ASSERT_TRUE(std::move(dequeueEndOfDataFuture)
                  .via(executor())
                  .wait(std::chrono::seconds{10})
                  .isReady());
  pages = client->next(1, 1, &atEnd, &dequeueEndOfDataFuture);
  ASSERT_EQ(0, pages.size());
  ASSERT_TRUE(atEnd);
}

TEST_P(ExchangeClientTest, minOutputBatchBytesInitialBatches) {
  // Initial batches should not block to avoid impacting latency of small
  // exchanges

  const auto minOutputBatchBytes = 10000;

  auto client = std::make_shared<ExchangeClient>(
      "test",
      17,
      ExchangeClient::kDefaultMaxQueuedBytes,
      1,
      minOutputBatchBytes,
      pool(),
      executor());

  const auto& queue = client->queue();
  addSources(*queue, 1);

  bool atEnd;
  ContinueFuture future = ContinueFuture::makeEmpty();

  // first page should unblock right away
  auto pages = client->next(1, 1, &atEnd, &future);
  ASSERT_FALSE(future.isReady());
  enqueue(*queue, makePage(2000));
  ASSERT_TRUE(future.isReady());
  pages = client->next(1, 1, &atEnd, &future);
  ASSERT_EQ(1, pages.size());

  // page larger than 1% of total should unblock right away
  pages = client->next(1, 1, &atEnd, &future);
  ASSERT_FALSE(future.isReady());
  enqueue(*queue, makePage(100));
  ASSERT_TRUE(future.isReady());
  pages = client->next(1, 1, &atEnd, &future);
  ASSERT_EQ(1, pages.size());

  // small page (<1% of total) should block
  pages = client->next(1, 1, &atEnd, &future);
  ASSERT_FALSE(future.isReady());
  enqueue(*queue, makePage(15));
  ASSERT_FALSE(future.isReady());
  // one more small page should unblock now
  enqueue(*queue, makePage(10));
  ASSERT_TRUE(future.isReady());
  pages = client->next(1, 100, &atEnd, &future);
  ASSERT_EQ(2, pages.size());

  // Signal no-more-data.
  enqueue(*queue, nullptr);

  pages = client->next(1, 10'000, &atEnd, &future);
  ASSERT_EQ(0, pages.size());
  ASSERT_TRUE(atEnd);

  client->close();
}

TEST_P(ExchangeClientTest, minOutputBatchBytesZero) {
  // When minOutputBatchBytes is zero always unblock on the first page

  auto client = std::make_shared<ExchangeClient>(
      "test",
      17,
      ExchangeClient::kDefaultMaxQueuedBytes,
      10,
      0,
      pool(),
      executor());

  const auto& queue = client->queue();
  addSources(*queue, 1);

  bool atEnd;
  ContinueFuture future = ContinueFuture::makeEmpty();

  auto pages = client->next(1, 1, &atEnd, &future);
  ASSERT_FALSE(future.isReady());
  enqueue(*queue, makePage(1));
  ASSERT_TRUE(future.isReady());
  pages = client->next(2, 1, &atEnd, &future);
  ASSERT_EQ(1, pages.size());

  pages = client->next(3, 1, &atEnd, &future);
  ASSERT_FALSE(future.isReady());
  enqueue(*queue, makePage(1));
  ASSERT_TRUE(future.isReady());
  pages = client->next(4, 1, &atEnd, &future);
  ASSERT_EQ(1, pages.size());

  // Signal no-more-data.
  enqueue(*queue, nullptr);

  pages = client->next(5, 10'000, &atEnd, &future);
  ASSERT_EQ(0, pages.size());
  ASSERT_TRUE(atEnd);

  client->close();
}

TEST_P(ExchangeClientTest, minOutputBatchBytesSingleConsumer) {
  const auto minOutputBatchBytes = 1000;

  auto client = std::make_shared<ExchangeClient>(
      "test",
      17,
      ExchangeClient::kDefaultMaxQueuedBytes,
      1,
      minOutputBatchBytes,
      pool(),
      executor());

  const auto& queue = client->queue();
  addSources(*queue, 1);

  bool atEnd;
  ContinueFuture future = ContinueFuture::makeEmpty();

  // first page should unblock right away
  auto pages = client->next(1, 1, &atEnd, &future);
  ASSERT_FALSE(future.isReady());
  enqueue(*queue, makePage(minOutputBatchBytes * 150));
  ASSERT_TRUE(future.isReady());
  pages = client->next(1, 1, &atEnd, &future);
  ASSERT_EQ(1, pages.size());

  pages = client->next(1, 1, &atEnd, &future);
  ASSERT_FALSE(future.isReady());
  enqueue(*queue, makePage(minOutputBatchBytes / 2));
  ASSERT_FALSE(future.isReady());
  enqueue(*queue, makePage(minOutputBatchBytes / 3));
  ASSERT_FALSE(future.isReady());
  enqueue(*queue, makePage(minOutputBatchBytes / 3));
  ASSERT_TRUE(future.isReady());

  pages = client->next(1, minOutputBatchBytes, &atEnd, &future);
  ASSERT_EQ(2, pages.size());

  pages = client->next(1, 1, &atEnd, &future);
  ASSERT_FALSE(future.isReady());
  enqueue(*queue, makePage(minOutputBatchBytes / 2));
  ASSERT_FALSE(future.isReady());

  // Signal no-more-data.
  enqueue(*queue, nullptr);
  ASSERT_TRUE(future.isReady());

  pages = client->next(1, 10'000, &atEnd, &future);
  ASSERT_EQ(2, pages.size());
  ASSERT_TRUE(atEnd);

  client->close();
}

TEST_P(ExchangeClientTest, minOutputBatchBytesMultipleConsumers) {
  const auto minOutputBatchBytes = 1000;
  const int numConsumers = 3;

  auto client = std::make_shared<ExchangeClient>(
      "test",
      17,
      ExchangeClient::kDefaultMaxQueuedBytes,
      numConsumers,
      minOutputBatchBytes,
      pool(),
      executor());

  const auto& queue = client->queue();
  addSources(*queue, 1);

  bool atEnd;

  std::vector<ContinueFuture> consumers;
  for (int consumerId = 0; consumerId < numConsumers; consumerId++) {
    consumers.push_back(ContinueFuture::makeEmpty());
  }

  for (int consumerId = 0; consumerId < numConsumers; consumerId++) {
    client->next(consumerId, 1, &atEnd, &consumers[consumerId]);
    ASSERT_FALSE(consumers[consumerId].isReady());
  }

  // first page should unblock all right away
  enqueue(*queue, makePage(minOutputBatchBytes * 150));
  ASSERT_EQ(
      std::count_if(
          consumers.begin(),
          consumers.end(),
          [](auto& consumer) { return consumer.isReady(); }),
      numConsumers);

  auto pages = client->next(1, 1, &atEnd, &consumers[1]);
  ASSERT_EQ(1, pages.size());

  for (int consumerId = 0; consumerId < numConsumers; consumerId++) {
    client->next(consumerId, 1, &atEnd, &consumers[consumerId]);
    ASSERT_FALSE(consumers[consumerId].isReady());
  }

  enqueue(*queue, makePage(minOutputBatchBytes));
  ASSERT_EQ(
      std::count_if(
          consumers.begin(),
          consumers.end(),
          [](auto& consumer) { return consumer.isReady(); }),
      1);

  enqueue(*queue, makePage(minOutputBatchBytes));
  ASSERT_EQ(
      std::count_if(
          consumers.begin(),
          consumers.end(),
          [](auto& consumer) { return consumer.isReady(); }),
      2);

  enqueue(*queue, makePage(minOutputBatchBytes / 2));
  ASSERT_EQ(
      std::count_if(
          consumers.begin(),
          consumers.end(),
          [](auto& consumer) { return consumer.isReady(); }),
      2);

  for (int consumerId = 0; consumerId < numConsumers; consumerId++) {
    if (consumers[consumerId].isReady()) {
      pages = client->next(consumerId, 1, &atEnd, &consumers[consumerId]);
      ASSERT_EQ(1, pages.size());
    }
  }

  for (int consumerId = 0; consumerId < numConsumers; consumerId++) {
    pages = client->next(consumerId, 1, &atEnd, &consumers[consumerId]);
    ASSERT_EQ(0, pages.size());
    EXPECT_FALSE(consumers[consumerId].isReady());
  }

  enqueue(*queue, makePage(minOutputBatchBytes / 2));
  ASSERT_EQ(
      std::count_if(
          consumers.begin(),
          consumers.end(),
          [](auto& consumer) { return consumer.isReady(); }),
      1);

  for (int consumerId = 0; consumerId < numConsumers; consumerId++) {
    if (consumers[consumerId].isReady()) {
      pages = client->next(
          consumerId, minOutputBatchBytes, &atEnd, &consumers[consumerId]);
      ASSERT_EQ(2, pages.size());
      pages = client->next(
          consumerId, minOutputBatchBytes, &atEnd, &consumers[consumerId]);
      ASSERT_EQ(0, pages.size());
    }
  }

  ASSERT_EQ(
      std::count_if(
          consumers.begin(),
          consumers.end(),
          [](auto& consumer) { return consumer.isReady(); }),
      0);

  // Signal no-more-data.
  enqueue(*queue, nullptr);
  ASSERT_EQ(
      std::count_if(
          consumers.begin(),
          consumers.end(),
          [](auto& consumer) { return consumer.isReady(); }),
      numConsumers);

  pages = client->next(1, 10'000, &atEnd, &consumers[1]);
  ASSERT_EQ(0, pages.size());
  ASSERT_TRUE(atEnd);

  client->close();
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    ExchangeClientTest,
    ExchangeClientTest,
    testing::Values(
        VectorSerde::Kind::kPresto,
        VectorSerde::Kind::kCompactRow,
        VectorSerde::Kind::kUnsafeRow));

} // namespace
} // namespace facebook::velox::exec
