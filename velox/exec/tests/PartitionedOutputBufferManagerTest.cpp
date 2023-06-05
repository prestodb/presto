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
#include "velox/exec/PartitionedOutputBufferManager.h"
#include <gtest/gtest.h>
#include <velox/common/memory/MemoryAllocator.h>
#include "folly/experimental/EventCount.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

using facebook::velox::test::BatchMaker;

class PartitionedOutputBufferManagerTest : public testing::Test {
 protected:
  PartitionedOutputBufferManagerTest() {
    std::vector<std::string> names = {"c0", "c1"};
    std::vector<TypePtr> types = {BIGINT(), VARCHAR()};
    rowType_ = ROW(std::move(names), std::move(types));
  }

  void SetUp() override {
    pool_ = facebook::velox::memory::addDefaultLeafMemoryPool();
    bufferManager_ = PartitionedOutputBufferManager::getInstance().lock();
    if (!isRegisteredVectorSerde()) {
      facebook::velox::serializer::presto::PrestoVectorSerde::
          registerVectorSerde();
      bufferManager_->setListenerFactory([]() {
        return std::make_unique<
            serializer::presto::PrestoOutputStreamListener>();
      });
    }
  }

  std::shared_ptr<Task> initializeTask(
      const std::string& taskId,
      const RowTypePtr& rowType,
      PartitionedOutputBuffer::Kind kind,
      int numDestinations,
      int numDrivers,
      int maxPartitionedOutputBufferSize = 0) {
    bufferManager_->removeTask(taskId);

    auto planFragment = exec::test::PlanBuilder()
                            .values({std::dynamic_pointer_cast<RowVector>(
                                BatchMaker::createBatch(rowType, 100, *pool_))})
                            .planFragment();
    std::unordered_map<std::string, std::string> configSettings;
    if (maxPartitionedOutputBufferSize != 0) {
      configSettings[core::QueryConfig::kMaxPartitionedOutputBufferSize] =
          std::to_string(maxPartitionedOutputBufferSize);
    }
    auto queryCtx = std::make_shared<core::QueryCtx>(
        executor_.get(), std::move(configSettings));

    auto task =
        Task::create(taskId, std::move(planFragment), 0, std::move(queryCtx));

    bufferManager_->initializeTask(task, kind, numDestinations, numDrivers);
    return task;
  }

  std::unique_ptr<SerializedPage> makeSerializedPage(
      std::shared_ptr<const RowType> rowType,
      vector_size_t size) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType, size, *pool_));
    return toSerializedPage(vector);
  }

  std::unique_ptr<SerializedPage> toSerializedPage(VectorPtr vector) {
    auto data = std::make_unique<VectorStreamGroup>(pool_.get());
    auto size = vector->size();
    auto range = IndexRange{0, size};
    data->createStreamTree(
        std::dynamic_pointer_cast<const RowType>(vector->type()), size);
    data->append(
        std::dynamic_pointer_cast<RowVector>(vector), folly::Range(&range, 1));
    auto listener = bufferManager_->newListener();
    IOBufOutputStream stream(*pool_, listener.get(), data->size());
    data->flush(&stream);
    return std::make_unique<SerializedPage>(stream.getIOBuf());
  }

  void enqueue(
      const std::string& taskId,
      std::shared_ptr<const RowType> rowType,
      vector_size_t size,
      bool expectedBlock = false) {
    enqueue(taskId, 0, std::move(rowType), size);
  }

  void enqueue(
      const std::string& taskId,
      int destination,
      std::shared_ptr<const RowType> rowType,
      vector_size_t size,
      bool expectedBlock = false) {
    ContinueFuture future;
    auto blockingReason = bufferManager_->enqueue(
        taskId, destination, makeSerializedPage(rowType, size), &future);
    if (!expectedBlock) {
      ASSERT_EQ(blockingReason, BlockingReason::kNotBlocked);
    }
    if (blockingReason != BlockingReason::kNotBlocked) {
      future.wait();
    }
  }

  void noMoreData(const std::string& taskId) {
    bufferManager_->noMoreData(taskId);
  }

  void fetch(
      const std::string& taskId,
      int destination,
      int64_t sequence,
      uint64_t maxBytes = 1024,
      int expectedGroups = 1,
      bool expectedEndMarker = false) {
    bool receivedData = false;
    ASSERT_TRUE(bufferManager_->getData(
        taskId,
        destination,
        maxBytes,
        sequence,
        [destination,
         sequence,
         expectedGroups,
         expectedEndMarker,
         &receivedData](
            std::vector<std::unique_ptr<folly::IOBuf>> pages,
            int64_t inSequence) {
          EXPECT_FALSE(receivedData) << "for destination " << destination;
          EXPECT_EQ(pages.size(), expectedGroups)
              << "for destination " << destination;
          for (int i = 0; i < pages.size(); ++i) {
            if (i == pages.size() - 1) {
              EXPECT_EQ(expectedEndMarker, pages[i] == nullptr)
                  << "for destination " << destination;
            } else {
              EXPECT_TRUE(pages[i] != nullptr)
                  << "for destination " << destination;
            }
          }
          EXPECT_EQ(inSequence, sequence) << "for destination " << destination;
          receivedData = true;
        }));
    EXPECT_TRUE(receivedData) << "for destination " << destination;
  }

  void fetchOne(
      const std::string& taskId,
      int destination,
      int64_t sequence,
      uint64_t maxBytes = 1024) {
    fetch(taskId, destination, sequence, maxBytes, 1);
  }

  void
  acknowledge(const std::string& taskId, int destination, int64_t sequence) {
    bufferManager_->acknowledge(taskId, destination, sequence);
  }

  void
  fetchOneAndAck(const std::string& taskId, int destination, int64_t sequence) {
    fetchOne(taskId, destination, sequence);
    acknowledge(taskId, destination, sequence + 1);
  }

  DataAvailableCallback
  receiveEndMarker(int destination, int64_t sequence, bool& receivedEndMarker) {
    return [destination, sequence, &receivedEndMarker](
               std::vector<std::unique_ptr<folly::IOBuf>> pages,
               int64_t inSequence) {
      EXPECT_FALSE(receivedEndMarker) << "for destination " << destination;
      EXPECT_EQ(pages.size(), 1) << "for destination " << destination;
      EXPECT_TRUE(pages[0] == nullptr) << "for destination " << destination;
      EXPECT_EQ(inSequence, sequence) << "for destination " << destination;
      receivedEndMarker = true;
    };
  }

  void
  fetchEndMarker(const std::string& taskId, int destination, int64_t sequence) {
    bool receivedData = false;
    ASSERT_TRUE(bufferManager_->getData(
        taskId,
        destination,
        std::numeric_limits<uint64_t>::max(),
        sequence,
        receiveEndMarker(destination, sequence, receivedData)));
    EXPECT_TRUE(receivedData) << "for destination " << destination;
    bufferManager_->deleteResults(taskId, destination);
  }

  void deleteResults(const std::string& taskId, int destination) {
    bufferManager_->deleteResults(taskId, destination);
  }

  void registerForEndMarker(
      const std::string& taskId,
      int destination,
      int64_t sequence,
      bool& receivedEndMarker) {
    receivedEndMarker = false;
    ASSERT_TRUE(bufferManager_->getData(
        taskId,
        destination,
        std::numeric_limits<uint64_t>::max(),
        sequence,
        receiveEndMarker(destination, sequence, receivedEndMarker)));
    EXPECT_FALSE(receivedEndMarker) << "for destination " << destination;
  }

  DataAvailableCallback receiveData(
      int destination,
      int64_t sequence,
      int expectedGroups,
      bool& receivedData) {
    receivedData = false;
    return [destination, sequence, expectedGroups, &receivedData](
               std::vector<std::unique_ptr<folly::IOBuf>> pages,
               int64_t inSequence) {
      EXPECT_FALSE(receivedData) << "for destination " << destination;
      EXPECT_EQ(pages.size(), expectedGroups)
          << "for destination " << destination;
      for (int i = 0; i < expectedGroups; i++) {
        EXPECT_TRUE(pages[i] != nullptr) << "for destination " << destination;
      }
      EXPECT_EQ(inSequence, sequence) << "for destination " << destination;
      receivedData = true;
    };
  }

  void registerForData(
      const std::string& taskId,
      int destination,
      int64_t sequence,
      int expectedGroups,
      bool& receivedData) {
    receivedData = false;
    ASSERT_TRUE(bufferManager_->getData(
        taskId,
        destination,
        1024,
        sequence,
        receiveData(destination, sequence, expectedGroups, receivedData)));
    EXPECT_FALSE(receivedData) << "for destination " << destination;
  }

  void dataFetcher(
      const std::string& taskId,
      int destination,
      int64_t& fetchedPages,
      bool earlyTermination) {
    folly::Random::DefaultGenerator rng;
    rng.seed(destination);
    int64_t nextSequence{0};
    while (true) {
      if (earlyTermination && folly::Random().oneIn(200)) {
        bufferManager_->deleteResults(taskId, destination);
        return;
      }
      const int64_t maxBytes = folly::Random().oneIn(4, rng) ? 32'000'000 : 1;
      int64_t receivedSequence;
      bool atEnd{false};
      folly::EventCount dataWait;
      auto dataWaitKey = dataWait.prepareWait();
      bufferManager_->getData(
          taskId,
          destination,
          maxBytes,
          nextSequence,
          [&](std::vector<std::unique_ptr<folly::IOBuf>> pages,
              int64_t inSequence) {
            ASSERT_EQ(inSequence, nextSequence);
            for (int i = 0; i < pages.size(); ++i) {
              if (pages[i] != nullptr) {
                ++nextSequence;
              } else {
                ASSERT_EQ(i, pages.size() - 1);
                atEnd = true;
              }
            }
            dataWait.notify();
          });
      dataWait.wait(dataWaitKey);
      if (atEnd) {
        break;
      }
    }
    bufferManager_->deleteResults(taskId, destination);
    fetchedPages = nextSequence;
  }

  std::shared_ptr<folly::Executor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency())};
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::shared_ptr<PartitionedOutputBufferManager> bufferManager_;
  RowTypePtr rowType_;
};

struct TestParam {
  PartitionedOutputBuffer::Kind kind;
};

class AllPartitionedOutputBufferManagerTest
    : public PartitionedOutputBufferManagerTest,
      public testing::WithParamInterface<PartitionedOutputBuffer::Kind> {
 public:
  AllPartitionedOutputBufferManagerTest() : kind_(GetParam()) {}

  static std::vector<PartitionedOutputBuffer::Kind> getTestParams() {
    static std::vector<PartitionedOutputBuffer::Kind> params = {
        PartitionedOutputBuffer::Kind::kBroadcast,
        PartitionedOutputBuffer::Kind::kPartitioned,
        PartitionedOutputBuffer::Kind::kArbitrary};
    return params;
  }

 protected:
  PartitionedOutputBuffer::Kind kind_;
};

TEST_F(PartitionedOutputBufferManagerTest, arbitrayBuffer) {
  {
    ArbitraryBuffer buffer;
    ASSERT_TRUE(buffer.empty());
    ASSERT_FALSE(buffer.hasNoMoreData());
    ASSERT_EQ(
        buffer.toString(), "[ARBITRARY_BUFFER PAGES[0] NO MORE DATA[false]]");
    VELOX_ASSERT_THROW(buffer.enqueue(nullptr), "Unexpected null page");
    ASSERT_EQ(
        buffer.toString(), "[ARBITRARY_BUFFER PAGES[0] NO MORE DATA[false]]");
    buffer.noMoreData();
    ASSERT_EQ(
        buffer.toString(), "[ARBITRARY_BUFFER PAGES[0] NO MORE DATA[true]]");
  }

  {
    ArbitraryBuffer buffer;
    ASSERT_TRUE(buffer.empty());
    ASSERT_FALSE(buffer.hasNoMoreData());
    auto page1 = makeSerializedPage(rowType_, 100);
    auto* rawPage1 = page1.get();
    buffer.enqueue(std::move(page1));
    ASSERT_FALSE(buffer.empty());
    ASSERT_FALSE(buffer.hasNoMoreData());
    ASSERT_EQ(
        buffer.toString(), "[ARBITRARY_BUFFER PAGES[1] NO MORE DATA[false]]");
    auto page2 = makeSerializedPage(rowType_, 100);
    auto* rawPage2 = page2.get();
    buffer.enqueue(std::move(page2));
    ASSERT_FALSE(buffer.empty());
    ASSERT_FALSE(buffer.hasNoMoreData());
    ASSERT_EQ(
        buffer.toString(), "[ARBITRARY_BUFFER PAGES[2] NO MORE DATA[false]]");

    auto pages = buffer.getPages(1);
    ASSERT_EQ(pages.size(), 1);
    ASSERT_EQ(pages[0].get(), rawPage1);
    auto page3 = makeSerializedPage(rowType_, 100);
    auto* rawPage3 = page3.get();
    buffer.enqueue(std::move(page3));
    ASSERT_EQ(
        buffer.toString(), "[ARBITRARY_BUFFER PAGES[2] NO MORE DATA[false]]");
    buffer.noMoreData();
    ASSERT_EQ(
        buffer.toString(), "[ARBITRARY_BUFFER PAGES[2] NO MORE DATA[true]]");
    pages = buffer.getPages(1'000'000'000);
    ASSERT_EQ(pages.size(), 3);
    ASSERT_EQ(pages[0].get(), rawPage2);
    ASSERT_EQ(pages[1].get(), rawPage3);
    ASSERT_EQ(pages[2].get(), nullptr);
    ASSERT_TRUE(buffer.empty());
    ASSERT_TRUE(buffer.hasNoMoreData());
    ASSERT_EQ(
        buffer.toString(), "[ARBITRARY_BUFFER PAGES[0] NO MORE DATA[true]]");
    auto page4 = makeSerializedPage(rowType_, 100);
    VELOX_ASSERT_THROW(
        buffer.enqueue(std::move(page4)),
        "Arbitrary buffer has set no more data marker");
    ASSERT_EQ(
        buffer.toString(), "[ARBITRARY_BUFFER PAGES[0] NO MORE DATA[true]]");
    buffer.noMoreData();
    VELOX_ASSERT_THROW(buffer.getPages(0), "maxBytes can't be zero");
    // Verify the end marker is persistent.
    for (int i = 0; i < 3; ++i) {
      pages = buffer.getPages(100);
      ASSERT_EQ(pages.size(), 1);
      ASSERT_EQ(pages[0].get(), nullptr);
    }
  }
}

TEST_F(PartitionedOutputBufferManagerTest, outputType) {
  ASSERT_EQ(
      PartitionedOutputBuffer::kindString(
          PartitionedOutputBuffer::Kind::kPartitioned),
      "PARTITIONED");
  ASSERT_EQ(
      PartitionedOutputBuffer::kindString(
          PartitionedOutputBuffer::Kind::kArbitrary),
      "ARBITRARY");
  ASSERT_EQ(
      PartitionedOutputBuffer::kindString(
          PartitionedOutputBuffer::Kind::kBroadcast),
      "BROADCAST");
  ASSERT_EQ(
      PartitionedOutputBuffer::kindString(
          static_cast<PartitionedOutputBuffer::Kind>(100)),
      "INVALID OUTPUT KIND 100");
}

TEST_F(PartitionedOutputBufferManagerTest, destinationBuffer) {
  {
    ArbitraryBuffer buffer;
    DestinationBuffer destinationBuffer;
    VELOX_ASSERT_THROW(
        destinationBuffer.loadData(&buffer, 0), "maxBytes can't be zero");
    destinationBuffer.loadData(&buffer, 100);
    std::atomic<bool> notified{false};
    destinationBuffer.getData(
        1'000'000,
        0,
        [&](std::vector<std::unique_ptr<folly::IOBuf>> buffers,
            int64_t sequence) {
          ASSERT_EQ(buffers.size(), 1);
          ASSERT_TRUE(buffers[0].get() == nullptr);
          notified = true;
        });
    ASSERT_TRUE(buffer.empty());
    ASSERT_FALSE(buffer.hasNoMoreData());
    ASSERT_FALSE(notified);
    VELOX_ASSERT_THROW(destinationBuffer.maybeLoadData(&buffer), "");
    buffer.noMoreData();
    destinationBuffer.maybeLoadData(&buffer);
    destinationBuffer.getAndClearNotify().notify();
    ASSERT_TRUE(notified);
  }

  {
    ArbitraryBuffer buffer;
    int64_t expectedNumBytes{0};
    for (int i = 0; i < 10; ++i) {
      auto page = makeSerializedPage(rowType_, 100);
      expectedNumBytes += page->size();
      buffer.enqueue(std::move(page));
    }
    DestinationBuffer destinationBuffer;
    destinationBuffer.loadData(&buffer, 1);
    ASSERT_EQ(
        buffer.toString(), "[ARBITRARY_BUFFER PAGES[9] NO MORE DATA[false]]");
    std::atomic<bool> notified{false};
    int64_t numBytes{0};
    auto buffers = destinationBuffer.getData(
        1'000'000'000,
        0,
        [&](std::vector<std::unique_ptr<folly::IOBuf>> /*unused*/,
            int64_t /*unused*/) { notified = true; });
    for (const auto& buffer : buffers) {
      numBytes += buffer->length();
    }
    ASSERT_GT(numBytes, 0);
    ASSERT_FALSE(notified);
    {
      auto result = destinationBuffer.getAndClearNotify();
      ASSERT_EQ(result.callback, nullptr);
      ASSERT_TRUE(result.data.empty());
      ASSERT_EQ(result.sequence, 0);
    }
    auto pages = destinationBuffer.acknowledge(1, false);
    ASSERT_EQ(pages.size(), 1);

    buffers = destinationBuffer.getData(
        1'000'000,
        1,
        [&](std::vector<std::unique_ptr<folly::IOBuf>> buffers,
            int64_t sequence) {
          ASSERT_EQ(sequence, 1);
          ASSERT_EQ(buffers.size(), 9);
          for (const auto& buffer : buffers) {
            numBytes += buffer->length();
          }
          notified = true;
        });
    ASSERT_TRUE(buffers.empty());
    ASSERT_FALSE(notified);

    destinationBuffer.maybeLoadData(&buffer);
    destinationBuffer.getAndClearNotify().notify();
    ASSERT_TRUE(notified);
    ASSERT_TRUE(buffer.empty());
    ASSERT_FALSE(buffer.hasNoMoreData());
    ASSERT_EQ(numBytes, expectedNumBytes);
  }
}

TEST_F(PartitionedOutputBufferManagerTest, basicPartitioned) {
  vector_size_t size = 100;

  std::string taskId = "t0";
  auto task = initializeTask(
      taskId, rowType_, PartitionedOutputBuffer::Kind::kPartitioned, 5, 1);

  // Partitioned output buffer doesn't allow to update output buffers once
  // created.
  VELOX_ASSERT_THROW(
      bufferManager_->updateOutputBuffers(taskId, 5 + 1, true),
      "updateOutputBuffers is not supported on PARTITIONED output buffer");
  VELOX_ASSERT_THROW(
      bufferManager_->updateOutputBuffers(taskId, 5 + 1, false),
      "updateOutputBuffers is not supported on PARTITIONED output buffer");
  VELOX_ASSERT_THROW(
      bufferManager_->updateOutputBuffers(taskId, 5 - 1, true),
      "updateOutputBuffers is not supported on PARTITIONED output buffer");
  VELOX_ASSERT_THROW(
      bufferManager_->updateOutputBuffers(taskId, 5 - 1, false),
      "updateOutputBuffers is not supported on PARTITIONED output buffer");

  // - enqueue one group per destination
  // - fetch and ask one group per destination
  // - enqueue one more group for destinations 0-2
  // - fetch and ask one group for destinations 0-2
  // - register end-marker callback for destination 3 and data callback for 4
  // - enqueue data for 4 and assert callback was called
  // - enqueue end marker for all destinations
  // - assert callback was called for destination 3
  // - fetch end markers for destinations 0-2 and 4

  for (int destination = 0; destination < 5; ++destination) {
    enqueue(taskId, destination, rowType_, size);
  }
  EXPECT_FALSE(bufferManager_->isFinished(taskId));

  for (int destination = 0; destination < 5; destination++) {
    fetchOneAndAck(taskId, destination, 0);
    // Try to re-fetch already ack-ed data - should fail
    EXPECT_THROW(fetchOne(taskId, destination, 0), std::exception);
  }
  EXPECT_FALSE(bufferManager_->isFinished(taskId));

  for (int destination = 0; destination < 3; destination++) {
    enqueue(taskId, destination, rowType_, size);
  }

  for (int destination = 0; destination < 3; destination++) {
    fetchOneAndAck(taskId, destination, 1);
  }

  // destinations 0-2 received and ack-ed 2 groups each
  // destinations 3-4 received and ack-ed 1 group each

  bool receivedEndMarker3;
  registerForEndMarker(taskId, 3, 1, receivedEndMarker3);

  bool receivedData4;
  registerForData(taskId, 4, 1, 1, receivedData4);

  enqueue(taskId, 4, rowType_, size);
  EXPECT_TRUE(receivedData4);

  noMoreData(taskId);
  EXPECT_TRUE(receivedEndMarker3);
  EXPECT_FALSE(bufferManager_->isFinished(taskId));

  for (int destination = 0; destination < 3; destination++) {
    fetchEndMarker(taskId, destination, 2);
  }
  EXPECT_TRUE(task->isRunning());
  deleteResults(taskId, 3);
  fetchEndMarker(taskId, 4, 2);
  bufferManager_->removeTask(taskId);
  EXPECT_TRUE(task->isFinished());
}

TEST_F(PartitionedOutputBufferManagerTest, basicBroadcast) {
  vector_size_t size = 100;

  std::string taskId = "t0";
  auto task = initializeTask(
      taskId, rowType_, PartitionedOutputBuffer::Kind::kBroadcast, 5, 1);
  VELOX_ASSERT_THROW(enqueue(taskId, 1, rowType_, size), "Bad destination 1");

  enqueue(taskId, rowType_, size);
  EXPECT_FALSE(bufferManager_->isFinished(taskId));

  for (int destination = 0; destination < 5; destination++) {
    fetchOneAndAck(taskId, destination, 0);
    // Try to re-fetch already ack-ed data - should fail
    EXPECT_THROW(fetchOne(taskId, destination, 0), std::exception);
  }
  EXPECT_FALSE(bufferManager_->isFinished(taskId));

  enqueue(taskId, rowType_, size);

  for (int destination = 0; destination < 5; destination++) {
    fetchOneAndAck(taskId, destination, 1);
  }

  bool receivedEndMarker3;
  registerForEndMarker(taskId, 3, 2, receivedEndMarker3);

  noMoreData(taskId);
  EXPECT_TRUE(receivedEndMarker3);
  EXPECT_FALSE(bufferManager_->isFinished(taskId));

  for (int destination = 0; destination < 5; ++destination) {
    if (destination != 3) {
      fetchEndMarker(taskId, destination, 2);
    }
  }
  EXPECT_FALSE(bufferManager_->isFinished(taskId));
  bufferManager_->updateOutputBuffers(taskId, 5, false);
  EXPECT_FALSE(bufferManager_->isFinished(taskId));
  bufferManager_->updateOutputBuffers(taskId, 6, false);

  // Fetch all for the new added destinations.
  fetch(taskId, 5, 0, 1'000'000'000, 3, true);
  VELOX_ASSERT_THROW(
      acknowledge(taskId, 5, 4),
      "(4 vs. 3) Ack received for a not yet produced item");
  acknowledge(taskId, 5, 0);
  acknowledge(taskId, 5, 1);
  acknowledge(taskId, 5, 2);
  acknowledge(taskId, 5, 3);
  EXPECT_FALSE(bufferManager_->isFinished(taskId));
  deleteResults(taskId, 5);
  VELOX_ASSERT_THROW(
      fetch(taskId, 5, 0, 1'000'000'000, 2),
      "getData received after its buffer is deleted. Destination: 5, sequence: 0");

  bufferManager_->updateOutputBuffers(taskId, 7, true);
  EXPECT_FALSE(bufferManager_->isFinished(taskId));
  fetch(taskId, 6, 0, 1'000'000'000, 3, true);
  acknowledge(taskId, 6, 2);
  EXPECT_FALSE(bufferManager_->isFinished(taskId));
  deleteResults(taskId, 6);
  EXPECT_FALSE(bufferManager_->isFinished(taskId));

  EXPECT_TRUE(task->isRunning());
  deleteResults(taskId, 3);
  EXPECT_TRUE(bufferManager_->isFinished(taskId));
  bufferManager_->removeTask(taskId);
  EXPECT_TRUE(task->isFinished());
}

TEST_F(PartitionedOutputBufferManagerTest, basicArbitrary) {
  const vector_size_t size = 100;
  int numDestinations = 5;
  const std::string taskId = "t0";
  auto task = initializeTask(
      taskId, rowType_, PartitionedOutputBuffer::Kind::kArbitrary, 5, 1);
  VELOX_ASSERT_THROW(
      enqueue(taskId, 1, rowType_, size), "(1 vs. 0) Bad destination 1");

  for (int i = 0; i < numDestinations; ++i) {
    enqueue(taskId, rowType_, size);
  }
  EXPECT_FALSE(bufferManager_->isFinished(taskId));

  std::unordered_map<int, int> ackedSeqbyDestination;

  for (int destination = 0; destination < numDestinations; destination++) {
    fetchOne(taskId, destination, 0, 1);
    acknowledge(taskId, destination, 1);
    ackedSeqbyDestination[destination] = 1;
    // Try to re-fetch already ack-ed data - should fail
    VELOX_ASSERT_THROW(fetchOne(taskId, destination, 0), "");
  }
  EXPECT_FALSE(bufferManager_->isFinished(taskId));

  bool receivedData0{false};
  bool receivedData1{false};
  registerForData(taskId, 0, 1, 1, receivedData0);
  registerForData(taskId, 1, 1, 1, receivedData1);
  enqueue(taskId, rowType_, size);
  enqueue(taskId, rowType_, size);
  ASSERT_TRUE(receivedData0);
  ASSERT_TRUE(receivedData1);
  acknowledge(taskId, 0, 2);
  ackedSeqbyDestination[0] = 2;
  acknowledge(taskId, 1, 2);
  ackedSeqbyDestination[1] = 2;
  bool receivedData{false};
  registerForData(taskId, 4, 1, 1, receivedData);
  enqueue(taskId, rowType_, size);
  enqueue(taskId, rowType_, size);
  ASSERT_TRUE(receivedData);
  acknowledge(taskId, 4, 2);
  ackedSeqbyDestination[4] = 2;

  bufferManager_->updateOutputBuffers(taskId, ++numDestinations, false);
  bufferManager_->updateOutputBuffers(taskId, ++numDestinations, false);
  fetchOneAndAck(taskId, numDestinations - 1, 0);
  ackedSeqbyDestination[numDestinations - 1] = 1;

  bufferManager_->updateOutputBuffers(taskId, numDestinations, true);
  VELOX_ASSERT_THROW(fetchOneAndAck(taskId, numDestinations, 0), "");

  receivedData = false;
  registerForData(taskId, numDestinations - 2, 0, 1, receivedData);
  enqueue(taskId, rowType_, size);
  ASSERT_TRUE(receivedData);
  acknowledge(taskId, numDestinations - 2, 1);
  ackedSeqbyDestination[numDestinations - 2] = 1;

  noMoreData(taskId);
  EXPECT_TRUE(task->isRunning());
  for (int i = 0; i < numDestinations; ++i) {
    fetchEndMarker(taskId, i, ackedSeqbyDestination[i]);
  }
  EXPECT_TRUE(bufferManager_->isFinished(taskId));

  EXPECT_FALSE(task->isRunning());
  EXPECT_TRUE(bufferManager_->isFinished(taskId));
  bufferManager_->removeTask(taskId);
  EXPECT_TRUE(task->isFinished());
}

TEST_F(
    PartitionedOutputBufferManagerTest,
    broadcastWithDynamicAddedDestination) {
  vector_size_t size = 100;

  std::string taskId = "t0";
  auto task = initializeTask(
      taskId, rowType_, PartitionedOutputBuffer::Kind::kBroadcast, 5, 1);

  const int numPages = 10;
  for (int i = 0; i < numPages; ++i) {
    enqueue(taskId, rowType_, size);
  }
  EXPECT_FALSE(bufferManager_->isFinished(taskId));

  for (int i = 0; i < numPages; ++i) {
    for (int destination = 0; destination < 5; ++destination) {
      fetchOneAndAck(taskId, destination, i);
    }
  }

  // Dynamic added destination.
  fetch(taskId, 5, 0, 1'000'000'000, 10);
  bufferManager_->updateOutputBuffers(taskId, 4, false);
  bufferManager_->updateOutputBuffers(taskId, 6, false);
  bufferManager_->updateOutputBuffers(taskId, 7, false);

  fetch(taskId, 6, 0, 1'000'000'000, 10);
  fetch(taskId, 7, 0, 1'000'000'000, 10);
  fetch(taskId, 8, 0, 1'000'000'000, 10);

  bufferManager_->updateOutputBuffers(taskId, 7, true);

  VELOX_ASSERT_THROW(fetch(taskId, 10, 0, 1'000'000'000, 10), "");

  ASSERT_FALSE(bufferManager_->isFinished(taskId));
  noMoreData(taskId);
  for (int i = 0; i < 9; ++i) {
    fetchEndMarker(taskId, i, numPages);
  }
  ASSERT_TRUE(bufferManager_->isFinished(taskId));

  bufferManager_->removeTask(taskId);
  EXPECT_TRUE(task->isFinished());
}

TEST_F(
    PartitionedOutputBufferManagerTest,
    arbitraryWithDynamicAddedDestination) {
  const vector_size_t size = 100;
  int numDestinations = 5;
  const std::string taskId = "t0";
  auto task = initializeTask(
      taskId, rowType_, PartitionedOutputBuffer::Kind::kArbitrary, 5, 1);

  for (int i = 0; i < numDestinations; ++i) {
    enqueue(taskId, rowType_, size);
  }
  std::unordered_map<int, int> ackedSeqbyDestination;

  for (int destination = 0; destination < 5; destination++) {
    fetchOne(taskId, destination, 0, 1);
    acknowledge(taskId, destination, 1);
    ackedSeqbyDestination[destination] = 1;
  }
  EXPECT_FALSE(bufferManager_->isFinished(taskId));

  // Dynamic added destination.
  enqueue(taskId, rowType_, size);
  fetchOneAndAck(taskId, numDestinations, 0);
  ackedSeqbyDestination[numDestinations] = 1;

  bufferManager_->updateOutputBuffers(taskId, numDestinations - 1, false);
  bufferManager_->updateOutputBuffers(taskId, numDestinations, false);
  bufferManager_->updateOutputBuffers(taskId, numDestinations + 1, false);

  enqueue(taskId, rowType_, size);
  fetchOneAndAck(taskId, numDestinations + 10, 0);
  ackedSeqbyDestination[numDestinations + 10] = 1;

  bufferManager_->updateOutputBuffers(taskId, numDestinations, true);

  enqueue(taskId, rowType_, size);
  fetchOneAndAck(taskId, numDestinations + 9, 0);
  ackedSeqbyDestination[numDestinations + 9] = 1;

  VELOX_ASSERT_THROW(
      fetch(taskId, numDestinations + 20, 0, 1'000'000'000, 1), "");

  ASSERT_FALSE(bufferManager_->isFinished(taskId));

  noMoreData(taskId);
  EXPECT_TRUE(task->isRunning());
  for (int i = 0; i <= numDestinations + 10; ++i) {
    fetchEndMarker(taskId, i, ackedSeqbyDestination[i]);
  }
  EXPECT_TRUE(bufferManager_->isFinished(taskId));

  EXPECT_FALSE(task->isRunning());
  EXPECT_TRUE(bufferManager_->isFinished(taskId));
  bufferManager_->removeTask(taskId);
  EXPECT_TRUE(task->isFinished());
}

TEST_P(AllPartitionedOutputBufferManagerTest, maxBytes) {
  const vector_size_t size = 100;
  const std::string taskId = "t0";
  initializeTask(taskId, rowType_, kind_, 1, 1);

  enqueue(taskId, 0, rowType_, size);
  enqueue(taskId, 0, rowType_, size);
  enqueue(taskId, 0, rowType_, size);

  // fetch up to 1Kb - 1 group
  fetchOne(taskId, 0, 0);
  // re-fetch with larger size limit - 2 groups
  fetch(taskId, 0, 0, std::numeric_limits<int64_t>::max(), 3);
  // re-fetch with 1Kb limit - 1 group
  fetchOneAndAck(taskId, 0, 0);
  fetchOneAndAck(taskId, 0, 1);
  fetchOneAndAck(taskId, 0, 2);

  if (kind_ != PartitionedOutputBuffer::Kind::kPartitioned) {
    bufferManager_->updateOutputBuffers(taskId, 0, true);
  }
  noMoreData(taskId);
  fetchEndMarker(taskId, 0, 3);
  bufferManager_->removeTask(taskId);
}

TEST_F(PartitionedOutputBufferManagerTest, outOfOrderAcks) {
  const vector_size_t size = 100;
  const std::string taskId = "t0";
  auto task = initializeTask(
      taskId, rowType_, PartitionedOutputBuffer::Kind::kPartitioned, 5, 1);

  enqueue(taskId, 0, rowType_, size);
  for (int i = 0; i < 10; i++) {
    enqueue(taskId, 1, rowType_, size);
  }

  // fetch first 3 groups
  fetchOne(taskId, 1, 0);
  fetchOne(taskId, 1, 1);
  fetchOne(taskId, 1, 2);

  // send acks out of order: 3, 1, 2
  acknowledge(taskId, 1, 3);
  acknowledge(taskId, 1, 1);
  acknowledge(taskId, 1, 2);

  // fetch and ack remaining 7 groups
  fetch(taskId, 1, 3, std::numeric_limits<uint64_t>::max(), 7);
  acknowledge(taskId, 1, 10);

  noMoreData(taskId);
  fetchEndMarker(taskId, 1, 10);
  task->requestCancel();
  bufferManager_->removeTask(taskId);
}

TEST_F(PartitionedOutputBufferManagerTest, errorInQueue) {
  auto queue = std::make_shared<ExchangeQueue>(1 << 20);
  auto page = std::make_unique<SerializedPage>(folly::IOBuf::copyBuffer("", 0));
  std::vector<ContinuePromise> promises;
  { queue->setError("error"); }
  for (auto& promise : promises) {
    promise.setValue();
  }
  ContinueFuture future;
  bool atEnd = false;
  EXPECT_THROW(
      auto page = queue->dequeueLocked(&atEnd, &future), std::runtime_error);
}

TEST_F(PartitionedOutputBufferManagerTest, setQueueErrorWithPendingPages) {
  const uint64_t kBufferSize = 128;
  auto iobuf = folly::IOBuf::create(kBufferSize);
  const std::string payload("setQueueErrorWithPendingPages");
  size_t payloadSize = payload.size();
  std::memcpy(iobuf->writableData(), payload.data(), payloadSize);
  iobuf->append(payloadSize);

  auto page = std::make_unique<SerializedPage>(std::move(iobuf));

  auto queue = std::make_shared<ExchangeQueue>(1 << 20);
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(queue->mutex());
    queue->enqueueLocked(std::move(page), promises);
  }

  queue->setError("error");

  // Expect a throw on dequeue after the queue has been set error.
  ContinueFuture future;
  bool atEnd = false;
  ASSERT_THROW(
      auto page = queue->dequeueLocked(&atEnd, &future), std::runtime_error);
}

TEST_F(PartitionedOutputBufferManagerTest, getDataOnFailedTask) {
  // Fetching data on a task which was either never initialized in the buffer
  // manager or was removed by a parallel thread must return false. The `notify`
  // callback must not be registered.
  ASSERT_FALSE(bufferManager_->getData(
      "test.0.1",
      1,
      10,
      1,
      [](std::vector<std::unique_ptr<folly::IOBuf>> pages, int64_t sequence) {
        VELOX_UNREACHABLE();
      }));
}

TEST_F(PartitionedOutputBufferManagerTest, updateBrodcastBufferOnFailedTask) {
  // Updating broadcast buffer count in the buffer manager for a given unknown
  // task must not throw exception, instead must return FALSE.
  ASSERT_FALSE(bufferManager_->updateBroadcastOutputBuffers(
      "test.0.1", /* unknown task */
      10,
      false));
}

TEST_P(AllPartitionedOutputBufferManagerTest, multiFetchers) {
  const std::vector<bool> earlyTerminations = {false, true};
  for (const auto earlyTermination : earlyTerminations) {
    SCOPED_TRACE(fmt::format("earlyTermination {}", earlyTermination));

    const vector_size_t size = 10;
    const std::string taskId = "t0";
    int numPartitions = 10;
    int extendedNumPartitions = numPartitions / 2;
    initializeTask(
        taskId,
        rowType_,
        kind_,
        numPartitions,
        1,
        kind_ == PartitionedOutputBuffer::Kind::kBroadcast ? 256 << 20 : 0);

    std::vector<std::thread> threads;
    std::vector<int64_t> fetchedPages(numPartitions + extendedNumPartitions, 0);
    for (size_t i = 0; i < numPartitions; ++i) {
      threads.emplace_back([&, i]() {
        dataFetcher(taskId, i, fetchedPages.at(i), earlyTermination);
      });
    }
    folly::Random::DefaultGenerator rng;
    rng.seed(1234);
    const int totalPages = 10'000;
    std::vector<int64_t> producedPages(
        numPartitions + extendedNumPartitions, 0);
    for (int i = 0; i < totalPages; ++i) {
      const int partition = kind_ == PartitionedOutputBuffer::Kind::kPartitioned
          ? folly::Random().rand32(rng) % numPartitions
          : 0;
      try {
        enqueue(taskId, partition, rowType_, size, true);
      } catch (...) {
        // Early termination might cause the task to fail early.
        ASSERT_TRUE(earlyTermination);
        break;
      }
      ++producedPages[partition];
      if (folly::Random().oneIn(4)) {
        std::this_thread::sleep_for(std::chrono::microseconds(5)); // NOLINT
      }
      if (i == 1000 && (kind_ != PartitionedOutputBuffer::Kind::kPartitioned)) {
        bufferManager_->updateOutputBuffers(
            taskId, numPartitions + extendedNumPartitions, false);
        for (size_t i = numPartitions;
             i < numPartitions + extendedNumPartitions;
             ++i) {
          threads.emplace_back([&, i]() {
            dataFetcher(taskId, i, fetchedPages.at(i), earlyTermination);
          });
          fetchedPages[i] = 0;
        }
        bufferManager_->updateOutputBuffers(taskId, 0, true);
      }
    }
    noMoreData(taskId);

    for (int i = 0; i < threads.size(); ++i) {
      threads[i].join();
    }

    if (!earlyTermination) {
      if (kind_ == PartitionedOutputBuffer::Kind::kPartitioned) {
        for (int i = 0; i < numPartitions; ++i) {
          ASSERT_EQ(fetchedPages[i], producedPages[i]);
        }
      } else if (kind_ == PartitionedOutputBuffer::Kind::kBroadcast) {
        int64_t totalFetchedPages{0};
        for (const auto& pages : fetchedPages) {
          totalFetchedPages += pages;
        }
        ASSERT_EQ(
            totalPages * (numPartitions + extendedNumPartitions),
            totalFetchedPages);
      } else {
        int64_t totalFetchedPages{0};
        for (const auto& pages : fetchedPages) {
          totalFetchedPages += pages;
        }
        ASSERT_EQ(totalPages, totalFetchedPages);
      }
    }
    bufferManager_->removeTask(taskId);
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    AllPartitionedOutputBufferManagerTestSuite,
    AllPartitionedOutputBufferManagerTest,
    testing::ValuesIn(AllPartitionedOutputBufferManagerTest::getTestParams()));
