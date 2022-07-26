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
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

using facebook::velox::test::BatchMaker;

class PartitionedOutputBufferManagerTest : public testing::Test {
 protected:
  void SetUp() override {
    pool_ = facebook::velox::memory::getDefaultScopedMemoryPool();
    mappedMemory_ = memory::MappedMemory::getInstance();
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
      int numDestinations,
      int numDrivers) {
    bufferManager_->removeTask(taskId);

    auto planFragment = exec::test::PlanBuilder()
                            .values({std::dynamic_pointer_cast<RowVector>(
                                BatchMaker::createBatch(rowType, 100, *pool_))})
                            .planFragment();
    auto task = std::make_shared<Task>(
        taskId, std::move(planFragment), 0, core::QueryCtx::createForTest());

    bufferManager_->initializeTask(task, false, numDestinations, numDrivers);
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
    auto data = std::make_unique<VectorStreamGroup>(mappedMemory_);
    auto size = vector->size();
    auto range = IndexRange{0, size};
    data->createStreamTree(
        std::dynamic_pointer_cast<const RowType>(vector->type()), size);
    data->append(
        std::dynamic_pointer_cast<RowVector>(vector), folly::Range(&range, 1));
    auto listener = bufferManager_->newListener();
    IOBufOutputStream stream(*mappedMemory_, listener.get(), data->size());
    data->flush(&stream);
    return std::make_unique<SerializedPage>(stream.getIOBuf());
  }

  void enqueue(
      const std::string& taskId,
      int destination,
      std::shared_ptr<const RowType> rowType,
      vector_size_t size) {
    ContinueFuture future;
    auto blockingReason = bufferManager_->enqueue(
        taskId, destination, makeSerializedPage(rowType, size), &future);
    ASSERT_EQ(blockingReason, BlockingReason::kNotBlocked);
  }

  void noMoreData(const std::string& taskId) {
    bufferManager_->noMoreData(taskId);
  }

  void fetch(
      const std::string& taskId,
      int destination,
      int64_t sequence,
      uint64_t maxBytes = 1024,
      int expectedGroups = 1) {
    bool receivedData = false;
    bufferManager_->getData(
        taskId,
        destination,
        maxBytes,
        sequence,
        [destination, sequence, expectedGroups, &receivedData](
            std::vector<std::unique_ptr<folly::IOBuf>> pages,
            int64_t inSequence) {
          EXPECT_FALSE(receivedData) << "for destination " << destination;
          EXPECT_EQ(pages.size(), expectedGroups)
              << "for destination " << destination;
          for (const auto& page : pages) {
            EXPECT_TRUE(page != nullptr) << "for destination " << destination;
          }
          EXPECT_EQ(inSequence, sequence) << "for destination " << destination;
          receivedData = true;
        });
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
    bufferManager_->getData(
        taskId,
        destination,
        std::numeric_limits<uint64_t>::max(),
        sequence,
        receiveEndMarker(destination, sequence, receivedData));
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
    bufferManager_->getData(
        taskId,
        destination,
        std::numeric_limits<uint64_t>::max(),
        sequence,
        receiveEndMarker(destination, 1, receivedEndMarker));
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
    bufferManager_->getData(
        taskId,
        destination,
        1024,
        sequence,
        receiveData(destination, sequence, expectedGroups, receivedData));
    EXPECT_FALSE(receivedData) << "for destination " << destination;
  }

  std::unique_ptr<facebook::velox::memory::ScopedMemoryPool> pool_;
  memory::MappedMemory* mappedMemory_;
  std::shared_ptr<PartitionedOutputBufferManager> bufferManager_;
};

TEST_F(PartitionedOutputBufferManagerTest, basic) {
  std::vector<std::string> names = {"c0", "c1"};
  std::vector<TypePtr> types = {BIGINT(), VARCHAR()};
  auto rowType = ROW(std::move(names), std::move(types));
  vector_size_t size = 100;

  std::string taskId = "t0";
  auto task = initializeTask(taskId, rowType, 5, 1);

  // - enqueue one group per destination
  // - fetch and ask one group per destination
  // - enqueue one more group for destinations 0-2
  // - fetch and ask one group for destinations 0-2
  // - register end-marker callback for destination 3 and data callback for 4
  // - enqueue data for 4 and assert callback was called
  // - enqueue end marker for all destinations
  // - assert callback was called for destination 3
  // - fetch end markers for destinations 0-2 and 4

  for (int destination = 0; destination < 5; destination++) {
    enqueue(taskId, destination, rowType, size);
  }
  EXPECT_FALSE(bufferManager_->isFinished(taskId));

  for (int destination = 0; destination < 5; destination++) {
    fetchOneAndAck(taskId, destination, 0);
    // Try to re-fetch already ack-ed data - should fail
    EXPECT_THROW(fetchOne(taskId, destination, 0), std::exception);
  }
  EXPECT_FALSE(bufferManager_->isFinished(taskId));

  for (int destination = 0; destination < 3; destination++) {
    enqueue(taskId, destination, rowType, size);
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

  enqueue(taskId, 4, rowType, size);
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

  EXPECT_TRUE(task->isFinished());
}

TEST_F(PartitionedOutputBufferManagerTest, maxBytes) {
  std::vector<std::string> names = {"c0", "c1"};
  std::vector<TypePtr> types = {BIGINT(), VARCHAR()};
  auto rowType = ROW(std::move(names), std::move(types));
  vector_size_t size = 100;

  std::string taskId = "t0";
  initializeTask(taskId, rowType, 5, 1);

  enqueue(taskId, 0, rowType, size);
  enqueue(taskId, 1, rowType, size);
  enqueue(taskId, 1, rowType, size);

  fetchOneAndAck(taskId, 0, 0);

  // fetch up to 1Kb - 1 group
  fetchOne(taskId, 1, 0);
  // re-fetch with larger size limit - 2 groups
  fetch(taskId, 1, 0, std::numeric_limits<int64_t>::max(), 2);
  // re-fetch with 1Kb limit - 1 group
  fetchOneAndAck(taskId, 1, 0);
  fetchOneAndAck(taskId, 1, 1);

  noMoreData(taskId);
  fetchEndMarker(taskId, 0, 1);
  fetchEndMarker(taskId, 1, 2);
  for (int destination = 2; destination < 5; destination++) {
    fetchEndMarker(taskId, destination, 0);
  }
}

TEST_F(PartitionedOutputBufferManagerTest, outOfOrderAcks) {
  std::vector<std::string> names = {"c0", "c1"};
  std::vector<TypePtr> types = {BIGINT(), VARCHAR()};
  auto rowType = ROW(std::move(names), std::move(types));
  vector_size_t size = 100;

  std::string taskId = "t0";
  auto task = initializeTask(taskId, rowType, 5, 1);

  enqueue(taskId, 0, rowType, size);
  for (int i = 0; i < 10; i++) {
    enqueue(taskId, 1, rowType, size);
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
  {
    std::lock_guard<std::mutex> l(queue->mutex());
    queue->setErrorLocked("error");
  }
  ContinueFuture future;
  bool atEnd = false;
  EXPECT_THROW(auto page = queue->dequeue(&atEnd, &future), std::runtime_error);
}

TEST_F(PartitionedOutputBufferManagerTest, serializedPage) {
  auto iobuf = folly::IOBuf::create(128);
  std::string payload = "abcdefghijklmnopq";
  size_t payloadSize = payload.size();
  std::memcpy(iobuf->writableData(), payload.data(), payloadSize);
  iobuf->append(payloadSize);

  EXPECT_EQ(0, pool_->getCurrentBytes());
  {
    auto serializedPage =
        std::make_shared<SerializedPage>(std::move(iobuf), pool_.get());
    EXPECT_EQ(payloadSize, pool_->getCurrentBytes());
  }
  EXPECT_EQ(0, pool_->getCurrentBytes());
}
