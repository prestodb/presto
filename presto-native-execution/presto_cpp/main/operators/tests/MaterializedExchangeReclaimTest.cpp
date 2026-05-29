/*
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

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <thread>

#include "presto_cpp/main/operators/MaterializedOutputBuffer.h"
#include "velox/common/memory/Memory.h"

using namespace facebook::velox;
using namespace facebook::presto::operators;

namespace facebook::presto::operators::test {

using Reclaimer = MaterializedOutputBuffer::Reclaimer;

constexpr int64_t kMB = 1L << 20;

namespace {

class NoOpShuffleWriter : public ShuffleWriter {
 public:
  void collect(int32_t, std::string_view, std::string_view) override {}
  void noMoreData(bool) override {}
  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {};
  }
};

class NoOpShuffleFactory : public ShuffleInterfaceFactory {
 public:
  std::shared_ptr<ShuffleReader> createReader(
      const std::string&,
      int32_t,
      velox::memory::MemoryPool*) override {
    VELOX_UNSUPPORTED();
  }
  std::shared_ptr<ShuffleWriter> createWriter(
      const std::string&,
      velox::memory::MemoryPool*) override {
    return std::make_shared<NoOpShuffleWriter>();
  }
};

} // namespace

class ReclaimerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    memory::MemoryManager::testingSetInstance({});
    rootPool_ = memory::memoryManager()->addRootPool(
        "reclaimerTest", 256 * kMB, memory::MemoryReclaimer::create());
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
};

TEST_F(ReclaimerTest, emptyPoolReturnsImmediately) {
  NoOpShuffleFactory factory;
  MaterializedOutputBuffer buffer(
      /*numPartitions=*/1,
      /*shuffleWriterInfo=*/"",
      &factory,
      "test.0.0.0.0",
      rootPool_.get());
  Reclaimer reclaimer(&buffer);

  uint64_t reclaimableBytes = 0;
  EXPECT_FALSE(reclaimer.reclaimableBytes(*buffer.pool(), reclaimableBytes));
  EXPECT_EQ(reclaimableBytes, 0);

  memory::MemoryReclaimer::Stats stats;
  EXPECT_EQ(reclaimer.reclaim(buffer.pool(), 10 * kMB, 5000, stats), 0);
}

TEST_F(ReclaimerTest, reclaimableReportsPoolUsage) {
  NoOpShuffleFactory factory;
  MaterializedOutputBuffer buffer(
      /*numPartitions=*/1,
      /*shuffleWriterInfo=*/"",
      &factory,
      "test.0.0.0.0",
      rootPool_.get());
  Reclaimer reclaimer(&buffer);

  void* allocation = buffer.pool()->allocate(10 * kMB);

  uint64_t reclaimableBytes = 0;
  EXPECT_TRUE(reclaimer.reclaimableBytes(*buffer.pool(), reclaimableBytes));
  EXPECT_EQ(reclaimableBytes, 10 * kMB);

  buffer.pool()->free(allocation, 10 * kMB);
}

TEST_F(ReclaimerTest, fullDrainFromBackgroundThread) {
  NoOpShuffleFactory factory;
  MaterializedOutputBuffer buffer(
      /*numPartitions=*/1,
      /*shuffleWriterInfo=*/"",
      &factory,
      "test.0.0.0.0",
      rootPool_.get());
  Reclaimer reclaimer(&buffer);

  void* allocation = buffer.pool()->allocate(10 * kMB);

  std::thread drainer([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    buffer.pool()->free(allocation, 10 * kMB);
  });

  memory::MemoryReclaimer::Stats stats;
  auto freedBytes = reclaimer.reclaim(buffer.pool(), 10 * kMB, 5000, stats);
  drainer.join();

  EXPECT_EQ(freedBytes, 10 * kMB);
  EXPECT_EQ(buffer.pool()->usedBytes(), 0);
}

TEST_F(ReclaimerTest, partialDrainBeforeTimeout) {
  NoOpShuffleFactory factory;
  MaterializedOutputBuffer buffer(
      /*numPartitions=*/1,
      /*shuffleWriterInfo=*/"",
      &factory,
      "test.0.0.0.0",
      rootPool_.get());
  Reclaimer reclaimer(&buffer);

  void* allocation1 = buffer.pool()->allocate(5 * kMB);
  void* allocation2 = buffer.pool()->allocate(5 * kMB);

  std::thread drainer([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    buffer.pool()->free(allocation1, 5 * kMB);
  });

  memory::MemoryReclaimer::Stats stats;
  auto freedBytes = reclaimer.reclaim(buffer.pool(), 10 * kMB, 200, stats);
  drainer.join();

  EXPECT_EQ(freedBytes, 5 * kMB);
  EXPECT_EQ(buffer.pool()->usedBytes(), 5 * kMB);

  buffer.pool()->free(allocation2, 5 * kMB);
}

TEST_F(ReclaimerTest, timeoutReturnsZeroWhenNothingDrains) {
  NoOpShuffleFactory factory;
  MaterializedOutputBuffer buffer(
      /*numPartitions=*/1,
      /*shuffleWriterInfo=*/"",
      &factory,
      "test.0.0.0.0",
      rootPool_.get());
  Reclaimer reclaimer(&buffer);

  void* allocation = buffer.pool()->allocate(10 * kMB);

  memory::MemoryReclaimer::Stats stats;
  auto freedBytes = reclaimer.reclaim(buffer.pool(), 10 * kMB, 100, stats);

  EXPECT_EQ(freedBytes, 0);
  EXPECT_EQ(buffer.pool()->usedBytes(), 10 * kMB);

  buffer.pool()->free(allocation, 10 * kMB);
}

TEST_F(ReclaimerTest, zeroWaitReturnsImmediately) {
  NoOpShuffleFactory factory;
  MaterializedOutputBuffer buffer(
      /*numPartitions=*/1,
      /*shuffleWriterInfo=*/"",
      &factory,
      "test.0.0.0.0",
      rootPool_.get());
  Reclaimer reclaimer(&buffer);

  void* allocation = buffer.pool()->allocate(10 * kMB);

  memory::MemoryReclaimer::Stats stats;
  auto freedBytes = reclaimer.reclaim(buffer.pool(), 10 * kMB, 0, stats);

  EXPECT_EQ(freedBytes, 0);

  buffer.pool()->free(allocation, 10 * kMB);
}

TEST_F(ReclaimerTest, targetLargerThanUsage) {
  NoOpShuffleFactory factory;
  MaterializedOutputBuffer buffer(
      /*numPartitions=*/1,
      /*shuffleWriterInfo=*/"",
      &factory,
      "test.0.0.0.0",
      rootPool_.get());
  Reclaimer reclaimer(&buffer);

  void* allocation = buffer.pool()->allocate(5 * kMB);

  std::thread drainer([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    buffer.pool()->free(allocation, 5 * kMB);
  });

  memory::MemoryReclaimer::Stats stats;
  auto freedBytes = reclaimer.reclaim(buffer.pool(), 100 * kMB, 5000, stats);
  drainer.join();

  EXPECT_EQ(freedBytes, 5 * kMB);
  EXPECT_EQ(buffer.pool()->usedBytes(), 0);
}

TEST_F(ReclaimerTest, incrementalDrain) {
  NoOpShuffleFactory factory;
  MaterializedOutputBuffer buffer(
      /*numPartitions=*/1,
      /*shuffleWriterInfo=*/"",
      &factory,
      "test.0.0.0.0",
      rootPool_.get());
  Reclaimer reclaimer(&buffer);

  void* allocation1 = buffer.pool()->allocate(3 * kMB);
  void* allocation2 = buffer.pool()->allocate(3 * kMB);
  void* allocation3 = buffer.pool()->allocate(4 * kMB);

  std::thread drainer([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    buffer.pool()->free(allocation1, 3 * kMB);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    buffer.pool()->free(allocation2, 3 * kMB);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    buffer.pool()->free(allocation3, 4 * kMB);
  });

  memory::MemoryReclaimer::Stats stats;
  auto freedBytes = reclaimer.reclaim(buffer.pool(), 10 * kMB, 5000, stats);
  drainer.join();

  EXPECT_EQ(freedBytes, 10 * kMB);
  EXPECT_EQ(buffer.pool()->usedBytes(), 0);
}

TEST_F(ReclaimerTest, consecutiveReclaims) {
  NoOpShuffleFactory factory;
  MaterializedOutputBuffer buffer(
      /*numPartitions=*/1,
      /*shuffleWriterInfo=*/"",
      &factory,
      "test.0.0.0.0",
      rootPool_.get());
  Reclaimer reclaimer(&buffer);

  void* allocation1 = buffer.pool()->allocate(5 * kMB);

  std::thread drainer1([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    buffer.pool()->free(allocation1, 5 * kMB);
  });

  memory::MemoryReclaimer::Stats stats;
  auto freedBytes1 = reclaimer.reclaim(buffer.pool(), 5 * kMB, 5000, stats);
  drainer1.join();
  EXPECT_EQ(freedBytes1, 5 * kMB);

  void* allocation2 = buffer.pool()->allocate(8 * kMB);

  std::thread drainer2([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    buffer.pool()->free(allocation2, 8 * kMB);
  });

  auto freedBytes2 = reclaimer.reclaim(buffer.pool(), 8 * kMB, 5000, stats);
  drainer2.join();
  EXPECT_EQ(freedBytes2, 8 * kMB);
}

TEST_F(ReclaimerTest, reclaimSkippedAfterClose) {
  NoOpShuffleFactory factory;
  MaterializedOutputBuffer buffer(
      /*numPartitions=*/1,
      /*shuffleWriterInfo=*/"",
      &factory,
      "test.0.0.0.0",
      rootPool_.get());
  Reclaimer reclaimer(&buffer);

  void* allocation = buffer.pool()->allocate(10 * kMB);

  buffer.noMoreData();
  EXPECT_EQ(buffer.state(), MaterializedOutputBuffer::State::kClosed);

  memory::MemoryReclaimer::Stats stats;
  auto freedBytes = reclaimer.reclaim(buffer.pool(), 10 * kMB, 100, stats);
  EXPECT_EQ(freedBytes, 0);

  buffer.pool()->free(allocation, 10 * kMB);
}

TEST_F(ReclaimerTest, reclaimBlockedWhenAborted) {
  NoOpShuffleFactory factory;
  MaterializedOutputBuffer buffer(
      /*numPartitions=*/1,
      /*shuffleWriterInfo=*/"",
      &factory,
      "test.0.0.0.0",
      rootPool_.get());
  Reclaimer reclaimer(&buffer);

  void* allocation = buffer.pool()->allocate(10 * kMB);

  buffer.abort();
  EXPECT_EQ(buffer.state(), MaterializedOutputBuffer::State::kAborted);

  memory::MemoryReclaimer::Stats stats;
  auto freedBytes = reclaimer.reclaim(buffer.pool(), 10 * kMB, 100, stats);
  EXPECT_EQ(freedBytes, 0);

  buffer.pool()->free(allocation, 10 * kMB);
}

} // namespace facebook::presto::operators::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init follyInit{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
