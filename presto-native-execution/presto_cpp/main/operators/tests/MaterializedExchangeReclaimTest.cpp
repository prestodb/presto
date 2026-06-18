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

constexpr int64_t kKB = 1L << 10;
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

TEST_F(ReclaimerTest, reclaimableReportsBufferedBytes) {
  NoOpShuffleFactory factory;
  MaterializedOutputBuffer buffer(
      /*numPartitions=*/1,
      /*shuffleWriterInfo=*/"",
      &factory,
      "test.0.0.0.0",
      rootPool_.get());
  Reclaimer reclaimer(&buffer);

  // Enqueue data above reclaimDrainThreshold (87KB) but below drainThreshold
  // (130KB) so it stays buffered and is reported as reclaimable.
  auto iobuf = buffer.allocateTrackedIOBuf(100 * kKB);
  iobuf->append(100 * kKB);
  buffer.enqueue(0, std::move(iobuf));

  EXPECT_GT(buffer.bufferedBytes(), 0);
  uint64_t reclaimableBytes = 0;
  EXPECT_TRUE(reclaimer.reclaimableBytes(*buffer.pool(), reclaimableBytes));
  EXPECT_GT(reclaimableBytes, 0);
}

TEST_F(ReclaimerTest, reclaimFlushesBufferedData) {
  NoOpShuffleFactory factory;
  MaterializedOutputBuffer buffer(
      /*numPartitions=*/1,
      /*shuffleWriterInfo=*/"",
      &factory,
      "test.0.0.0.0",
      rootPool_.get());
  Reclaimer reclaimer(&buffer);

  // Enqueue data above reclaimDrainThreshold so reclaim flushes it.
  auto iobuf = buffer.allocateTrackedIOBuf(100 * kKB);
  iobuf->append(100 * kKB);
  buffer.enqueue(0, std::move(iobuf));

  auto prevUsed = buffer.pool()->usedBytes();
  EXPECT_GT(prevUsed, 0);

  memory::MemoryReclaimer::Stats stats;
  auto freedBytes = reclaimer.reclaim(buffer.pool(), 100 * kKB, 5000, stats);
  EXPECT_GT(freedBytes, 0);
}

TEST_F(ReclaimerTest, reclaimReturnsImmediatelyWhenNothingBuffered) {
  NoOpShuffleFactory factory;
  MaterializedOutputBuffer buffer(
      /*numPartitions=*/1,
      /*shuffleWriterInfo=*/"",
      &factory,
      "test.0.0.0.0",
      rootPool_.get());
  Reclaimer reclaimer(&buffer);

  // Allocate directly from the pool (not via enqueue). The reclaimer cannot
  // free this because it only knows about partition buffers.
  void* allocation = buffer.pool()->allocate(10 * kMB);

  memory::MemoryReclaimer::Stats stats;
  auto freedBytes = reclaimer.reclaim(buffer.pool(), 10 * kMB, 100, stats);
  EXPECT_EQ(freedBytes, 0);

  buffer.pool()->free(allocation, 10 * kMB);
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

} // namespace facebook::presto::operators::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init follyInit{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
