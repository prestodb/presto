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

#include "folly/Random.h"
#include "folly/futures/Barrier.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/memory/MemoryUsageTracker.h"
#include "velox/common/testutil/TestValue.h"

using namespace ::testing;
using namespace ::facebook::velox::memory;
using namespace ::facebook::velox;
using namespace ::facebook::velox::common::testutil;

class MemoryUsageTrackerTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    TestValue::enable();
  }
};

TEST_F(MemoryUsageTrackerTest, constructor) {
  std::vector<std::shared_ptr<MemoryUsageTracker>> trackers;
  auto tracker = MemoryUsageTracker::create();
  trackers.push_back(tracker);
  trackers.push_back(tracker->addChild());
  trackers.push_back(tracker->addChild());

  for (unsigned i = 0; i < trackers.size(); ++i) {
    ASSERT_EQ(trackers[i]->currentBytes(), 0);
    ASSERT_EQ(trackers[i]->peakBytes(), 0);
  }
  for (int32_t i = 0; i < trackers.size(); ++i) {
    const MemoryUsageTracker::Stats stats = trackers[i]->stats();
    if (i == 0) {
      ASSERT_EQ(stats.numChildren, 2);
    } else {
      ASSERT_EQ(stats.numChildren, 0);
    }
    ASSERT_EQ(stats.peakBytes, 0);
    ASSERT_EQ(stats.cumulativeBytes, 0);
  }
}

TEST_F(MemoryUsageTrackerTest, stats) {
  constexpr int64_t kMaxSize = 1 << 30; // 1GB
  constexpr int64_t kMB = 1 << 20;
  auto parent = MemoryUsageTracker::create(kMaxSize);

  auto child = parent->addChild();

  child->update(1000);
  child->update(8 * kMB);
  child->update(-(8 * kMB));
  ASSERT_EQ(
      parent->stats().toString(),
      "peakBytes:9437184 cumulativeBytes:9437184 numAllocs:0 numFrees:0 numReserves:0 numReleases:0 numCollisions:0 numChildren:1");
  ASSERT_EQ(
      child->stats().toString(),
      "peakBytes:9437184 cumulativeBytes:9437184 numAllocs:2 numFrees:1 numReserves:0 numReleases:0 numCollisions:0 numChildren:0");
  ASSERT_EQ(
      child->toString(),
      "<tracker used 1000B available 1023.02KB limit 1.00GB reservation [used 1000B, granted 1.00MB, reserved 1.00MB, min 0B] counters [allocs 2, frees 1, reserves 0, releases 0, collisions 0, children 0])>");
  child->update(-1000);
  ASSERT_EQ(
      child->toString(),
      "<tracker used 0B available 0B limit 1.00GB reservation [used 0B, granted 0B, reserved 0B, min 0B] counters [allocs 2, frees 2, reserves 0, releases 0, collisions 0, children 0])>");
}

TEST_F(MemoryUsageTrackerTest, update) {
  constexpr int64_t kMaxSize = 1 << 30; // 1GB
  constexpr int64_t kMB = 1 << 20;
  auto parent = MemoryUsageTracker::create(kMaxSize);

  auto child1 = parent->addChild();
  auto child2 = parent->addChild();

  ASSERT_THROW(child1->reserve(2 * kMaxSize), VeloxRuntimeError);

  ASSERT_EQ(0, parent->currentBytes());
  ASSERT_EQ(0, parent->cumulativeBytes());
  ASSERT_EQ(0, parent->usedReservationBytes());
  child1->update(1000);
  ASSERT_EQ(1000, child1->usedReservationBytes());
  ASSERT_EQ(kMB, parent->currentBytes());
  ASSERT_EQ(0, parent->usedReservationBytes());
  ASSERT_EQ(kMB, parent->cumulativeBytes());
  ASSERT_EQ(kMB - 1000, child1->availableReservation());
  child1->update(1000);
  ASSERT_EQ(2000, child1->usedReservationBytes());
  ASSERT_EQ(kMB, parent->currentBytes());
  ASSERT_EQ(0, parent->usedReservationBytes());
  ASSERT_EQ(kMB, parent->cumulativeBytes());
  child1->update(kMB);
  ASSERT_EQ(2000 + kMB, child1->usedReservationBytes());
  ASSERT_EQ(2 * kMB, parent->currentBytes());
  ASSERT_EQ(2 * kMB, parent->cumulativeBytes());

  child1->update(100 * kMB);
  ASSERT_EQ(2000 + 101 * kMB, child1->usedReservationBytes());
  // Larger sizes round up to next 8MB.
  ASSERT_EQ(104 * kMB, parent->currentBytes());
  ASSERT_EQ(0, parent->usedReservationBytes());
  ASSERT_EQ(104 * kMB, parent->cumulativeBytes());

  child1->update(-kMB);
  ASSERT_EQ(2000 + 100 * kMB, child1->usedReservationBytes());
  // 1MB less does not decrease the reservation.
  ASSERT_EQ(104 * kMB, parent->currentBytes());
  ASSERT_EQ(104 * kMB, parent->cumulativeBytes());

  child1->update(-7 * kMB);
  ASSERT_EQ(2000 + 93 * kMB, child1->usedReservationBytes());
  ASSERT_EQ(96 * kMB, parent->currentBytes());
  ASSERT_EQ(104 * kMB, parent->cumulativeBytes());

  child1->update(-92 * kMB);
  ASSERT_EQ(2000 + kMB, child1->usedReservationBytes());
  ASSERT_EQ(2 * kMB, parent->currentBytes());
  ASSERT_EQ(104 * kMB, parent->cumulativeBytes());

  child1->update(-kMB);
  ASSERT_EQ(2000, child1->usedReservationBytes());
  ASSERT_EQ(kMB, parent->currentBytes());
  ASSERT_EQ(104 * kMB, parent->cumulativeBytes());

  child1->update(-2000);
  ASSERT_EQ(0, child1->usedReservationBytes());
  ASSERT_EQ(0, parent->currentBytes());
  ASSERT_EQ(104 * kMB, parent->cumulativeBytes());

  MemoryUsageTracker::Stats expectedStats;
  MemoryUsageTracker::Stats stats = child2->stats();
  ASSERT_EQ(stats, expectedStats);
  expectedStats.peakBytes = 109051904;
  expectedStats.cumulativeBytes = 109051904;
  expectedStats.numAllocs = 4;
  expectedStats.numFrees = 5;
  expectedStats.numReserves = 1;
  stats = child1->stats();
  ASSERT_EQ(stats, expectedStats);
  stats = parent->stats();
  expectedStats.numAllocs = 0;
  expectedStats.numFrees = 0;
  expectedStats.numReserves = 0;
  expectedStats.numChildren = 2;
  ASSERT_EQ(stats, expectedStats);

  child1->release();
  stats = parent->stats();
  ASSERT_EQ(stats, expectedStats);

  stats = child1->stats();
  expectedStats.numChildren = 0;
  expectedStats.numReserves = 1;
  expectedStats.numReleases = 1;
  expectedStats.numAllocs = 4;
  expectedStats.numFrees = 5;
  ASSERT_EQ(stats, expectedStats);

  child1->update(0);
  ASSERT_EQ(child1->stats(), stats);
}

TEST_F(MemoryUsageTrackerTest, reserve) {
  constexpr int64_t kMaxSize = 1 << 30;
  constexpr int64_t kMB = 1 << 20;
  auto parent = MemoryUsageTracker::create(kMaxSize);

  auto child = parent->addChild();

  EXPECT_THROW(child->reserve(2 * kMaxSize), VeloxRuntimeError);

  child->reserve(100 * kMB);
  EXPECT_EQ(0, child->currentBytes());
  // The reservation child shows up as a reservation on the child and as an
  // allocation on the parent.
  EXPECT_EQ(104 * kMB, child->availableReservation());
  EXPECT_EQ(0, child->currentBytes());
  EXPECT_EQ(104 * kMB, parent->currentBytes());
  child->update(60 * kMB);
  EXPECT_EQ(60 * kMB, child->usedReservationBytes());
  EXPECT_EQ(60 * kMB, child->currentBytes());
  EXPECT_EQ(104 * kMB, parent->currentBytes());
  ASSERT_EQ(0, parent->usedReservationBytes());
  EXPECT_EQ((104 - 60) * kMB, child->availableReservation());
  child->update(70 * kMB);
  EXPECT_EQ(130 * kMB, child->usedReservationBytes());
  // Extended and rounded up the reservation to then next 8MB.
  EXPECT_EQ(136 * kMB, parent->currentBytes());
  ASSERT_EQ(0, parent->usedReservationBytes());
  child->update(-130 * kMB);
  // The reservation goes down to the explicitly made reservation.
  EXPECT_EQ(104 * kMB, parent->currentBytes());
  EXPECT_EQ(104 * kMB, child->availableReservation());
  EXPECT_EQ(0, child->usedReservationBytes());
  child->release();
  EXPECT_EQ(0, parent->currentBytes());

  MemoryUsageTracker::Stats stats = parent->stats();
  ASSERT_EQ(stats.numReserves, 0);
  ASSERT_EQ(stats.numReleases, 0);
  ASSERT_EQ(stats.numCollisions, 0);
  stats = child->stats();
  ASSERT_EQ(stats.numReserves, 2);
  ASSERT_EQ(stats.numReleases, 1);
  ASSERT_EQ(stats.numCollisions, 0);
  child->release();
  stats = child->stats();
  ASSERT_EQ(stats.numReserves, 2);
  ASSERT_EQ(stats.numReleases, 2);
  ASSERT_EQ(stats.numCollisions, 0);
  child->reserve(0);
  ASSERT_EQ(child->stats(), stats);
  child->release();
  ++stats.numReleases;
  ASSERT_EQ(child->stats(), stats);
}

TEST_F(MemoryUsageTrackerTest, reserveAndUpdate) {
  constexpr int64_t kMaxSize = 1 << 30; // 1GB
  constexpr int64_t kMB = 1 << 20;
  auto parent = MemoryUsageTracker::create(kMaxSize);

  auto child = parent->addChild();

  child->update(1000);
  ASSERT_EQ(parent->usedReservationBytes(), 0);
  EXPECT_EQ(kMB, parent->currentBytes());
  ASSERT_EQ(child->usedReservationBytes(), 1000);
  EXPECT_EQ(kMB - 1000, child->availableReservation());
  child->update(1000);
  ASSERT_EQ(child->usedReservationBytes(), 2000);
  EXPECT_EQ(kMB, parent->currentBytes());
  ASSERT_EQ(parent->usedReservationBytes(), 0);

  child->reserve(kMB);
  ASSERT_EQ(child->usedReservationBytes(), 2000);
  EXPECT_EQ(2 * kMB, parent->currentBytes());
  ASSERT_EQ(parent->usedReservationBytes(), 0);
  child->update(kMB);
  ASSERT_EQ(child->usedReservationBytes(), 2000 + kMB);

  // release has no effect  since usage within quantum of reservation.
  child->release();
  EXPECT_EQ(2 * kMB, parent->currentBytes());
  EXPECT_EQ(2000 + kMB, child->currentBytes());
  EXPECT_EQ(kMB - 2000, child->availableReservation());
  ASSERT_EQ(child->usedReservationBytes(), 2000 + kMB);

  // We reserve 20MB, consume 9MB and release the unconsumed.
  child->reserve(20 * kMB);
  ASSERT_EQ(child->usedReservationBytes(), 2000 + kMB);
  // 22 rounded up to 24.
  EXPECT_EQ(24 * kMB, parent->currentBytes());
  child->update(7 * kMB);
  ASSERT_EQ(child->usedReservationBytes(), 2000 + 8 * kMB);
  EXPECT_EQ(16 * kMB - 2000, child->availableReservation());
  child->release();
  ASSERT_EQ(child->usedReservationBytes(), 2000 + 8 * kMB);
  EXPECT_EQ(kMB - 2000, child->availableReservation());
  EXPECT_EQ(9 * kMB, parent->currentBytes());

  // We reserve another 20 MB, consume 25 and release nothing because
  // reservation is already taken.
  child->reserve(20 * kMB);
  child->update(25 * kMB);
  ASSERT_EQ(child->usedReservationBytes(), 2000 + 33 * kMB);
  EXPECT_EQ(36 * kMB, parent->currentBytes());
  EXPECT_EQ(3 * kMB - 2000, child->availableReservation());
  child->release();

  // Nothing changed by release since already over the explicit reservation.
  EXPECT_EQ(36 * kMB, parent->currentBytes());
  EXPECT_EQ(3 * kMB - 2000, child->availableReservation());
  ASSERT_EQ(child->usedReservationBytes(), 2000 + 33 * kMB);
  ASSERT_EQ(parent->usedReservationBytes(), 0);

  // We reserve 20MB and free 5MB and release. Expect 25MB drop.
  child->reserve(20 * kMB);
  child->update(-5 * kMB);
  EXPECT_EQ(28 * kMB - 2000, child->availableReservation());
  EXPECT_EQ(56 * kMB, parent->currentBytes());
  ASSERT_EQ(child->usedReservationBytes(), 2000 + 28 * kMB);
  ASSERT_EQ(parent->usedReservationBytes(), 0);

  // Reservation drops by 25, rounded to  quantized size of 32.
  child->release();

  EXPECT_EQ(32 * kMB, parent->currentBytes());
  EXPECT_EQ(4 * kMB - 2000, child->availableReservation());
  ASSERT_EQ(child->usedReservationBytes(), 2000 + 28 * kMB);
  ASSERT_EQ(parent->usedReservationBytes(), 0);

  // We reserve 20MB, allocate 25 and free 15
  child->reserve(20 * kMB);
  child->update(25 * kMB);
  EXPECT_EQ(56 * kMB, parent->currentBytes());
  EXPECT_EQ(3 * kMB - 2000, child->availableReservation());
  ASSERT_EQ(child->usedReservationBytes(), 2000 + 53 * kMB);
  ASSERT_EQ(parent->usedReservationBytes(), 0);

  child->update(-15 * kMB);

  // There is 14MB - 2000  of available reservation because the reservation does
  // not drop below the bar set in reserve(). The used size reflected in the
  // parent drops a little to match the level given in reserver().
  EXPECT_EQ(52 * kMB, parent->currentBytes());
  EXPECT_EQ(14 * kMB - 2000, child->availableReservation());
  ASSERT_EQ(child->usedReservationBytes(), 2000 + 38 * kMB);
  ASSERT_EQ(parent->usedReservationBytes(), 0);

  // The unused reservation is freed.
  child->release();
  EXPECT_EQ(40 * kMB, parent->currentBytes());
  EXPECT_EQ(2 * kMB - 2000, child->availableReservation());
  ASSERT_EQ(child->usedReservationBytes(), 2000 + 38 * kMB);
  ASSERT_EQ(parent->usedReservationBytes(), 0);

  // Free the pending reserved bytes before destruction.
  child->update(-child->usedReservationBytes());
}

namespace {
// Model implementation of a GrowCallback.
bool grow(int64_t /*size*/, int64_t hardLimit, MemoryUsageTracker& tracker) {
  static std::mutex mutex;
  // The calls from different threads on the same tracker must be serialized.
  std::lock_guard<std::mutex> l(mutex);
  // The total includes the allocation that exceeded the limit. This function's
  // job is to raise the limit to >= current.
  auto current = tracker.reservedBytes();
  auto limit = tracker.maxMemory();
  if (current <= limit) {
    // No need to increase. It could be another thread already
    // increased the cap far enough while this thread was waiting to
    // enter the lock_guard.
    return true;
  }
  if (current > hardLimit) {
    // The caller will revert the allocation that called this and signal an
    // error.
    return false;
  }
  // We set the new limit to be the requested size.
  tracker.testingUpdateMaxMemory(current);
  return true;
}
} // namespace

TEST_F(MemoryUsageTrackerTest, grow) {
  constexpr int64_t kMB = 1 << 20;
  auto parent = MemoryUsageTracker::create(10 * kMB);

  auto child = parent->addChild();
  child->testingUpdateMaxMemory(5 * kMB);
  int64_t parentLimit = 100 * kMB;
  parent->setGrowCallback([&](int64_t size, MemoryUsageTracker& tracker) {
    return grow(size, parentLimit, tracker);
  });
  int64_t childLimit = 150 * kMB;
  ASSERT_THROW(
      child->setGrowCallback([&](int64_t size, MemoryUsageTracker& tracker) {
        return grow(size, childLimit, tracker);
      }),
      VeloxRuntimeError);

  child->update(10 * kMB);
  ASSERT_EQ(parent->currentBytes(), 10 * kMB);
  ASSERT_EQ(child->maxMemory(), 10 * kMB);
  ASSERT_THROW(child->update(100 * kMB), VeloxRuntimeError);
  ASSERT_EQ(child->currentBytes(), 10 * kMB);
  // The parent failed to increase limit, the child'd limit should be unchanged.
  ASSERT_EQ(child->maxMemory(), 10 * kMB);
  ASSERT_EQ(parent->maxMemory(), 10 * kMB);
  ASSERT_THROW(child->update(100 * kMB);, VeloxException);
  ASSERT_EQ(child->currentBytes(), 10 * kMB);

  // We pass the parent limit but fail te child limit. leaves a raised
  // limit on the parent. Rolling back the increment of parent limit
  // is not deterministic if other threads are running at the same
  // time. Lowering a tracker's limits requires stopping the threads
  // that may be using the tracker.  Expected uses have one level of
  // trackers with a limit but we cover this for documentation.
  parentLimit = 176 * kMB;
  child->update(160 * kMB);
  ASSERT_EQ(child->currentBytes(), 170 * kMB);
  ASSERT_EQ(child->reservedBytes(), 176 * kMB);
  ASSERT_EQ(parent->currentBytes(), 176 * kMB);
  // The parent limit got set to 170, rounded up to 176.
  ASSERT_EQ(parent->maxMemory(), parentLimit);
  ASSERT_EQ(child->maxMemory(), parentLimit);
  child->update(-child->usedReservationBytes());
}

TEST_F(MemoryUsageTrackerTest, maybeReserve) {
  constexpr int64_t kMB = 1 << 20;
  auto parent = memory::MemoryUsageTracker::create(10 * kMB);
  auto child = parent->addChild();
  child->testingUpdateMaxMemory(kMaxMemory);
  // 1MB can be reserved, rounds up to 8 and leaves 2 unreserved in parent.
  EXPECT_TRUE(child->maybeReserve(kMB));
  EXPECT_EQ(0, child->currentBytes());
  ASSERT_EQ(child->usedReservationBytes(), 0);
  ASSERT_EQ(parent->usedReservationBytes(), 0);
  EXPECT_EQ(8 * kMB, child->availableReservation());
  EXPECT_EQ(8 * kMB, parent->currentBytes());
  // Fails to reserve 100MB, existing reservations are unchanged.
  EXPECT_FALSE(child->maybeReserve(100 * kMB));
  EXPECT_EQ(0, child->currentBytes());
  ASSERT_EQ(child->usedReservationBytes(), 0);
  ASSERT_EQ(parent->usedReservationBytes(), 0);
  // Use some memory from child and expect there is no memory usage change in
  // parent.
  constexpr int64_t kB = 1 << 10;
  constexpr int64_t childMemUsageBytes = 10 * kB;
  child->update(childMemUsageBytes);
  EXPECT_EQ(8 * kMB - childMemUsageBytes, child->availableReservation());
  EXPECT_EQ(8 * kMB, parent->currentBytes());
  ASSERT_EQ(child->usedReservationBytes(), childMemUsageBytes);
  ASSERT_EQ(child->currentBytes(), childMemUsageBytes);
  ASSERT_EQ(parent->usedReservationBytes(), 0);
  // Free up the memory usage and expect the reserved memory is still available,
  // and there is no memory usage change in parent.
  child->update(-childMemUsageBytes);
  EXPECT_EQ(8 * kMB, child->availableReservation());
  EXPECT_EQ(8 * kMB, parent->currentBytes());
  ASSERT_EQ(child->usedReservationBytes(), 0);
  ASSERT_EQ(parent->usedReservationBytes(), 0);
  // Release the child reserved memory.
  child->release();
  ASSERT_EQ(parent->currentBytes(), 0);
  ASSERT_EQ(child->currentBytes(), 0);
  ASSERT_EQ(child->usedReservationBytes(), 0);
  ASSERT_EQ(parent->usedReservationBytes(), 0);

  child = parent->addChild();
  EXPECT_TRUE(child->maybeReserve(kMB));
  ASSERT_EQ(child->currentBytes(), 0);
  ASSERT_EQ(child->usedReservationBytes(), 0);
  EXPECT_EQ(8 * kMB, child->availableReservation());
  EXPECT_EQ(8 * kMB, parent->currentBytes());
  child->release();

  MemoryUsageTracker::Stats stats = child->stats();
  ASSERT_EQ(stats.numReserves, 1);
  ASSERT_EQ(stats.numReleases, 1);
  ASSERT_EQ(stats.numCollisions, 0);
  stats = parent->stats();
  ASSERT_EQ(stats.numReserves, 0);
  ASSERT_EQ(stats.numReleases, 0);
  ASSERT_EQ(stats.numCollisions, 0);
}

// Class used to test operations on MemoryUsageTracker.
class MemoryUsageTrackTester {
 public:
  MemoryUsageTrackTester(
      int32_t id,
      int64_t maxMemory,
      memory::MemoryUsageTracker& tracker)
      : id_(id), maxMemory_(maxMemory), tracker_(tracker) {}

  ~MemoryUsageTrackTester() {
    VELOX_CHECK_GE(usedBytes_, 0);
    if (usedBytes_ != 0) {
      tracker_.update(-usedBytes_);
    }
  }

  void run() {
    const int32_t op = folly::Random().rand32() % 5;
    switch (op) {
      case 0: {
        // update increase.
        const int64_t updateBytes = folly::Random().rand32() % maxMemory_;
        try {
          tracker_.update(updateBytes);
        } catch (VeloxException& e) {
          // Ignore memory limit exception.
          ASSERT_TRUE(e.message().find("Negative") == std::string::npos);
          return;
        }
        usedBytes_ += updateBytes;
        break;
      }
      case 1: {
        // update decrease.
        if (usedBytes_ > 0) {
          const int64_t updateBytes = folly::Random().rand32() % usedBytes_;
          tracker_.update(-updateBytes);
          usedBytes_ -= updateBytes;
          ASSERT_GE(usedBytes_, 0);
        }
        break;
      }
      case 2: {
        // reserve.
        const int64_t reservedBytes = folly::Random().rand32() % maxMemory_;
        try {
          tracker_.reserve(reservedBytes);
        } catch (VeloxException& e) {
          // Ignore memory limit exception.
          ASSERT_TRUE(e.message().find("Negative") == std::string::npos);
          return;
        }
        break;
      }
      case 3: {
        // maybe reserve.
        const int64_t reservedBytes = folly::Random().rand32() % maxMemory_;
        tracker_.maybeReserve(reservedBytes);
        break;
      }
      case 4:
        // release.
        tracker_.release();
        ASSERT_LE(usedBytes_, tracker_.usedReservationBytes());
        break;
    }
  }

 private:
  const int32_t id_;
  const int64_t maxMemory_;
  memory::MemoryUsageTracker& tracker_;
  int64_t usedBytes_{0};
};

TEST_F(MemoryUsageTrackerTest, concurrentUpdateToDifferentPools) {
  constexpr int64_t kMB = 1 << 20;
  constexpr int64_t kMaxMemory = 10 * kMB;
  auto parent = memory::MemoryUsageTracker::create(kMaxMemory);
  const int32_t kNumThreads = 10;
  // Create one memory tracker per each thread.
  std::vector<std::shared_ptr<MemoryUsageTracker>> childTrackers;
  for (int32_t i = 0; i < kNumThreads; ++i) {
    childTrackers.push_back(parent->addChild());
  }

  folly::Random::DefaultGenerator rng;
  rng.seed(1234);

  const int32_t kNumOpsPerThread = 50'000;
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      // Set 2x of actual limit to trigger memory limit exception more
      // frequently.
      MemoryUsageTrackTester tester(i, kMaxMemory, *childTrackers[i]);
      for (int32_t iter = 0; iter < kNumOpsPerThread; ++iter) {
        tester.run();
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }
  ASSERT_EQ(parent->availableReservation(), 0);
  for (int32_t i = 0; i < kNumThreads; ++i) {
    auto& child = childTrackers[i];
    ASSERT_EQ(child->currentBytes(), 0);
    child->release();
    ASSERT_EQ(child->reservedBytes(), 0);
    ASSERT_EQ(child->availableReservation(), 0);
    ASSERT_EQ(child->currentBytes(), 0);
    ASSERT_LE(child->peakBytes(), child->cumulativeBytes());
  }
  ASSERT_LE(parent->peakBytes(), parent->cumulativeBytes());
  childTrackers.clear();
  ASSERT_LE(parent->peakBytes(), parent->cumulativeBytes());
}

TEST_F(MemoryUsageTrackerTest, concurrentUpdatesToTheSamePool) {
  const std::vector<int> concurrentLevels({0, 1});
  for (const bool concurrentLevel : concurrentLevels) {
    SCOPED_TRACE(fmt::format("concurrentLevel:{}", concurrentLevel));
    constexpr int64_t kMB = 1 << 20;
    constexpr int64_t kMaxMemory = 10 * kMB;
    auto parent = memory::MemoryUsageTracker::create(kMaxMemory);
    const int32_t kNumThreads = 10;
    const int32_t kNumChildPools = 2;
    std::vector<std::shared_ptr<MemoryUsageTracker>> childTrackers;
    if (concurrentLevel == 1) {
      for (int32_t i = 0; i < kNumChildPools; ++i) {
        childTrackers.push_back(parent->addChild());
      }
    }

    folly::Random::DefaultGenerator rng;
    rng.seed(1234);

    const int32_t kNumOpsPerThread = 50'000;
    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);
    for (size_t i = 0; i < kNumThreads; ++i) {
      threads.emplace_back([&, i]() {
        // Set 2x of actual limit to trigger memory limit exception more
        // frequently.
        MemoryUsageTrackTester tester(
            i,
            kMaxMemory,
            concurrentLevel == 0 ? *parent
                                 : *childTrackers[i % kNumChildPools]);
        for (int32_t iter = 0; iter < kNumOpsPerThread; ++iter) {
          tester.run();
        }
      });
    }

    for (auto& th : threads) {
      th.join();
    }
    if (concurrentLevel == 1) {
      ASSERT_EQ(parent->availableReservation(), 0);
      for (int32_t i = 0; i < 2; ++i) {
        auto& child = childTrackers[i];
        ASSERT_EQ(child->currentBytes(), 0);
        child->release();
        ASSERT_EQ(child->reservedBytes(), 0);
        ASSERT_EQ(child->availableReservation(), 0);
        ASSERT_EQ(child->currentBytes(), 0);
        ASSERT_LE(child->peakBytes(), child->cumulativeBytes());
      }
      ASSERT_LE(parent->peakBytes(), parent->cumulativeBytes());
      childTrackers.clear();
      ASSERT_LE(parent->peakBytes(), parent->cumulativeBytes());
    } else {
      ASSERT_EQ(parent->currentBytes(), 0);
      parent->release();
      ASSERT_EQ(parent->reservedBytes(), 0);
      ASSERT_EQ(parent->availableReservation(), 0);
      ASSERT_EQ(parent->currentBytes(), 0);
      ASSERT_LE(parent->peakBytes(), parent->cumulativeBytes());
    }
  }
}

TEST_F(MemoryUsageTrackerTest, concurrentAllocates) {
  const int32_t kNumAllocs = 3;
  folly::futures::Barrier barrier(kNumAllocs);
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::MemoryUsageTracker::reserve",
      std::function<void(MemoryUsageTracker*)>(
          [&](MemoryUsageTracker* dummy) { barrier.wait(); }));
  // NOTE: the allocation sizes are chosen based on the memory thresholds
  // defined in quantizedSize().
  //
  // The test leverage the test value to ensure the memory reservations are
  // granted after all the memory reservation increment sizes are determined.
  //
  // The following is the sequence of allocation/free/grant events and the
  // corresponding usedReservationBytes_, grantedReservationBytes_ and the
  // quantized grantedReservationBytes_ changes:
  //
  // 1. ALLOC 15MB - 0MB    0MB   0MB
  // 2. ALLOC 2MB  - 0MB    0MB   0MB
  // 3. ALLOC 2MB  - 0MB    0MB   0MB
  // 4. GRANT      - 19MB   19MB  20MB* inconsistent caused by concurrent alloc
  // 5. FREE  2MB  - 17MB   17MB  20MB* inconsistent caused by concurrent alloc
  // 6. FREE  2MB  - 15MB   15MB  15MB
  // 7. FREE 15MB  - 0MB    0MB   0MB
  const int64_t kLargeAllocSize = 15 << 20;
  const int64_t kSmallAllocSize = 2 << 20;

  auto tracker = memory::MemoryUsageTracker::create(kMaxMemory);
  std::vector<std::thread> allocThreads;
  for (int32_t i = 0; i < kNumAllocs; ++i) {
    allocThreads.push_back(std::thread(
        [&, allocSize = i == 0 ? kLargeAllocSize : kSmallAllocSize]() {
          tracker->update(allocSize);
        }));
  }
  for (int32_t i = 0; i < kNumAllocs; ++i) {
    allocThreads[i].join();
  }
  tracker->update(-kSmallAllocSize);
  tracker->update(-kSmallAllocSize);
  tracker->update(-kLargeAllocSize);
}

// TODO: add collision tests and stats verification.
