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
#include "velox/common/memory/MemoryUsageTracker.h"

using namespace ::testing;
using namespace ::facebook::velox::memory;
using namespace ::facebook::velox;

TEST(MemoryUsageTrackerTest, constructor) {
  std::vector<std::shared_ptr<MemoryUsageTracker>> trackers;
  auto tracker = MemoryUsageTracker::create();
  trackers.push_back(tracker);
  trackers.push_back(tracker->addChild());
  trackers.push_back(tracker->addChild());

  for (unsigned i = 0; i < trackers.size(); ++i) {
    EXPECT_EQ(0, trackers[i]->currentBytes());
    EXPECT_EQ(0, trackers[i]->peakBytes());
  }
}

TEST(MemoryUsageTrackerTest, update) {
  constexpr int64_t kMaxSize = 1 << 30; // 1GB
  constexpr int64_t kMB = 1 << 20;
  auto parent = MemoryUsageTracker::create(kMaxSize);

  auto child1 = parent->addChild();
  auto child2 = parent->addChild();

  ASSERT_THROW(child1->reserve(2 * kMaxSize), VeloxRuntimeError);

  ASSERT_EQ(0, parent->currentBytes());
  ASSERT_EQ(0, parent->cumulativeBytes());
  child1->update(1000);
  ASSERT_EQ(kMB, parent->currentBytes());
  ASSERT_EQ(kMB, parent->cumulativeBytes());
  ASSERT_EQ(kMB - 1000, child1->availableReservation());
  child1->update(1000);
  ASSERT_EQ(kMB, parent->currentBytes());
  ASSERT_EQ(kMB, parent->cumulativeBytes());
  child1->update(kMB);
  ASSERT_EQ(2 * kMB, parent->currentBytes());
  ASSERT_EQ(2 * kMB, parent->cumulativeBytes());

  child1->update(100 * kMB);
  // Larger sizes round up to next 8MB.
  ASSERT_EQ(104 * kMB, parent->currentBytes());
  ASSERT_EQ(104 * kMB, parent->cumulativeBytes());
  child1->update(-kMB);
  // 1MB less does not decrease the reservation.
  ASSERT_EQ(104 * kMB, parent->currentBytes());
  ASSERT_EQ(104 * kMB, parent->cumulativeBytes());

  child1->update(-7 * kMB);
  ASSERT_EQ(96 * kMB, parent->currentBytes());
  ASSERT_EQ(104 * kMB, parent->cumulativeBytes());
  child1->update(-92 * kMB);
  ASSERT_EQ(2 * kMB, parent->currentBytes());
  ASSERT_EQ(104 * kMB, parent->cumulativeBytes());
  child1->update(-kMB);
  ASSERT_EQ(kMB, parent->currentBytes());
  ASSERT_EQ(104 * kMB, parent->cumulativeBytes());

  child1->update(-2000);
  ASSERT_EQ(0, parent->currentBytes());
  ASSERT_EQ(104 * kMB, parent->cumulativeBytes());
}

TEST(MemoryUsageTrackerTest, reserve) {
  constexpr int64_t kMaxSize = 1 << 30;
  constexpr int64_t kMB = 1 << 20;
  auto parent = MemoryUsageTracker::create(kMaxSize);

  auto child = parent->addChild();

  EXPECT_THROW(child->reserve(2 * kMaxSize), VeloxRuntimeError);

  child->reserve(100 * kMB);
  EXPECT_EQ(0, child->currentBytes());
  // The reservationon child shows up as a reservation on the child
  // and as an allocation on the parent.
  EXPECT_EQ(104 * kMB, child->availableReservation());
  EXPECT_EQ(0, child->currentBytes());
  EXPECT_EQ(104 * kMB, parent->currentBytes());
  child->update(60 * kMB);
  EXPECT_EQ(60 * kMB, child->currentBytes());
  EXPECT_EQ(104 * kMB, parent->currentBytes());
  EXPECT_EQ((104 - 60) * kMB, child->availableReservation());
  child->update(70 * kMB);
  // Extended and rounded up the reservation to then next 8MB.
  EXPECT_EQ(136 * kMB, parent->currentBytes());
  child->update(-130 * kMB);
  // The reservation goes down to the explicitly made reservation.
  EXPECT_EQ(104 * kMB, parent->currentBytes());
  EXPECT_EQ(104 * kMB, child->availableReservation());
  child->release();
  EXPECT_EQ(0, parent->currentBytes());
}

TEST(MemoryUsageTrackerTest, reserveAndUpdate) {
  constexpr int64_t kMaxSize = 1 << 30; // 1GB
  constexpr int64_t kMB = 1 << 20;
  auto parent = MemoryUsageTracker::create(kMaxSize);

  auto child1 = parent->addChild();

  child1->update(1000);
  EXPECT_EQ(kMB, parent->currentBytes());
  EXPECT_EQ(kMB - 1000, child1->availableReservation());
  child1->update(1000);
  EXPECT_EQ(kMB, parent->currentBytes());

  child1->reserve(kMB);
  EXPECT_EQ(2 * kMB, parent->currentBytes());
  child1->update(kMB);

  // release has no effect  since usage within quantum of reservation.
  child1->release();
  EXPECT_EQ(2 * kMB, parent->currentBytes());
  EXPECT_EQ(2000 + kMB, child1->currentBytes());
  EXPECT_EQ(kMB - 2000, child1->availableReservation());

  // We reserve 20MB, consume 9MB and release the unconsumed.
  child1->reserve(20 * kMB);
  // 22 rounded up to 24.
  EXPECT_EQ(24 * kMB, parent->currentBytes());
  child1->update(7 * kMB);
  EXPECT_EQ(16 * kMB - 2000, child1->availableReservation());
  child1->release();
  EXPECT_EQ(kMB - 2000, child1->availableReservation());
  EXPECT_EQ(9 * kMB, parent->currentBytes());

  // We reserve another 20 MB, consume 25 and release nothing because
  // reservation is already taken.
  child1->reserve(20 * kMB);
  child1->update(25 * kMB);
  EXPECT_EQ(36 * kMB, parent->currentBytes());
  EXPECT_EQ(3 * kMB - 2000, child1->availableReservation());
  child1->release();

  // Nothing changed by release since already over the explicit reservation.
  EXPECT_EQ(36 * kMB, parent->currentBytes());
  EXPECT_EQ(3 * kMB - 2000, child1->availableReservation());

  // We reserve 20MB and free 5MB and release. Expect 25MB drop.
  child1->reserve(20 * kMB);
  child1->update(-5 * kMB);
  EXPECT_EQ(28 * kMB - 2000, child1->availableReservation());
  EXPECT_EQ(56 * kMB, parent->currentBytes());

  // Reservation drops by 25, rounded to  quantized size of 32.
  child1->release();

  EXPECT_EQ(32 * kMB, parent->currentBytes());
  EXPECT_EQ(4 * kMB - 2000, child1->availableReservation());

  // We reserve 20MB, allocate 25 and free 15
  child1->reserve(20 * kMB);
  child1->update(25 * kMB);
  EXPECT_EQ(56 * kMB, parent->currentBytes());
  EXPECT_EQ(3 * kMB - 2000, child1->availableReservation());
  child1->update(-15 * kMB);

  // There is 14MB - 2000  of available reservation because the reservation does
  // not drop below the bar set in reserve(). The used size reflected in the
  // parent drops a little to match the level given in reserver().
  EXPECT_EQ(52 * kMB, parent->currentBytes());
  EXPECT_EQ(14 * kMB - 2000, child1->availableReservation());

  // The unused reservation is freed.
  child1->release();
  EXPECT_EQ(40 * kMB, parent->currentBytes());
  EXPECT_EQ(2 * kMB - 2000, child1->availableReservation());
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

TEST(MemoryUsageTrackerTest, grow) {
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
}

TEST(MemoryUsageTrackerTest, maybeReserve) {
  constexpr int64_t kMB = 1 << 20;
  auto parent = memory::MemoryUsageTracker::create(10 * kMB);
  auto child = parent->addChild();
  child->testingUpdateMaxMemory(kMaxMemory);
  // 1MB can be reserved, rounds up to 8 and leaves 2 unreserved in parent.
  EXPECT_TRUE(child->maybeReserve(kMB));
  EXPECT_EQ(0, child->currentBytes());
  EXPECT_EQ(8 * kMB, child->availableReservation());
  EXPECT_EQ(8 * kMB, parent->currentBytes());
  // Fails to reserve 100MB, existing reservations are unchanged.
  EXPECT_FALSE(child->maybeReserve(100 * kMB));
  EXPECT_EQ(0, child->currentBytes());
  // Use some memory from child and expect there is no memory usage change in
  // parent.
  constexpr int64_t kB = 1 << 10;
  constexpr int64_t childMemUsageBytes = 10 * kB;
  child->update(childMemUsageBytes);
  EXPECT_EQ(8 * kMB - childMemUsageBytes, child->availableReservation());
  EXPECT_EQ(8 * kMB, parent->currentBytes());
  // Free up the memory usage and expect the reserved memory is still available,
  // and there is no memory usage change in parent.
  child->update(-childMemUsageBytes);
  EXPECT_EQ(8 * kMB, child->availableReservation());
  EXPECT_EQ(8 * kMB, parent->currentBytes());
  // Release the child reserved memory.
  child->release();
  EXPECT_EQ(0, parent->currentBytes());

  child = parent->addChild();
  EXPECT_TRUE(child->maybeReserve(kMB));
  EXPECT_EQ(0, child->currentBytes());
  EXPECT_EQ(8 * kMB, child->availableReservation());
  EXPECT_EQ(8 * kMB, parent->currentBytes());
  child.reset();
  // The child destruction won't release the reserved memory back to the parent.
  EXPECT_EQ(8 * kMB, parent->currentBytes());
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
        ASSERT_LE(usedBytes_, tracker_.currentBytes());
        break;
    }
  }

 private:
  const int32_t id_;
  const int64_t maxMemory_;
  memory::MemoryUsageTracker& tracker_;
  int64_t usedBytes_{0};
};

TEST(MemoryUsageTrackerTest, concurrentUpdate) {
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
