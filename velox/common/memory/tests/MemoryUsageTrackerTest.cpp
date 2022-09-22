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

#include "velox/common/memory/MemoryUsageTracker.h"

using namespace ::testing;
using namespace ::facebook::velox::memory;
using namespace ::facebook::velox;

TEST(MemoryUsageTrackerTest, constructor) {
  std::vector<std::shared_ptr<MemoryUsageTracker>> trackers;
  auto tracker = MemoryUsageTracker::create();
  trackers.push_back(tracker);
  trackers.push_back(tracker->addChild());
  trackers.push_back(tracker->addChild(true));

  for (unsigned i = 0; i < trackers.size(); ++i) {
    EXPECT_EQ(0, trackers[i]->getCurrentUserBytes());
    EXPECT_EQ(0, trackers[i]->getCurrentSystemBytes());
    EXPECT_EQ(0, trackers[i]->getCurrentTotalBytes());
    EXPECT_EQ(0, trackers[i]->getPeakUserBytes());
    EXPECT_EQ(0, trackers[i]->getPeakSystemBytes());
    EXPECT_EQ(0, trackers[i]->getPeakTotalBytes());
  }
}

TEST(MemoryUsageTrackerTest, update) {
  constexpr int64_t kMaxSize = 1 << 30; // 1GB
  constexpr int64_t kMB = 1 << 20;
  auto config = MemoryUsageConfigBuilder().maxUserMemory(kMaxSize).build();
  auto parent = MemoryUsageTracker::create(config);

  auto child1 = parent->addChild();
  auto child2 = parent->addChild();

  EXPECT_THROW(child1->reserve(2 * kMaxSize), VeloxRuntimeError);

  EXPECT_EQ(0, parent->getCurrentTotalBytes());
  child1->update(1000);
  EXPECT_EQ(kMB, parent->getCurrentTotalBytes());
  EXPECT_EQ(kMB - 1000, child1->getAvailableReservation());
  child1->update(1000);
  EXPECT_EQ(kMB, parent->getCurrentTotalBytes());
  child1->update(kMB);
  EXPECT_EQ(2 * kMB, parent->getCurrentTotalBytes());
  child1->update(100 * kMB);
  // Larger sizes round up to next 8MB.
  EXPECT_EQ(104 * kMB, parent->getCurrentTotalBytes());
  child1->update(-kMB);
  // 1MB less does not decrease the reservation.
  EXPECT_EQ(104 * kMB, parent->getCurrentTotalBytes());

  child1->update(-7 * kMB);
  EXPECT_EQ(96 * kMB, parent->getCurrentTotalBytes());
  child1->update(-92 * kMB);
  EXPECT_EQ(2 * kMB, parent->getCurrentTotalBytes());
  child1->update(-kMB);
  EXPECT_EQ(kMB, parent->getCurrentTotalBytes());

  child1->update(-2000);
  EXPECT_EQ(0, parent->getCurrentTotalBytes());
}

TEST(MemoryUsageTrackerTest, reserve) {
  constexpr int64_t kMaxSize = 1 << 30;
  constexpr int64_t kMB = 1 << 20;
  auto config = MemoryUsageConfigBuilder().maxUserMemory(kMaxSize).build();
  auto parent = MemoryUsageTracker::create(config);

  auto child = parent->addChild();

  EXPECT_THROW(child->reserve(2 * kMaxSize), VeloxRuntimeError);

  child->reserve(100 * kMB);
  EXPECT_EQ(0, child->getCurrentTotalBytes());
  // The reservationon child shows up as a reservation on the child
  // and as an allocation on the parent.
  EXPECT_EQ(104 * kMB, child->getAvailableReservation());
  EXPECT_EQ(0, child->getCurrentTotalBytes());
  EXPECT_EQ(104 * kMB, parent->getCurrentTotalBytes());
  child->update(60 * kMB);
  EXPECT_EQ(60 * kMB, child->getCurrentTotalBytes());
  EXPECT_EQ(104 * kMB, parent->getCurrentTotalBytes());
  EXPECT_EQ((104 - 60) * kMB, child->getAvailableReservation());
  child->update(70 * kMB);
  // Extended and rounded up the reservation to then next 8MB.
  EXPECT_EQ(136 * kMB, parent->getCurrentTotalBytes());
  child->update(-130 * kMB);
  // The reservation goes down to the explicitly made reservation.
  EXPECT_EQ(104 * kMB, parent->getCurrentTotalBytes());
  EXPECT_EQ(104 * kMB, child->getAvailableReservation());
  child->release();
  EXPECT_EQ(0, parent->getCurrentTotalBytes());
}

TEST(MemoryUsageTrackerTest, reserveAndUpdate) {
  constexpr int64_t kMaxSize = 1 << 30; // 1GB
  constexpr int64_t kMB = 1 << 20;
  auto config = MemoryUsageConfigBuilder().maxUserMemory(kMaxSize).build();
  auto parent = MemoryUsageTracker::create(config);

  auto child1 = parent->addChild();

  child1->update(1000);
  EXPECT_EQ(kMB, parent->getCurrentTotalBytes());
  EXPECT_EQ(kMB - 1000, child1->getAvailableReservation());
  child1->update(1000);
  EXPECT_EQ(kMB, parent->getCurrentTotalBytes());

  child1->reserve(kMB);
  EXPECT_EQ(2 * kMB, parent->getCurrentTotalBytes());
  child1->update(kMB);

  // release has no effect  since usage within quantum of reservation.
  child1->release();
  EXPECT_EQ(2 * kMB, parent->getCurrentTotalBytes());
  EXPECT_EQ(2000 + kMB, child1->getCurrentTotalBytes());
  EXPECT_EQ(kMB - 2000, child1->getAvailableReservation());

  // We reserve 20MB, consume 9MB and release the unconsumed.
  child1->reserve(20 * kMB);
  // 22 rounded up to 24.
  EXPECT_EQ(24 * kMB, parent->getCurrentTotalBytes());
  child1->update(7 * kMB);
  EXPECT_EQ(16 * kMB - 2000, child1->getAvailableReservation());
  child1->release();
  EXPECT_EQ(kMB - 2000, child1->getAvailableReservation());
  EXPECT_EQ(9 * kMB, parent->getCurrentTotalBytes());

  // We reserve another 20 MB, consume 25 and release nothing because
  // reservation is already taken.
  child1->reserve(20 * kMB);
  child1->update(25 * kMB);
  EXPECT_EQ(36 * kMB, parent->getCurrentTotalBytes());
  EXPECT_EQ(3 * kMB - 2000, child1->getAvailableReservation());
  child1->release();

  // Nothing changed by release since already over the explicit reservation.
  EXPECT_EQ(36 * kMB, parent->getCurrentTotalBytes());
  EXPECT_EQ(3 * kMB - 2000, child1->getAvailableReservation());

  // We reserve 20MB and free 5MB and release. Expect 25MB drop.
  child1->reserve(20 * kMB);
  child1->update(-5 * kMB);
  EXPECT_EQ(28 * kMB - 2000, child1->getAvailableReservation());
  EXPECT_EQ(56 * kMB, parent->getCurrentTotalBytes());

  // Reservation drops by 25, rounded to  quantized size of 32.
  child1->release();

  EXPECT_EQ(32 * kMB, parent->getCurrentTotalBytes());
  EXPECT_EQ(4 * kMB - 2000, child1->getAvailableReservation());

  // We reserve 20MB, allocate 25 and free 15
  child1->reserve(20 * kMB);
  child1->update(25 * kMB);
  EXPECT_EQ(56 * kMB, parent->getCurrentTotalBytes());
  EXPECT_EQ(3 * kMB - 2000, child1->getAvailableReservation());
  child1->update(-15 * kMB);

  // There is 14MB - 2000  of available reservation because the reservation does
  // not drop below the bar set in reserve(). The used size reflected in the
  // parent drops a little to match the level given in reserver().
  EXPECT_EQ(52 * kMB, parent->getCurrentTotalBytes());
  EXPECT_EQ(14 * kMB - 2000, child1->getAvailableReservation());

  // The unused reservation is freed.
  child1->release();
  EXPECT_EQ(40 * kMB, parent->getCurrentTotalBytes());
  EXPECT_EQ(2 * kMB - 2000, child1->getAvailableReservation());
}

namespace {
// Model implementation of a GrowCallback.
bool grow(
    MemoryUsageTracker::UsageType /*type*/,
    int64_t /*size*/,
    int64_t hardLimit,
    MemoryUsageTracker& tracker) {
  static std::mutex mutex;
  // The calls from different threads on the same tracker must be serialized.
  std::lock_guard<std::mutex> l(mutex);
  // The total includes the allocation that exceeded the limit. This function's
  // job is to raise the limit to >= current.
  auto current = tracker.totalReservedBytes();
  auto limit = tracker.maxTotalBytes();
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
  auto config = MemoryUsageConfigBuilder().maxTotalMemory(current).build();
  tracker.updateConfig(config);
  return true;
}
} // namespace

TEST(MemoryUsageTrackerTest, grow) {
  constexpr int64_t kMB = 1 << 20;
  auto config = MemoryUsageConfigBuilder().maxTotalMemory(10 * kMB).build();
  auto parent = MemoryUsageTracker::create(config);

  auto child = parent->addChild();
  auto childConfig = MemoryUsageConfigBuilder().maxTotalMemory(5 * kMB).build();
  child->updateConfig(childConfig);
  int64_t parentLimit = 100 * kMB;
  int64_t childLimit = 150 * kMB;
  parent->setGrowCallback([&](MemoryUsageTracker::UsageType type,
                              int64_t size,
                              MemoryUsageTracker& tracker) {
    return grow(type, size, parentLimit, tracker);
  });
  child->setGrowCallback([&](MemoryUsageTracker::UsageType type,
                             int64_t size,
                             MemoryUsageTracker& tracker) {
    return grow(type, size, childLimit, tracker);
  });

  child->update(10 * kMB);
  EXPECT_EQ(10 * kMB, parent->getCurrentTotalBytes());
  EXPECT_EQ(10 * kMB, child->maxTotalBytes());
  EXPECT_THROW(child->update(100 * kMB), VeloxRuntimeError);
  EXPECT_EQ(10 * kMB, child->getCurrentTotalBytes());
  // The parent failed to increase limit, the child'd limit should be unchanged.
  EXPECT_EQ(10 * kMB, child->maxTotalBytes());
  EXPECT_EQ(10 * kMB, parent->maxTotalBytes());

  // We pass the parent limit but fail te child limit. leaves a raised
  // limit on the parent. Rolling back the increment of parent limit
  // is not deterministic if other threads are running at the same
  // time. Lowering a tracker's limits requires stopping the threads
  // that may be using the tracker.  Expected uses have one level of
  // trackers with a limit but we cover this for documentation.
  parentLimit = 200 * kMB;
  EXPECT_THROW(child->update(160 * kMB);, VeloxException);
  EXPECT_EQ(10 * kMB, parent->getCurrentTotalBytes());
  EXPECT_EQ(10 * kMB, child->getCurrentTotalBytes());
  // The child limit could not be raised.
  EXPECT_EQ(10 * kMB, child->maxTotalBytes());
  // The parent limit got set to 170, rounded up to 176
  EXPECT_EQ(176 * kMB, parent->maxTotalBytes());
}

TEST(MemoryUsageTrackerTest, maybeReserve) {
  constexpr int64_t kMB = 1 << 20;
  auto config =
      memory::MemoryUsageConfigBuilder().maxTotalMemory(10 * kMB).build();
  auto parent = memory::MemoryUsageTracker::create(config);
  auto child = parent->addChild();
  auto childConfig = memory::MemoryUsageConfigBuilder().build();
  child->updateConfig(childConfig);
  // 1MB can be reserved, rounds up to 8 and leaves 2 unreserved in parent.
  EXPECT_TRUE(child->maybeReserve(kMB));
  EXPECT_EQ(0, child->getCurrentUserBytes());
  EXPECT_EQ(8 * kMB, child->getAvailableReservation());
  EXPECT_EQ(8 * kMB, parent->getCurrentUserBytes());
  // Fails to reserve 100MB, existing reservations are unchanged.
  EXPECT_FALSE(child->maybeReserve(100 * kMB));
  EXPECT_EQ(0, child->getCurrentTotalBytes());
  // Use some memory from child and expect there is no memory usage change in
  // parent.
  constexpr int64_t kB = 1 << 10;
  constexpr int64_t childMemUsageBytes = 10 * kB;
  child->update(childMemUsageBytes);
  EXPECT_EQ(8 * kMB - childMemUsageBytes, child->getAvailableReservation());
  EXPECT_EQ(8 * kMB, parent->getCurrentUserBytes());
  // Free up the memory usage and expect the reserved memory is still available,
  // and there is no memory usage change in parent.
  child->update(-childMemUsageBytes);
  EXPECT_EQ(8 * kMB, child->getAvailableReservation());
  EXPECT_EQ(8 * kMB, parent->getCurrentUserBytes());
  // Release the child reserved memory.
  child->release();
  EXPECT_EQ(0, parent->getCurrentUserBytes());

  child = parent->addChild();
  EXPECT_TRUE(child->maybeReserve(kMB));
  EXPECT_EQ(0, child->getCurrentUserBytes());
  EXPECT_EQ(8 * kMB, child->getAvailableReservation());
  EXPECT_EQ(8 * kMB, parent->getCurrentUserBytes());
  child.reset();
  // The child destruction won't release the reserved memory back to the parent.
  EXPECT_EQ(8 * kMB, parent->getCurrentUserBytes());
}
