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
