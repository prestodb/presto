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
  auto parent = MemoryUsageTracker::create();

  auto verifyUsage = [](MemoryUsageTracker* userTracker,
                        MemoryUsageTracker* systemTracker,
                        MemoryUsageTracker* parent,
                        std::vector<int64_t> currentUserBytes,
                        std::vector<int64_t> currentSystemBytes,
                        std::vector<int64_t> currentTotalBytes,
                        std::vector<int64_t> peakUserBytes,
                        std::vector<int64_t> peakSystemBytes,
                        std::vector<int64_t> peakTotalBytes) {
    std::vector<MemoryUsageTracker*> trackers = {
        userTracker, systemTracker, parent};
    for (int i = 0; i < 3; ++i) {
      EXPECT_EQ(trackers[i]->getCurrentUserBytes(), currentUserBytes[i]);
      EXPECT_EQ(trackers[i]->getCurrentSystemBytes(), currentSystemBytes[i]);
      EXPECT_EQ(trackers[i]->getCurrentTotalBytes(), currentTotalBytes[i]);
      EXPECT_EQ(trackers[i]->getPeakUserBytes(), peakUserBytes[i]);
      EXPECT_EQ(trackers[i]->getPeakSystemBytes(), peakSystemBytes[i]);
      EXPECT_EQ(trackers[i]->getPeakTotalBytes(), peakTotalBytes[i]);
    }
  };

  auto userTracker = parent->addChild(false);
  auto systemTracker = parent->addChild(true);

  userTracker->update(20);

  verifyUsage(
      userTracker.get(),
      systemTracker.get(),
      parent.get(),
      {20, 0, 20},
      {0, 0, 0},
      {20, 0, 20},
      {20, 0, 20},
      {0, 0, 0},
      {20, 0, 20});

  userTracker->update(30);
  verifyUsage(
      userTracker.get(),
      systemTracker.get(),
      parent.get(),
      {50, 0, 50},
      {0, 0, 0},
      {50, 0, 50},
      {50, 0, 50},
      {0, 0, 0},
      {50, 0, 50});

  userTracker->update(-20);
  verifyUsage(
      userTracker.get(),
      systemTracker.get(),
      parent.get(),
      {30, 0, 30},
      {0, 0, 0},
      {30, 0, 30},
      {50, 0, 50},
      {0, 0, 0},
      {50, 0, 50});

  systemTracker->update(100);
  verifyUsage(
      userTracker.get(),
      systemTracker.get(),
      parent.get(),
      {30, 0, 30},
      {0, 100, 100},
      {30, 100, 130},
      {50, 0, 50},
      {0, 100, 100},
      {50, 100, 130});

  systemTracker->update(-80);
  verifyUsage(
      userTracker.get(),
      systemTracker.get(),
      parent.get(),
      {30, 0, 30},
      {0, 20, 20},
      {30, 20, 50},
      {50, 0, 50},
      {0, 100, 100},
      {50, 100, 130});

  userTracker->update(50);
  verifyUsage(
      userTracker.get(),
      systemTracker.get(),
      parent.get(),
      {80, 0, 80},
      {0, 20, 20},
      {80, 20, 100},
      {80, 0, 80},
      {0, 100, 100},
      {80, 100, 130});
}

TEST(MemoryUsageTrackerTest, reserve) {
  constexpr int64_t kMaxSize = 1 << 20;
  auto config = MemoryUsageConfigBuilder().maxUserMemory(kMaxSize).build();
  auto parent = MemoryUsageTracker::create(config);

  auto child = parent->addChild();

  EXPECT_THROW(child->reserve(2 * kMaxSize), VeloxRuntimeError);

  child->reserve(1024);
  EXPECT_THROW(child->update(2 * kMaxSize), VeloxRuntimeError);
  EXPECT_EQ(child->getAvailableReservation(), 1024);
  EXPECT_EQ(child->getCurrentTotalBytes(), 1024);
  EXPECT_EQ(parent->getCurrentTotalBytes(), 1024);

  child->update(512);
  EXPECT_EQ(child->getAvailableReservation(), 512);
  EXPECT_EQ(child->getCurrentTotalBytes(), 1024);
  EXPECT_EQ(parent->getCurrentTotalBytes(), 1024);

  child->update(1024);
  EXPECT_EQ(child->getAvailableReservation(), 0);
  EXPECT_EQ(child->getCurrentTotalBytes(), 1536);
  EXPECT_EQ(parent->getCurrentTotalBytes(), 1536);

  child->update(512);
  EXPECT_EQ(child->getAvailableReservation(), 0);
  EXPECT_EQ(child->getCurrentTotalBytes(), 2048);
  EXPECT_EQ(parent->getCurrentTotalBytes(), 2048);

  child->update(-512);
  EXPECT_EQ(child->getAvailableReservation(), 0);
  EXPECT_EQ(child->getCurrentTotalBytes(), 1536);
  EXPECT_EQ(parent->getCurrentTotalBytes(), 1536);

  child->update(-1024);
  EXPECT_EQ(child->getAvailableReservation(), 512);
  EXPECT_EQ(child->getCurrentTotalBytes(), 1024);
  EXPECT_EQ(parent->getCurrentTotalBytes(), 1024);

  child->update(-512);
  EXPECT_EQ(child->getAvailableReservation(), 1024);
  EXPECT_EQ(child->getCurrentTotalBytes(), 1024);
  EXPECT_EQ(parent->getCurrentTotalBytes(), 1024);

  // We free past the allocated size at reservation time. 'usedReservation_'
  // goes negative.
  child->update(-512);
  EXPECT_EQ(child->getAvailableReservation(), 1536);

  child->release();
  EXPECT_EQ(child->getAvailableReservation(), 0);
  EXPECT_EQ(child->getCurrentTotalBytes(), -512);
  EXPECT_EQ(parent->getCurrentTotalBytes(), -512);
}
