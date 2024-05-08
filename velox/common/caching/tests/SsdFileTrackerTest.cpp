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

#include "velox/common/caching/SsdFileTracker.h"

#include <gtest/gtest.h>

using namespace facebook::velox::cache;

TEST(SsdFileTrackerTest, tracker) {
  constexpr int32_t kNumRegions = 16;
  SsdFileTracker tracker;
  tracker.resize(kNumRegions);
  // Simulate a sequence of access that periodically adds a new region and keeps
  // accessing the 4 most recently added regions.
  for (auto lastRegion = 0; lastRegion < kNumRegions; ++lastRegion) {
    tracker.regionFilled(lastRegion);
    for (auto i = 0; i < 2000; ++i) {
      for (auto region = std::max(lastRegion - 3, 0); region <= lastRegion;
           ++region) {
        // fileTouched means a lookup. This decays scores so that new uses are
        // more relevant than old ones. The actual read is tracked by
        // regionRead()..
        tracker.fileTouched(10000);
        tracker.regionRead(region, 100000);
      }
    }
  }
  std::vector<int32_t> pins(kNumRegions);
  pins[2] = 1;
  pins[3] = 2;
  // Get up to 10 low-use regions out of kNumRegions used regions, excluding
  // regions that have a non-zero in 'pins'.
  auto candidates =
      tracker.findEvictionCandidates(kNumRegions, kNumRegions, pins);
  std::vector<int32_t> expected{0, 1, 4, 5, 6, 7, 8, 9};
  EXPECT_EQ(candidates, expected);

  // Test large region scores.
  tracker.testingClear();
  for (auto region = 0; region < kNumRegions; ++region) {
    tracker.regionRead(region, INT32_MAX);
    tracker.regionRead(region, region * 100'000'000);
  }
  for (int i = 0; i < 999; ++i) {
    for (auto region = 0; region < kNumRegions; ++region) {
      tracker.regionFilled(region);
    }
  }
  for (const auto score : tracker.copyScores()) {
    EXPECT_TRUE(std::isinf(score));
  }
  // Mark all regions to be evictable.
  std::fill(pins.begin(), pins.end(), 0);
  candidates = tracker.findEvictionCandidates(3, kNumRegions, pins);
  EXPECT_EQ(candidates.size(), 3);
}
