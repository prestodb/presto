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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <thread>

#include "velox/common/time/CpuWallTimer.h"

using namespace facebook::velox;

namespace facebook::velox::test {

class CpuWallTimerTest : public testing::Test {
 protected:
  static void workAndSleep(std::chrono::nanoseconds sleepDuration) {
    // Use some cpu here.
    size_t n{0};
    for (size_t i = 0; i < 10'000; ++i) {
      n += 5 + i * 10;
    }

    // Condition to use variable 'n' otherwise the compiler might opt out the
    // computation loop above since 'n' is not used.
    if (n >= 5) {
      /* sleep override */
      std::this_thread::sleep_for(sleepDuration);
    }
  }
};

TEST_F(CpuWallTimerTest, cpuWallTiming) {
  const CpuWallTiming timingZero;
  EXPECT_EQ(0, timingZero.count);
  EXPECT_EQ(0, timingZero.wallNanos);
  EXPECT_EQ(0, timingZero.cpuNanos);

  constexpr uint64_t count11{11};
  constexpr uint64_t wallNanos11{1'500'345'000};
  constexpr uint64_t cpuNanos11{345'000};
  constexpr uint64_t count17{17};
  constexpr uint64_t wallNanos17{1'500'345'000};
  constexpr uint64_t cpuNanos17{6'789'000};

  CpuWallTiming timing11{count11, wallNanos11, cpuNanos11};
  CpuWallTiming timing17{count17, wallNanos17, cpuNanos17};
  EXPECT_EQ(count11, timing11.count);
  EXPECT_EQ(wallNanos11, timing11.wallNanos);
  EXPECT_EQ(cpuNanos11, timing11.cpuNanos);
  EXPECT_EQ(count17, timing17.count);
  EXPECT_EQ(wallNanos17, timing17.wallNanos);
  EXPECT_EQ(cpuNanos17, timing17.cpuNanos);

  timing11.add(timing17);
  EXPECT_EQ(count11 + count17, timing11.count);
  EXPECT_EQ(wallNanos11 + wallNanos17, timing11.wallNanos);
  EXPECT_EQ(cpuNanos11 + cpuNanos17, timing11.cpuNanos);

  timing17.clear();
  EXPECT_EQ(timingZero.count, timing17.count);
  EXPECT_EQ(timingZero.wallNanos, timing17.wallNanos);
  EXPECT_EQ(timingZero.cpuNanos, timing17.cpuNanos);
}

TEST_F(CpuWallTimerTest, cpuWallTimer) {
  CpuWallTiming timing;
  // Everything should be zero.
  EXPECT_EQ(0, timing.count);
  EXPECT_EQ(0, timing.wallNanos);
  EXPECT_EQ(0, timing.cpuNanos);

  constexpr std::chrono::nanoseconds sleepTime{1'100'000'000};

  {
    CpuWallTimer timer{timing};
    workAndSleep(sleepTime);
  }
  // We added a single measurement with sleep + some execution.
  EXPECT_EQ(1, timing.count);
  EXPECT_LT(sleepTime.count(), timing.wallNanos);
  EXPECT_LT(0, timing.cpuNanos);
  const auto cpuFirstTime = timing.cpuNanos;

  {
    CpuWallTimer timer{timing};
    workAndSleep(sleepTime);
  }
  // We added two measurements with sleep + some execution each.
  EXPECT_EQ(2, timing.count);
  EXPECT_LT(sleepTime.count() * 2, timing.wallNanos);
  EXPECT_LT(cpuFirstTime, timing.cpuNanos);
}

TEST_F(CpuWallTimerTest, deltaCpuWallTimer) {
  CpuWallTiming timing;
  // Everything should be zero.
  EXPECT_EQ(0, timing.count);
  EXPECT_EQ(0, timing.wallNanos);
  EXPECT_EQ(0, timing.cpuNanos);

  constexpr std::chrono::nanoseconds sleepTime{1'100'000'000};

  {
    DeltaCpuWallTimer timer{
        [&](const CpuWallTiming& deltaTiming) { timing.add(deltaTiming); }};
    workAndSleep(sleepTime);
  }
  // We added a single measurement with sleep + some execution.
  EXPECT_EQ(1, timing.count);
  EXPECT_LT(sleepTime.count(), timing.wallNanos);
  EXPECT_LT(0, timing.cpuNanos);
  const auto cpuFirstTime = timing.cpuNanos;

  {
    DeltaCpuWallTimer timer{
        [&](const CpuWallTiming& deltaTiming) { timing.add(deltaTiming); }};
    workAndSleep(sleepTime);
  }
  // We added two measurements with sleep + some execution each.
  EXPECT_EQ(2, timing.count);
  EXPECT_LT(sleepTime.count() * 2, timing.wallNanos);
  EXPECT_LT(cpuFirstTime, timing.cpuNanos);
}

} // namespace facebook::velox::test
