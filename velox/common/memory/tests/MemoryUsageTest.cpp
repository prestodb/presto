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

#include "velox/common/memory/MemoryUsage.h"

using namespace ::testing;
using namespace ::facebook::velox::memory;

TEST(MemoryUsageTest, Ctor) {
  MemoryUsage usage;
  EXPECT_EQ(0, usage.getCurrentBytes());
  EXPECT_EQ(0, usage.getMaxBytes());
}

TEST(MemoryUsageTest, Set) {
  MemoryUsage usage;
  usage.setCurrentBytes(0);
  EXPECT_EQ(0, usage.getCurrentBytes());
  EXPECT_EQ(0, usage.getMaxBytes());

  usage.setCurrentBytes(20);
  EXPECT_EQ(20, usage.getCurrentBytes());
  EXPECT_EQ(20, usage.getMaxBytes());

  usage.setCurrentBytes(40);
  EXPECT_EQ(40, usage.getCurrentBytes());
  EXPECT_EQ(40, usage.getMaxBytes());

  usage.setCurrentBytes(20);
  EXPECT_EQ(20, usage.getCurrentBytes());
  EXPECT_EQ(40, usage.getMaxBytes());

  usage.setCurrentBytes(60);
  EXPECT_EQ(60, usage.getCurrentBytes());
  EXPECT_EQ(60, usage.getMaxBytes());

  usage.setCurrentBytes(0);
  EXPECT_EQ(0, usage.getCurrentBytes());
  EXPECT_EQ(60, usage.getMaxBytes());
}

TEST(MemoryUsageTest, Incr) {
  MemoryUsage usage;
  usage.incrementCurrentBytes(0);
  EXPECT_EQ(0, usage.getCurrentBytes());
  EXPECT_EQ(0, usage.getMaxBytes());

  usage.incrementCurrentBytes(20);
  EXPECT_EQ(20, usage.getCurrentBytes());
  EXPECT_EQ(20, usage.getMaxBytes());

  usage.incrementCurrentBytes(20);
  EXPECT_EQ(40, usage.getCurrentBytes());
  EXPECT_EQ(40, usage.getMaxBytes());

  usage.incrementCurrentBytes(-20);
  EXPECT_EQ(20, usage.getCurrentBytes());
  EXPECT_EQ(40, usage.getMaxBytes());

  usage.incrementCurrentBytes(40);
  EXPECT_EQ(60, usage.getCurrentBytes());
  EXPECT_EQ(60, usage.getMaxBytes());
}
