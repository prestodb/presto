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

#include "velox/dwio/dwrf/reader/StreamLabels.h"

using namespace ::testing;
using facebook::velox::memory::AllocationPool;
using namespace facebook::velox::dwrf;

class StreamLabelsTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    facebook::velox::memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(StreamLabelsTest, E2E) {
  auto pool = facebook::velox::memory::memoryManager()->addLeafPool();
  AllocationPool allocationPool(pool.get());
  StreamLabels root(allocationPool);
  auto c0 = root.append("c0");
  auto c1 = root.append("c1");
  auto c0_f0 = c0.append("f0");
  auto c1_f0 = c1.append("f0");
  EXPECT_EQ(root.label(), "/");
  EXPECT_EQ(c0.label(), "/c0");
  EXPECT_EQ(c1.label(), "/c1");
  EXPECT_EQ(c0_f0.label(), "/c0/f0");
  EXPECT_EQ(c1_f0.label(), "/c1/f0");
}
