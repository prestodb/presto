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

#include "velox/common/memory/Memory.h"

using namespace ::testing;
using namespace facebook::velox::memory;

TEST(MemoryHeaderTest, GetProcessDefaultMemoryManager) {
  auto& managerA = getProcessDefaultMemoryManager();
  auto& managerB = getProcessDefaultMemoryManager();
  ASSERT_EQ(0, managerA.getRoot().getChildCount());
  ASSERT_EQ(0, managerB.getRoot().getChildCount());

  auto child1 = managerA.getRoot().addChild("child_1");
  auto child2 = managerB.getRoot().addChild("child_2");
  EXPECT_EQ(2, managerA.getRoot().getChildCount());
  EXPECT_EQ(2, managerB.getRoot().getChildCount());
  child1.reset();
  child2.reset();
  EXPECT_EQ(0, managerB.getRoot().getChildCount());
}

TEST(MemoryHeaderTest, getDefaultMemoryPool) {
  auto& manager = getProcessDefaultMemoryManager();
  ASSERT_EQ(0, manager.getRoot().getChildCount());
  {
    auto poolA = getDefaultMemoryPool();
    auto poolB = getDefaultMemoryPool();
    EXPECT_EQ(2, manager.getRoot().getChildCount());
    {
      auto poolC = getDefaultMemoryPool();
      EXPECT_EQ(3, manager.getRoot().getChildCount());
      {
        auto poolD = getDefaultMemoryPool();
        EXPECT_EQ(4, manager.getRoot().getChildCount());
      }
      EXPECT_EQ(3, manager.getRoot().getChildCount());
    }
    EXPECT_EQ(2, manager.getRoot().getChildCount());
  }
  EXPECT_EQ(0, manager.getRoot().getChildCount());
}
