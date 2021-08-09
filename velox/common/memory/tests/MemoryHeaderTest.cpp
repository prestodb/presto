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

#include "velox/common/memory/Memory.h"

using namespace ::testing;
using namespace facebook::velox::memory;

TEST(MemoryHeaderTest, GetProcessDefaultMemoryManager) {
  auto& manager_a = getProcessDefaultMemoryManager();
  auto& manager_b = getProcessDefaultMemoryManager();
  ASSERT_EQ(0, manager_a.getRoot().getChildCount());
  ASSERT_EQ(0, manager_b.getRoot().getChildCount());

  auto& child1 = manager_a.getRoot().addChild("child_1");
  auto& child2 = manager_b.getRoot().addChild("child_2");
  EXPECT_EQ(2, manager_a.getRoot().getChildCount());
  EXPECT_EQ(2, manager_b.getRoot().getChildCount());
  child1.removeSelf();
  child2.removeSelf();
  EXPECT_EQ(0, manager_b.getRoot().getChildCount());
}

TEST(MemoryHeaderTest, getDefaultScopedMemoryPool) {
  auto& manager = getProcessDefaultMemoryManager();
  ASSERT_EQ(0, manager.getRoot().getChildCount());
  {
    auto pool_a = getDefaultScopedMemoryPool();
    auto pool_b = getDefaultScopedMemoryPool(42);
    EXPECT_EQ(2, manager.getRoot().getChildCount());
    EXPECT_EQ(42, pool_b->getPool().getCap());
    {
      auto pool_c = getDefaultScopedMemoryPool();
      EXPECT_EQ(3, manager.getRoot().getChildCount());
      {
        auto pool_d = getDefaultScopedMemoryPool();
        EXPECT_EQ(4, manager.getRoot().getChildCount());
      }
      EXPECT_EQ(3, manager.getRoot().getChildCount());
    }
    EXPECT_EQ(2, manager.getRoot().getChildCount());
  }
  EXPECT_EQ(0, manager.getRoot().getChildCount());
}
