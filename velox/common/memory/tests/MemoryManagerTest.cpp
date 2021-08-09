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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/base/VeloxException.h"
#include "velox/common/memory/Memory.h"

using namespace ::testing;

namespace facebook {
namespace velox {
namespace memory {

TEST(MemoryManagerTest, Ctor) {
  {
    MemoryManager<MemoryAllocator> manager{};
    const auto& root = manager.getRoot();

    EXPECT_EQ(std::numeric_limits<int64_t>::max(), root.getCap());
    EXPECT_EQ(0, root.getChildCount());
    EXPECT_EQ(0, root.getCurrentBytes());
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), manager.getMemoryQuota());
    EXPECT_EQ(0, manager.getTotalBytes());
  }
  {
    MemoryManager<MemoryAllocator> manager{8L * 1024 * 1024};
    const auto& root = manager.getRoot();

    EXPECT_EQ(8L * 1024 * 1024, root.getCap());
    EXPECT_EQ(0, root.getChildCount());
    EXPECT_EQ(0, root.getCurrentBytes());
    EXPECT_EQ(8L * 1024 * 1024, manager.getMemoryQuota());
    EXPECT_EQ(0, manager.getTotalBytes());
  }
  { EXPECT_ANY_THROW(MemoryManager<MemoryAllocator> manager{-1}); }
}

TEST(MemoryManagerTest, AggregateStats) {
  MemoryManager<MemoryAllocator, kNoAlignment> manager{100};
  manager.pauseAggregation();
  auto& root = manager.getRoot();

  // Build a tree of pools. Encode the node names with insertion order.
  auto& pool_a = root.addChild("pool_a");
  auto& pool_b = root.addChild("pool_b", 32);
  auto& pool_c = root.addChild("pool_c");

  auto& pool_ba = pool_b.addChild("pool_ba");
  auto& pool_bb = pool_b.addChild("pool_bb");

  // Allocate (at the leaf) and check the appropriate aggregates.
  ASSERT_EQ(0, root.getCurrentBytes());
  ASSERT_EQ(0, pool_b.getCurrentBytes());

  void* p1 = pool_a.allocate(4);
  void* p2 = pool_ba.allocate(12);
  void* p3 = pool_bb.allocate(16);
  void* p4 = pool_c.allocate(8);

  // Check that leaves have up to date tracking due to local
  // updates, but nothing is pushed to aggregate nodes.
  EXPECT_EQ(0, root.getCurrentBytes());
  EXPECT_EQ(0, pool_b.getCurrentBytes());
  EXPECT_EQ(4, pool_a.getCurrentBytes());
  EXPECT_EQ(12, pool_ba.getCurrentBytes());
  EXPECT_EQ(16, pool_bb.getCurrentBytes());
  EXPECT_EQ(8, pool_c.getCurrentBytes());

  manager.aggregateStats(&root);
  EXPECT_EQ(40, root.getCurrentBytes());
  EXPECT_EQ(28, pool_b.getCurrentBytes());
  EXPECT_EQ(4, pool_a.getCurrentBytes());
  EXPECT_EQ(12, pool_ba.getCurrentBytes());
  EXPECT_EQ(16, pool_bb.getCurrentBytes());
  EXPECT_EQ(8, pool_c.getCurrentBytes());

  // Trigger capping.
  p2 = pool_ba.reallocate(p2, 12, 24);

  manager.aggregateStats(&root);
  EXPECT_EQ(52, root.getCurrentBytes());
  EXPECT_EQ(40, pool_b.getCurrentBytes());
  EXPECT_EQ(4, pool_a.getCurrentBytes());
  EXPECT_EQ(24, pool_ba.getCurrentBytes());
  EXPECT_EQ(16, pool_bb.getCurrentBytes());
  EXPECT_EQ(8, pool_c.getCurrentBytes());

  EXPECT_TRUE(pool_b.isMemoryCapped());
  EXPECT_TRUE(pool_ba.isMemoryCapped());
  EXPECT_TRUE(pool_bb.isMemoryCapped());

  // Capping decisions should only be reverted by parent.
  manager.aggregateStats(&root);
  EXPECT_TRUE(pool_b.isMemoryCapped());
  EXPECT_TRUE(pool_ba.isMemoryCapped());
  EXPECT_TRUE(pool_bb.isMemoryCapped());

  // Trigger uncapping.
  p2 = pool_ba.reallocate(p2, 24, 16);

  manager.aggregateStats(&root);
  EXPECT_EQ(44, root.getCurrentBytes());
  EXPECT_EQ(32, pool_b.getCurrentBytes());
  EXPECT_EQ(4, pool_a.getCurrentBytes());
  EXPECT_EQ(16, pool_ba.getCurrentBytes());
  EXPECT_EQ(16, pool_bb.getCurrentBytes());
  EXPECT_EQ(8, pool_c.getCurrentBytes());

  EXPECT_FALSE(pool_b.isMemoryCapped());
  EXPECT_FALSE(pool_ba.isMemoryCapped());
  EXPECT_FALSE(pool_bb.isMemoryCapped());

  pool_a.free(p1, 4);
  pool_ba.free(p2, 16);
  pool_bb.free(p3, 16);
  pool_c.free(p4, 8);
}

// TODO: when run sequentially, e.g. `buck run dwio/memory/...`, this has side
// effects for other tests using process singleton memory manager. Might need to
// use folly::Singleton for isolation by tag.
TEST(MemoryManagerTest, GlobalMemoryManager) {
  auto& manager = MemoryManager<>::getProcessDefaultManager();
  auto& managerII = MemoryManager<>::getProcessDefaultManager();

  auto& root = manager.getRoot();
  root.addChild("some_child", 42);
  ASSERT_EQ(1, root.getChildCount());

  auto& rootII = managerII.getRoot();
  EXPECT_EQ(1, rootII.getChildCount());
  std::vector<MemoryPool*> pools{};
  rootII.visitChildren(
      [&pools](MemoryPool* child) { pools.emplace_back(child); });
  ASSERT_EQ(1, pools.size());
  auto& pool = *pools.back();
  EXPECT_EQ("some_child", pool.getName());
  EXPECT_EQ(42, pool.getCap());
}

TEST(MemoryManagerTest, Reserve) {
  {
    MemoryManager<MemoryAllocator> manager{};
    EXPECT_TRUE(manager.reserve(0));
    EXPECT_EQ(0, manager.getTotalBytes());
    manager.release(0);
    EXPECT_TRUE(manager.reserve(42));
    EXPECT_EQ(42, manager.getTotalBytes());
    manager.release(42);
    EXPECT_TRUE(manager.reserve(std::numeric_limits<int64_t>::max()));
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), manager.getTotalBytes());
  }
  {
    MemoryManager<MemoryAllocator> manager{42};
    EXPECT_TRUE(manager.reserve(1));
    EXPECT_TRUE(manager.reserve(1));
    EXPECT_TRUE(manager.reserve(2));
    EXPECT_TRUE(manager.reserve(3));
    EXPECT_TRUE(manager.reserve(5));
    EXPECT_TRUE(manager.reserve(8));
    EXPECT_TRUE(manager.reserve(13));
    EXPECT_FALSE(manager.reserve(21));
    EXPECT_FALSE(manager.reserve(1));
    EXPECT_FALSE(manager.reserve(2));
    EXPECT_FALSE(manager.reserve(3));
    manager.release(20);
    EXPECT_TRUE(manager.reserve(1));
    EXPECT_FALSE(manager.reserve(2));
  }
}

TEST(MemoryManagerTest, GlobalMemoryManagerQuota) {
  auto& manager = MemoryManager<>::getProcessDefaultManager();
  EXPECT_THROW(
      MemoryManager<>::getProcessDefaultManager(42, true),
      velox::VeloxUserError);

  auto& coercedManager = MemoryManager<>::getProcessDefaultManager(42);
  EXPECT_EQ(manager.getMemoryQuota(), coercedManager.getMemoryQuota());
}
} // namespace memory
} // namespace velox
} // namespace facebook
