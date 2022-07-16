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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/memory/Memory.h"

using namespace ::testing;

constexpr int64_t MB = 1024L * 1024L;
constexpr int64_t GB = 1024L * MB;

namespace facebook {
namespace velox {
namespace memory {

TEST(MemoryPoolTest, Ctor) {
  MemoryManager<MemoryAllocator, 64> manager{8 * GB};
  // While not recomended, the root allocator should be valid.
  auto& root =
      dynamic_cast<MemoryPoolImpl<MemoryAllocator, 64>&>(manager.getRoot());

  EXPECT_EQ(8 * GB, root.cap_);
  EXPECT_EQ(0, root.getCurrentBytes());

  {
    auto fakeRoot = std::make_shared<MemoryPoolImpl<MemoryAllocator, 64>>(
        manager,
        "fake_root",
        std::weak_ptr<MemoryPoolImpl<MemoryAllocator>>(),
        4 * GB);
    EXPECT_EQ("fake_root", fakeRoot->getName());
    EXPECT_EQ(4 * GB, fakeRoot->cap_);
    EXPECT_EQ(&root.allocator_, &fakeRoot->allocator_);
    EXPECT_EQ(0, fakeRoot->getCurrentBytes());
  }
  {
    auto& favoriteChild = dynamic_cast<MemoryPoolImpl<MemoryAllocator, 64>&>(
        root.addChild("favorite_child"));
    EXPECT_EQ("favorite_child", favoriteChild.getName());
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), favoriteChild.cap_);
    EXPECT_EQ(&root.allocator_, &favoriteChild.allocator_);
    EXPECT_EQ(0, favoriteChild.getCurrentBytes());
  }
  {
    auto& naughtyChild = dynamic_cast<MemoryPoolImpl<MemoryAllocator, 64>&>(
        root.addChild("naughty_child", 3 * GB));
    EXPECT_EQ("naughty_child", naughtyChild.getName());
    EXPECT_EQ(3 * GB, naughtyChild.cap_);
    EXPECT_EQ(&root.allocator_, &naughtyChild.allocator_);
    EXPECT_EQ(0, naughtyChild.getCurrentBytes());
  }
  {
    auto fosterChild = std::make_shared<MemoryPoolImpl<MemoryAllocator, 64>>(
        manager, "foster_child", root.getWeakPtr(), 1 * GB);
    EXPECT_EQ("foster_child", fosterChild->getName());
    EXPECT_EQ(1 * GB, fosterChild->cap_);
    EXPECT_EQ(&root.allocator_, &fosterChild->allocator_);
    EXPECT_EQ(0, fosterChild->getCurrentBytes());
  }
}

TEST(MemoryPoolTest, AddChild) {
  MemoryManager<MemoryAllocator> manager{};
  auto& root = manager.getRoot();

  ASSERT_EQ(0, root.getChildCount());
  auto& childOne = root.addChild("child_one");
  auto& childTwo = root.addChild("child_two", 4L * 1024L * 1024L);

  std::vector<MemoryPool*> nodes{};
  ASSERT_EQ(2, root.getChildCount());
  root.visitChildren(
      [&nodes](MemoryPool* child) { nodes.emplace_back(child); });
  EXPECT_THAT(nodes, UnorderedElementsAreArray({&childOne, &childTwo}));

  // We no longer care about name uniqueness.
  EXPECT_NO_THROW(root.addChild("child_one"));
  EXPECT_EQ(3, root.getChildCount());

  // Adding child while capped.
  root.capMemoryAllocation();
  auto& childThree = root.addChild("child_three");
  EXPECT_TRUE(childThree.isMemoryCapped());
}

TEST(MemoryPoolTest, DropChild) {
  MemoryManager<MemoryAllocator> manager{};
  auto& root = manager.getRoot();

  ASSERT_EQ(0, root.getChildCount());
  auto& childOne = root.addChild("child_one");
  auto& childTwo = root.addChild("child_two", 4L * 1024L * 1024L);
  ASSERT_EQ(2, root.getChildCount());

  MemoryPool* childOnePtr = &childOne;
  root.dropChild(childOnePtr);
  EXPECT_EQ(1, root.getChildCount());
  // Node memory would effectively be released after aggregation, but that's
  // not part of the contract, and effectively an undesirable side effect.

  // Remove invalid address
  root.dropChild(childOnePtr);
  EXPECT_EQ(1, root.getChildCount());
}

TEST(MemoryPoolTest, RemoveSelf) {
  MemoryManager<MemoryAllocator> manager{};
  auto& root = manager.getRoot();
  ASSERT_EQ(0, root.getChildCount());

  auto& childOne = root.addChild("child_one");
  auto& childTwo = root.addChild("child_two", 4L * 1024L * 1024L);
  ASSERT_EQ(2, root.getChildCount());

  childOne.removeSelf();
  EXPECT_EQ(1, root.getChildCount());

  childTwo.addChild("child_two_a");
  childTwo.addChild("child_two_b");
  childTwo.removeSelf();
  EXPECT_EQ(0, root.getChildCount());
}

TEST(MemoryPoolTest, CapSubtree) {
  MemoryManager<MemoryAllocator> manager{};
  auto& root = manager.getRoot();

  // left subtree.
  auto& node_a = root.addChild("node_a");
  auto& node_aa = node_a.addChild("node_aa");
  auto& node_ab = node_a.addChild("node_ab");
  auto& node_aba = node_ab.addChild("node_aba");

  // right subtree
  auto& node_b = root.addChild("node_b");
  auto& node_ba = node_b.addChild("node_ba");
  auto& node_bb = node_b.addChild("node_bb");
  auto& node_bc = node_b.addChild("node_bc");

  // Cap left subtree and check that right subtree is not impacted.
  node_a.capMemoryAllocation();
  EXPECT_TRUE(node_a.isMemoryCapped());
  EXPECT_TRUE(node_aa.isMemoryCapped());
  EXPECT_TRUE(node_ab.isMemoryCapped());
  EXPECT_TRUE(node_aba.isMemoryCapped());

  EXPECT_FALSE(root.isMemoryCapped());
  EXPECT_FALSE(node_b.isMemoryCapped());
  EXPECT_FALSE(node_ba.isMemoryCapped());
  EXPECT_FALSE(node_bb.isMemoryCapped());
  EXPECT_FALSE(node_bc.isMemoryCapped());

  // Cap the entire tree.
  root.capMemoryAllocation();
  EXPECT_TRUE(root.isMemoryCapped());
  EXPECT_TRUE(node_a.isMemoryCapped());
  EXPECT_TRUE(node_aa.isMemoryCapped());
  EXPECT_TRUE(node_ab.isMemoryCapped());
  EXPECT_TRUE(node_aba.isMemoryCapped());
  EXPECT_TRUE(node_b.isMemoryCapped());
  EXPECT_TRUE(node_ba.isMemoryCapped());
  EXPECT_TRUE(node_bb.isMemoryCapped());
  EXPECT_TRUE(node_bc.isMemoryCapped());
}

TEST(MemoryPoolTest, UncapMemory) {
  MemoryManager<MemoryAllocator> manager{};
  auto& root = manager.getRoot();

  auto& node_a = root.addChild("node_a");
  auto& node_aa = node_a.addChild("node_aa");
  auto& node_ab = node_a.addChild("node_ab", 31);
  auto& node_aba = node_ab.addChild("node_aba");

  auto& node_b = root.addChild("node_b");
  auto& node_ba = node_b.addChild("node_ba");
  auto& node_bb = node_b.addChild("node_bb");
  auto& node_bc = node_b.addChild("node_bc");

  // Uncap should be recursive.
  node_a.capMemoryAllocation();
  node_b.capMemoryAllocation();
  ASSERT_FALSE(root.isMemoryCapped());
  ASSERT_TRUE(node_a.isMemoryCapped());
  ASSERT_TRUE(node_aa.isMemoryCapped());
  ASSERT_TRUE(node_ab.isMemoryCapped());
  ASSERT_TRUE(node_aba.isMemoryCapped());
  ASSERT_TRUE(node_b.isMemoryCapped());
  ASSERT_TRUE(node_ba.isMemoryCapped());
  ASSERT_TRUE(node_bb.isMemoryCapped());
  ASSERT_TRUE(node_bc.isMemoryCapped());

  node_a.uncapMemoryAllocation();
  EXPECT_FALSE(root.isMemoryCapped());
  EXPECT_FALSE(node_a.isMemoryCapped());
  EXPECT_FALSE(node_aa.isMemoryCapped());
  EXPECT_FALSE(node_ab.isMemoryCapped());
  EXPECT_FALSE(node_aba.isMemoryCapped());

  EXPECT_TRUE(node_b.isMemoryCapped());
  EXPECT_TRUE(node_ba.isMemoryCapped());
  EXPECT_TRUE(node_bb.isMemoryCapped());
  EXPECT_TRUE(node_bc.isMemoryCapped());

  // Cannot uncap a node when parent is still capped.
  ASSERT_TRUE(node_b.isMemoryCapped());
  ASSERT_TRUE(node_bb.isMemoryCapped());
  node_bb.uncapMemoryAllocation();
  EXPECT_TRUE(node_b.isMemoryCapped());
  EXPECT_TRUE(node_bb.isMemoryCapped());

  // Don't uncap if the local cap is exceeded when intermediate
  // caps are supported again.
}

// Mainly tests how it tracks externally allocated memory.
TEST(MemoryPoolTest, ReserveTest) {
  MemoryManager<MemoryAllocator> manager{8 * GB};
  auto& root = manager.getRoot();

  auto& child = root.addChild("elastic_quota");

  const int64_t kChunkSize{32L * MB};

  child.reserve(kChunkSize);
  EXPECT_EQ(child.getCurrentBytes(), kChunkSize);

  child.reserve(2 * kChunkSize);
  EXPECT_EQ(child.getCurrentBytes(), 3 * kChunkSize);

  child.release(1 * kChunkSize);
  EXPECT_EQ(child.getCurrentBytes(), 2 * kChunkSize);

  child.release(2 * kChunkSize);
  EXPECT_EQ(child.getCurrentBytes(), 0);
}

// Mainly tests how it updates the memory usage in MemoryPool.
TEST(MemoryPoolTest, AllocTest) {
  MemoryManager<MemoryAllocator> manager{8 * GB};
  auto& root = manager.getRoot();

  auto& child = root.addChild("elastic_quota");

  const int64_t kChunkSize{32L * MB};

  void* oneChunk = child.allocate(kChunkSize);
  EXPECT_EQ(kChunkSize, child.getCurrentBytes());
  EXPECT_EQ(kChunkSize, child.getMaxBytes());

  void* threeChunks = child.allocate(3 * kChunkSize);
  EXPECT_EQ(4 * kChunkSize, child.getCurrentBytes());
  EXPECT_EQ(4 * kChunkSize, child.getMaxBytes());

  child.free(threeChunks, 3 * kChunkSize);
  EXPECT_EQ(kChunkSize, child.getCurrentBytes());
  EXPECT_EQ(4 * kChunkSize, child.getMaxBytes());

  child.free(oneChunk, kChunkSize);
  EXPECT_EQ(0, child.getCurrentBytes());
  EXPECT_EQ(4 * kChunkSize, child.getMaxBytes());
}

TEST(MemoryPoolTest, ReallocTestSameSize) {
  MemoryManager<MemoryAllocator> manager{8 * GB};
  auto& root = manager.getRoot();

  auto& pool = root.addChild("elastic_quota");

  const int64_t kChunkSize{32L * MB};

  // Realloc the same size.

  void* oneChunk = pool.allocate(kChunkSize);
  EXPECT_EQ(kChunkSize, pool.getCurrentBytes());
  EXPECT_EQ(kChunkSize, pool.getMaxBytes());

  void* anotherChunk = pool.reallocate(oneChunk, kChunkSize, kChunkSize);
  EXPECT_EQ(kChunkSize, pool.getCurrentBytes());
  EXPECT_EQ(kChunkSize, pool.getMaxBytes());

  pool.free(anotherChunk, kChunkSize);
  EXPECT_EQ(0, pool.getCurrentBytes());
  EXPECT_EQ(kChunkSize, pool.getMaxBytes());
}

TEST(MemoryPoolTest, ReallocTestHigher) {
  MemoryManager<MemoryAllocator> manager{8 * GB};
  auto& root = manager.getRoot();

  auto& pool = root.addChild("elastic_quota");

  const int64_t kChunkSize{32L * MB};
  // Realloc higher.
  void* oneChunk = pool.allocate(kChunkSize);
  EXPECT_EQ(kChunkSize, pool.getCurrentBytes());
  EXPECT_EQ(kChunkSize, pool.getMaxBytes());

  void* threeChunks = pool.reallocate(oneChunk, kChunkSize, 3 * kChunkSize);
  EXPECT_EQ(3 * kChunkSize, pool.getCurrentBytes());
  EXPECT_EQ(3 * kChunkSize, pool.getMaxBytes());

  pool.free(threeChunks, 3 * kChunkSize);
  EXPECT_EQ(0, pool.getCurrentBytes());
  EXPECT_EQ(3 * kChunkSize, pool.getMaxBytes());
}

TEST(MemoryPoolTest, ReallocTestLower) {
  MemoryManager<MemoryAllocator> manager{8 * GB};
  auto& root = manager.getRoot();
  auto& pool = root.addChild("elastic_quota");

  const int64_t kChunkSize{32L * MB};
  // Realloc lower.
  void* threeChunks = pool.allocate(3 * kChunkSize);
  EXPECT_EQ(3 * kChunkSize, pool.getCurrentBytes());
  EXPECT_EQ(3 * kChunkSize, pool.getMaxBytes());

  void* oneChunk = pool.reallocate(threeChunks, 3 * kChunkSize, kChunkSize);
  EXPECT_EQ(kChunkSize, pool.getCurrentBytes());
  EXPECT_EQ(3 * kChunkSize, pool.getMaxBytes());

  pool.free(oneChunk, kChunkSize);
  EXPECT_EQ(0, pool.getCurrentBytes());
  EXPECT_EQ(3 * kChunkSize, pool.getMaxBytes());
}

TEST(MemoryPoolTest, CapAllocation) {
  MemoryManager<MemoryAllocator> manager{8 * GB};
  auto& root = manager.getRoot();

  auto& pool = root.addChild("static_quota", 64L * MB);

  // Capping malloc.
  {
    ASSERT_EQ(0, pool.getCurrentBytes());
    ASSERT_FALSE(pool.isMemoryCapped());
    void* oneChunk = pool.allocate(32L * MB);
    ASSERT_EQ(32L * MB, pool.getCurrentBytes());
    EXPECT_THROW(pool.allocate(34L * MB), velox::VeloxRuntimeError);
    EXPECT_FALSE(pool.isMemoryCapped());

    pool.free(oneChunk, 32L * MB);
  }
  // Capping realloc.
  {
    ASSERT_EQ(0, pool.getCurrentBytes());
    ASSERT_FALSE(pool.isMemoryCapped());
    void* oneChunk = pool.allocate(32L * MB);
    ASSERT_EQ(32L * MB, pool.getCurrentBytes());
    EXPECT_THROW(
        pool.reallocate(oneChunk, 32L * MB, 66L * MB),
        velox::VeloxRuntimeError);
    EXPECT_FALSE(pool.isMemoryCapped());

    pool.free(oneChunk, 32L * MB);
  }
}

TEST(MemoryPoolTest, MemoryCapExceptions) {
  MemoryManager<MemoryAllocator> manager{127L * MB};
  auto& root = manager.getRoot();

  auto& pool = root.addChild("static_quota", 63L * MB);

  // Capping locally.
  {
    ASSERT_EQ(0, pool.getCurrentBytes());
    ASSERT_FALSE(pool.isMemoryCapped());
    try {
      pool.allocate(64L * MB);
    } catch (const velox::VeloxRuntimeError& ex) {
      EXPECT_EQ(error_source::kErrorSourceRuntime.c_str(), ex.errorSource());
      EXPECT_EQ(error_code::kMemCapExceeded.c_str(), ex.errorCode());
      EXPECT_TRUE(ex.isRetriable());
      EXPECT_EQ("Exceeded memory cap of 63 MB", ex.message());
    }
    ASSERT_FALSE(pool.isMemoryCapped());
  }
  // Capping memory manager.
  {
    ASSERT_EQ(0, pool.getCurrentBytes());
    ASSERT_FALSE(pool.isMemoryCapped());
    try {
      pool.allocate(128L * MB);
    } catch (const velox::VeloxRuntimeError& ex) {
      EXPECT_EQ(error_source::kErrorSourceRuntime.c_str(), ex.errorSource());
      EXPECT_EQ(error_code::kMemCapExceeded.c_str(), ex.errorCode());
      EXPECT_TRUE(ex.isRetriable());
      EXPECT_EQ("Exceeded memory manager cap of 127 MB", ex.message());
    }
    ASSERT_FALSE(pool.isMemoryCapped());
  }
  // Capping manually.
  {
    ASSERT_EQ(0, pool.getCurrentBytes());
    pool.capMemoryAllocation();
    ASSERT_TRUE(pool.isMemoryCapped());
    try {
      pool.allocate(8L * MB);
    } catch (const velox::VeloxRuntimeError& ex) {
      EXPECT_EQ(error_source::kErrorSourceRuntime.c_str(), ex.errorSource());
      EXPECT_EQ(error_code::kMemCapExceeded.c_str(), ex.errorCode());
      EXPECT_TRUE(ex.isRetriable());
      EXPECT_EQ("Memory allocation manually capped", ex.message());
    }
  }
}

TEST(MemoryPoolTest, GetAlignment) {
  {
    EXPECT_EQ(
        kNoAlignment,
        MemoryManager<MemoryAllocator>{32 * MB}.getRoot().getAlignment());
  }
  {
    MemoryManager<MemoryAllocator, 64> manager{32 * MB};
    EXPECT_EQ(64, manager.getRoot().getAlignment());
  }
}

TEST(MemoryPoolTest, ScopedMemoryPoolSemantics) {
  MemoryManager<MemoryAllocator> manager{32 * MB};
  auto& root = manager.getRoot();
  {
    auto& pool = root.addChild("unfortunate");
    auto weakPtr = pool.getWeakPtr();
    pool.removeSelf();
    EXPECT_ANY_THROW(ScopedMemoryPool{weakPtr});
  }
  // This checks that we can exit without crashing on
  // "dangling" reference.
  {
    auto& pool = root.addChild("irresponsible");
    auto weakPtr = pool.getWeakPtr();
    ScopedMemoryPool scopedPool{weakPtr};
    pool.removeSelf();
  }
}

TEST(MemoryPoolTest, MemoryManagerGlobalCap) {
  MemoryManager<MemoryAllocator> manager{32 * MB};

  auto& root = manager.getRoot();
  auto& pool = root.addChild("unbounded");
  auto& child = pool.addChild("unbounded");
  void* oneChunk = child.allocate(32L * MB);
  ASSERT_FALSE(root.isMemoryCapped());
  ASSERT_EQ(0L, root.getCurrentBytes());
  ASSERT_FALSE(child.isMemoryCapped());
  EXPECT_THROW(child.allocate(32L * MB), velox::VeloxRuntimeError);
  ASSERT_FALSE(root.isMemoryCapped());
  ASSERT_EQ(0L, root.getCurrentBytes());
  ASSERT_FALSE(child.isMemoryCapped());
  EXPECT_THROW(
      child.reallocate(oneChunk, 32L * MB, 64L * MB), velox::VeloxRuntimeError);
  child.free(oneChunk, 32L * MB);
}

// Tests how ScopedMemPool updates its and its parent's memory usage
// and what it returns for getCurrentBytes()/getMaxBytes and
// with memoryUsageTracker.
TEST(MemoryPoolTest, scopedChildUsageTest) {
  MemoryManager<MemoryAllocator> manager{8 * GB};
  auto& root = manager.getRoot();

  auto& pool = root.addChild("main_pool");

  auto verifyUsage = [](std::vector<std::unique_ptr<MemoryPool>>& tree,
                        std::vector<int> currentBytes,
                        std::vector<int> maxBytes,
                        std::vector<int> trackerCurrentBytes,
                        std::vector<int> trackerMaxBytes) {
    ASSERT_TRUE(
        tree.size() == currentBytes.size() && tree.size() == maxBytes.size() &&
        tree.size() == trackerCurrentBytes.size() &&
        tree.size() == trackerMaxBytes.size());
    for (unsigned i = 0, e = tree.size(); i != e; ++i) {
      EXPECT_EQ(tree[i]->getCurrentBytes(), currentBytes[i]);
      EXPECT_EQ(tree[i]->getMaxBytes(), maxBytes[i]);
      auto tracker = tree[i]->getMemoryUsageTracker();
      ASSERT_TRUE(tracker);
      EXPECT_GE(tracker->getCurrentUserBytes(), trackerCurrentBytes[i]);
      EXPECT_GE(tracker->getPeakTotalBytes(), trackerMaxBytes[i]);
    }
  };

  // Create the following MemoryPool tree.
  //              p0
  //              |
  //      +-------+--------+
  //      |                |
  //     p1                p2
  //      |                |
  //  +------+         +---+---+
  // p3      p4       p5       p6
  //
  std::vector<std::unique_ptr<MemoryPool>> tree;
  tree.push_back(pool.addScopedChild("p0"));
  tree[0]->setMemoryUsageTracker(MemoryUsageTracker::create());

  // first level: p1, p2.
  tree.push_back(tree[0]->addScopedChild("p1"));
  tree.push_back(tree[0]->addScopedChild("p2"));

  // second level: p3, p4, p5, p6.
  tree.push_back(tree[1]->addScopedChild("p3"));
  tree.push_back(tree[1]->addScopedChild("p4"));
  tree.push_back(tree[2]->addScopedChild("p5"));
  tree.push_back(tree[2]->addScopedChild("p6"));

  verifyUsage(
      tree,
      {0, 0, 0, 0, 0, 0, 0},
      {0, 0, 0, 0, 0, 0, 0},
      {0, 0, 0, 0, 0, 0, 0},
      {0, 0, 0, 0, 0, 0, 0});

  void* p3Chunk0 = tree[3]->allocate(16);
  verifyUsage(
      tree,
      {0, 0, 0, 16, 0, 0, 0},
      {0, 0, 0, 16, 0, 0, 0},
      {16, 16, 0, 16, 0, 0, 0},
      {16, 16, 0, 16, 0, 0, 0});

  void* p5Chunk0 = tree[5]->allocate(64);
  verifyUsage(
      tree,
      {0, 0, 0, 16, 0, 64, 0},
      {0, 0, 0, 16, 0, 64, 0},
      {80, 16, 64, 16, 0, 64, 0},
      {80, 16, 64, 16, 0, 64, 0});

  tree[3]->free(p3Chunk0, 16);

  verifyUsage(
      tree,
      {0, 0, 0, 0, 0, 64, 0},
      {0, 0, 0, 16, 0, 64, 0},
      {64, 0, 64, 0, 0, 64, 0},
      {80, 16, 64, 16, 0, 64, 0});

  tree[5]->free(p5Chunk0, 64);
  verifyUsage(
      tree,
      {0, 0, 0, 0, 0, 0, 0},
      {0, 0, 0, 16, 0, 64, 0},
      {0, 0, 0, 0, 0, 0, 0},
      {80, 16, 64, 16, 0, 64, 0});

  std::vector<std::shared_ptr<MemoryUsageTracker>> trackers;
  for (unsigned i = 0, e = tree.size(); i != e; ++i) {
    trackers.push_back(tree[i]->getMemoryUsageTracker());
  }

  // Release all memory pool.
  tree.clear();

  std::vector<int64_t> expectedCurrentBytes({0, 0, 0, 0, 0, 0, 0});
  std::vector<int64_t> expectedMaxBytes({80, 16, 64, 16, 0, 64, 0});

  // Verify the stats still holds the correct stats.
  for (unsigned i = 0, e = trackers.size(); i != e; ++i) {
    EXPECT_GE(trackers[i]->getCurrentUserBytes(), expectedCurrentBytes[i]);
    EXPECT_GE(trackers[i]->getPeakTotalBytes(), expectedMaxBytes[i]);
  }
}

TEST(MemoryPoolTest, getPreferredSize) {
  MemoryManager<MemoryAllocator, 64> manager{};
  auto& pool =
      dynamic_cast<MemoryPoolImpl<MemoryAllocator, 64>&>(manager.getRoot());

  // size < 8
  EXPECT_EQ(8, pool.getPreferredSize(1));
  EXPECT_EQ(8, pool.getPreferredSize(2));
  EXPECT_EQ(8, pool.getPreferredSize(4));
  EXPECT_EQ(8, pool.getPreferredSize(7));
  // size >=8, pick 2^k or 1.5 * 2^k
  EXPECT_EQ(8, pool.getPreferredSize(8));
  EXPECT_EQ(24, pool.getPreferredSize(24));
  EXPECT_EQ(32, pool.getPreferredSize(25));
  EXPECT_EQ(1024 * 1536, pool.getPreferredSize(1024 * 1024 + 1));
  EXPECT_EQ(1024 * 1024 * 2, pool.getPreferredSize(1024 * 1536 + 1));
}

TEST(MemoryPoolTest, getPreferredSizeOverflow) {
  MemoryManager<MemoryAllocator, 64> manager{};
  auto& pool =
      dynamic_cast<MemoryPoolImpl<MemoryAllocator, 64>&>(manager.getRoot());

  EXPECT_EQ(1ULL << 32, pool.getPreferredSize((1ULL << 32) - 1));
  EXPECT_EQ(1ULL << 63, pool.getPreferredSize((1ULL << 62) - 1 + (1ULL << 62)));
}
} // namespace memory
} // namespace velox
} // namespace facebook
