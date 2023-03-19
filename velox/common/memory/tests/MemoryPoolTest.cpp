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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MmapAllocator.h"

using namespace ::testing;
using namespace facebook::velox::cache;

constexpr int64_t KB = 1024L;
constexpr int64_t MB = 1024L * KB;
constexpr int64_t GB = 1024L * MB;

namespace facebook {
namespace velox {
namespace memory {

struct TestParam {
  bool useMmap;
  bool useCache;

  TestParam(bool _useMmap, bool _useCache)
      : useMmap(_useMmap), useCache(_useCache) {}

  std::string toString() const {
    return fmt::format("useMmap{} useCache{}", useMmap, useCache);
  }
};

class MemoryPoolTest : public testing::TestWithParam<TestParam> {
 public:
  static const std::vector<TestParam> getTestParams() {
    std::vector<TestParam> params;
    params.push_back({true, true});
    params.push_back({true, false});
    params.push_back({false, true});
    params.push_back({false, false});
    return params;
  }

 protected:
  MemoryPoolTest()
      : useMmap_(GetParam().useMmap), useCache_(GetParam().useCache) {}

  void SetUp() override {
    // For duration of the test, make a local MmapAllocator that will not be
    // seen by any other test.
    const uint64_t kCapacity = 8UL << 30;
    if (useMmap_) {
      MmapAllocator::Options opts{8UL << 30};
      allocator_ = std::make_shared<MmapAllocator>(opts);
      if (useCache_) {
        cache_ =
            std::make_shared<AsyncDataCache>(allocator_, kCapacity, nullptr);
        MemoryAllocator::setDefaultInstance(cache_.get());
      } else {
        MemoryAllocator::setDefaultInstance(allocator_.get());
      }
    } else {
      allocator_ = MemoryAllocator::createDefaultInstance();
      if (useCache_) {
        cache_ =
            std::make_shared<AsyncDataCache>(allocator_, kCapacity, nullptr);
        MemoryAllocator::setDefaultInstance(cache_.get());
      } else {
        MemoryAllocator::setDefaultInstance(allocator_.get());
      }
    }
    const auto seed =
        std::chrono::system_clock::now().time_since_epoch().count();
    rng_.seed(seed);
    LOG(INFO) << "Random seed: " << seed;
  }

  void TearDown() override {
    allocator_->testingClearFailureInjection();
    MmapAllocator::setDefaultInstance(nullptr);
  }

  void reset() {
    TearDown();
    SetUp();
  }

  std::shared_ptr<IMemoryManager> getMemoryManager(int64_t quota) {
    return std::make_shared<MemoryManager>(
        IMemoryManager::Options{.capacity = quota});
  }

  std::shared_ptr<IMemoryManager> getMemoryManager(
      const IMemoryManager::Options& options) {
    return std::make_shared<MemoryManager>(options);
  }

  const bool useMmap_;
  const bool useCache_;
  folly::Random::DefaultGenerator rng_;
  std::shared_ptr<MemoryAllocator> allocator_;
  std::shared_ptr<AsyncDataCache> cache_;
};

TEST(MemoryPoolTest, Ctor) {
  constexpr uint16_t kAlignment = 64;
  MemoryManager manager{{.alignment = kAlignment, .capacity = 8 * GB}};
  // While not recommended, the root allocator should be valid.
  auto root = manager.getPool("Ctor");
  ASSERT_EQ(root->kind(), MemoryPool::Kind::kAggregate);
  ASSERT_EQ(root->getCurrentBytes(), 0);
  ASSERT_EQ(root->parent(), nullptr);

  {
    auto fakeRoot = std::make_shared<MemoryPoolImpl>(
        &manager, "fake_root", MemoryPool::Kind::kAggregate, nullptr);
    ASSERT_EQ("fake_root", fakeRoot->name());
    ASSERT_EQ(
        static_cast<MemoryPoolImpl*>(root.get())->testingAllocator(),
        fakeRoot->testingAllocator());
    ASSERT_EQ(0, fakeRoot->getCurrentBytes());
    ASSERT_EQ(fakeRoot->parent(), nullptr);
  }
  {
    auto child = root->addChild("child", MemoryPool::Kind::kLeaf);
    ASSERT_EQ(child->parent(), root.get());
    auto& favoriteChild = dynamic_cast<MemoryPoolImpl&>(*child);
    ASSERT_EQ("child", favoriteChild.name());
    ASSERT_EQ(
        static_cast<MemoryPoolImpl*>(root.get())->testingAllocator(),
        favoriteChild.testingAllocator());
    ASSERT_EQ(favoriteChild.getCurrentBytes(), 0);
  }
}

TEST(MemoryPoolTest, AddChild) {
  MemoryManager manager{};
  auto root = manager.getPool("root");
  ASSERT_EQ(root->parent(), nullptr);

  ASSERT_EQ(0, root->getChildCount());
  auto childOne = root->addChild("child_one", MemoryPool::Kind::kLeaf);
  auto childTwo = root->addChild("child_two", MemoryPool::Kind::kAggregate);

  std::vector<MemoryPool*> nodes{};
  ASSERT_EQ(2, root->getChildCount());
  root->visitChildren(
      [&nodes](MemoryPool* child) { nodes.emplace_back(child); });
  ASSERT_THAT(
      nodes, UnorderedElementsAreArray({childOne.get(), childTwo.get()}));
  // Child pool name collision.
  ASSERT_THROW(
      root->addChild("child_one", MemoryPool::Kind::kAggregate),
      VeloxRuntimeError);
  ASSERT_EQ(root->getChildCount(), 2);
  childOne.reset();
  ASSERT_EQ(root->getChildCount(), 1);
  childOne = root->addChild("child_one", MemoryPool::Kind::kLeaf);
  ASSERT_EQ(root->getChildCount(), 2);
}

TEST_P(MemoryPoolTest, dropChild) {
  MemoryManager manager{};
  auto root = manager.getPool("root");
  ASSERT_EQ(root->parent(), nullptr);

  ASSERT_EQ(root->getChildCount(), 0);
  auto childOne = root->addChild("child_one", MemoryPool::Kind::kLeaf);
  ASSERT_EQ(childOne->parent(), root.get());
  auto childTwo = root->addChild("child_two", MemoryPool::Kind::kAggregate);
  ASSERT_EQ(childTwo->parent(), root.get());
  ASSERT_EQ(root->getChildCount(), 2);

  childOne.reset();
  ASSERT_EQ(root->getChildCount(), 1);

  // Remove invalid address.
  childTwo.reset();
  ASSERT_EQ(root->getChildCount(), 0);

  // Check parent pool is alive until all the children has been destroyed.
  auto child = root->addChild("child", MemoryPool::Kind::kAggregate);
  ASSERT_EQ(child->parent(), root.get());
  auto* rawChild = child.get();
  auto grandChild1 = child->addChild("grandChild1");
  ASSERT_EQ(grandChild1->parent(), child.get());
  ASSERT_THROW(
      child->addChild("grandChild1", MemoryPool::Kind::kAggregate),
      VeloxRuntimeError);
  auto grandChild2 = child->addChild("grandChild2");
  ASSERT_EQ(grandChild2->parent(), child.get());
  ASSERT_EQ(1, root->getChildCount());
  ASSERT_EQ(2, child->getChildCount());
  ASSERT_EQ(0, grandChild1->getChildCount());
  ASSERT_EQ(0, grandChild2->getChildCount());
  child.reset();
  ASSERT_EQ(1, root->getChildCount());
  ASSERT_EQ(2, rawChild->getChildCount());
  grandChild1.reset();
  ASSERT_EQ(1, root->getChildCount());
  ASSERT_EQ(1, rawChild->getChildCount());
  grandChild2.reset();
  ASSERT_EQ(0, root->getChildCount());
}

// Mainly tests how it tracks externally allocated memory.
TEST(MemoryPoolTest, ReserveTest) {
  MemoryManager manager{{.capacity = 8 * GB}};
  auto root = manager.getPool();

  auto child = root->addChild("elastic_quota");

  const int64_t kChunkSize{32L * MB};

  child->reserve(kChunkSize);
  ASSERT_EQ(child->getCurrentBytes(), kChunkSize);

  child->reserve(2 * kChunkSize);
  ASSERT_EQ(child->getCurrentBytes(), 3 * kChunkSize);

  child->release(1 * kChunkSize);
  ASSERT_EQ(child->getCurrentBytes(), 2 * kChunkSize);

  child->release(2 * kChunkSize);
  ASSERT_EQ(child->getCurrentBytes(), 0);
}

MachinePageCount numPagesNeeded(
    const MmapAllocator* mmapAllocator,
    MachinePageCount numPages) {
  auto& sizeClasses = mmapAllocator->sizeClasses();
  if (numPages > sizeClasses.back()) {
    return numPages;
  }
  for (auto& sizeClass : sizeClasses) {
    if (sizeClass >= numPages) {
      return sizeClass;
    }
  }
  VELOX_UNREACHABLE();
}

void testMmapMemoryAllocation(
    const MmapAllocator* mmapAllocator,
    MachinePageCount allocPages,
    size_t allocCount) {
  MemoryManager manager({.capacity = 8 * GB});
  const auto kPageSize = 4 * KB;

  auto root = manager.getPool();
  auto child = root->addChild("elastic_quota");

  std::vector<void*> allocations;
  uint64_t totalPageAllocated = 0;
  uint64_t totalPageMapped = 0;
  const auto pageIncrement = numPagesNeeded(mmapAllocator, allocPages);
  const auto isSizeClassAlloc =
      allocPages <= mmapAllocator->sizeClasses().back();
  const auto byteSize = allocPages * kPageSize;
  const std::string buffer(byteSize, 'x');
  for (size_t i = 0; i < allocCount; i++) {
    void* allocResult = nullptr;
    ASSERT_NO_THROW(allocResult = child->allocate(byteSize));
    ASSERT_TRUE(allocResult != nullptr);

    // Write data to let mapped address to be backed by physical memory
    memcpy(allocResult, buffer.data(), byteSize);
    allocations.emplace_back(allocResult);
    totalPageAllocated += pageIncrement;
    totalPageMapped += pageIncrement;
    ASSERT_EQ(mmapAllocator->numAllocated(), totalPageAllocated);
    ASSERT_EQ(
        isSizeClassAlloc ? mmapAllocator->numMapped()
                         : mmapAllocator->numExternalMapped(),
        totalPageMapped);
  }
  for (size_t i = 0; i < allocCount; i++) {
    ASSERT_NO_THROW(child->free(allocations[i], byteSize));
    totalPageAllocated -= pageIncrement;
    ASSERT_EQ(mmapAllocator->numAllocated(), totalPageAllocated);
    if (isSizeClassAlloc) {
      ASSERT_EQ(mmapAllocator->numMapped(), totalPageMapped);
    } else {
      totalPageMapped -= pageIncrement;
      ASSERT_EQ(mmapAllocator->numExternalMapped(), totalPageMapped);
    }
  }
}

TEST(MemoryPoolTest, SmallMmapMemoryAllocation) {
  MmapAllocator::Options options;
  options.capacity = 8 * GB;
  auto mmapAllocator = std::make_shared<memory::MmapAllocator>(options);
  MemoryAllocator::setDefaultInstance(mmapAllocator.get());
  testMmapMemoryAllocation(mmapAllocator.get(), 6, 100);
  MemoryAllocator::setDefaultInstance(nullptr);
}

TEST(MemoryPoolTest, BigMmapMemoryAllocation) {
  MmapAllocator::Options options;
  options.capacity = 8 * GB;
  auto mmapAllocator = std::make_shared<memory::MmapAllocator>(options);
  MemoryAllocator::setDefaultInstance(mmapAllocator.get());
  testMmapMemoryAllocation(
      mmapAllocator.get(), mmapAllocator->sizeClasses().back() + 56, 20);
  MemoryAllocator::setDefaultInstance(nullptr);
}

// Mainly tests how it updates the memory usage in Memorypool->
TEST_P(MemoryPoolTest, AllocTest) {
  auto manager = getMemoryManager(8 * GB);
  auto root = manager->getPool();

  auto child = root->addChild("elastic_quota");

  const int64_t kChunkSize{32L * MB};

  void* oneChunk = child->allocate(kChunkSize);
  ASSERT_EQ(reinterpret_cast<uint64_t>(oneChunk) % child->getAlignment(), 0);
  ASSERT_EQ(kChunkSize, child->getCurrentBytes());
  ASSERT_EQ(kChunkSize, child->getMaxBytes());

  void* threeChunks = child->allocate(3 * kChunkSize);
  ASSERT_EQ(4 * kChunkSize, child->getCurrentBytes());
  ASSERT_EQ(4 * kChunkSize, child->getMaxBytes());

  child->free(threeChunks, 3 * kChunkSize);
  ASSERT_EQ(kChunkSize, child->getCurrentBytes());
  ASSERT_EQ(4 * kChunkSize, child->getMaxBytes());

  child->free(oneChunk, kChunkSize);
  ASSERT_EQ(0, child->getCurrentBytes());
  ASSERT_EQ(4 * kChunkSize, child->getMaxBytes());
}

TEST_P(MemoryPoolTest, ReallocTestSameSize) {
  auto manager = getMemoryManager(8 * GB);
  auto root = manager->getPool();

  auto pool = root->addChild("elastic_quota");

  const int64_t kChunkSize{32L * MB};

  // Realloc the same size.

  void* oneChunk = pool->allocate(kChunkSize);
  ASSERT_EQ(kChunkSize, pool->getCurrentBytes());
  ASSERT_EQ(kChunkSize, pool->getMaxBytes());

  void* anotherChunk = pool->reallocate(oneChunk, kChunkSize, kChunkSize);
  ASSERT_EQ(kChunkSize, pool->getCurrentBytes());
  ASSERT_EQ(kChunkSize, pool->getMaxBytes());

  pool->free(anotherChunk, kChunkSize);
  ASSERT_EQ(0, pool->getCurrentBytes());
  ASSERT_EQ(kChunkSize, pool->getMaxBytes());
}

TEST_P(MemoryPoolTest, ReallocTestHigher) {
  auto manager = getMemoryManager(8 * GB);
  auto root = manager->getPool();

  auto pool = root->addChild("elastic_quota");

  const int64_t kChunkSize{32L * MB};
  // Realloc higher.
  void* oneChunk = pool->allocate(kChunkSize);
  EXPECT_EQ(kChunkSize, pool->getCurrentBytes());
  EXPECT_EQ(kChunkSize, pool->getMaxBytes());

  void* threeChunks = pool->reallocate(oneChunk, kChunkSize, 3 * kChunkSize);
  EXPECT_EQ(3 * kChunkSize, pool->getCurrentBytes());
  EXPECT_EQ(3 * kChunkSize, pool->getMaxBytes());

  pool->free(threeChunks, 3 * kChunkSize);
  EXPECT_EQ(0, pool->getCurrentBytes());
  EXPECT_EQ(3 * kChunkSize, pool->getMaxBytes());
}

TEST_P(MemoryPoolTest, ReallocTestLower) {
  auto manager = getMemoryManager(8 * GB);
  auto root = manager->getPool();
  auto pool = root->addChild("elastic_quota");

  const int64_t kChunkSize{32L * MB};
  // Realloc lower.
  void* threeChunks = pool->allocate(3 * kChunkSize);
  EXPECT_EQ(3 * kChunkSize, pool->getCurrentBytes());
  EXPECT_EQ(3 * kChunkSize, pool->getMaxBytes());

  void* oneChunk = pool->reallocate(threeChunks, 3 * kChunkSize, kChunkSize);
  EXPECT_EQ(kChunkSize, pool->getCurrentBytes());
  EXPECT_EQ(3 * kChunkSize, pool->getMaxBytes());

  pool->free(oneChunk, kChunkSize);
  EXPECT_EQ(0, pool->getCurrentBytes());
  EXPECT_EQ(3 * kChunkSize, pool->getMaxBytes());
}

TEST_P(MemoryPoolTest, allocateZeroFilled) {
  auto manager = getMemoryManager(8 * GB);
  auto root = manager->getPool();

  auto pool = root->addChild("elastic_quota");

  const std::vector<int64_t> numEntriesVector({1, 2, 10});
  const std::vector<int64_t> sizeEachVector({1, 117, 2467});
  std::vector<void*> allocationPtrs;
  std::vector<int64_t> allocationSizes;
  for (const auto& numEntries : numEntriesVector) {
    for (const auto& sizeEach : sizeEachVector) {
      SCOPED_TRACE(
          fmt::format("numEntries{}, sizeEach{}", numEntries, sizeEach));
      void* ptr = pool->allocateZeroFilled(numEntries, sizeEach);
      uint8_t* bytes = reinterpret_cast<uint8_t*>(ptr);
      for (int32_t i = 0; i < numEntries * sizeEach; ++i) {
        ASSERT_EQ(bytes[i], 0);
      }
      allocationPtrs.push_back(ptr);
      allocationSizes.push_back(numEntries * sizeEach);
    }
  }
  for (int32_t i = 0; i < allocationPtrs.size(); ++i) {
    pool->free(allocationPtrs[i], allocationSizes[i]);
  }
  ASSERT_EQ(0, pool->getCurrentBytes());
}

TEST_P(MemoryPoolTest, alignmentCheck) {
  std::vector<uint16_t> alignments = {
      0,
      MemoryAllocator::kMinAlignment,
      MemoryAllocator::kMinAlignment * 2,
      MemoryAllocator::kMaxAlignment};
  for (const auto& alignment : alignments) {
    SCOPED_TRACE(fmt::format("alignment:{}", alignment));
    IMemoryManager::Options options;
    options.capacity = 8 * GB;
    options.alignment = alignment;
    auto manager =
        getMemoryManager({.alignment = alignment, .capacity = 8 * GB});
    auto pool = manager->getPool("alignmentCheck", MemoryPool::Kind::kLeaf);
    ASSERT_EQ(
        pool->getAlignment(),
        alignment == 0 ? MemoryAllocator::kMinAlignment : alignment);
    const int32_t kTestIterations = 10;
    for (int32_t i = 0; i < 10; ++i) {
      const int64_t bytesToAlloc = 1 + folly::Random::rand32() % (1 << 20);
      void* ptr = pool->allocate(bytesToAlloc);
      if (alignment != 0) {
        ASSERT_EQ(reinterpret_cast<uint64_t>(ptr) % alignment, 0);
      }
      pool->free(ptr, bytesToAlloc);
    }
    ASSERT_EQ(0, pool->getCurrentBytes());
  }
}

TEST_P(MemoryPoolTest, MemoryCapExceptions) {
  MemoryManager manager{{.capacity = 127L * MB}};
  auto root = manager.getPool();
  root->setMemoryUsageTracker(
      MemoryUsageTracker::create(manager.getMemoryQuota()));
  auto pool = root->addChild("static_quota");

  // Capping memory manager.
  {
    ASSERT_EQ(0, pool->getCurrentBytes());
    try {
      pool->allocate(128L * MB);
    } catch (const velox::VeloxRuntimeError& ex) {
      ASSERT_EQ(error_source::kErrorSourceRuntime.c_str(), ex.errorSource());
      ASSERT_EQ(error_code::kMemCapExceeded.c_str(), ex.errorCode());
      ASSERT_TRUE(ex.isRetriable());
      ASSERT_EQ(
          "Exceeded memory cap of 127.00MB when requesting 128.00MB",
          ex.message());
    }
  }
}

TEST(MemoryPoolTest, GetAlignment) {
  {
    EXPECT_EQ(
        MemoryAllocator::kMaxAlignment,
        MemoryManager{{.capacity = 32 * MB}}.getPool()->getAlignment());
  }
  {
    MemoryManager manager{{.alignment = 64, .capacity = 32 * MB}};
    EXPECT_EQ(64, manager.getPool()->getAlignment());
  }
}

TEST_P(MemoryPoolTest, MemoryManagerGlobalCap) {
  MemoryManager manager{{.capacity = 32 * MB}};

  auto root = manager.getPool();
  auto pool = root->addChild("unbounded", MemoryPool::Kind::kAggregate);
  auto child = pool->addChild("unbounded");
  void* oneChunk = child->allocate(32L * MB);
  ASSERT_EQ(0L, root->getCurrentBytes());
  EXPECT_THROW(child->allocate(32L * MB), velox::VeloxRuntimeError);
  ASSERT_EQ(0L, root->getCurrentBytes());
  EXPECT_THROW(
      child->reallocate(oneChunk, 32L * MB, 64L * MB),
      velox::VeloxRuntimeError);
  child->free(oneChunk, 32L * MB);
}

// Tests how child updates itself and its parent's memory usage
// and what it returns for getCurrentBytes()/getMaxBytes and
// with memoryUsageTracker.
TEST_P(MemoryPoolTest, childUsageTest) {
  MemoryManager manager{{.capacity = 8 * GB}};
  auto root = manager.getPool();
  root->setMemoryUsageTracker(MemoryUsageTracker::create());

  auto pool = root->addChild("main_pool", MemoryPool::Kind::kAggregate);

  auto verifyUsage = [](std::vector<std::shared_ptr<MemoryPool>>& tree,
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
      EXPECT_GE(tracker->currentBytes(), trackerCurrentBytes[i]);
      EXPECT_GE(tracker->peakBytes(), trackerMaxBytes[i]);
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
  std::vector<std::shared_ptr<MemoryPool>> tree;
  tree.push_back(pool->addChild("p0", MemoryPool::Kind::kAggregate));

  // first level: p1, p2.
  tree.push_back(tree[0]->addChild("p1", MemoryPool::Kind::kAggregate));
  tree.push_back(tree[0]->addChild("p2", MemoryPool::Kind::kAggregate));

  // second level: p3, p4, p5, p6.
  tree.push_back(tree[1]->addChild("p3", MemoryPool::Kind::kLeaf));
  tree.push_back(tree[1]->addChild("p4", MemoryPool::Kind::kLeaf));
  tree.push_back(tree[2]->addChild("p5", MemoryPool::Kind::kLeaf));
  tree.push_back(tree[2]->addChild("p6", MemoryPool::Kind::kLeaf));

  verifyUsage(
      tree,
      {0, 0, 0, 0, 0, 0, 0},
      {0, 0, 0, 0, 0, 0, 0},
      {0, 0, 0, 0, 0, 0, 0},
      {0, 0, 0, 0, 0, 0, 0});

  void* p3Chunk0 = tree[3]->allocate(16);
  verifyUsage(
      tree,
      {0, 0, 0, 64, 0, 0, 0},
      {0, 0, 0, 64, 0, 0, 0},
      {64, 64, 0, 64, 0, 0, 0},
      {64, 64, 0, 64, 0, 0, 0});

  void* p5Chunk0 = tree[5]->allocate(64);
  verifyUsage(
      tree,
      {0, 0, 0, 64, 0, 64, 0},
      {0, 0, 0, 64, 0, 64, 0},
      {128, 64, 64, 64, 0, 64, 0},
      {128, 64, 64, 64, 0, 64, 0});

  tree[3]->free(p3Chunk0, 16);

  verifyUsage(
      tree,
      {0, 0, 0, 0, 0, 64, 0},
      {0, 0, 0, 64, 0, 64, 0},
      {64, 0, 64, 0, 0, 64, 0},
      {128, 64, 64, 64, 0, 64, 0});

  tree[5]->free(p5Chunk0, 64);
  verifyUsage(
      tree,
      {0, 0, 0, 0, 0, 0, 0},
      {0, 0, 0, 64, 0, 64, 0},
      {0, 0, 0, 0, 0, 0, 0},
      {128, 64, 64, 64, 0, 64, 0});

  std::vector<std::shared_ptr<MemoryUsageTracker>> trackers;
  for (unsigned i = 0, e = tree.size(); i != e; ++i) {
    trackers.push_back(tree[i]->getMemoryUsageTracker());
  }

  // Release all memory pool->
  tree.clear();

  std::vector<int64_t> expectedCurrentBytes({0, 0, 0, 0, 0, 0, 0});
  std::vector<int64_t> expectedMaxBytes({128, 64, 64, 64, 0, 64, 0});

  // Verify the stats still holds the correct stats.
  for (unsigned i = 0, e = trackers.size(); i != e; ++i) {
    ASSERT_GE(trackers[i]->currentBytes(), expectedCurrentBytes[i]);
    ASSERT_GE(trackers[i]->peakBytes(), expectedMaxBytes[i]);
  }
}

TEST_P(MemoryPoolTest, getPreferredSize) {
  MemoryManager manager;
  auto& pool = dynamic_cast<MemoryPoolImpl&>(manager.testingDefaultRoot());

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

TEST_P(MemoryPoolTest, getPreferredSizeOverflow) {
  MemoryManager manager;
  auto& pool = dynamic_cast<MemoryPoolImpl&>(manager.testingDefaultRoot());

  EXPECT_EQ(1ULL << 32, pool.getPreferredSize((1ULL << 32) - 1));
  EXPECT_EQ(1ULL << 63, pool.getPreferredSize((1ULL << 62) - 1 + (1ULL << 62)));
}

TEST_P(MemoryPoolTest, allocatorOverflow) {
  MemoryManager manager;
  auto& pool = dynamic_cast<MemoryPoolImpl&>(manager.testingDefaultRoot());
  StlAllocator<int64_t> alloc(pool);
  EXPECT_THROW(alloc.allocate(1ULL << 62), VeloxException);
  EXPECT_THROW(alloc.deallocate(nullptr, 1ULL << 62), VeloxException);
}

TEST_P(MemoryPoolTest, contiguousAllocate) {
  auto manager = getMemoryManager(8 * GB);
  auto pool = manager->getPool("contiguousAllocate", MemoryPool::Kind::kLeaf);
  const auto largestSizeClass =
      MemoryAllocator::getInstance()->largestSizeClass();
  struct {
    MachinePageCount numAllocPages;
    std::string debugString() const {
      return fmt::format("numAllocPages:{}", numAllocPages);
    }
  } testSettings[] = {
      {largestSizeClass},
      {largestSizeClass + 1},
      {largestSizeClass / 10},
      {1},
      {largestSizeClass * 2},
      {largestSizeClass * 3 + 1}};
  std::vector<ContiguousAllocation> allocations;
  const char c('M');
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    ContiguousAllocation allocation;
    ASSERT_TRUE(allocation.empty());
    pool->allocateContiguous(testData.numAllocPages, allocation);
    ASSERT_FALSE(allocation.empty());
    ASSERT_EQ(allocation.pool(), pool.get());
    ASSERT_EQ(allocation.numPages(), testData.numAllocPages);
    ASSERT_EQ(
        allocation.size(),
        testData.numAllocPages * AllocationTraits::kPageSize);
    for (int32_t i = 0; i < allocation.size(); ++i) {
      allocation.data()[i] = c;
    }
    allocations.push_back(std::move(allocation));
  }
  // Verify data.
  for (auto& allocation : allocations) {
    for (int32_t i = 0; i < allocation.size(); ++i) {
      ASSERT_EQ(allocation.data()[i], c);
    }
  }
  allocations.clear();

  // Random tests.
  const int32_t numIterations = 100;
  const MachinePageCount kMaxAllocationPages = 32 << 10; // Total 128MB
  int32_t numAllocatedPages = 0;
  for (int32_t i = 0; i < numIterations; ++i) {
    const MachinePageCount pagesToAllocate =
        1 + folly::Random().rand32() % kMaxAllocationPages;
    ContiguousAllocation allocation;
    if (folly::Random().oneIn(2) && !allocations.empty()) {
      const int32_t freeAllocationIdx =
          folly::Random().rand32() % allocations.size();
      allocation = std::move(allocations[freeAllocationIdx]);
      numAllocatedPages -= allocation.numPages();
      ASSERT_GE(numAllocatedPages, 0);
      allocations.erase(allocations.begin() + freeAllocationIdx);
    }
    const MachinePageCount minSizeClass = folly::Random().oneIn(4)
        ? 0
        : std::min(
              MemoryAllocator::getInstance()->largestSizeClass(),
              folly::Random().rand32() % kMaxAllocationPages);
    pool->allocateContiguous(pagesToAllocate, allocation);
    numAllocatedPages += allocation.numPages();
    for (int32_t j = 0; j < allocation.size(); ++j) {
      allocation.data()[j] = c;
    }
    allocations.push_back(std::move(allocation));
    while (numAllocatedPages > kMaxAllocationPages) {
      numAllocatedPages -= allocations.back().numPages();
      ASSERT_GE(numAllocatedPages, 0);
      allocations.pop_back();
    }
  }
  // Verify data.
  for (auto& allocation : allocations) {
    for (int32_t i = 0; i < allocation.size(); ++i) {
      ASSERT_EQ(allocation.data()[i], c);
    }
  }
}

TEST_P(MemoryPoolTest, contiguousAllocateExceedLimit) {
  const MachinePageCount kMaxNumPages = 1 << 10;
  const auto kMemoryCapBytes = kMaxNumPages * AllocationTraits::kPageSize;
  auto manager = getMemoryManager(kMemoryCapBytes);
  auto root = manager->getPool();
  auto tracker = MemoryUsageTracker::create(kMemoryCapBytes);
  root->setMemoryUsageTracker(tracker);
  auto pool = root->addChild("child");
  ContiguousAllocation allocation;
  pool->allocateContiguous(kMaxNumPages, allocation);
  ASSERT_THROW(
      pool->allocateContiguous(2 * kMaxNumPages, allocation),
      VeloxRuntimeError);
  ASSERT_TRUE(allocation.empty());
  ASSERT_THROW(
      pool->allocateContiguous(2 * kMaxNumPages, allocation),
      VeloxRuntimeError);
  ASSERT_TRUE(allocation.empty());
}

TEST_P(MemoryPoolTest, badContiguousAllocation) {
  auto manager = getMemoryManager(8 * GB);
  auto pool =
      manager->getPool("badContiguousAllocation", MemoryPool::Kind::kLeaf);
  constexpr MachinePageCount kAllocSize = 8;
  ContiguousAllocation allocation;
  ASSERT_THROW(pool->allocateContiguous(0, allocation), VeloxRuntimeError);
}

TEST_P(MemoryPoolTest, nonContiguousAllocate) {
  auto manager = getMemoryManager(8 * GB);
  auto pool =
      manager->getPool("nonContiguousAllocate", MemoryPool::Kind::kLeaf);
  const auto& sizeClasses = MemoryAllocator::getInstance()->sizeClasses();
  for (const auto& sizeClass : sizeClasses) {
    SCOPED_TRACE(fmt::format("sizeClass:{}", sizeClass));
    struct {
      MachinePageCount numAllocPages;
      MachinePageCount minSizeClass;
      std::string debugString() const {
        return fmt::format(
            "numAllocPages:{}, minSizeClass:{}", numAllocPages, minSizeClass);
      }
    } testSettings[] = {
        {sizeClass, 0},
        {sizeClass, 1},
        {sizeClass, 10},
        {sizeClass, sizeClass},
        {sizeClass, sizeClass + 1},
        {sizeClass, sizeClass * 2},
        {sizeClass + 10, 0},
        {sizeClass + 10, 1},
        {sizeClass + 10, 10},
        {sizeClass + 10, sizeClass},
        {sizeClass + 10, sizeClass + 1},
        {sizeClass + 10, sizeClass * 2},
        {sizeClass * 2, 0},
        {sizeClass * 2, 1},
        {sizeClass * 2, 10},
        {sizeClass * 2, sizeClass},
        {sizeClass * 2, sizeClass + 1},
        {sizeClass * 2, sizeClass + 10},
        {sizeClass * 2, sizeClass * 2},
        {sizeClass * 2, sizeClass * 2 + 1},
        {sizeClass * 2, sizeClass * 2 * 2}};
    std::vector<Allocation> allocations;
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(testData.debugString());
      Allocation allocation;
      ASSERT_TRUE(allocation.empty());
      pool->allocateNonContiguous(
          testData.numAllocPages,
          allocation,
          std::min(
              testData.minSizeClass,
              MemoryAllocator::getInstance()->largestSizeClass()));
      ASSERT_FALSE(allocation.empty());
      ASSERT_EQ(allocation.pool(), pool.get());
      ASSERT_GT(allocation.numRuns(), 0);
      ASSERT_GE(allocation.numPages(), testData.numAllocPages);
    }
  }

  // Random tests.
  const int32_t numIterations = 100;
  const MachinePageCount kMaxAllocationPages = 32 << 10; // Total 128MB
  int32_t numAllocatedPages = 0;
  std::vector<Allocation> allocations;
  for (int32_t i = 0; i < numIterations; ++i) {
    const MachinePageCount pagesToAllocate =
        1 + folly::Random().rand32() % kMaxAllocationPages;
    Allocation allocation;
    if (folly::Random().oneIn(2) && !allocations.empty()) {
      const int32_t freeAllocationIdx =
          folly::Random().rand32() % allocations.size();
      allocation = std::move(allocations[freeAllocationIdx]);
      numAllocatedPages -= allocation.numPages();
      ASSERT_GE(numAllocatedPages, 0);
      allocations.erase(allocations.begin() + freeAllocationIdx);
    }
    const MachinePageCount minSizeClass = folly::Random().oneIn(4)
        ? 0
        : std::min(
              MemoryAllocator::getInstance()->largestSizeClass(),
              folly::Random().rand32() % kMaxAllocationPages);
    pool->allocateNonContiguous(pagesToAllocate, allocation, minSizeClass);
    numAllocatedPages += allocation.numPages();
    allocations.push_back(std::move(allocation));
    while (numAllocatedPages > kMaxAllocationPages) {
      numAllocatedPages -= allocations.back().numPages();
      ASSERT_GE(numAllocatedPages, 0);
      allocations.pop_back();
    }
  }
}

TEST_P(MemoryPoolTest, nonContiguousAllocateWithOldAllocation) {
  auto manager = getMemoryManager(8 * GB);
  auto root = manager->getPool();
  root->setMemoryUsageTracker(MemoryUsageTracker::create());
  auto pool = root->addChild(
      "nonContiguousAllocateWithOldAllocation", MemoryPool::Kind::kLeaf);
  struct {
    MachinePageCount numOldPages;
    MachinePageCount numNewPages;
    std::string debugString() const {
      return fmt::format(
          "numOldPages:{}, numNewPages:{}", numOldPages, numNewPages);
    }
  } testSettings[] = {
      {0, 100},
      {0, Allocation::PageRun::kMaxPagesInRun / 2},
      {0, Allocation::PageRun::kMaxPagesInRun},
      {100, 100},
      {Allocation::PageRun::kMaxPagesInRun / 2,
       Allocation::PageRun::kMaxPagesInRun / 2},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun},
      {200, 100},
      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
       Allocation::PageRun::kMaxPagesInRun / 2},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun - 1},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun / 2}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    Allocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateNonContiguous(testData.numOldPages, allocation);
    }
    ASSERT_GE(allocation.numPages(), testData.numOldPages);
    pool->allocateNonContiguous(testData.numNewPages, allocation);
    ASSERT_GE(allocation.numPages(), testData.numNewPages);
  }
}

TEST_P(MemoryPoolTest, persistentNonContiguousAllocateFailure) {
  struct {
    MachinePageCount numOldPages;
    MachinePageCount numNewPages;
    MemoryAllocator::InjectedFailure injectedFailure;
    std::string debugString() const {
      return fmt::format(
          "numOldPages:{}, numNewPages:{}, injectedFailure:{}",
          numOldPages,
          numNewPages,
          injectedFailure);
    }
  } testSettings[] = {// Cap failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kCap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {100, 100, MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {200, 100, MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      // Allocate failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kAllocate},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {100, 100, MemoryAllocator::InjectedFailure::kAllocate},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      {200, 100, MemoryAllocator::InjectedFailure::kAllocate},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      // Madvise failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {200, 100, MemoryAllocator::InjectedFailure::kMadvise}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(fmt::format(
        "{}, useMmap:{}, useCache:{}",
        testData.debugString(),
        useMmap_,
        useCache_));
    if ((testData.injectedFailure !=
         MemoryAllocator::InjectedFailure::kAllocate) &&
        !useMmap_) {
      // Non-Allocate failure injection only applies for MmapAllocator.
      continue;
    }
    reset();
    auto manager = getMemoryManager(8 * GB);
    auto root = manager->getPool();
    root->setMemoryUsageTracker(MemoryUsageTracker::create());
    auto pool = root->addChild(
        "persistentNonContiguousAllocateFailure", MemoryPool::Kind::kLeaf);

    Allocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateNonContiguous(testData.numOldPages, allocation);
    }
    ASSERT_GE(allocation.numPages(), testData.numOldPages);
    allocator_->testingSetFailureInjection(testData.injectedFailure, true);
    ASSERT_THROW(
        pool->allocateNonContiguous(testData.numNewPages, allocation),
        VeloxRuntimeError);
    allocator_->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, transientNonContiguousAllocateFailure) {
  struct {
    MachinePageCount numOldPages;
    MachinePageCount numNewPages;
    MemoryAllocator::InjectedFailure injectedFailure;
    std::string debugString() const {
      return fmt::format(
          "numOldPages:{}, numNewPages:{}, injectedFailure:{}",
          numOldPages,
          numNewPages,
          injectedFailure);
    }
  } testSettings[] = {// Cap failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kCap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {100, 100, MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {200, 100, MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      // Allocate failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kAllocate},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {100, 100, MemoryAllocator::InjectedFailure::kAllocate},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      {200, 100, MemoryAllocator::InjectedFailure::kAllocate},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kAllocate},
                      // Madvise failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {200, 100, MemoryAllocator::InjectedFailure::kMadvise}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(fmt::format(
        "{}, useMmap:{}, useCache:{}",
        testData.debugString(),
        useMmap_,
        useCache_));
    if ((testData.injectedFailure !=
         MemoryAllocator::InjectedFailure::kAllocate) &&
        !useMmap_) {
      // Non-Allocate failure injection only applies for MmapAllocator.
      continue;
    }
    reset();
    auto manager = getMemoryManager(8 * GB);
    auto root = manager->getPool();
    root->setMemoryUsageTracker(MemoryUsageTracker::create());
    auto pool = root->addChild(
        "transientNonContiguousAllocateFailure", MemoryPool::Kind::kLeaf);
    Allocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateNonContiguous(testData.numOldPages, allocation);
    }
    ASSERT_GE(allocation.numPages(), testData.numOldPages);
    allocator_->testingSetFailureInjection(testData.injectedFailure);
    if (useCache_) {
      pool->allocateNonContiguous(testData.numNewPages, allocation);
      ASSERT_EQ(allocation.numPages(), testData.numNewPages);
    } else {
      ASSERT_THROW(
          pool->allocateNonContiguous(testData.numNewPages, allocation),
          VeloxRuntimeError);
    }
    allocator_->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, contiguousAllocateWithOldAllocation) {
  auto manager = getMemoryManager(8 * GB);
  auto root = manager->getPool();
  root->setMemoryUsageTracker(MemoryUsageTracker::create());
  auto pool = root->addChild(
      "contiguousAllocateWithOldAllocation", MemoryPool::Kind::kLeaf);
  struct {
    MachinePageCount numOldPages;
    MachinePageCount numNewPages;
    std::string debugString() const {
      return fmt::format(
          "numOldPages:{}, numNewPages:{}", numOldPages, numNewPages);
    }
  } testSettings[] = {
      {0, 100},
      {0, Allocation::PageRun::kMaxPagesInRun / 2},
      {0, Allocation::PageRun::kMaxPagesInRun},
      {100, 100},
      {Allocation::PageRun::kMaxPagesInRun / 2,
       Allocation::PageRun::kMaxPagesInRun / 2},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun * 2},
      {200, 100},
      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
       Allocation::PageRun::kMaxPagesInRun / 2},
      {Allocation::PageRun::kMaxPagesInRun,
       Allocation::PageRun::kMaxPagesInRun - 1},
      {Allocation::PageRun::kMaxPagesInRun * 2,
       Allocation::PageRun::kMaxPagesInRun}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    ContiguousAllocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateContiguous(testData.numOldPages, allocation);
    }
    ASSERT_EQ(allocation.numPages(), testData.numOldPages);
    pool->allocateContiguous(testData.numNewPages, allocation);
    ASSERT_EQ(allocation.numPages(), testData.numNewPages);
  }
}

TEST_P(MemoryPoolTest, persistentContiguousAllocateFailure) {
  struct {
    MachinePageCount numOldPages;
    MachinePageCount numNewPages;
    MemoryAllocator::InjectedFailure injectedFailure;
    std::string debugString() const {
      return fmt::format(
          "numOldPages:{}, numNewPages:{}, injectedFailure:{}",
          numOldPages,
          numNewPages,
          injectedFailure);
    }
  } testSettings[] = {// Cap failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kCap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {100, 100, MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {200, 100, MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      // Mmap failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kMmap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {100, 100, MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {200, 100, MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      // Madvise failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {100, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {200, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    if (!useMmap_) {
      // No failure injections supported for contiguous allocation of
      // MallocAllocator.
      continue;
    }
    auto manager = getMemoryManager(8 * GB);
    auto root = manager->getPool();
    root->setMemoryUsageTracker(MemoryUsageTracker::create());
    auto pool = root->addChild(
        "persistentContiguousAllocateFailure", MemoryPool::Kind::kLeaf);

    ContiguousAllocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateContiguous(testData.numOldPages, allocation);
    }
    allocator_->testingSetFailureInjection(testData.injectedFailure, true);
    ASSERT_EQ(allocation.numPages(), testData.numOldPages);
    if ((testData.numOldPages >= testData.numNewPages) &&
        testData.injectedFailure != MemoryAllocator::InjectedFailure::kMmap) {
      pool->allocateContiguous(testData.numNewPages, allocation);
      ASSERT_EQ(allocation.numPages(), testData.numNewPages);
    } else {
      ASSERT_THROW(
          pool->allocateContiguous(testData.numNewPages, allocation),
          VeloxRuntimeError);
    }
    allocator_->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, transientContiguousAllocateFailure) {
  struct {
    MachinePageCount numOldPages;
    MachinePageCount numNewPages;
    MemoryAllocator::InjectedFailure injectedFailure;
    std::string debugString() const {
      return fmt::format(
          "numOldPages:{}, numNewPages:{}, injectedFailure:{}",
          numOldPages,
          numNewPages,
          injectedFailure);
    }
  } testSettings[] = {// Cap failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kCap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {100, 100, MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {200, 100, MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kCap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kCap},
                      // Mmap failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kMmap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kCap},
                      {100, 100, MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {200, 100, MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kMmap},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMmap},
                      // Madvise failure injection.
                      {0, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {0,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {100, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun / 2,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {200, 100, MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun / 2 + 100,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun - 1,
                       MemoryAllocator::InjectedFailure::kMadvise},
                      {Allocation::PageRun::kMaxPagesInRun,
                       Allocation::PageRun::kMaxPagesInRun / 2,
                       MemoryAllocator::InjectedFailure::kMadvise}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(fmt::format(
        "{}, useCache:{} , useMmap:{}",
        testData.debugString(),
        useCache_,
        useMmap_));
    if (!useMmap_) {
      // No failure injections supported for contiguous allocation of
      // MallocAllocator.
      continue;
    }
    auto manager = getMemoryManager(8 * GB);
    auto root = manager->getPool();
    root->setMemoryUsageTracker(MemoryUsageTracker::create());
    auto pool = root->addChild(
        "transientContiguousAllocateFailure", MemoryPool::Kind::kLeaf);
    ContiguousAllocation allocation;
    if (testData.numOldPages > 0) {
      pool->allocateContiguous(testData.numOldPages, allocation);
    }
    allocator_->testingSetFailureInjection(testData.injectedFailure);
    ASSERT_EQ(allocation.numPages(), testData.numOldPages);
    // NOTE: AsyncDataCache will retry on the transient memory allocation
    // failures from the underlying allocator.
    if (useCache_ ||
        ((testData.numOldPages >= testData.numNewPages) &&
         testData.injectedFailure != MemoryAllocator::InjectedFailure::kMmap)) {
      pool->allocateContiguous(testData.numNewPages, allocation);
      ASSERT_EQ(allocation.numPages(), testData.numNewPages);
    } else {
      ASSERT_THROW(
          pool->allocateContiguous(testData.numNewPages, allocation),
          VeloxRuntimeError);
    }
    allocator_->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, badNonContiguousAllocation) {
  auto manager = getMemoryManager(8 * GB);
  auto pool =
      manager->getPool("badNonContiguousAllocation", MemoryPool::Kind::kLeaf);
  Allocation allocation;
  // Bad zero page allocation size.
  ASSERT_THROW(pool->allocateNonContiguous(0, allocation), VeloxRuntimeError);

  // Set the num of pages to allocate exceeds one PageRun limit.
  constexpr MachinePageCount kNumPages =
      Allocation::PageRun::kMaxPagesInRun + 1;
  ASSERT_THROW(
      pool->allocateNonContiguous(kNumPages, allocation), VeloxRuntimeError);
  pool->allocateNonContiguous(kNumPages - 1, allocation);
  ASSERT_GE(allocation.numPages(), kNumPages - 1);
  pool->freeNonContiguous(allocation);
}

TEST_P(MemoryPoolTest, nonContiguousAllocateExceedLimit) {
  const MachinePageCount kMaxNumPages = 1 << 10;
  const auto kMemoryCapBytes = kMaxNumPages * AllocationTraits::kPageSize;
  auto manager = getMemoryManager(kMemoryCapBytes);
  auto root = manager->getPool();
  root->setMemoryUsageTracker(MemoryUsageTracker::create(kMemoryCapBytes));
  auto pool = root->addChild(
      "nonContiguousAllocateExceedLimit", MemoryPool::Kind::kLeaf);

  Allocation allocation;
  pool->allocateNonContiguous(kMaxNumPages, allocation);
  ASSERT_THROW(
      pool->allocateNonContiguous(2 * kMaxNumPages, allocation),
      VeloxRuntimeError);
  ASSERT_TRUE(allocation.empty());
  ASSERT_THROW(
      pool->allocateNonContiguous(2 * kMaxNumPages, allocation),
      VeloxRuntimeError);
  ASSERT_TRUE(allocation.empty());
}

TEST_P(MemoryPoolTest, nonContiguousAllocateError) {
  auto manager = getMemoryManager(8 * GB);
  auto pool =
      manager->getPool("nonContiguousAllocateError", MemoryPool::Kind::kLeaf);
  allocator_->testingSetFailureInjection(
      MemoryAllocator::InjectedFailure::kAllocate, true);
  constexpr MachinePageCount kAllocSize = 8;
  std::unique_ptr<Allocation> allocation(new Allocation());
  ASSERT_THROW(
      pool->allocateNonContiguous(kAllocSize, *allocation), VeloxRuntimeError);
  ASSERT_TRUE(allocation->empty());
  allocator_->testingClearFailureInjection();
  pool->allocateNonContiguous(kAllocSize, *allocation);
  pool->freeNonContiguous(*allocation);
  ASSERT_TRUE(allocation->empty());
}

TEST_P(MemoryPoolTest, mmapAllocatorCapAllocationError) {
  if (!useMmap_) {
    return;
  }
  struct {
    int64_t allocateBytes;
    bool expectedFailure;
    bool persistentErrorInjection;
    std::string debugString() const {
      return fmt::format(
          "allocateBytes {}, expectFailure {}, persistentErrorInjection {}",
          allocateBytes,
          expectedFailure,
          persistentErrorInjection);
    }
  } testSettings[] = {// NOTE: the failure injection only applies for
                      // allocations that are not delegated to malloc.
                      {MemoryAllocator::kMaxMallocBytes - 1, false, false},
                      {MemoryAllocator::kMaxMallocBytes, false, false},
                      {MemoryAllocator::kMaxMallocBytes + 1, true, false},
                      {MemoryAllocator::kMaxMallocBytes - 1, false, true},
                      {MemoryAllocator::kMaxMallocBytes, false, true},
                      {MemoryAllocator::kMaxMallocBytes + 1, true, true}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto manager = getMemoryManager(8 * GB);
    auto root = manager->getPool();
    root->setMemoryUsageTracker(MemoryUsageTracker::create());
    auto pool = root->addChild(
        "mmapAllocatorCapAllocationError", MemoryPool::Kind::kLeaf);

    allocator_->testingSetFailureInjection(
        MemoryAllocator::InjectedFailure::kCap,
        testData.persistentErrorInjection);
    // Async data cache will retry transient memory allocation failure.
    if (!testData.expectedFailure ||
        (useCache_ && !testData.persistentErrorInjection)) {
      void* buffer = pool->allocate(testData.allocateBytes);
      pool->free(buffer, testData.allocateBytes);
    } else {
      ASSERT_THROW(pool->allocate(testData.allocateBytes), VeloxRuntimeError);
    }
    allocator_->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, mmapAllocatorCapAllocationZeroFilledError) {
  if (!useMmap_) {
    return;
  }

  struct {
    int64_t numEntries;
    int64_t sizeEach;
    bool expectedFailure;
    bool persistentErrorInjection;
    std::string debugString() const {
      return fmt::format(
          "numEntries {}, sizeEach {}, expectFailure {}, persistentErrorInjection {}",
          numEntries,
          sizeEach,
          expectedFailure,
          persistentErrorInjection);
    }
  } testSettings[] = {// NOTE: the failure injection only applies for
                      // allocations that are not delegated to malloc.
                      {MemoryAllocator::kMaxMallocBytes - 1, 1, false, false},
                      {MemoryAllocator::kMaxMallocBytes, 1, false, false},
                      {MemoryAllocator::kMaxMallocBytes + 1, 1, true, false},
                      {MemoryAllocator::kMaxMallocBytes - 1, 1, false, true},
                      {MemoryAllocator::kMaxMallocBytes, 1, false, true},
                      {MemoryAllocator::kMaxMallocBytes + 1, 1, true, true}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto manager = getMemoryManager(8 * GB);
    auto root = manager->getPool();
    root->setMemoryUsageTracker(MemoryUsageTracker::create());
    auto pool = root->addChild(
        "mmapAllocatorCapAllocationZeroFilledError", MemoryPool::Kind::kLeaf);

    allocator_->testingSetFailureInjection(
        MemoryAllocator::InjectedFailure::kCap,
        testData.persistentErrorInjection);
    // Async data cache will retry transient memory allocation failure.
    if (!testData.expectedFailure ||
        (useCache_ && !testData.persistentErrorInjection)) {
      void* buffer =
          pool->allocateZeroFilled(testData.numEntries, testData.sizeEach);
      pool->free(buffer, testData.numEntries * testData.sizeEach);
    } else {
      ASSERT_THROW(
          pool->allocateZeroFilled(testData.numEntries, testData.sizeEach),
          VeloxRuntimeError);
    }
    allocator_->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, mmapAllocatorCapReallocateError) {
  if (!useMmap_) {
    return;
  }
  struct {
    int64_t allocateBytes;
    bool expectedFailure;
    bool persistentErrorInjection;
    std::string debugString() const {
      return fmt::format(
          "allocateBytes {}, expectFailure {}, persistentErrorInjection {}",
          allocateBytes,
          expectedFailure,
          persistentErrorInjection);
    }
  } testSettings[] = {// NOTE: the failure injection only applies for
                      // allocations that are not delegated to malloc.
                      {MemoryAllocator::kMaxMallocBytes - 1, false, false},
                      {MemoryAllocator::kMaxMallocBytes, false, false},
                      {MemoryAllocator::kMaxMallocBytes + 1, true, false},
                      {MemoryAllocator::kMaxMallocBytes - 1, false, true},
                      {MemoryAllocator::kMaxMallocBytes, false, true},
                      {MemoryAllocator::kMaxMallocBytes + 1, true, true}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto manager = getMemoryManager(8 * GB);
    auto root = manager->getPool();
    root->setMemoryUsageTracker(MemoryUsageTracker::create());
    auto pool = root->addChild(
        "mmapAllocatorCapReallocateError", MemoryPool::Kind::kLeaf);

    allocator_->testingSetFailureInjection(
        MemoryAllocator::InjectedFailure::kCap,
        testData.persistentErrorInjection);
    // Async data cache will retry transient memory allocation failure.
    if (!testData.expectedFailure ||
        (useCache_ && !testData.persistentErrorInjection)) {
      void* buffer = pool->reallocate(nullptr, 0, testData.allocateBytes);
      pool->free(buffer, testData.allocateBytes);
    } else {
      ASSERT_THROW(
          pool->reallocate(nullptr, 0, testData.allocateBytes),
          VeloxRuntimeError);
    }
    allocator_->testingClearFailureInjection();
  }
}

TEST_P(MemoryPoolTest, validCheck) {
  auto manager = getMemoryManager(8 * GB);
  auto root = manager->getPool();
  ASSERT_ANY_THROW(root->allocate(100));
  ASSERT_ANY_THROW(root->reallocate(static_cast<void*>(this), 100, 300));
  ASSERT_ANY_THROW(root->allocateZeroFilled(100, 100));
  ASSERT_ANY_THROW(root->free(static_cast<void*>(this), 100));
  {
    Allocation out;
    ASSERT_ANY_THROW(root->allocateNonContiguous(100, out));
    ASSERT_ANY_THROW(root->freeNonContiguous(out));
  }
  {
    ContiguousAllocation out;
    ASSERT_ANY_THROW(root->allocateContiguous(100, out));
    ASSERT_ANY_THROW(root->freeContiguous(out));
  }
  auto child = root->addChild("validCheck");
  ASSERT_ANY_THROW(child->addChild("validCheck", MemoryPool::Kind::kLeaf));
  ASSERT_ANY_THROW(child->addChild("validCheck", MemoryPool::Kind::kAggregate));
}

// Class used to test operations on MemoryPool.
class MemoryPoolTester {
 public:
  MemoryPoolTester(int32_t id, int64_t maxMemory, memory::MemoryPool& pool)
      : id_(id), maxMemory_(maxMemory), pool_(pool) {}

  ~MemoryPoolTester() {
    for (auto& allocation : contiguousAllocations_) {
      pool_.freeContiguous(allocation);
    }
    for (auto& allocation : nonContiguiusAllocations_) {
      pool_.freeNonContiguous(allocation);
    }
    for (auto& bufferEntry : allocBuffers_) {
      pool_.free(bufferEntry.first, bufferEntry.second);
    }
    if (reservedBytes_ != 0) {
      pool_.getMemoryUsageTracker()->release();
    }
  }

  void run() {
    const int32_t op = folly::Random().rand32() % 4;
    switch (op) {
      case 0: {
        // Allocate buffers.
        if ((folly::Random().rand32() % 2) && !allocBuffers_.empty()) {
          pool_.free(allocBuffers_.back().first, allocBuffers_.back().second);
          allocBuffers_.pop_back();
        } else {
          const int64_t allocateBytes =
              folly::Random().rand32() % (maxMemory_ / 64);
          try {
            void* buffer = pool_.allocate(allocateBytes);
            VELOX_CHECK_NOT_NULL(buffer);
            allocBuffers_.push_back({buffer, allocateBytes});
          } catch (VeloxException& e) {
            // Ignore memory limit exception.
            ASSERT_TRUE(e.message().find("Negative") == std::string::npos);
            return;
          }
        }
        break;
      }
      case 2: {
        // Contiguous allocations.
        if ((folly::Random().rand32() % 2) && !contiguousAllocations_.empty()) {
          pool_.freeContiguous(contiguousAllocations_.back());
          contiguousAllocations_.pop_back();
        } else {
          int64_t allocatePages = std::max<int64_t>(
              1, folly::Random().rand32() % (maxMemory_ / 32) / 4096);
          ContiguousAllocation allocation;
          if (folly::Random().oneIn(2) && !contiguousAllocations_.empty()) {
            allocation = std::move(contiguousAllocations_.back());
            contiguousAllocations_.pop_back();
            if (folly::Random().oneIn(2)) {
              allocatePages = std::max<int64_t>(1, allocation.numPages() - 4);
            }
          }
          try {
            pool_.allocateContiguous(allocatePages, allocation);
            contiguousAllocations_.push_back(std::move(allocation));
          } catch (VeloxException& e) {
            // Ignore memory limit exception.
            ASSERT_TRUE(e.message().find("Negative") == std::string::npos);
            return;
          }
        }
        break;
      }
      case 1: {
        // Non-contiguous allocations.
        if ((folly::Random().rand32() % 2) &&
            !nonContiguiusAllocations_.empty()) {
          pool_.freeNonContiguous(nonContiguiusAllocations_.back());
          nonContiguiusAllocations_.pop_back();
        } else {
          const int64_t allocatePages = std::max<int64_t>(
              1, folly::Random().rand32() % (maxMemory_ / 32) / 4096);
          Allocation allocation;
          try {
            pool_.allocateNonContiguous(allocatePages, allocation);
            nonContiguiusAllocations_.push_back(std::move(allocation));
          } catch (VeloxException& e) {
            // Ignore memory limit exception.
            ASSERT_TRUE(e.message().find("Negative") == std::string::npos);
            return;
          }
        }
        break;
      }
      case 3: {
        // maybe reserve.
        if (reservedBytes_ == 0) {
          const uint64_t reservedBytes =
              folly::Random().rand32() % (maxMemory_ / 32);
          if (pool_.getMemoryUsageTracker()->maybeReserve(reservedBytes)) {
            reservedBytes_ = reservedBytes;
          }
        } else {
          pool_.getMemoryUsageTracker()->release();
          reservedBytes_ = 0;
        }
        break;
      }
    }
  }

 private:
  const int32_t id_;
  const int64_t maxMemory_;
  memory::MemoryPool& pool_;
  uint64_t reservedBytes_{0};
  std::vector<ContiguousAllocation> contiguousAllocations_;
  std::vector<Allocation> nonContiguiusAllocations_;
  std::vector<std::pair<void*, uint64_t>> allocBuffers_;
};

TEST_P(MemoryPoolTest, concurrentUpdateToDifferentPools) {
  constexpr int64_t kMaxMemory = 10 * GB;
  MemoryManager manager{{.capacity = kMaxMemory}};
  auto root = manager.getPool("concurrentUpdateToDifferentPools");
  root->setMemoryUsageTracker(memory::MemoryUsageTracker::create(kMaxMemory));
  const int32_t kNumThreads = 5;
  // Create one memory tracker per each thread.
  std::vector<std::shared_ptr<MemoryPool>> childPools;
  for (int32_t i = 0; i < kNumThreads; ++i) {
    childPools.push_back(root->addChild(fmt::format("{}", i)));
  }

  folly::Random::DefaultGenerator rng;
  rng.seed(1234);

  const int32_t kNumOpsPerThread = 1'000;
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      MemoryPoolTester tester(i, kMaxMemory, *childPools[i]);
      for (int32_t iter = 0; iter < kNumOpsPerThread; ++iter) {
        tester.run();
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }
}

TEST_P(MemoryPoolTest, concurrentUpdatesToTheSamePool) {
  constexpr int64_t kMaxMemory = 8 * GB;
  MemoryManager manager{{.capacity = kMaxMemory}};
  auto root = manager.getPool();
  root->setMemoryUsageTracker(memory::MemoryUsageTracker::create(kMaxMemory));

  const int32_t kNumThreads = 5;
  const int32_t kNumChildPools = 2;
  std::vector<std::shared_ptr<MemoryPool>> childPools;
  for (int32_t i = 0; i < kNumChildPools; ++i) {
    childPools.push_back(root->addChild(fmt::format("{}", i)));
  }

  folly::Random::DefaultGenerator rng;
  rng.seed(1234);

  const int32_t kNumOpsPerThread = 1'000;
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      MemoryPoolTester tester(i, kMaxMemory, *childPools[i % kNumChildPools]);
      for (int32_t iter = 0; iter < kNumOpsPerThread; ++iter) {
        tester.run();
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }
}

TEST_P(MemoryPoolTest, concurrentPoolStructureAccess) {
  folly::Random::DefaultGenerator rng;
  rng.seed(1234);
  constexpr int64_t kMaxMemory = 8 * GB;
  MemoryManager manager{{.capacity = kMaxMemory}};
  auto root = manager.getPool();
  std::atomic<int64_t> poolId{0};
  std::mutex lock;
  std::vector<std::shared_ptr<MemoryPool>> pools;
  const int32_t kNumThreads = 20;
  const int32_t kNumOpsPerThread = 1'000;
  const std::string kPoolNamePrefix("concurrent");

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&]() {
      for (int32_t op = 0; op < kNumOpsPerThread; ++op) {
        std::shared_ptr<MemoryPool> pool;
        {
          std::lock_guard<std::mutex> l(lock);
          if (pools.empty() || folly::Random().oneIn(5)) {
            auto pool =
                root->addChild(fmt::format("{}{}", kPoolNamePrefix, poolId++));
            pools.push_back(pool);
            continue;
          }
          const auto idx = folly::Random().rand32() % pools.size();
          if (folly::Random().oneIn(3)) {
            pools.erase(pools.begin() + idx);
            continue;
          }
          pool = pools[idx];
        }
        VELOX_CHECK_NOT_NULL(pool);

        if (pool->kind() == MemoryPool::Kind::kAggregate &&
            !folly::Random().oneIn(3)) {
          auto childPool = pool->addChild(
              fmt::format("{}{}", kPoolNamePrefix, poolId++),
              folly::Random().oneIn(4) ? MemoryPool::Kind::kLeaf
                                       : MemoryPool::Kind::kAggregate);
          std::lock_guard<std::mutex> l(lock);
          pools.push_back(std::move(childPool));
          continue;
        }

        pool->visitChildren([&](MemoryPool* pool) {
          VELOX_CHECK_EQ(
              pool->name().compare(0, kPoolNamePrefix.size(), kPoolNamePrefix),
              0);
        });
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }
  pools.clear();
  ASSERT_EQ(root->getChildCount(), 0);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    MemoryPoolTestSuite,
    MemoryPoolTest,
    testing::ValuesIn(MemoryPoolTest::getTestParams()));

} // namespace memory
} // namespace velox
} // namespace facebook
